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
  description = "Kubernetes version for EKS (use 1.31+ for standard support pricing)"
  type        = string
  default     = "1.34"
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

variable "goqueue_image_repository" {
  description = "GoQueue Docker image repository"
  type        = string
  default     = "ghcr.io/abd-ulbasit/goqueue"
}

variable "goqueue_image_tag" {
  description = "GoQueue Docker image tag (latest includes cluster support)"
  type        = string
  default     = "latest"
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
    Project   = "goqueue"
    ManagedBy = "terraform"
  }
}

# =============================================================================
# SECURITY CONFIGURATION (M21)
# =============================================================================
#
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ SECURITY FEATURES                                                           │
# │                                                                             │
# │ 1. TLS: Encrypt all HTTP/gRPC traffic (self-signed for dev)                │
# │ 2. mTLS: Encrypt inter-node cluster communication                           │
# │ 3. API Key Auth: Require authentication for API access                      │
# │ 4. RBAC: Role-based access control for topics/groups                        │
# └─────────────────────────────────────────────────────────────────────────────┘
#
variable "security_enabled" {
  description = "Enable security features (TLS, Auth, RBAC)"
  type        = bool
  default     = false
}

variable "security_tls_self_signed" {
  description = "Use self-signed TLS certificates (dev only)"
  type        = bool
  default     = true
}

variable "security_root_api_key" {
  description = "Root API key for admin access (generate with: openssl rand -hex 32)"
  type        = string
  default     = ""
  sensitive   = true
}

# =============================================================================
# M23: BACKUP & RESTORE CONFIGURATION
# =============================================================================
#
# KUBERNETES-FIRST BACKUP STRATEGY:
#   1. VolumeSnapshot (CSI) - Fast point-in-time snapshots of EBS volumes
#   2. Metadata Backup (CronJob) - Topics, offsets, schemas exported to S3
#
# COST CONSIDERATION:
#   - EBS Snapshots: ~$0.05/GB-month (incremental)
#   - S3 storage: ~$0.023/GB-month (Standard)
#   - Backup job CPU: Minimal (runs for seconds)
#
# =============================================================================

variable "backup_enabled" {
  description = "Enable backup CronJob and VolumeSnapshot configuration"
  type        = bool
  default     = false
}

variable "backup_schedule" {
  description = "Cron schedule for metadata backups (default: 2 AM daily)"
  type        = string
  default     = "0 2 * * *"
}

variable "backup_volume_snapshot_enabled" {
  description = "Enable VolumeSnapshot-based backups (requires CSI driver)"
  type        = bool
  default     = false
}

variable "backup_volume_snapshot_schedule" {
  description = "Cron schedule for VolumeSnapshots (default: 3 AM daily)"
  type        = string
  default     = "0 3 * * *"
}

variable "backup_retention_days" {
  description = "Number of days to retain backups"
  type        = number
  default     = 7
}

variable "backup_s3_bucket" {
  description = "S3 bucket for backup storage (leave empty to disable S3 upload)"
  type        = string
  default     = ""
}

variable "backup_s3_prefix" {
  description = "S3 key prefix for backups"
  type        = string
  default     = "goqueue/backups"
}

# ═══════════════════════════════════════════════════════════════════════════════
# TRACING VARIABLES (M25)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Controls distributed tracing via OpenTelemetry (OTLP).
# GoQueue's built-in tracer exports spans to an OTLP-compatible backend
# (Grafana Tempo, Jaeger, etc.) for end-to-end request visibility.
#
# TYPICAL SETUP:
#   1. Deploy Grafana Tempo alongside kube-prometheus-stack
#   2. Set tracing_enabled = true
#   3. Set tracing_otlp_endpoint to Tempo's OTLP gRPC endpoint
#   4. Sampling rate: 1.0 for dev, 0.1-0.01 for production
# ═══════════════════════════════════════════════════════════════════════════════

variable "tracing_enabled" {
  description = "Enable distributed tracing (OTLP export)"
  type        = bool
  default     = false
}

variable "tracing_otlp_endpoint" {
  description = "OTLP gRPC endpoint for trace export (e.g., tempo.monitoring:4317)"
  type        = string
  default     = ""
}

variable "tracing_sampling_rate" {
  description = "Trace sampling rate (1.0 = all traces, 0.1 = 10% of traces)"
  type        = number
  default     = 1.0
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

  enable_nat_gateway   = true
  single_nat_gateway   = var.environment != "prod" # Cost optimization for non-prod
  enable_dns_hostnames = true
  enable_dns_support   = true

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ SUBNET TAGS FOR EKS                                                     │
  # │                                                                         │
  # │ These tags tell the AWS Load Balancer Controller where to place ALBs:   │
  # │   - kubernetes.io/role/elb: Public subnets for internet-facing LBs      │
  # │   - kubernetes.io/role/internal-elb: Private subnets for internal LBs   │
  # └─────────────────────────────────────────────────────────────────────────┘
  public_subnet_tags = {
    "kubernetes.io/role/elb"                    = 1
    "kubernetes.io/cluster/${var.cluster_name}" = "owned"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"           = 1
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

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ CLUSTER ACCESS CONFIGURATION                                            │
  # │                                                                         │
  # │ With EKS 1.28+, AWS uses the EKS Access Entry API by default.           │
  # │ This replaces the old aws-auth ConfigMap approach.                      │
  # │                                                                         │
  # │ enable_cluster_creator_admin_permissions = true:                        │
  # │   Automatically grants the IAM user/role that creates the cluster       │
  # │   full admin access. Without this, even the cluster creator can't       │
  # │   access Kubernetes APIs!                                               │
  # │                                                                         │
  # │ COMPARISON:                                                             │
  # │   - Old way (pre-1.28): aws-auth ConfigMap in kube-system               │
  # │   - New way (1.28+): EKS Access Entry API (more secure, auditable)      │
  # └─────────────────────────────────────────────────────────────────────────┘
  enable_cluster_creator_admin_permissions = true

  # Cluster endpoint access
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
      most_recent              = true
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

# Create namespace for monitoring
resource "kubernetes_namespace" "monitoring" {
  count = var.install_prometheus ? 1 : 0

  metadata {
    name = "monitoring"
    labels = {
      name = "monitoring"
    }
  }

  depends_on = [module.eks]
}

# =============================================================================
# PROMETHEUS OPERATOR (kube-prometheus-stack)
# =============================================================================
#
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ WHY PROMETHEUS OPERATOR?                                                    │
# │                                                                             │
# │ The Prometheus Operator provides:                                           │
# │   1. CRDs: ServiceMonitor, PrometheusRule, PodMonitor                       │
# │   2. Automatic Prometheus config updates (no manual prometheus.yml)         │
# │   3. Grafana with pre-built dashboards                                      │
# │   4. Alertmanager for alerting                                              │
# │                                                                             │
# │ WITHOUT THIS: GoQueue's ServiceMonitor/PrometheusRule resources will fail   │
# │ with "no matches for kind ServiceMonitor in version monitoring.coreos.com"  │
# │                                                                             │
# │ PREVIOUS BUG: The agent disabled metrics instead of installing this first!  │
# └─────────────────────────────────────────────────────────────────────────────┘

resource "helm_release" "prometheus_operator" {
  count = var.install_prometheus ? 1 : 0

  name       = "kube-prometheus-stack"
  namespace  = kubernetes_namespace.monitoring[0].metadata[0].name
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  version    = "65.1.0"  # Latest stable as of 2026

  # Reduced resource footprint for dev environment
  values = [
    yamlencode({
      # Prometheus
      prometheus = {
        prometheusSpec = {
          retention = "7d"
          resources = {
            limits = {
              cpu    = "500m"
              memory = "1Gi"
            }
            requests = {
              cpu    = "200m"
              memory = "512Mi"
            }
          }
          # Scrape GoQueue namespace
          serviceMonitorSelectorNilUsesHelmValues = false
          podMonitorSelectorNilUsesHelmValues     = false
          ruleSelectorNilUsesHelmValues           = false
        }
      }

      # Grafana
      grafana = {
        enabled = true
        adminPassword = "admin"  # Change in production!
        resources = {
          limits = {
            cpu    = "200m"
            memory = "256Mi"
          }
          requests = {
            cpu    = "100m"
            memory = "128Mi"
          }
        }
      }

      # Alertmanager (minimal for dev)
      alertmanager = {
        enabled = true
        alertmanagerSpec = {
          resources = {
            limits = {
              cpu    = "100m"
              memory = "128Mi"
            }
            requests = {
              cpu    = "50m"
              memory = "64Mi"
            }
          }
        }
      }

      # Disable unused exporters for dev
      kubeStateMetrics = {
        enabled = true
      }
      nodeExporter = {
        enabled = true
      }
      kubeEtcd = {
        enabled = false
      }
      kubeControllerManager = {
        enabled = false
      }
      kubeScheduler = {
        enabled = false
      }
      kubeProxy = {
        enabled = false
      }
    })
  ]

  timeout = 600  # 10 minutes for CRD installation
  wait    = true

  depends_on = [
    kubernetes_namespace.monitoring,
    module.eks
  ]
}

# GoQueue Helm release
resource "helm_release" "goqueue" {
  name      = "goqueue"
  namespace = kubernetes_namespace.goqueue.metadata[0].name
  # path.module is the directory containing this .tf file
  # From deploy/terraform/modules/aws, we go up 3 levels to deploy/kubernetes/helm/goqueue
  chart     = abspath("${path.module}/../../../kubernetes/helm/goqueue")

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ HELM VALUES CONFIGURATION                                               │
  # │                                                                         │
  # │ These values override the default values.yaml in the Helm chart.        │
  # │ Key settings:                                                           │
  # │   - image.tag: Use cluster-enabled version (v0.3.4-cluster)             │
  # │   - config.cluster.enabled: Must be true for multi-node                 │
  # │   - resources: Tuned for c5.xlarge (4 vCPU, 8GB RAM)                    │
  # │   - persistence: Use gp3 SSD for production workloads                   │
  # └─────────────────────────────────────────────────────────────────────────┘
  values = [
    yamlencode({
      replicaCount = var.goqueue_replicas

      # Image configuration - use cluster-enabled version
      image = {
        repository = var.goqueue_image_repository
        tag        = var.goqueue_image_tag
        pullPolicy = "IfNotPresent"
      }

      # Cluster configuration - REQUIRED for multi-node deployment
      config = {
        cluster = {
          enabled            = true
          replicationFactor  = 2
          minInSyncReplicas  = 1
          replicationTimeout = "5s"
        }
        storage = {
          segmentSize   = 1073741824 # 1GB
          indexInterval = 4096
          syncOnWrite   = false
          syncInterval  = "1s"
        }
        logging = {
          level  = "info"
          format = "json"
        }
      }

      # Resources - tuned for c5.xlarge (4 vCPU, 8GB RAM)
      # Reserve ~1 CPU and 1.5GB for system/kubelet
      resources = {
        limits = {
          cpu    = "3"
          memory = "5Gi"
        }
        requests = {
          cpu    = "2"
          memory = "4Gi"
        }
      }

      # Service configuration
      service = {
        type         = "ClusterIP"
        httpPort     = 8080
        grpcPort     = 9000
        internalPort = 7000
      }

      # Pod anti-affinity - one pod per node
      affinity = {
        podAntiAffinity = {
          requiredDuringSchedulingIgnoredDuringExecution = [
            {
              labelSelector = {
                matchExpressions = [
                  {
                    key      = "app.kubernetes.io/name"
                    operator = "In"
                    values   = ["goqueue"]
                  }
                ]
              }
              topologyKey = "kubernetes.io/hostname"
            }
          ]
        }
      }

      # Metrics - enable Prometheus scraping
      metrics = {
        enabled = var.install_prometheus
        serviceMonitor = {
          enabled = var.install_prometheus
        }
        prometheusRule = {
          enabled = var.install_prometheus
        }
      }

      # ═══════════════════════════════════════════════════════════════════════
      # SECURITY CONFIGURATION (M21)
      # ═══════════════════════════════════════════════════════════════════════
      #
      # When security_enabled is true, this configures:
      #   - TLS for client connections (HTTPS/gRPC)
      #   - mTLS for inter-node cluster communication
      #   - API Key authentication with RBAC
      #
      security = var.security_enabled ? {
        tls = {
          enabled    = true
          selfSigned = var.security_tls_self_signed
          minVersion = "1.2"
          keepOnDelete = false  # Don't persist TLS secrets after helm uninstall
        }
        clusterTls = {
          enabled           = true
          selfSigned        = var.security_tls_self_signed
          requireClientCert = true
          keepOnDelete      = false  # Don't persist cluster TLS secrets
        }
        auth = {
          enabled                = true
          allowHealthWithoutAuth = true
          rootKey                = var.security_root_api_key
        }
        rbac = {
          enabled     = true
          defaultRole = "readonly"
        }
      } : {}

      # ═══════════════════════════════════════════════════════════════════════
      # BACKUP CONFIGURATION (M23)
      # ═══════════════════════════════════════════════════════════════════════
      #
      # Two-tier backup strategy:
      #   1. VolumeSnapshot: CSI-level snapshots of EBS volumes (fast, cheap)
      #   2. Metadata backup: Topics, offsets, schemas to S3 (portable)
      #
      backup = var.backup_enabled ? {
        schedule = {
          enabled = true
          cron    = var.backup_schedule
        }
        volumeSnapshot = {
          enabled          = var.backup_volume_snapshot_enabled
          driver           = "ebs.csi.aws.com"
          deletionPolicy   = "Delete"
          schedule         = var.backup_volume_snapshot_schedule
          retention        = var.backup_retention_days
        }
        objectStorage = var.backup_s3_bucket != "" ? {
          enabled  = true
          provider = "s3"
          bucket   = var.backup_s3_bucket
          prefix   = var.backup_s3_prefix
          region   = var.region
        } : {}
        include = {
          topics  = true
          offsets = true
          schemas = true
        }
      } : {}

      # ═══════════════════════════════════════════════════════════════════════
      # TRACING CONFIGURATION (M25)
      # ═══════════════════════════════════════════════════════════════════════
      #
      # Enables OTLP trace export to a distributed tracing backend.
      # In production, set tracing_otlp_endpoint to your Tempo/Jaeger instance.
      #
      # SAMPLING GUIDANCE:
      #   - Development:  1.0 (capture everything for debugging)
      #   - Staging:      0.5 (reasonable visibility without noise)
      #   - Production:   0.01-0.1 (cost-effective, captures enough)
      #
      tracing = var.tracing_enabled ? {
        enabled      = true
        samplingRate = var.tracing_sampling_rate
        otlp = {
          enabled  = true
          endpoint = var.tracing_otlp_endpoint
          protocol = "grpc"
          insecure = false
        }
      } : {}

      # Storage class configuration - use custom StorageClass with Delete policy
      persistence = {
        enabled      = true
        storageClass = "gp3"
        size         = var.goqueue_storage_size
        accessMode   = "ReadWriteOnce"
        createStorageClass = {
          enabled       = true
          provisioner   = "ebs.csi.aws.com"
          reclaimPolicy = "Delete"   # Prevents orphaned EBS volumes
        }
      }
    })
  ]

  # Wait for storage class, EKS, and Prometheus Operator (if enabled) to be ready
  timeout = 600 # 10 minutes for initial deployment
  wait    = true

  depends_on = [
    kubernetes_storage_class.gp3,
    module.eks,
    helm_release.prometheus_operator  # CRITICAL: Wait for Prometheus Operator CRDs!
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
