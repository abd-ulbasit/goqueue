#!/bin/bash
# =============================================================================
# GOQUEUE TERRAFORM DEPLOYMENT SCRIPT
# =============================================================================
#
# Usage:
#   ./deploy.sh up     - Deploy GoQueue cluster to AWS
#   ./deploy.sh down   - Destroy GoQueue cluster and all resources
#   ./deploy.sh status - Show current infrastructure status
#
# Prerequisites:
#   - AWS CLI configured with appropriate credentials
#   - Terraform >= 1.5.0
#   - kubectl (for post-deployment verification)
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$SCRIPT_DIR/deploy/terraform/environments/dev"
AWS_REGION="ap-south-1"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        log_error "Terraform is not installed. Please install it first."
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials are not configured. Run 'aws configure' first."
        exit 1
    fi
    
    log_success "All prerequisites met"
}

deploy_up() {
    log_info "Starting GoQueue deployment to AWS..."
    
    check_prerequisites
    
    cd "$TF_DIR"
    
    # Initialize Terraform
    log_info "Initializing Terraform..."
    terraform init -upgrade
    
    # Plan
    log_info "Creating deployment plan..."
    terraform plan -out=tfplan
    
    # Apply
    log_info "Applying infrastructure changes (this will take 15-20 minutes)..."
    terraform apply tfplan
    
    # Get outputs
    log_info "Fetching cluster information..."
    CLUSTER_NAME=$(terraform output -raw cluster_name 2>/dev/null || echo "goqueue-dev")
    KUBECTL_CMD=$(terraform output -raw configure_kubectl 2>/dev/null || echo "aws eks update-kubeconfig --region $AWS_REGION --name $CLUSTER_NAME")
    
    # Configure kubectl
    log_info "Configuring kubectl..."
    $KUBECTL_CMD
    
    # Wait for pods to be ready
    log_info "Waiting for GoQueue pods to be ready..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=goqueue -n goqueue --timeout=300s
    
    # Verify cluster health
    log_info "Verifying cluster health..."
    kubectl exec goqueue-0 -n goqueue -- wget -qO- http://localhost:7000/cluster/health 2>/dev/null || \
        log_warning "Could not verify cluster health (this is normal if using distroless image)"
    
    log_success "Deployment complete!"
    echo ""
    echo "=========================================="
    echo "GoQueue Cluster Information"
    echo "=========================================="
    echo "Cluster Name: $CLUSTER_NAME"
    echo "Region: $AWS_REGION"
    echo ""
    echo "To access the cluster:"
    echo "  kubectl port-forward svc/goqueue 8080:8080 -n goqueue"
    echo "  curl http://localhost:8080/health"
    echo ""
    echo "To check cluster status:"
    echo "  kubectl get pods -n goqueue"
    echo "=========================================="
}

deploy_down() {
    log_info "Destroying GoQueue infrastructure..."
    
    check_prerequisites
    
    cd "$TF_DIR"
    
    # Confirm destruction
    echo ""
    log_warning "This will destroy ALL resources including:"
    echo "  - EKS Cluster"
    echo "  - EC2 Nodes"
    echo "  - NAT Gateway"
    echo "  - VPC and Subnets"
    echo "  - EBS Volumes"
    echo ""
    read -p "Are you sure you want to continue? (yes/no): " confirm
    
    if [[ "$confirm" != "yes" ]]; then
        log_info "Destruction cancelled."
        exit 0
    fi
    
    # Initialize Terraform (in case state is stale)
    log_info "Initializing Terraform..."
    terraform init -upgrade
    
    # Destroy
    log_info "Destroying infrastructure (this will take 15-20 minutes)..."
    terraform destroy -auto-approve
    
    log_success "Infrastructure destroyed successfully!"
}

show_status() {
    log_info "Checking infrastructure status..."
    
    echo ""
    echo "=========================================="
    echo "AWS Resources in $AWS_REGION"
    echo "=========================================="
    
    # EKS Clusters
    echo ""
    echo "EKS Clusters:"
    aws eks list-clusters --region $AWS_REGION --output text --no-cli-pager || echo "  None"
    
    # EC2 Instances
    echo ""
    echo "EC2 Instances (running):"
    aws ec2 describe-instances --region $AWS_REGION \
        --filters "Name=instance-state-name,Values=running" \
        --query 'Reservations[*].Instances[*].[InstanceId,InstanceType,Tags[?Key==`Name`].Value|[0]]' \
        --output text --no-cli-pager || echo "  None"
    
    # NAT Gateways
    echo ""
    echo "NAT Gateways:"
    aws ec2 describe-nat-gateways --region $AWS_REGION \
        --filter "Name=state,Values=available" \
        --query 'NatGateways[*].[NatGatewayId,State]' \
        --output text --no-cli-pager || echo "  None"
    
    # S3 Buckets
    echo ""
    echo "S3 Buckets (goqueue related):"
    aws s3 ls --no-cli-pager 2>/dev/null | grep goqueue || echo "  None"
    
    # Estimated monthly cost
    echo ""
    echo "=========================================="
    
    # Check if cluster exists and show kubectl info
    if aws eks describe-cluster --name goqueue-dev --region $AWS_REGION &>/dev/null; then
        echo ""
        log_info "GoQueue cluster is running. Configure kubectl with:"
        echo "  aws eks update-kubeconfig --region $AWS_REGION --name goqueue-dev"
    fi
}

# Main
case "${1:-}" in
    up|deploy)
        deploy_up
        ;;
    down|destroy)
        deploy_down
        ;;
    status)
        show_status
        ;;
    *)
        echo "Usage: $0 {up|down|status}"
        echo ""
        echo "Commands:"
        echo "  up      - Deploy GoQueue cluster to AWS"
        echo "  down    - Destroy all AWS resources"
        echo "  status  - Show current infrastructure status"
        exit 1
        ;;
esac
