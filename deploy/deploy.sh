#!/bin/bash
# =============================================================================
# GOQUEUE DEPLOYMENT SCRIPT
# =============================================================================
#
# This script simplifies deploying GoQueue to AWS EKS using Terraform and Helm.
#
# PREREQUISITES:
#   - AWS CLI configured (aws configure)
#   - kubectl installed
#   - helm installed
#   - terraform installed
#
# USAGE:
#   # Deploy to dev environment (creates EKS cluster + GoQueue)
#   ./deploy.sh deploy dev
#
#   # Only deploy GoQueue to existing cluster
#   ./deploy.sh goqueue dev
#
#   # Get LoadBalancer URL
#   ./deploy.sh url dev
#
#   # Check status
#   ./deploy.sh status dev
#
#   # Destroy everything
#   ./deploy.sh destroy dev
#
#   # Run benchmarks
#   ./deploy.sh benchmark dev
#
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
DEFAULT_REGION="ap-south-1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="${SCRIPT_DIR}/terraform/environments"
HELM_CHART="${SCRIPT_DIR}/kubernetes/helm/goqueue"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

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
    
    local missing=()
    
    command -v aws >/dev/null 2>&1 || missing+=("aws")
    command -v kubectl >/dev/null 2>&1 || missing+=("kubectl")
    command -v helm >/dev/null 2>&1 || missing+=("helm")
    command -v terraform >/dev/null 2>&1 || missing+=("terraform")
    
    if [ ${#missing[@]} -ne 0 ]; then
        log_error "Missing required tools: ${missing[*]}"
        echo ""
        echo "Install them:"
        echo "  - aws: https://aws.amazon.com/cli/"
        echo "  - kubectl: https://kubernetes.io/docs/tasks/tools/"
        echo "  - helm: https://helm.sh/docs/intro/install/"
        echo "  - terraform: https://terraform.io/downloads"
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        log_error "AWS credentials not configured. Run 'aws configure' first."
        exit 1
    fi
    
    log_success "All prerequisites satisfied"
}

# =============================================================================
# TERRAFORM FUNCTIONS
# =============================================================================

terraform_init() {
    local env=$1
    local tf_dir="${TERRAFORM_DIR}/${env}"
    
    if [ ! -d "$tf_dir" ]; then
        log_error "Environment '${env}' not found at ${tf_dir}"
        exit 1
    fi
    
    log_info "Initializing Terraform for '${env}'..."
    cd "$tf_dir"
    terraform init
}

terraform_plan() {
    local env=$1
    local tf_dir="${TERRAFORM_DIR}/${env}"
    
    cd "$tf_dir"
    log_info "Planning Terraform changes..."
    terraform plan -out=tfplan
}

terraform_apply() {
    local env=$1
    local tf_dir="${TERRAFORM_DIR}/${env}"
    
    cd "$tf_dir"
    log_info "Applying Terraform changes..."
    terraform apply tfplan
}

terraform_destroy() {
    local env=$1
    local tf_dir="${TERRAFORM_DIR}/${env}"
    
    cd "$tf_dir"
    log_warning "This will destroy all infrastructure for '${env}'!"
    read -p "Are you sure? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        log_info "Aborted."
        exit 0
    fi
    
    log_info "Destroying Terraform infrastructure..."
    terraform destroy -auto-approve
}

# =============================================================================
# KUBERNETES FUNCTIONS
# =============================================================================

configure_kubectl() {
    local env=$1
    local cluster_name="goqueue-${env}"
    local region="${AWS_REGION:-$DEFAULT_REGION}"
    
    log_info "Configuring kubectl for cluster '${cluster_name}'..."
    aws eks update-kubeconfig --name "$cluster_name" --region "$region"
    
    log_success "kubectl configured for '${cluster_name}'"
}

deploy_goqueue() {
    local env=$1
    local namespace="default"
    
    log_info "Deploying GoQueue via Helm..."
    
    # Install or upgrade
    helm upgrade --install goqueue "$HELM_CHART" \
        --namespace "$namespace" \
        --set replicaCount=1 \
        --set image.tag=latest \
        --set service.type=LoadBalancer \
        --wait \
        --timeout 5m
    
    log_success "GoQueue deployed successfully"
}

get_goqueue_url() {
    log_info "Getting GoQueue LoadBalancer URL..."
    
    local url=""
    local attempts=0
    local max_attempts=30
    
    while [ -z "$url" ] && [ $attempts -lt $max_attempts ]; do
        url=$(kubectl get svc goqueue -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null)
        if [ -z "$url" ]; then
            url=$(kubectl get svc goqueue -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
        fi
        if [ -z "$url" ]; then
            log_info "Waiting for LoadBalancer... (${attempts}/${max_attempts})"
            sleep 10
            ((attempts++))
        fi
    done
    
    if [ -z "$url" ]; then
        log_error "Timed out waiting for LoadBalancer URL"
        exit 1
    fi
    
    echo ""
    log_success "GoQueue is accessible at:"
    echo ""
    echo "  HTTP API:  http://${url}:8080"
    echo "  gRPC:      ${url}:9000"
    echo ""
    echo "  Health:    curl http://${url}:8080/health"
    echo "  Topics:    curl http://${url}:8080/topics"
    echo ""
}

check_status() {
    log_info "Checking GoQueue status..."
    
    echo ""
    echo "=== Pods ==="
    kubectl get pods -l app.kubernetes.io/name=goqueue -o wide
    
    echo ""
    echo "=== Services ==="
    kubectl get svc goqueue -o wide
    
    echo ""
    echo "=== StatefulSet ==="
    kubectl get statefulset goqueue
    
    # Try to get URL
    local url=$(kubectl get svc goqueue -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null)
    if [ -n "$url" ]; then
        echo ""
        echo "=== Health Check ==="
        curl -s "http://${url}:8080/health" | head -c 200
        echo ""
    fi
}

run_benchmark() {
    local env=$1
    local url=$(kubectl get svc goqueue -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null)
    
    if [ -z "$url" ]; then
        log_error "Could not get LoadBalancer URL. Make sure GoQueue is deployed."
        exit 1
    fi
    
    local benchmark_dir="${SCRIPT_DIR}/testing/benchmark"
    
    echo ""
    log_info "Running Go benchmark..."
    cd "$benchmark_dir"
    GOQUEUE_URL="http://${url}:8080" go test -bench=BenchmarkPublishBatch -run='^$' -benchtime=10s
    
    echo ""
    log_info "Running Python benchmark..."
    if command -v python3 >/dev/null 2>&1; then
        GOQUEUE_URL="http://${url}:8080" python3 benchmark.py
    else
        log_warning "Python3 not found, skipping Python benchmark"
    fi
    
    echo ""
    log_info "Running TypeScript benchmark..."
    if [ -f "package.json" ] && command -v npm >/dev/null 2>&1; then
        npm install 2>/dev/null
        GOQUEUE_URL="http://${url}:8080" npm run benchmark
    else
        log_warning "npm not found, skipping TypeScript benchmark"
    fi
}

# =============================================================================
# MAIN COMMANDS
# =============================================================================

cmd_deploy() {
    local env=${1:-dev}
    
    check_prerequisites
    
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║                  GOQUEUE EKS DEPLOYMENT                          ║"
    echo "║                  Environment: ${env}                               ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo ""
    
    # Step 1: Terraform
    log_info "Step 1/4: Provisioning AWS infrastructure..."
    terraform_init "$env"
    terraform_plan "$env"
    terraform_apply "$env"
    
    # Step 2: Configure kubectl
    log_info "Step 2/4: Configuring kubectl..."
    configure_kubectl "$env"
    
    # Step 3: Deploy GoQueue
    log_info "Step 3/4: Deploying GoQueue..."
    deploy_goqueue "$env"
    
    # Step 4: Get URL
    log_info "Step 4/4: Getting LoadBalancer URL..."
    get_goqueue_url
    
    log_success "Deployment complete!"
}

cmd_goqueue() {
    local env=${1:-dev}
    
    check_prerequisites
    configure_kubectl "$env"
    deploy_goqueue "$env"
    get_goqueue_url
}

cmd_url() {
    local env=${1:-dev}
    
    configure_kubectl "$env"
    get_goqueue_url
}

cmd_status() {
    local env=${1:-dev}
    
    configure_kubectl "$env"
    check_status
}

cmd_destroy() {
    local env=${1:-dev}
    
    check_prerequisites
    terraform_destroy "$env"
}

cmd_benchmark() {
    local env=${1:-dev}
    
    configure_kubectl "$env"
    run_benchmark "$env"
}

cmd_help() {
    echo ""
    echo "GoQueue Deployment Script"
    echo ""
    echo "Usage: $0 <command> [environment]"
    echo ""
    echo "Commands:"
    echo "  deploy <env>     Full deployment (Terraform + Helm)"
    echo "  goqueue <env>    Deploy only GoQueue (to existing cluster)"
    echo "  url <env>        Get LoadBalancer URL"
    echo "  status <env>     Check deployment status"
    echo "  destroy <env>    Destroy all infrastructure"
    echo "  benchmark <env>  Run benchmarks against deployed cluster"
    echo "  help             Show this help"
    echo ""
    echo "Environments: dev, prod"
    echo ""
    echo "Examples:"
    echo "  $0 deploy dev              # Full deployment to dev"
    echo "  $0 goqueue dev             # Deploy GoQueue only"
    echo "  $0 url dev                 # Get LoadBalancer URL"
    echo "  $0 benchmark dev           # Run benchmarks"
    echo "  $0 destroy dev             # Tear down everything"
    echo ""
}

# =============================================================================
# ENTRY POINT
# =============================================================================

case "${1:-help}" in
    deploy)
        cmd_deploy "$2"
        ;;
    goqueue)
        cmd_goqueue "$2"
        ;;
    url)
        cmd_url "$2"
        ;;
    status)
        cmd_status "$2"
        ;;
    destroy)
        cmd_destroy "$2"
        ;;
    benchmark)
        cmd_benchmark "$2"
        ;;
    help|--help|-h)
        cmd_help
        ;;
    *)
        log_error "Unknown command: $1"
        cmd_help
        exit 1
        ;;
esac
