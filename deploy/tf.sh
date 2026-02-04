#!/bin/bash

# =============================================================================
# GoQueue Terraform Control Script
# =============================================================================
#
# USAGE:
#   ./tf.sh up      - Deploy EKS cluster with GoQueue
#   ./tf.sh down    - Destroy all infrastructure
#   ./tf.sh status  - Check deployment status
#   ./tf.sh url     - Get GoQueue LoadBalancer URL
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$SCRIPT_DIR/terraform/environments/dev"
REGION="ap-south-1"
CLUSTER_NAME="goqueue-dev"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_prerequisites() {
    local missing=0
    
    if ! command -v terraform &> /dev/null; then
        log_error "terraform not found. Install: https://www.terraform.io/downloads"
        missing=1
    fi
    
    if ! command -v aws &> /dev/null; then
        log_error "aws CLI not found. Install: https://aws.amazon.com/cli/"
        missing=1
    fi
    
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found. Install: https://kubernetes.io/docs/tasks/tools/"
        missing=1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured. Run: aws configure"
        missing=1
    fi
    
    return $missing
}

terraform_up() {
    log_info "Deploying GoQueue to AWS EKS..."
    log_info "This will take approximately 15-20 minutes"
    
    cd "$TF_DIR"
    
    # Initialize if needed
    if [ ! -d ".terraform" ]; then
        log_info "Initializing Terraform..."
        terraform init
    fi
    
    # Plan and apply
    log_info "Creating infrastructure..."
    terraform apply -auto-approve
    
    # Configure kubectl
    log_info "Configuring kubectl..."
    aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER_NAME"
    
    # Wait for GoQueue pods
    log_info "Waiting for GoQueue pods to be ready..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=goqueue -n goqueue --timeout=300s 2>/dev/null || true
    
    # Get LoadBalancer URL
    log_info "Getting LoadBalancer URL (may take 1-2 minutes)..."
    for i in {1..30}; do
        LB_URL=$(kubectl get svc -n goqueue goqueue-lb -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
        if [ -n "$LB_URL" ]; then
            break
        fi
        sleep 5
    done
    
    echo ""
    log_success "GoQueue deployed successfully!"
    echo ""
    echo "=============================================="
    echo "  GoQueue Cluster Information"
    echo "=============================================="
    echo ""
    echo "  Cluster:    $CLUSTER_NAME"
    echo "  Region:     $REGION"
    echo "  Namespace:  goqueue"
    echo ""
    if [ -n "$LB_URL" ]; then
        echo "  HTTP API:   http://$LB_URL:8080"
        echo "  gRPC:       $LB_URL:9000"
        echo ""
        echo "  Health:     curl http://$LB_URL:8080/health"
    else
        echo "  LoadBalancer pending... Run: ./tf.sh url"
    fi
    echo ""
    echo "=============================================="
}

terraform_down() {
    log_warn "This will DESTROY all GoQueue infrastructure!"
    read -p "Are you sure? (yes/no): " confirm
    
    if [ "$confirm" != "yes" ]; then
        log_info "Cancelled."
        exit 0
    fi
    
    cd "$TF_DIR"
    
    log_info "Destroying infrastructure..."
    terraform destroy -auto-approve
    
    log_success "All infrastructure destroyed!"
}

get_status() {
    log_info "Checking GoQueue status..."
    
    # Check if cluster exists
    if ! aws eks describe-cluster --name "$CLUSTER_NAME" --region "$REGION" &>/dev/null; then
        log_warn "EKS cluster not found. Run: ./tf.sh up"
        exit 1
    fi
    
    # Update kubeconfig
    aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER_NAME" 2>/dev/null
    
    echo ""
    echo "=== EKS Cluster ==="
    aws eks describe-cluster --name "$CLUSTER_NAME" --region "$REGION" \
        --query 'cluster.{Name:name,Status:status,Version:version}' --output table
    
    echo ""
    echo "=== Kubernetes Nodes ==="
    kubectl get nodes
    
    echo ""
    echo "=== GoQueue Pods ==="
    kubectl get pods -n goqueue
    
    echo ""
    echo "=== GoQueue Services ==="
    kubectl get svc -n goqueue
}

get_url() {
    # Update kubeconfig
    aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER_NAME" 2>/dev/null
    
    LB_URL=$(kubectl get svc -n goqueue goqueue-lb -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
    
    if [ -n "$LB_URL" ]; then
        echo ""
        echo "GoQueue LoadBalancer URL:"
        echo ""
        echo "  HTTP: http://$LB_URL:8080"
        echo "  gRPC: $LB_URL:9000"
        echo ""
        echo "Quick test:"
        echo "  curl http://$LB_URL:8080/health"
        echo ""
    else
        log_warn "LoadBalancer not ready yet. Try again in a minute."
    fi
}

run_benchmark() {
    log_info "Running in-cluster benchmark..."
    
    # Update kubeconfig
    aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER_NAME" 2>/dev/null
    
    # Apply benchmark job
    kubectl delete job -n goqueue goqueue-publish-bench 2>/dev/null || true
    kubectl apply -f "$SCRIPT_DIR/kubernetes/manual/publish-benchmark.yaml"
    
    # Wait for completion
    log_info "Waiting for benchmark to complete (usually ~60 seconds)..."
    kubectl wait --for=condition=complete job/goqueue-publish-bench -n goqueue --timeout=300s
    
    # Get results
    echo ""
    echo "=== Benchmark Results ==="
    kubectl logs -n goqueue job/goqueue-publish-bench
}

show_help() {
    echo ""
    echo "GoQueue Terraform Control"
    echo ""
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  up        Deploy EKS cluster with GoQueue (15-20 minutes)"
    echo "  down      Destroy all infrastructure"
    echo "  status    Show cluster and pod status"
    echo "  url       Get LoadBalancer URL"
    echo "  bench     Run in-cluster benchmark"
    echo "  help      Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 up          # Deploy everything"
    echo "  $0 url         # Get the API endpoint"
    echo "  $0 bench       # Run performance benchmark"
    echo "  $0 down        # Tear down everything"
    echo ""
}

# Main
case "${1:-help}" in
    up)
        check_prerequisites || exit 1
        terraform_up
        ;;
    down)
        check_prerequisites || exit 1
        terraform_down
        ;;
    status)
        check_prerequisites || exit 1
        get_status
        ;;
    url)
        get_url
        ;;
    bench|benchmark)
        check_prerequisites || exit 1
        run_benchmark
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        log_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac
