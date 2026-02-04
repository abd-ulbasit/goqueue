#!/bin/bash
# =============================================================================
# GOQUEUE AWS EKS DEPLOYMENT VERIFICATION & BENCHMARK RUNNER
# =============================================================================
#
# This script verifies the GoQueue deployment on AWS EKS and runs benchmarks.
#
# USAGE:
#   ./verify-and-benchmark.sh
#
# PREREQUISITES:
#   - AWS CLI configured
#   - kubectl installed
#   - terraform installed
#   - Node.js or Python (for benchmarks)
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TF_DIR="$PROJECT_ROOT/deploy/terraform/environments/dev"
BENCHMARK_DIR="$PROJECT_ROOT/deploy/testing/benchmark"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "╔════════════════════════════════════════════════════════════════════════════════════╗"
echo "║              GOQUEUE AWS EKS DEPLOYMENT VERIFICATION & BENCHMARK                   ║"
echo "╚════════════════════════════════════════════════════════════════════════════════════╝"

# =============================================================================
# STEP 1: Verify AWS Credentials
# =============================================================================

log_info "Step 1: Verifying AWS credentials..."

if ! aws sts get-caller-identity > /dev/null 2>&1; then
    log_error "AWS credentials not configured. Run 'aws configure' first."
    exit 1
fi

AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
AWS_USER=$(aws sts get-caller-identity --query Arn --output text | cut -d'/' -f2)
log_success "AWS Account: $AWS_ACCOUNT (User: $AWS_USER)"

# =============================================================================
# STEP 2: Check Terraform State
# =============================================================================

log_info "Step 2: Checking Terraform state..."

if [ ! -f "$TF_DIR/terraform.tfstate" ]; then
    log_warn "No Terraform state found. Has the infrastructure been deployed?"
    log_info "To deploy, run:"
    log_info "  cd $TF_DIR"
    log_info "  terraform init && terraform apply"
    exit 1
fi

# Get outputs from Terraform
CLUSTER_NAME=$(terraform -chdir="$TF_DIR" output -raw cluster_name 2>/dev/null || echo "")
CONFIGURE_KUBECTL=$(terraform -chdir="$TF_DIR" output -raw configure_kubectl 2>/dev/null || echo "")

if [ -z "$CLUSTER_NAME" ]; then
    log_error "Could not get cluster name from Terraform outputs."
    exit 1
fi

log_success "Found EKS cluster: $CLUSTER_NAME"

# =============================================================================
# STEP 3: Configure kubectl
# =============================================================================

log_info "Step 3: Configuring kubectl..."

eval "$CONFIGURE_KUBECTL"
log_success "kubectl configured for $CLUSTER_NAME"

# =============================================================================
# STEP 4: Verify Cluster Health
# =============================================================================

log_info "Step 4: Verifying cluster health..."

# Check nodes
NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | wc -l)
NODE_READY=$(kubectl get nodes --no-headers 2>/dev/null | grep -c " Ready" || echo 0)

if [ "$NODE_READY" -lt 3 ]; then
    log_warn "Not all nodes are ready. Nodes: $NODE_READY/$NODE_COUNT"
    kubectl get nodes
    log_info "Waiting for nodes to be ready..."
    kubectl wait --for=condition=Ready nodes --all --timeout=300s
fi

log_success "All $NODE_READY nodes are ready"

# =============================================================================
# STEP 5: Verify GoQueue Deployment
# =============================================================================

log_info "Step 5: Verifying GoQueue deployment..."

# Check namespace
if ! kubectl get namespace goqueue > /dev/null 2>&1; then
    log_error "GoQueue namespace not found"
    exit 1
fi

# Check pods
GOQUEUE_PODS=$(kubectl get pods -n goqueue --no-headers 2>/dev/null | wc -l)
GOQUEUE_READY=$(kubectl get pods -n goqueue --no-headers 2>/dev/null | grep -c "Running" || echo 0)

if [ "$GOQUEUE_READY" -lt 1 ]; then
    log_warn "GoQueue pods not ready yet. Waiting..."
    kubectl wait --for=condition=Ready pods -n goqueue --all --timeout=300s
fi

kubectl get pods -n goqueue
log_success "GoQueue pods are running"

# =============================================================================
# STEP 6: Get GoQueue Service URL
# =============================================================================

log_info "Step 6: Getting GoQueue service URL..."

# Try to get LoadBalancer URL first
GOQUEUE_LB=$(kubectl get svc -n goqueue goqueue -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")

if [ -z "$GOQUEUE_LB" ]; then
    # Fall back to port-forwarding
    log_warn "No LoadBalancer found. Setting up port-forward..."
    
    # Kill any existing port-forwards
    pkill -f "kubectl port-forward.*goqueue" || true
    
    # Start port-forward in background
    kubectl port-forward svc/goqueue -n goqueue 8080:8080 &
    PF_PID=$!
    sleep 3
    
    GOQUEUE_URL="http://localhost:8080"
    log_info "Port-forward started (PID: $PF_PID)"
else
    GOQUEUE_URL="http://$GOQUEUE_LB:8080"
fi

log_success "GoQueue URL: $GOQUEUE_URL"

# =============================================================================
# STEP 7: Health Check
# =============================================================================

log_info "Step 7: Running health check..."

MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s "$GOQUEUE_URL/health" > /dev/null 2>&1; then
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    log_info "Waiting for GoQueue to be ready... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 5
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    log_error "GoQueue health check failed after $MAX_RETRIES attempts"
    exit 1
fi

HEALTH_RESPONSE=$(curl -s "$GOQUEUE_URL/health" | head -c 200)
log_success "Health check passed: $HEALTH_RESPONSE"

# =============================================================================
# STEP 8: Run Benchmarks
# =============================================================================

log_info "Step 8: Running benchmarks..."

cd "$BENCHMARK_DIR"

# Check which runtime is available
if command -v python3 &> /dev/null; then
    log_info "Running Python benchmark..."
    GOQUEUE_URL="$GOQUEUE_URL" python3 benchmark.py
elif command -v npx &> /dev/null; then
    log_info "Running TypeScript benchmark..."
    npm install 2>/dev/null || true
    GOQUEUE_URL="$GOQUEUE_URL" npx tsx benchmark.ts
else
    log_warn "Neither Python nor Node.js found. Skipping benchmarks."
fi

# =============================================================================
# STEP 9: Summary
# =============================================================================

echo ""
echo "╔════════════════════════════════════════════════════════════════════════════════════╗"
echo "║                              DEPLOYMENT SUMMARY                                     ║"
echo "╠════════════════════════════════════════════════════════════════════════════════════╣"
echo "║ Cluster Name:    $CLUSTER_NAME"
echo "║ GoQueue URL:     $GOQUEUE_URL"
echo "║ Namespace:       goqueue"
echo "║ Node Count:      $NODE_READY"
echo "║ Pod Count:       $GOQUEUE_READY"
echo "╚════════════════════════════════════════════════════════════════════════════════════╝"

echo ""
log_info "Useful commands:"
echo "  kubectl get pods -n goqueue                    # View pods"
echo "  kubectl logs -f -n goqueue goqueue-0           # View logs"
echo "  kubectl top pods -n goqueue                    # Resource usage"
echo "  kubectl exec -it goqueue-0 -n goqueue -- sh    # Shell into pod"

# Cleanup port-forward if we started one
if [ -n "$PF_PID" ]; then
    echo ""
    log_info "Port-forward is running (PID: $PF_PID). Kill with: kill $PF_PID"
fi

log_success "Deployment verification complete!"
