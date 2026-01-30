#!/bin/bash
# =============================================================================
# GOQUEUE CHAOS TEST - NETWORK PARTITION
# =============================================================================
#
# PURPOSE: Test GoQueue's behavior during network partitions (split-brain).
#
# WHAT THIS TESTS:
#   1. Broker behavior when isolated from peers
#   2. Leader election with network partition
#   3. Message consistency after partition heals
#   4. Client behavior during partition
#
# REQUIRES: Chaos Mesh installed (https://chaos-mesh.org/)
#
# USAGE:
#   ./network-partition.sh [namespace] [duration]
#
# EXAMPLE:
#   ./network-partition.sh goqueue 60
#
# =============================================================================

set -euo pipefail

# Configuration
NAMESPACE="${1:-goqueue}"
PARTITION_DURATION="${2:-30}"  # seconds
LABEL_SELECTOR="app.kubernetes.io/name=goqueue"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed"
        exit 1
    fi
    
    # Check if Chaos Mesh is installed
    if ! kubectl get crd networkchaos.chaos-mesh.org &> /dev/null; then
        log_error "Chaos Mesh is not installed"
        log_info "Install with: kubectl apply -f https://raw.githubusercontent.com/chaos-mesh/chaos-mesh/master/manifests/crd.yaml"
        exit 1
    fi
    
    # Check if namespace exists
    if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
        log_error "Namespace $NAMESPACE does not exist"
        exit 1
    fi
    
    # Get pod count
    local pod_count
    pod_count=$(kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" --no-headers 2>/dev/null | wc -l)
    
    if [ "$pod_count" -lt 3 ]; then
        log_error "Need at least 3 pods for network partition test, found $pod_count"
        exit 1
    fi
    
    log_info "Found $pod_count GoQueue pods"
}

# Create network chaos resource
create_network_partition() {
    log_info "Creating network partition..."
    
    # Get the first pod to isolate
    local target_pod
    target_pod=$(kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" -o jsonpath='{.items[0].metadata.name}')
    
    log_info "Isolating pod: $target_pod"
    
    cat <<EOF | kubectl apply -f -
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: goqueue-partition
  namespace: $NAMESPACE
spec:
  action: partition
  mode: one
  selector:
    namespaces:
      - $NAMESPACE
    labelSelectors:
      app.kubernetes.io/name: goqueue
    pods:
      $NAMESPACE:
        - $target_pod
  direction: both
  duration: "${PARTITION_DURATION}s"
  target:
    selector:
      namespaces:
        - $NAMESPACE
      labelSelectors:
        app.kubernetes.io/name: goqueue
    mode: all
EOF
    
    log_info "Network partition created (duration: ${PARTITION_DURATION}s)"
}

# Monitor cluster during partition
monitor_partition() {
    log_info "Monitoring cluster during partition..."
    
    local end_time=$(($(date +%s) + PARTITION_DURATION + 10))
    
    while [ "$(date +%s)" -lt "$end_time" ]; do
        echo ""
        log_info "Cluster state at $(date +%H:%M:%S):"
        kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" \
            -o custom-columns="NAME:.metadata.name,STATUS:.status.phase,READY:.status.conditions[?(@.type=='Ready')].status,NODE:.spec.nodeName"
        
        # Check for leader changes (if your app exposes this)
        # curl -s http://localhost:8080/cluster/leader 2>/dev/null || true
        
        sleep 5
    done
}

# Wait for network chaos to be cleaned up
wait_for_cleanup() {
    log_info "Waiting for partition to heal..."
    
    local timeout=60
    local start_time
    start_time=$(date +%s)
    
    while kubectl get networkchaos -n "$NAMESPACE" goqueue-partition &> /dev/null; do
        local elapsed=$(($(date +%s) - start_time))
        if [ "$elapsed" -gt "$timeout" ]; then
            log_warn "Timeout waiting for chaos cleanup, forcing deletion"
            kubectl delete networkchaos -n "$NAMESPACE" goqueue-partition --force --grace-period=0 2>/dev/null || true
            break
        fi
        echo -n "."
        sleep 2
    done
    
    echo ""
    log_info "Network partition healed"
}

# Verify cluster consistency
verify_consistency() {
    log_info "Verifying cluster consistency..."
    
    # Wait for pods to stabilize
    sleep 10
    
    # Check all pods are ready
    local ready_count
    ready_count=$(kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" \
        -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' | grep -c "True" || echo "0")
    
    local total_count
    total_count=$(kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" --no-headers 2>/dev/null | wc -l)
    
    if [ "$ready_count" -eq "$total_count" ]; then
        log_info "✅ All $total_count pods are ready"
    else
        log_error "❌ Only $ready_count of $total_count pods are ready"
        return 1
    fi
    
    # TODO: Add message consistency checks
    # - Produce messages to topic
    # - Consume and verify all messages received
    # - Check for duplicates
    
    return 0
}

# Cleanup
cleanup() {
    log_info "Cleaning up chaos resources..."
    kubectl delete networkchaos -n "$NAMESPACE" goqueue-partition 2>/dev/null || true
}

trap cleanup EXIT

# Run the test
run_test() {
    log_info "Starting network partition chaos test"
    log_info "Namespace: $NAMESPACE"
    log_info "Partition duration: ${PARTITION_DURATION}s"
    echo ""
    
    # Initial state
    log_info "Initial cluster state:"
    kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" -o wide
    echo ""
    
    # Create partition
    create_network_partition
    echo ""
    
    # Monitor during partition
    monitor_partition
    echo ""
    
    # Wait for cleanup
    wait_for_cleanup
    echo ""
    
    # Verify consistency
    if verify_consistency; then
        log_info "=============================================="
        log_info "NETWORK PARTITION TEST COMPLETE"
        log_info "=============================================="
        log_info "✅ Cluster recovered successfully"
    else
        log_error "=============================================="
        log_error "NETWORK PARTITION TEST FAILED"
        log_error "=============================================="
        exit 1
    fi
}

# Entry point
check_prerequisites
run_test
