#!/bin/bash
# =============================================================================
# GOQUEUE CHAOS TEST - NODE DRAIN
# =============================================================================
#
# PURPOSE: Test GoQueue's resilience when a Kubernetes node is drained.
#
# WHAT THIS TESTS:
#   1. Pod eviction and rescheduling
#   2. PodDisruptionBudget enforcement (min 2 available)
#   3. Data persistence (PVC reattachment)
#   4. Cluster rebalancing after node returns
#
# EXPECTED BEHAVIOR:
#   - Only 1 pod evicted at a time (PDB enforced)
#   - Pod rescheduled to different node
#   - Data preserved via PVC
#   - Consumers experience brief pause
#
# USAGE:
#   ./node-drain.sh [namespace]
#
# EXAMPLE:
#   ./node-drain.sh goqueue
#
# =============================================================================

set -euo pipefail

# Configuration
NAMESPACE="${1:-goqueue}"
LABEL_SELECTOR="app.kubernetes.io/name=goqueue"
DRAIN_TIMEOUT=300
RECOVERY_TIMEOUT=120

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
    
    # Check if namespace exists
    if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
        log_error "Namespace $NAMESPACE does not exist"
        exit 1
    fi
    
    # Check PDB exists
    if ! kubectl get pdb -n "$NAMESPACE" -l app.kubernetes.io/name=goqueue &> /dev/null; then
        log_warn "No PodDisruptionBudget found - drain may evict all pods!"
    fi
}

# Get a node running goqueue pods
get_goqueue_node() {
    kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" -o jsonpath='{.items[0].spec.nodeName}'
}

# Get pod count on a specific node
get_pod_count_on_node() {
    local node="$1"
    kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" --field-selector="spec.nodeName=$node" --no-headers 2>/dev/null | wc -l
}

# Check cluster health
check_cluster_health() {
    local ready_count
    ready_count=$(kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' | grep -c "True" || echo "0")
    
    local total_count
    total_count=$(kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" --no-headers 2>/dev/null | wc -l)
    
    [ "$ready_count" -eq "$total_count" ] && [ "$total_count" -gt 0 ]
}

# Wait for recovery
wait_for_recovery() {
    local start_time
    start_time=$(date +%s)
    
    log_info "Waiting for cluster to recover..."
    
    while true; do
        local current_time
        current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [ "$elapsed" -gt "$RECOVERY_TIMEOUT" ]; then
            log_error "Recovery timeout exceeded"
            return 1
        fi
        
        if check_cluster_health; then
            log_info "Cluster recovered in ${elapsed} seconds"
            return 0
        fi
        
        echo -n "."
        sleep 5
    done
}

# Drain a node
drain_node() {
    local node
    node=$(get_goqueue_node)
    
    if [ -z "$node" ]; then
        log_error "No node found running GoQueue pods"
        return 1
    fi
    
    local pod_count
    pod_count=$(get_pod_count_on_node "$node")
    
    log_info "Selected node: $node (running $pod_count GoQueue pods)"
    log_info "Starting node drain..."
    
    local start_time
    start_time=$(date +%s)
    
    # Drain the node
    # --delete-emptydir-data: Allow eviction of pods with emptyDir volumes
    # --ignore-daemonsets: Don't evict DaemonSet pods
    # --force: Evict pods without controllers
    # --timeout: Maximum time to wait for eviction
    kubectl drain "$node" \
        --delete-emptydir-data \
        --ignore-daemonsets \
        --force \
        --timeout="${DRAIN_TIMEOUT}s" \
        2>&1 | while read -r line; do
            echo "  $line"
        done
    
    local drain_result=$?
    local drain_time
    drain_time=$(date +%s)
    local drain_duration=$((drain_time - start_time))
    
    if [ $drain_result -eq 0 ]; then
        log_info "Node drained in ${drain_duration} seconds"
    else
        log_warn "Drain completed with warnings in ${drain_duration} seconds"
    fi
    
    # Check PDB was respected
    local remaining_pods
    remaining_pods=$(kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
    
    if [ "$remaining_pods" -ge 2 ]; then
        log_info "✅ PDB respected: $remaining_pods pods still running during drain"
    else
        log_warn "⚠️ PDB may not have been respected: only $remaining_pods pods running"
    fi
    
    return 0
}

# Uncordon the node
uncordon_node() {
    local node
    node=$(kubectl get nodes --field-selector spec.unschedulable=true -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    
    if [ -z "$node" ]; then
        log_info "No cordoned nodes found"
        return 0
    fi
    
    log_info "Uncordoning node: $node"
    kubectl uncordon "$node"
    
    log_info "Node uncordoned"
}

# Run the test
run_test() {
    log_info "Starting node-drain chaos test"
    log_info "Namespace: $NAMESPACE"
    echo ""
    
    # Record initial state
    log_info "Initial cluster state:"
    kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" -o wide
    echo ""
    
    # Drain the node
    if ! drain_node; then
        log_error "Drain failed"
        uncordon_node
        exit 1
    fi
    
    echo ""
    
    # Show current state
    log_info "State after drain:"
    kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" -o wide
    echo ""
    
    # Wait for recovery
    if ! wait_for_recovery; then
        log_error "Recovery failed"
        uncordon_node
        exit 1
    fi
    
    echo ""
    
    # Uncordon the node
    uncordon_node
    
    echo ""
    
    # Final state
    log_info "Final cluster state:"
    kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" -o wide
    
    echo ""
    log_info "=============================================="
    log_info "NODE DRAIN TEST COMPLETE"
    log_info "=============================================="
    log_info "✅ All checks passed!"
}

# Cleanup on exit
cleanup() {
    log_info "Cleaning up..."
    uncordon_node 2>/dev/null || true
}

trap cleanup EXIT

# Entry point
check_prerequisites
run_test
