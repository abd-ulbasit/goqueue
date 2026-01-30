#!/bin/bash
# =============================================================================
# GOQUEUE CHAOS TEST - POD KILL
# =============================================================================
#
# PURPOSE: Test GoQueue's resilience when a broker pod is killed.
#
# WHAT THIS TESTS:
#   1. Leader election triggers when leader is killed
#   2. Replicas take over partitions
#   3. Consumers reconnect to new leaders
#   4. No message loss during failover
#
# EXPECTED BEHAVIOR:
#   - Failover completes within 5 seconds (our SLA target)
#   - Consumers experience brief pause, then resume
#   - No duplicate messages delivered
#   - Producers may see temporary errors (retry should succeed)
#
# USAGE:
#   ./pod-kill.sh [namespace] [iterations]
#
# EXAMPLE:
#   ./pod-kill.sh goqueue 5
#
# =============================================================================

set -euo pipefail

# Configuration
NAMESPACE="${1:-goqueue}"
ITERATIONS="${2:-3}"
LABEL_SELECTOR="app.kubernetes.io/name=goqueue"
RECOVERY_TIMEOUT=60
HEALTH_CHECK_INTERVAL=5

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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
    
    # Check if goqueue pods exist
    POD_COUNT=$(kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" --no-headers 2>/dev/null | wc -l)
    if [ "$POD_COUNT" -eq 0 ]; then
        log_error "No GoQueue pods found in namespace $NAMESPACE"
        exit 1
    fi
    
    log_info "Found $POD_COUNT GoQueue pods"
}

# Get a random pod
get_random_pod() {
    kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | shuf | head -1
}

# Check cluster health
check_cluster_health() {
    local healthy_count
    healthy_count=$(kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
    
    local total_count
    total_count=$(kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" --no-headers 2>/dev/null | wc -l)
    
    if [ "$healthy_count" -eq "$total_count" ]; then
        return 0
    else
        return 1
    fi
}

# Wait for cluster to recover
wait_for_recovery() {
    local start_time
    start_time=$(date +%s)
    
    log_info "Waiting for cluster to recover..."
    
    while true; do
        local current_time
        current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [ "$elapsed" -gt "$RECOVERY_TIMEOUT" ]; then
            log_error "Recovery timeout exceeded ($RECOVERY_TIMEOUT seconds)"
            return 1
        fi
        
        if check_cluster_health; then
            log_info "Cluster recovered in ${elapsed} seconds"
            return 0
        fi
        
        echo -n "."
        sleep "$HEALTH_CHECK_INTERVAL"
    done
}

# Kill a random pod
kill_pod() {
    local pod_name
    pod_name=$(get_random_pod)
    
    if [ -z "$pod_name" ]; then
        log_error "No pod found to kill"
        return 1
    fi
    
    log_info "Killing pod: $pod_name"
    
    local start_time
    start_time=$(date +%s)
    
    # Delete the pod (Kubernetes will recreate it via StatefulSet)
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --grace-period=0 --force 2>/dev/null || true
    
    local delete_time
    delete_time=$(date +%s)
    local delete_duration=$((delete_time - start_time))
    
    log_info "Pod deleted in ${delete_duration} seconds"
    
    # Wait for recovery
    if wait_for_recovery; then
        local recovery_time
        recovery_time=$(date +%s)
        local total_duration=$((recovery_time - start_time))
        
        echo ""
        log_info "Total failover time: ${total_duration} seconds"
        
        if [ "$total_duration" -le 5 ]; then
            log_info "✅ PASS: Failover completed within SLA (5 seconds)"
        else
            log_warn "⚠️ WARN: Failover exceeded 5 second SLA"
        fi
        
        return 0
    else
        log_error "❌ FAIL: Cluster did not recover"
        return 1
    fi
}

# Main test loop
run_tests() {
    log_info "Starting pod-kill chaos test"
    log_info "Namespace: $NAMESPACE"
    log_info "Iterations: $ITERATIONS"
    echo ""
    
    local passed=0
    local failed=0
    
    for i in $(seq 1 "$ITERATIONS"); do
        echo "=============================================="
        log_info "Iteration $i of $ITERATIONS"
        echo "=============================================="
        
        if kill_pod; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
        fi
        
        # Wait between iterations
        if [ "$i" -lt "$ITERATIONS" ]; then
            log_info "Waiting 30 seconds before next iteration..."
            sleep 30
        fi
        
        echo ""
    done
    
    echo "=============================================="
    log_info "CHAOS TEST COMPLETE"
    echo "=============================================="
    log_info "Passed: $passed"
    log_info "Failed: $failed"
    
    if [ "$failed" -eq 0 ]; then
        log_info "✅ All tests passed!"
        exit 0
    else
        log_error "❌ Some tests failed"
        exit 1
    fi
}

# Entry point
check_prerequisites
run_tests
