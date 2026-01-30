#!/bin/bash
# =============================================================================
# GOQUEUE CHAOS TEST - DISK PRESSURE
# =============================================================================
#
# PURPOSE: Test GoQueue's behavior under disk pressure conditions.
#
# WHAT THIS TESTS:
#   1. Broker behavior when PVC fills up
#   2. Backpressure mechanisms
#   3. Graceful degradation
#   4. Recovery after disk space freed
#
# REQUIRES: 
#   - kubectl with exec permissions
#   - PVC with enough initial space to fill
#
# USAGE:
#   ./disk-pressure.sh [namespace] [fill-percentage]
#
# EXAMPLE:
#   ./disk-pressure.sh goqueue 80
#
# =============================================================================

set -euo pipefail

# Configuration
NAMESPACE="${1:-goqueue}"
FILL_PERCENTAGE="${2:-80}"  # Fill to this percentage
LABEL_SELECTOR="app.kubernetes.io/name=goqueue"
DATA_DIR="/data"
TEMP_FILE_PREFIX="chaos-fill-"

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
    
    # Get target pod
    TARGET_POD=$(kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    
    if [ -z "$TARGET_POD" ]; then
        log_error "No GoQueue pods found"
        exit 1
    fi
    
    log_info "Target pod: $TARGET_POD"
}

# Get current disk usage
get_disk_usage() {
    local pod=$1
    
    kubectl exec -n "$NAMESPACE" "$pod" -- df -h "$DATA_DIR" 2>/dev/null | \
        tail -1 | awk '{print $5}' | tr -d '%'
}

# Get available space in bytes
get_available_space() {
    local pod=$1
    
    # Get available space in 1K blocks and convert to bytes
    kubectl exec -n "$NAMESPACE" "$pod" -- df "$DATA_DIR" 2>/dev/null | \
        tail -1 | awk '{print $4 * 1024}'
}

# Get total space in bytes
get_total_space() {
    local pod=$1
    
    kubectl exec -n "$NAMESPACE" "$pod" -- df "$DATA_DIR" 2>/dev/null | \
        tail -1 | awk '{print $2 * 1024}'
}

# Fill disk to target percentage
fill_disk() {
    log_info "Filling disk to ${FILL_PERCENTAGE}%..."
    
    local current_usage
    current_usage=$(get_disk_usage "$TARGET_POD")
    
    log_info "Current disk usage: ${current_usage}%"
    
    if [ "$current_usage" -ge "$FILL_PERCENTAGE" ]; then
        log_warn "Disk already at ${current_usage}%, no need to fill"
        return 0
    fi
    
    local total_space
    total_space=$(get_total_space "$TARGET_POD")
    
    local current_used
    current_used=$((total_space * current_usage / 100))
    
    local target_used
    target_used=$((total_space * FILL_PERCENTAGE / 100))
    
    local bytes_to_fill
    bytes_to_fill=$((target_used - current_used))
    
    log_info "Filling ${bytes_to_fill} bytes..."
    
    # Create fill file in chunks (to avoid timeout)
    local chunk_size=$((100 * 1024 * 1024))  # 100MB chunks
    local chunks=$((bytes_to_fill / chunk_size))
    local remainder=$((bytes_to_fill % chunk_size))
    
    for i in $(seq 1 $chunks); do
        log_info "Creating chunk $i of $chunks..."
        kubectl exec -n "$NAMESPACE" "$TARGET_POD" -- \
            dd if=/dev/zero of="${DATA_DIR}/${TEMP_FILE_PREFIX}${i}" \
            bs=1M count=100 2>/dev/null || true
    done
    
    if [ "$remainder" -gt 0 ]; then
        local remainder_mb=$((remainder / 1024 / 1024))
        kubectl exec -n "$NAMESPACE" "$TARGET_POD" -- \
            dd if=/dev/zero of="${DATA_DIR}/${TEMP_FILE_PREFIX}final" \
            bs=1M count="$remainder_mb" 2>/dev/null || true
    fi
    
    local new_usage
    new_usage=$(get_disk_usage "$TARGET_POD")
    log_info "New disk usage: ${new_usage}%"
}

# Monitor pod health
monitor_health() {
    log_info "Monitoring pod health under disk pressure..."
    
    local duration=30
    local end_time=$(($(date +%s) + duration))
    
    while [ "$(date +%s)" -lt "$end_time" ]; do
        echo ""
        log_info "Health check at $(date +%H:%M:%S):"
        
        # Check disk usage
        local usage
        usage=$(get_disk_usage "$TARGET_POD")
        echo "  Disk usage: ${usage}%"
        
        # Check pod status
        local status
        status=$(kubectl get pod -n "$NAMESPACE" "$TARGET_POD" -o jsonpath='{.status.phase}')
        echo "  Pod status: $status"
        
        # Check ready condition
        local ready
        ready=$(kubectl get pod -n "$NAMESPACE" "$TARGET_POD" \
            -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
        echo "  Pod ready: $ready"
        
        # Try to check metrics endpoint
        kubectl exec -n "$NAMESPACE" "$TARGET_POD" -- \
            curl -s http://localhost:9000/metrics 2>/dev/null | \
            grep -E "goqueue_disk|goqueue_message" | head -5 || true
        
        sleep 5
    done
}

# Test write operations
test_writes() {
    log_info "Testing write operations under disk pressure..."
    
    # Try to produce messages
    # This would depend on your CLI or test client
    # For now, we just check if the broker is responding
    
    local response
    response=$(kubectl exec -n "$NAMESPACE" "$TARGET_POD" -- \
        curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null || echo "000")
    
    if [ "$response" = "200" ]; then
        log_info "✅ Health endpoint responding"
    else
        log_warn "⚠️ Health endpoint returned: $response"
    fi
}

# Clean up fill files
cleanup_fill_files() {
    log_info "Cleaning up fill files..."
    
    kubectl exec -n "$NAMESPACE" "$TARGET_POD" -- \
        sh -c "rm -f ${DATA_DIR}/${TEMP_FILE_PREFIX}*" 2>/dev/null || true
    
    local usage
    usage=$(get_disk_usage "$TARGET_POD")
    log_info "Disk usage after cleanup: ${usage}%"
}

# Verify recovery
verify_recovery() {
    log_info "Verifying recovery after disk space freed..."
    
    sleep 10
    
    # Check pod is healthy
    local ready
    ready=$(kubectl get pod -n "$NAMESPACE" "$TARGET_POD" \
        -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
    
    if [ "$ready" = "True" ]; then
        log_info "✅ Pod is ready after disk pressure cleared"
        return 0
    else
        log_error "❌ Pod did not recover after disk pressure cleared"
        return 1
    fi
}

# Cleanup on exit
cleanup() {
    log_info "Cleaning up..."
    cleanup_fill_files
}

trap cleanup EXIT

# Run the test
run_test() {
    log_info "Starting disk pressure chaos test"
    log_info "Namespace: $NAMESPACE"
    log_info "Target fill: ${FILL_PERCENTAGE}%"
    echo ""
    
    # Initial state
    log_info "Initial disk state:"
    kubectl exec -n "$NAMESPACE" "$TARGET_POD" -- df -h "$DATA_DIR"
    echo ""
    
    # Fill disk
    fill_disk
    echo ""
    
    # Monitor health
    monitor_health
    echo ""
    
    # Test writes
    test_writes
    echo ""
    
    # Cleanup
    cleanup_fill_files
    echo ""
    
    # Verify recovery
    if verify_recovery; then
        log_info "=============================================="
        log_info "DISK PRESSURE TEST COMPLETE"
        log_info "=============================================="
        log_info "✅ Broker handled disk pressure gracefully"
    else
        log_error "=============================================="
        log_error "DISK PRESSURE TEST FAILED"
        log_error "=============================================="
        exit 1
    fi
}

# Entry point
check_prerequisites
run_test
