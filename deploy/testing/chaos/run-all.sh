#!/bin/bash
# =============================================================================
# GOQUEUE CHAOS TEST RUNNER
# =============================================================================
#
# PURPOSE: Run all chaos tests in sequence with reports.
#
# USAGE:
#   ./run-all.sh [namespace]
#
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${1:-goqueue}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_header() {
    echo ""
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
}

# Results
declare -A RESULTS
TESTS_PASSED=0
TESTS_FAILED=0

run_test() {
    local name=$1
    local script=$2
    shift 2
    local args=("$@")
    
    log_header "Running: $name"
    
    if "$SCRIPT_DIR/$script" "${args[@]}"; then
        RESULTS["$name"]="PASS"
        ((TESTS_PASSED++))
    else
        RESULTS["$name"]="FAIL"
        ((TESTS_FAILED++))
    fi
    
    # Cool down between tests
    echo ""
    echo "Cooling down for 30 seconds..."
    sleep 30
}

# Print summary
print_summary() {
    log_header "CHAOS TEST SUMMARY"
    
    for test in "${!RESULTS[@]}"; do
        local result="${RESULTS[$test]}"
        if [ "$result" = "PASS" ]; then
            echo -e "${GREEN}✅ $test: $result${NC}"
        else
            echo -e "${RED}❌ $test: $result${NC}"
        fi
    done
    
    echo ""
    echo "================================="
    echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Failed: ${RED}$TESTS_FAILED${NC}"
    echo "================================="
    
    if [ "$TESTS_FAILED" -gt 0 ]; then
        exit 1
    fi
}

# Check prerequisites
log_header "PRE-FLIGHT CHECKS"

echo "Checking cluster connectivity..."
if ! kubectl cluster-info &> /dev/null; then
    echo -e "${RED}Cannot connect to Kubernetes cluster${NC}"
    exit 1
fi

echo "Checking namespace $NAMESPACE..."
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo -e "${RED}Namespace $NAMESPACE does not exist${NC}"
    exit 1
fi

echo "Checking GoQueue pods..."
POD_COUNT=$(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/name=goqueue" --no-headers 2>/dev/null | wc -l)
echo "Found $POD_COUNT GoQueue pods"

if [ "$POD_COUNT" -lt 3 ]; then
    echo -e "${YELLOW}Warning: Recommended minimum 3 pods for chaos testing${NC}"
fi

echo ""
echo -e "${GREEN}Pre-flight checks passed${NC}"

# Run tests
run_test "Pod Kill" "pod-kill.sh" "$NAMESPACE"
run_test "Node Drain" "node-drain.sh" "$NAMESPACE"

# Only run network partition if Chaos Mesh is installed
if kubectl get crd networkchaos.chaos-mesh.org &> /dev/null; then
    run_test "Network Partition" "network-partition.sh" "$NAMESPACE" 30
else
    echo -e "${YELLOW}Skipping Network Partition test (Chaos Mesh not installed)${NC}"
    RESULTS["Network Partition"]="SKIP"
fi

run_test "Disk Pressure" "disk-pressure.sh" "$NAMESPACE" 75

# Print summary
print_summary
