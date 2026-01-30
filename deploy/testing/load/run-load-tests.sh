#!/bin/bash
# =============================================================================
# GOQUEUE LOAD TEST RUNNER
# =============================================================================
#
# PURPOSE: Run k6 load tests with different configurations
#
# USAGE:
#   ./run-load-tests.sh [test-type] [options]
#
# EXAMPLES:
#   ./run-load-tests.sh quick          # Quick smoke test
#   ./run-load-tests.sh standard       # Standard load test
#   ./run-load-tests.sh stress         # Full stress test
#   ./run-load-tests.sh custom 100 5m  # Custom VUs and duration
#
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOQUEUE_URL="${GOQUEUE_URL:-http://localhost:8080}"
TOPIC="${TOPIC:-load-test-$(date +%s)}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_header() {
    echo ""
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v k6 &> /dev/null; then
        log_error "k6 is not installed"
        log_info "Install with: brew install k6"
        exit 1
    fi

    # Check if GoQueue is reachable
    if ! curl -s -o /dev/null -w "%{http_code}" "$GOQUEUE_URL/health" | grep -q "200\|204"; then
        log_warn "GoQueue at $GOQUEUE_URL may not be reachable"
    fi

    log_info "Prerequisites OK"
}

# Quick smoke test
run_quick() {
    log_header "Running Quick Smoke Test"
    
    k6 run \
        --vus 10 \
        --duration 30s \
        --env GOQUEUE_URL="$GOQUEUE_URL" \
        --env TOPIC="$TOPIC" \
        "$SCRIPT_DIR/produce.js"
}

# Standard load test
run_standard() {
    log_header "Running Standard Load Test"
    
    # Run producers
    log_info "Starting producer load test..."
    k6 run \
        --env GOQUEUE_URL="$GOQUEUE_URL" \
        --env TOPIC="$TOPIC" \
        "$SCRIPT_DIR/produce.js" &
    
    PRODUCER_PID=$!
    
    sleep 10
    
    # Run consumers
    log_info "Starting consumer load test..."
    k6 run \
        --env GOQUEUE_URL="$GOQUEUE_URL" \
        --env TOPIC="$TOPIC" \
        --env GROUP="standard-test-group" \
        "$SCRIPT_DIR/consume.js" &
    
    CONSUMER_PID=$!
    
    # Wait for both
    wait $PRODUCER_PID
    wait $CONSUMER_PID
}

# Full stress test
run_stress() {
    log_header "Running Full Stress Test"
    
    k6 run \
        --env GOQUEUE_URL="$GOQUEUE_URL" \
        --env TOPIC="$TOPIC" \
        "$SCRIPT_DIR/stress.js"
}

# Custom test
run_custom() {
    local vus=${1:-50}
    local duration=${2:-2m}
    
    log_header "Running Custom Test (VUs: $vus, Duration: $duration)"
    
    k6 run \
        --vus "$vus" \
        --duration "$duration" \
        --env GOQUEUE_URL="$GOQUEUE_URL" \
        --env TOPIC="$TOPIC" \
        "$SCRIPT_DIR/produce.js"
}

# Print usage
usage() {
    echo "Usage: $0 [test-type] [options]"
    echo ""
    echo "Test types:"
    echo "  quick     - Quick smoke test (10 VUs, 30s)"
    echo "  standard  - Standard load test with producers and consumers"
    echo "  stress    - Full stress test (15 minutes)"
    echo "  custom    - Custom test (requires VUs and duration)"
    echo ""
    echo "Options:"
    echo "  --url URL     Set GoQueue URL (default: http://localhost:8080)"
    echo "  --topic NAME  Set topic name (default: auto-generated)"
    echo ""
    echo "Examples:"
    echo "  $0 quick"
    echo "  $0 standard --url http://goqueue:8080"
    echo "  $0 custom 100 5m"
}

# Parse arguments
TEST_TYPE="${1:-quick}"
shift || true

while [[ $# -gt 0 ]]; do
    case $1 in
        --url)
            GOQUEUE_URL="$2"
            shift 2
            ;;
        --topic)
            TOPIC="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            # Custom arguments
            break
            ;;
    esac
done

# Main
check_prerequisites

log_info "GoQueue URL: $GOQUEUE_URL"
log_info "Topic: $TOPIC"

case $TEST_TYPE in
    quick)
        run_quick
        ;;
    standard)
        run_standard
        ;;
    stress)
        run_stress
        ;;
    custom)
        run_custom "$@"
        ;;
    *)
        log_error "Unknown test type: $TEST_TYPE"
        usage
        exit 1
        ;;
esac

log_header "Load Test Complete"
