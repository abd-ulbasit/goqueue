#!/bin/sh
# Parallel benchmark script for GoQueue cluster
# Uses xargs for parallel execution

HOST="${1:-goqueue-0.goqueue-headless.goqueue.svc.cluster.local:8080}"
TOPIC="${2:-parallel-bench}"
COUNT="${3:-5000}"
PARALLEL="${4:-50}"

echo "=== GOQUEUE PARALLEL BENCHMARK ==="
echo "Host: $HOST"
echo "Topic: $TOPIC"
echo "Messages: $COUNT"
echo "Parallelism: $PARALLEL"
echo ""

# Create topic if not exists
echo "Creating topic..."
curl -s -X POST -H "Content-Type: application/json" \
  -d "{\"name\":\"$TOPIC\",\"partitions\":6}" \
  "http://$HOST/topics" 2>/dev/null || true
echo ""

# Generate message IDs
echo "=== PARALLEL WRITE TEST ==="
START=$(date +%s)
seq 1 $COUNT | xargs -P $PARALLEL -I {} curl -s -o /dev/null -X POST \
  -H "Content-Type: application/json" \
  -d "{\"payload\":\"parallel msg {}\"}" \
  "http://$HOST/topics/$TOPIC/messages"
END=$(date +%s)
DURATION=$((END - START))
if [ $DURATION -gt 0 ]; then
  RATE=$((COUNT / DURATION))
else
  DURATION=1
  RATE=$COUNT
fi
echo "Duration: ${DURATION}s"
echo "Write Rate: ~${RATE} msgs/sec (${PARALLEL} parallel workers)"
echo ""

# Get stats
echo "=== QUEUE STATS ==="
curl -s "http://$HOST/topics/$TOPIC"
echo ""

# Parallel read benchmark
echo "=== PARALLEL READ TEST ==="
START=$(date +%s)
seq 1 $COUNT | xargs -P $PARALLEL -I {} curl -s -o /dev/null \
  "http://$HOST/topics/$TOPIC/messages?partition=0"
END=$(date +%s)
DURATION=$((END - START))
if [ $DURATION -gt 0 ]; then
  RATE=$((COUNT / DURATION))
else
  DURATION=1
  RATE=$COUNT
fi
echo "Duration: ${DURATION}s"
echo "Read Rate: ~${RATE} msgs/sec (${PARALLEL} parallel workers)"
echo ""

echo "=== BENCHMARK COMPLETE ==="
