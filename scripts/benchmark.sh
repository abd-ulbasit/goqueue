#!/bin/sh
# Benchmark script for GoQueue cluster
# Run from inside the Kubernetes cluster

HOST="${1:-goqueue-0.goqueue-headless.goqueue.svc.cluster.local:8080}"
TOPIC="${2:-benchmark-test}"
COUNT="${3:-1000}"

echo "=== GOQUEUE BENCHMARK ==="
echo "Host: $HOST"
echo "Topic: $TOPIC"
echo "Messages: $COUNT"
echo ""

# Create topic if not exists
echo "Creating topic..."
curl -s -X POST -H "Content-Type: application/json" \
  -d "{\"name\":\"$TOPIC\",\"partitions\":6}" \
  "http://$HOST/topics" || true
echo ""

# Sequential write benchmark using Unix timestamp (seconds precision)
echo "=== SEQUENTIAL WRITE TEST ==="
START=$(date +%s)
i=0
while [ $i -lt $COUNT ]; do
  curl -s -o /dev/null -X POST \
    -H "Content-Type: application/json" \
    -d "{\"payload\":\"benchmark message $i\"}" \
    "http://$HOST/topics/$TOPIC/messages"
  i=$((i + 1))
done
END=$(date +%s)
DURATION=$((END - START))
if [ $DURATION -gt 0 ]; then
  RATE=$((COUNT / DURATION))
else
  DURATION=1
  RATE=$COUNT
fi
echo "Duration: ${DURATION}s"
echo "Write Rate: ~${RATE} msgs/sec"
echo ""

# Get stats
echo "=== QUEUE STATS ==="
curl -s "http://$HOST/topics/$TOPIC"
echo ""

# Read benchmark
echo "=== SEQUENTIAL READ TEST ==="
START=$(date +%s)
i=0
while [ $i -lt $COUNT ]; do
  curl -s -o /dev/null "http://$HOST/topics/$TOPIC/messages?partition=$((i % 6))"
  i=$((i + 1))
done
END=$(date +%s)
DURATION=$((END - START))
if [ $DURATION -gt 0 ]; then
  RATE=$((COUNT / DURATION))
else
  DURATION=1
  RATE=$COUNT
fi
echo "Duration: ${DURATION}s"
echo "Read Rate: ~${RATE} msgs/sec"
echo ""

echo "=== BENCHMARK COMPLETE ==="
