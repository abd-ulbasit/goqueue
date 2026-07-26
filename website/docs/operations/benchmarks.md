---
layout: default
title: Performance Benchmarks
description: GoQueue performance benchmarks and optimization guide
parent: Operations
nav_order: 5
---

# Performance Benchmarks

Every number on this page came from one harness on one cluster configuration.
The harness is described before the results because several of its properties
change what the numbers mean.

## Test Environment

### Cluster Configuration

| Component | Specification |
|-----------|---------------|
| **Platform** | AWS EKS (Kubernetes 1.29) |
| **Region** | ap-south-1 (Mumbai) |
| **Nodes** | 3 × c5.xlarge (4 vCPU, 8GB RAM) |
| **Storage** | EBS gp3 SSD (50GB per node) |
| **GoQueue Version** | v0.4.1 |
| **Test Date** | February 2026 |

### Deployment

```yaml
# 3-node StatefulSet with pod anti-affinity
replicas: 3
resources:
  requests: { cpu: "2", memory: "4Gi" }
  limits: { cpu: "3", memory: "5Gi" }
```

---

## Harness

Everything below comes from `deploy/kubernetes/manual/publish-benchmark.yaml`,
which runs as a Job inside the cluster. These properties are load-bearing:

| Property | Value |
|----------|-------|
| **Client** | Python 3.12 `urllib.request` — **no connection pooling**, so every publish opens a new TCP connection |
| **Client resources** | 1 vCPU limit, 512 MiB. The generator is not over-provisioned relative to the broker |
| **Transport** | HTTP API on port 8080 (not gRPC) |
| **Payload** | ~10 bytes. Isolates per-request overhead rather than serialization or disk bandwidth |
| **Topic** | 6 partitions, freshly created per run |
| **Scope** | Publish path only. Consume, replication lag and end-to-end latency are not measured |

Consequence: the low-concurrency rows are bounded by the client, not the broker.
They are useful as a per-request cost model and misleading as a capacity ceiling.

---

## Publish Throughput

All benchmarks run from **within the EKS cluster** to minimize network latency effects.

### Results Summary

| Mode | Configuration | Throughput | Per-message cost | Speedup |
|------|---------------|------------|------------------|---------|
| **Sequential** | 1 message at a time | ~320 msgs/sec | ~3.1 ms | baseline |
| **Concurrent** | 8 parallel threads | ~1,300 msgs/sec | ~770 µs | 4× |
| **Batch (100)** | 100 messages/batch | ~30,000 msgs/sec | ~33 µs | 100× |
| **Batch (1000)** | 1000 messages/batch | ~220,000 msgs/sec | ~4.5 µs | 700× |

The per-message cost column is the useful artifact. Its progression shows the
cost is per-*request*, not per-*message*, which is why batching buys ~700× and
concurrency buys ~4× and then goes backwards past 8 threads.

### Scaling with Batch Size

```
Batch Size    Throughput       Improvement vs Sequential
───────────────────────────────────────────────────────
     1        ~320/s           baseline
    10        ~3,000/s         10×
    50        ~15,000/s        50×
   100        ~30,000/s        100×
   200        ~60,000/s        200×
   500        ~130,000/s       400×
  1000        ~220,000/s       700×
```

### Concurrent Thread Scaling

```
Threads    Throughput    Speedup
────────────────────────────────
   1       ~320/s        baseline
   4       ~1,140/s      3.5×
   8       ~1,300/s      4× (optimal)
  16       ~1,030/s      diminishing returns
  32       ~1,030/s      contention
```

---

## Detailed Results

### Sequential Publish

```
Messages    Duration    Throughput
────────────────────────────────────
   100      0.34s       293 msgs/sec
   500      1.56s       321 msgs/sec
  1000      3.22s       310 msgs/sec
  2000      6.10s       328 msgs/sec
```

### Concurrent Publish (varying threads)

```
Threads × Messages    Total    Duration    Throughput
─────────────────────────────────────────────────────
  4 × 100             400      0.35s       1,140/s
  8 × 100             800      0.62s       1,293/s
 16 × 100            1600      1.55s       1,031/s
 32 × 100            3200      3.09s       1,035/s
```

### Batch Publish (varying batch size)

```
Batches × Size    Total     Duration    Throughput
──────────────────────────────────────────────────
50 ×  10          500       0.16s       3,049/s
50 ×  50         2500       0.17s       15,022/s
50 × 100         5000       0.17s       29,704/s
50 × 200        10000       0.17s       59,266/s
```

### Large Batch Stress Test

```
Batches × Size    Total     Duration    Throughput
──────────────────────────────────────────────────
10 ×  500         5000      0.04s       131,347/s
10 × 1000        10000      0.05s       221,822/s
```

---

## Comparison with Other Systems

Removed. An earlier revision of this page carried a table putting these numbers
next to published figures for Kafka, RabbitMQ, SQS and Redis Streams. That table
was not a measurement — the other rows were quoted from memory, not produced on
this hardware, with this harness, at this payload size, under this durability
configuration. Comparing a `urllib`-bound HTTP benchmark against someone else's
tuned producer benchmark tells you about the two harnesses, not the two systems.

If a comparison is needed, it has to be run: same instance types, same payload,
same acks/fsync settings, same client library quality on both sides. Until that
exists, the honest statement is that GoQueue's cost model is documented above
and nothing here is a claim about anyone else's.

### When to Choose GoQueue

✅ **Choose GoQueue when you need:**
- Simple deployment (single binary)
- No external dependencies (no ZK, no JVM)
- Kafka-like features (partitions, consumer groups, replay)
- SQS-like reliability (ACK/NACK, DLQ, visibility timeout)
- Delayed/scheduled messages (built-in timer wheel)

❌ **Consider Kafka/Pulsar when you need:**
- Multi-million msgs/sec throughput
- Geo-replication across data centers
- Tiered storage (hot/cold data)
- Enterprise support contracts

---

## Performance Optimization Guide

### For Publishers

1. **Always use batch publishing for high throughput**
   ```bash
   # Instead of 1000 individual requests...
   curl -X POST /topics/orders/messages -d '{"messages": [...]}'  # 100 msgs at once
   ```

2. **Use connection pooling**
   - HTTP/1.1 Keep-Alive is enabled by default
   - For gRPC, use a single long-lived connection

3. **Parallelize across topics**
   - Each topic can be written to concurrently
   - Use partitioning keys to spread load

### For Consumers

1. **Increase batch size**
   ```bash
   # Fetch 100 messages at once
   GET /groups/{group}/poll?max_messages=100
   ```

2. **Use multiple consumers per group**
   - 1 consumer per partition is optimal
   - Beyond that, consumers will be idle

3. **Commit offsets in batches**
   - Don't commit after every message
   - Commit periodically or after N messages

### For Operators

1. **CPU allocation**
   - 2-4 vCPU per GoQueue instance
   - More helps with concurrent connections

2. **Memory allocation**
   - 4-8 GB per instance
   - More helps with message caching

3. **Storage**
   - Use SSD (gp3 on AWS)
   - Enable async writes for throughput (default)
   - Enable sync writes for durability

---

## Running Your Own Benchmarks

### Deploy to AWS EKS

```bash
# One command to deploy everything
cd deploy/terraform/environments/dev
terraform init
terraform apply

# Configure kubectl
aws eks update-kubeconfig --region ap-south-1 --name goqueue-dev

# Get LoadBalancer URL
kubectl get svc -n goqueue goqueue-lb -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'
```

### Run In-Cluster Benchmark

```bash
# Apply benchmark job
kubectl apply -f deploy/kubernetes/manual/publish-benchmark.yaml

# Wait and get results
kubectl logs -n goqueue job/goqueue-publish-bench
```

### Cleanup

```bash
# Destroy all infrastructure
cd deploy/terraform/environments/dev
terraform destroy
```

---

## Sizing

Only one configuration has been measured: 3 nodes of c5.xlarge, reaching
~220,000 msgs/sec with 1,000-message batches under the harness described above.

Everything else is untested. This page previously listed expected throughput for
t3.small, t3.medium and 5+ node c5.2xlarge clusters; those figures were
extrapolations presented as results and have been removed rather than restated as
guesses. Scaling is not linear in node count for a partitioned log — placement,
replication factor and partition count all matter — so extrapolating from a
single data point would be inventing numbers.

Full runs, including the concurrency levels that regressed, are in
[docs/BENCHMARKS.md](https://github.com/abd-ulbasit/goqueue/blob/main/docs/BENCHMARKS.md).
