---
layout: default
title: Performance Benchmarks
description: GoQueue performance benchmarks and optimization guide
parent: Operations
nav_order: 5
---

# Performance Benchmarks

GoQueue delivers high throughput with minimal operational complexity. This page documents our benchmark methodology and results.

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

## Publish Throughput

All benchmarks run from **within the EKS cluster** to minimize network latency effects.

### Results Summary

| Mode | Configuration | Throughput | Speedup |
|------|---------------|------------|---------|
| **Sequential** | 1 message at a time | ~320 msgs/sec | baseline |
| **Concurrent** | 8 parallel threads | ~1,300 msgs/sec | 4× |
| **Batch (100)** | 100 messages/batch | ~30,000 msgs/sec | 100× |
| **Batch (1000)** | 1000 messages/batch | ~220,000 msgs/sec | 700× |

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

| System | Sequential | Batch (100) | Notes |
|--------|------------|-------------|-------|
| **GoQueue** | ~320/s | ~30,000/s | Single binary, no dependencies |
| Apache Kafka | ~100K/s | ~1M/s | Requires ZooKeeper/KRaft, JVM |
| RabbitMQ | ~10K/s | ~50K/s | Erlang-based, complex clustering |
| Amazon SQS | ~300/s | ~3,000/s | AWS managed, 10 msg batch limit |
| Redis Streams | ~100K/s | ~500K/s | In-memory only, no persistence |

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

## Hardware Recommendations

| Workload | Nodes | Instance Type | Expected Throughput |
|----------|-------|---------------|---------------------|
| Development | 1 | t3.small | ~5,000 msgs/sec |
| Low volume | 3 | t3.medium | ~20,000 msgs/sec |
| Medium volume | 3 | c5.xlarge | ~100,000 msgs/sec |
| High volume | 5 | c5.2xlarge | ~500,000 msgs/sec |

*Throughput assumes batch publishing. Sequential will be ~100× lower.*
