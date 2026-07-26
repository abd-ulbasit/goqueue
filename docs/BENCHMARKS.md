# GoQueue Performance Benchmarks

## Environment

- **Cluster**: 3-node Kubernetes (EKS)
- **Instance Type**: c5.xlarge (4 vCPU, 8GB RAM per node)
- **Region**: AWS ap-south-1 (Mumbai)
- **GoQueue Version**: v0.4.1
- **Test Date**: February 2026

## Harness

Everything below comes from `deploy/kubernetes/manual/publish-benchmark.yaml`,
which runs as a Job inside the cluster. Read the harness before reading the
numbers — several of its properties are load-bearing:

- **Client**: Python 3.12 `urllib.request`. **No connection pooling** — each
  publish opens a new TCP connection.
- **Client resources**: 1 vCPU limit, 512 MiB. The generator is not
  over-provisioned relative to the broker.
- **Transport**: HTTP API on port 8080 (not gRPC).
- **Payload**: ~10 bytes (`"seq-{i}"` and similar). Not representative of real
  message sizes; it isolates per-request overhead rather than serialization or
  disk bandwidth.
- **Topic**: 6 partitions, freshly created per run.
- **Scope**: publish path only. Consume, replication lag and end-to-end latency
  are not measured here.

Consequence: the low-concurrency rows are bounded by the client, not the
broker. They are useful as a per-request cost model and misleading as a
capacity ceiling.

## Publish Throughput

All tests run from within the EKS cluster to eliminate network latency effects.

### Summary Table

| Mode | Configuration | Throughput | Notes |
|------|--------------|------------|-------|
| Sequential | 1 msg at a time | **~320 msgs/sec** | Baseline |
| Concurrent | 8 threads | **~1,300 msgs/sec** | 4x sequential |
| Batch (100) | 100 msgs/batch | **~30,000 msgs/sec** | 100x sequential |
| Batch (1000) | 1000 msgs/batch | **~220,000 msgs/sec** | 700x sequential |

### Detailed Results

#### Sequential Publishing
```
  100 msgs: 0.34s →    293 msgs/sec
  500 msgs: 1.56s →    321 msgs/sec
 1000 msgs: 3.22s →    310 msgs/sec
 2000 msgs: 6.10s →    328 msgs/sec
```

#### Concurrent Publishing
```
 4 threads x 100 =   400 msgs: 0.35s →  1,140 msgs/sec
 8 threads x 100 =   800 msgs: 0.62s →  1,293 msgs/sec
16 threads x 100 = 1,600 msgs: 1.55s →  1,031 msgs/sec
32 threads x 100 = 3,200 msgs: 3.09s →  1,035 msgs/sec
```

#### Batch Publishing
```
50 batches x  10 =   500 msgs: 0.16s →   3,049 msgs/sec
50 batches x  50 = 2,500 msgs: 0.17s →  15,022 msgs/sec
50 batches x 100 = 5,000 msgs: 0.17s →  29,704 msgs/sec
50 batches x 200 =10,000 msgs: 0.17s →  59,266 msgs/sec
```

#### Large Batch Publishing
```
10 batches x  500 =  5,000 msgs: 0.04s → 131,347 msgs/sec
10 batches x 1000 = 10,000 msgs: 0.05s → 221,822 msgs/sec
```

## Key Insights

### 1. Batch Mode is Essential for High Throughput

Sequential publishing is limited by per-request overhead (TCP connection, HTTP parsing, etc.). Batch mode amortizes this overhead across many messages:

- **Sequential**: ~320 msgs/sec
- **Batch (100)**: ~30,000 msgs/sec (**100x improvement**)
- **Batch (1000)**: ~220,000 msgs/sec (**700x improvement**)

**Recommendation**: Always use batch publishing for high-throughput workloads.

### 2. Concurrent Client Scaling

Multiple concurrent publishers scale well up to 8 threads:

- 4 threads: ~1,140 msgs/sec (3.5x single thread)
- 8 threads: ~1,293 msgs/sec (4x single thread)
- 16+ threads: Diminishing returns due to contention

**Recommendation**: Use 4-8 concurrent connections per client application.

### 3. Network Latency Impact

When testing from outside the cluster (local machine → AWS Mumbai):
- Sequential: ~32 msgs/sec (10x slower due to network RTT)

When testing from within the cluster:
- Sequential: ~320 msgs/sec (minimal network latency)

**Recommendation**: Deploy producers close to GoQueue nodes to minimize latency.

## Comparison with Other Systems

Removed. An earlier revision of this document carried a table putting these
numbers next to published figures for Kafka, RabbitMQ, SQS and Redis Streams.
That table was not a measurement — the other rows were quoted from memory, not
produced on this hardware, with this harness, at this payload size, under this
durability configuration. Comparing a `urllib`-bound HTTP benchmark against
someone else's tuned producer benchmark tells you about the two harnesses, not
the two systems.

If a comparison is needed, it has to be run: same instance types, same payload,
same acks/fsync settings, same client library quality on both sides. Until that
exists, the honest statement is that GoQueue's cost model is documented above
and nothing here is a claim about anyone else's.

## Sizing

Only one configuration has been measured: 3 nodes of c5.xlarge, reaching
~220,000 msgs/sec with 1,000-message batches under the harness described above.

Everything else is untested. This document previously listed expected
throughput for t3.small, t3.medium and 5+ node c5.2xlarge clusters; those
figures were extrapolations presented as results and have been removed rather
than restated as guesses. Scaling is not linear in node count for a partitioned
log — placement, replication factor and partition count all matter — so
extrapolating from a single data point would be inventing numbers.

## Running the Benchmark

The benchmark can be run on any Kubernetes cluster:

```bash
# Deploy GoQueue cluster
kubectl apply -f deploy/kubernetes/manual/statefulset.yaml

# Run publish benchmark
kubectl apply -f deploy/kubernetes/manual/publish-benchmark.yaml

# View results
kubectl logs -n goqueue job/goqueue-publish-bench
```

## Notes

- These benchmarks measure publish throughput only
- Consume throughput varies based on consumer group configuration
- Results may vary based on network conditions, disk performance, and instance type
- Durability settings (fsync) can affect throughput significantly
