<p align="center">
  <img src="docs/assets/logo.svg" alt="GoQueue Logo" width="180">
</p>

<h1 align="center">GoQueue</h1>

<p align="center">
  <strong>The Message Queue That Doesn't Make You Choose</strong><br>
  <em>Kafka's durability + SQS's simplicity + Features neither has</em>
</p>

<p align="center">
  <a href="#the-problem">The Problem</a> •
  <a href="#the-solution">The Solution</a> •
  <a href="#unique-features">Unique Features</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#comparison">Comparison</a> •
  <a href="#architecture">Architecture</a>
</p>

---

## The Problem

Every distributed message queue forces painful trade-offs:

### 😤 Kafka Frustrations
| Pain Point | Impact |
|------------|--------|
| **Stop-the-world rebalancing** | All consumers pause when one joins/leaves. 30+ second latency spikes. |
| **No per-message acknowledgment** | Must commit offsets in batches. One poison message blocks the partition. |
| **No native delay/scheduled messages** | Need Kafka Streams, external scheduler, or hack with partitions. |
| **Operational complexity** | ZooKeeper (legacy) or KRaft. Cluster management is a full-time job. |
| **No built-in DLQ** | Have to build your own dead letter handling. |

### 😤 SQS Frustrations
| Pain Point | Impact |
|------------|--------|
| **No ordering guarantees** | FIFO queues have 300 msg/s limit. Standard queues = chaos. |
| **No replay capability** | Message gone after processing. Can't reprocess historical data. |
| **No consumer groups** | Every consumer sees every message or you manage routing yourself. |
| **Polling only** | No push model. Paying for empty receives. |
| **Vendor lock-in** | Your queue logic married to AWS forever. |

### 😤 RabbitMQ Frustrations
| Pain Point | Impact |
|------------|--------|
| **Not designed for log streaming** | No offset replay, no partitions, memory-bound queues. |
| **Complex routing overkill** | Exchange/binding/queue model when you just want pub/sub. |
| **No horizontal consumer scaling** | Competing consumers, but no partition-based parallelism. |

---

## The Solution

**GoQueue** combines the best of all worlds:

```
┌─────────────────────────────────────────────────────────────────────┐
│                           GoQueue                                   │
│                                                                     │
│  ┌─────────────────────┐    ┌─────────────────────┐                 │
│  │   Kafka's Log       │ +  │   SQS's Simplicity  │                 │
│  │                     │    │                     │                 │
│  │ • Append-only log   │    │ • Per-message ACK   │                 │
│  │ • Partition scaling │    │ • Visibility timeout│                 │
│  │ • Offset replay     │    │ • Dead letter queue │                 │
│  │ • Consumer groups   │    │ • Simple API        │                 │
│  └─────────────────────┘    └─────────────────────┘                 │
│                                                                     │
│                    + Features Neither Has                           │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ • Zero-downtime rebalancing (incremental, not stop-world)   │    │
│  │ • Native delay/scheduled messages (built-in timer wheel)    │    │
│  │ • Message-level tracing (track any message's journey)       │    │
│  │ • Point-in-time replay (not just offset, but timestamp)     │    │
│  │ • Priority lanes (fast-track within same topic)             │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Unique Features

### 1. 🔄 Zero-Downtime Rebalancing

**The Problem:** Kafka's rebalancing protocol stops ALL consumers in a group, even if only one partition is affected. This causes latency spikes and message processing delays.

**GoQueue's Solution:** Cooperative incremental rebalancing. Only affected partitions pause. Other consumers continue processing uninterrupted.

```
Kafka Rebalance (Stop-the-World):
─────────────────────────────────
Consumer A: [P0, P1, P2] ──STOP──────────────────────► [P0, P1] ──RESUME──►
Consumer B: (joining)    ──WAIT──────────────────────► [P2]     ──START───►
                              ▲                    ▲
                              └── ALL STOPPED ─────┘
                                  (30+ seconds)

GoQueue Rebalance (Cooperative):
────────────────────────────────
Consumer A: [P0, P1, P2] ───────[P0, P1]──────────────────────────────────►
                         (P2 handoff)  ↘
Consumer B: (joining)    ─────────────────[P2]────────────────────────────►
                                          ▲
                                          └── Only P2 paused briefly
                                              (< 1 second)
```

**Interview Talking Point:**
> "Kafka's rebalancing is the #1 operational pain point I've seen. Even Confluent added incremental cooperative rebalancing in 2.4, but it's opt-in and complex. I built it as the default behavior."

---

### 2. ⏰ Native Delay & Scheduled Messages

**The Problem:** Kafka has no built-in way to delay message delivery. Common workarounds:
- Kafka Streams with windowing (complex)
- External scheduler polling the queue (inefficient)
- Multiple topics with naming conventions (hacky)

**GoQueue's Solution:** First-class delay support with hierarchical timer wheel.

```go
// Publish with delay
client.Publish("orders", &Message{
    Key:   "order-123",
    Value: orderJSON,
    Delay: 30 * time.Minute,  // Deliver in 30 minutes
})

// Publish at specific time
client.Publish("reminders", &Message{
    Key:       "user-456",
    Value:     reminderJSON,
    DeliverAt: time.Date(2025, 12, 25, 9, 0, 0, 0, time.UTC),
})
```

**Use Cases:**
- Order cancellation after timeout
- Retry with exponential backoff
- Scheduled notifications
- Saga pattern timeouts
- Rate limiting with delay buckets

**Implementation:** Hierarchical timing wheel (O(1) insert/delete) + persistent delay index

**Interview Talking Point:**
> "Timer wheels are used in Linux kernel, Netty, and Kafka's purgatory. I implemented the hierarchical variant for O(1) operations across a wide time range - from milliseconds to days."

---

### 3. 🎯 Message-Level ACKs with Log Durability

**The Problem:**
- **Kafka:** Batch offset commit. One slow/failing message blocks entire partition.
- **SQS:** Per-message ACK, but no replay. Message is gone after deletion.

**GoQueue's Solution:** Both! Per-message acknowledgment with visibility timeout, but messages stay in the log for replay.

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Message Lifecycle                             │
│                                                                      │
│  Published ──► Visible ──► In-Flight ──► Acknowledged                │
│                  │             │              │                      │
│                  │             │              └──► Still in log!     │
│                  │             │                   (replay possible) │
│                  │             │                                     │
│                  │             └──► Visibility timeout expires       │
│                  │                  ↓                                │
│                  │             Redelivered (retry_count++)           │
│                  │                  ↓                                │
│                  │             max_retries exceeded                  │
│                  │                  ↓                                │
│                  │             Sent to DLQ                           │
│                  │                                                   │
│                  └──► Can replay from any offset or timestamp!       │
└──────────────────────────────────────────────────────────────────────┘
```

**Interview Talking Point:**
> "SQS users love per-message ACKs. Kafka users love replay. I asked: why not both? The log stores everything, but a visibility index tracks what's currently being processed."

---

### 4. 🔍 Built-in Message Tracing

**The Problem:** "Where did my message go?" requires external tracing systems (Jaeger, Zipkin) and careful instrumentation.

**GoQueue's Solution:** Every message has a lifecycle tracked internally.

```bash
$ goqueue-cli trace msg-abc123

Message: msg-abc123
Topic: orders
Partition: 2
Offset: 45678

Timeline:
├─ 2025-01-15T10:30:00.000Z  PUBLISHED     producer=order-service
├─ 2025-01-15T10:30:00.005Z  REPLICATED    nodes=[node-1, node-2]
├─ 2025-01-15T10:30:00.123Z  DELIVERED     consumer=processor-1
├─ 2025-01-15T10:30:30.456Z  REDELIVERED   reason=visibility_timeout
├─ 2025-01-15T10:30:35.789Z  DELIVERED     consumer=processor-2
├─ 2025-01-15T10:30:36.012Z  ACKNOWLEDGED  consumer=processor-2
└─ 2025-01-15T10:30:36.015Z  RETAINED      expires=2025-01-22T10:30:00Z

Delivery Attempts: 2
Total Latency: 36.012s (publish to final ack)
```

**Interview Talking Point:**
> "Observability is usually bolted on. I made it intrinsic. Every message carries its history, queryable without external tools."

---

### 5. ⏪ Point-in-Time Replay

**The Problem:** Kafka can replay from an offset, but:
- What offset corresponds to "yesterday at 3pm"?
- What if you need messages that were already deleted by retention?

**GoQueue's Solution:** Timestamp-indexed replay with optional snapshot retention.

```bash
# Replay all messages from a specific time
$ goqueue-cli consume orders --group replay-test --from-time "2025-01-14T15:00:00Z"

# Replay to a specific time (stop there)
$ goqueue-cli consume orders --group replay-test --from-time "2025-01-14T15:00:00Z" --to-time "2025-01-14T16:00:00Z"

# Replay even deleted messages (if snapshots enabled)
$ goqueue-cli consume orders --group audit --from-snapshot "2025-01-01" --include-deleted
```

**Interview Talking Point:**
> "Event sourcing needs deterministic replay. Kafka gives you offsets, but mapping timestamps to offsets is manual. I built a time-indexed log that makes point-in-time queries natural."

---

### 6. 🚀 Priority Lanes

**The Problem:** Important messages stuck behind bulk messages. Solutions:
- Separate topics (routing complexity)
- Partition hacks (lose ordering)

**GoQueue's Solution:** Lanes within partitions. Same ordering guarantees, but priority delivery.

```go
// High-priority payment notification
client.Publish("notifications", &Message{
    Key:      "user-123",
    Value:    paymentJSON,
    Priority: PriorityHigh,  // Delivered first
})

// Low-priority marketing email
client.Publish("notifications", &Message{
    Key:      "user-123",
    Value:    marketingJSON,
    Priority: PriorityLow,   // Delivered when high lane empty
})
```

**Within same partition**, messages from the same key maintain order, but high-priority messages are delivered before low-priority ones at partition level.

---

## Comparison

| Feature | GoQueue | Kafka | SQS | RabbitMQ |
|---------|---------|-------|-----|----------|
| **Append-only log** | ✅ | ✅ | ❌ | ❌ |
| **Offset replay** | ✅ | ✅ | ❌ | ❌ |
| **Consumer groups** | ✅ | ✅ | ❌ | Partial |
| **Partition scaling** | ✅ | ✅ | ❌ | ❌ |
| **Per-message ACK** | ✅ | ❌ | ✅ | ✅ |
| **Visibility timeout** | ✅ | ❌ | ✅ | ✅ |
| **Built-in DLQ** | ✅ | ❌ | ✅ | ✅ |
| **Native delay messages** | ✅ | ❌ | ❌ | ✅ (plugin) |
| **Zero-downtime rebalance** | ✅ | Partial | N/A | N/A |
| **Message tracing** | ✅ | ❌ | ❌ | ❌ |
| **Point-in-time replay** | ✅ | ❌ | ❌ | ❌ |
| **Priority lanes** | ✅ | ❌ | ❌ | ✅ |
| **No external deps** | ✅ | ❌ (ZK/KRaft) | N/A | ❌ (Erlang) |

---

## Performance

### Cluster Benchmark Results

Benchmarks run from **within** a 3-node EKS cluster (c5.xlarge instances, 4 vCPU, 8GB RAM each):

| Mode | Configuration | Throughput |
|------|--------------|------------|
| **Sequential** | Single message at a time | **~320 msgs/sec** |
| **Concurrent** | 8 parallel threads | **~1,300 msgs/sec** |
| **Batch** | 100 msgs/batch | **~30,000 msgs/sec** |
| **Large Batch** | 1000 msgs/batch | **~220,000 msgs/sec** |

### Scaling with Batch Size

```
Batch Size    Throughput       Improvement
─────────────────────────────────────────
     1        ~320/s           baseline
    10        ~3,000/s         ~10x
    50        ~15,000/s        ~50x
   100        ~30,000/s        ~100x
   200        ~60,000/s        ~200x
   500        ~130,000/s       ~400x
  1000        ~220,000/s       ~700x
```

**Key Insight**: Batch publishing amortizes per-request overhead, enabling massive throughput improvements.

### Remote Client Benchmarks

Benchmarks from remote client (network latency ~150ms RTT to ap-south-1):

| Test | Go Client | Python Client | TypeScript Client |
|------|-----------|---------------|-------------------|
| **Single message** | 6.3 msg/s | - | - |
| **Batch 100 × 1KB** | 112 msg/s | 331 msg/s | 284 msg/s |
| **8 Concurrent** | 152 msg/s | 567 msg/s | 571 msg/s |

> **Note:** Remote throughput is network-bound. Deploy producers close to GoQueue nodes for best performance.

### Performance Comparison

| System | Sequential | Batch (100) | Notes |
|--------|-----------|-------------|-------|
| **GoQueue** | ~320/s | ~30,000/s | Single binary, no dependencies |
| Kafka | ~100K/s | ~1M/s | Requires ZooKeeper/KRaft, JVM |
| RabbitMQ | ~10K/s | ~50K/s | Erlang-based |
| SQS | ~300/s | ~3,000/s | AWS managed, 10 msg batch limit |

For detailed benchmarks, see [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

---

## Quick Start

### Using Docker (Local Development)

```bash
# Single node
docker run -p 8080:8080 -p 9000:9000 \
  -v goqueue-data:/var/lib/goqueue \
  ghcr.io/abd-ulbasit/goqueue:latest

# Verify it's running
curl http://localhost:8080/health
```

### Deploy to Kubernetes (AWS EKS)

```bash
# One-command deployment (creates EKS cluster + GoQueue)
cd deploy
./deploy.sh deploy dev

# Or deploy to existing cluster
./deploy.sh goqueue dev

# Get the LoadBalancer URL
./deploy.sh url dev

# Run benchmarks
./deploy.sh benchmark dev
```

### Using Helm (Custom Cluster)

```bash
# Add GoQueue to your cluster
helm install goqueue ./deploy/kubernetes/helm/goqueue \
  --set replicaCount=3 \
  --set service.type=LoadBalancer
```

### Using Go

```bash
go install github.com/abd-ulbasit/goqueue/cmd/goqueue@latest
goqueue -config config.yaml
```

### Produce Messages

```bash
# HTTP API
curl -X POST http://localhost:8080/v1/topics/orders/publish \
  -H "Content-Type: application/json" \
  -d '{
    "key": "order-123",
    "value": "eyJvcmRlcl9pZCI6MTIzfQ==",
    "delay": "5m"
  }'

# CLI
echo '{"order_id": 123}' | goqueue-cli produce orders --key order-123

# Go client
client.Publish(ctx, "orders", &goqueue.Message{
    Key:   []byte("order-123"),
    Value: orderJSON,
    Delay: 5 * time.Minute,
})
```

### Consume Messages

```bash
# CLI
goqueue-cli consume orders --group processors

# Go client
consumer := client.NewConsumer("processors", []string{"orders"})
for msg := range consumer.Messages() {
    process(msg)
    msg.Ack()  // Per-message acknowledgment
}
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              GoQueue Cluster                                 │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                         Coordination Layer                             │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │  │
│  │  │   Metadata   │  │   Leader     │  │  Consumer    │                  │  │
│  │  │    Store     │  │  Election    │  │  Coordinator │                  │  │
│  │  │  (embedded)  │  │  (Raft-lite) │  │ (rebalancing)│                  │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                  │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                          Storage Layer                                 │  │
│  │                                                                        │  │
│  │  Topic: orders                                                         │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         │  │
│  │  │  Partition 0    │  │  Partition 1    │  │  Partition 2    │         │  │
│  │  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │         │  │
│  │  │  │ Log       │  │  │  │ Log       │  │  │  │ Log       │  │         │  │
│  │  │  │ Segments  │  │  │  │ Segments  │  │  │  │ Segments  │  │         │  │
│  │  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │         │  │
│  │  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │         │  │
│  │  │  │ Offset    │  │  │  │ Offset    │  │  │  │ Offset    │  │         │  │
│  │  │  │ Index     │  │  │  │ Index     │  │  │  │ Index     │  │         │  │
│  │  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │         │  │
│  │  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │         │  │
│  │  │  │ Time      │  │  │  │ Time      │  │  │  │ Time      │  │         │  │
│  │  │  │ Index     │  │  │  │ Index     │  │  │  │ Index     │  │         │  │
│  │  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │         │  │
│  │  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │         │  │
│  │  │  │ Delay     │  │  │  │ Delay     │  │  │  │ Delay     │  │         │  │
│  │  │  │ Index     │  │  │  │ Index     │  │  │  │ Index     │  │         │  │
│  │  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │         │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘         │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                        Delivery Layer                                  │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │  │
│  │  │ Timer Wheel  │  │  Visibility  │  │   Priority   │                  │  │
│  │  │ (delays)     │  │   Tracker    │  │    Router    │                  │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                  │  │
│  │                                                                        │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │  │
│  │  │  DLQ Router  │  │   Tracing    │  │   Metrics    │                  │  │
│  │  │              │  │   Recorder   │  │   Collector  │                  │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                  │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Configuration

```yaml
broker:
  nodeId: "node-1"
  dataDir: "/var/lib/goqueue"
  
  listeners:
    http: ":8080"
    grpc: ":9000"
    
defaults:
  topic:
    partitions: 6
    replicationFactor: 2
    retention:
      hours: 168          # 7 days
      bytes: 10737418240  # 10GB
      
  delivery:
    visibilityTimeout: 30s
    maxRetries: 3
    dlqEnabled: true
    
  delay:
    enabled: true
    maxDelay: 168h        # 7 days max delay
    
  priority:
    lanes: 3              # high, normal, low
```

See [config.example.yaml](config.example.yaml) for full reference.

---

## Roadmap

See [ROADMAP.md](ROADMAP.md) for detailed milestone breakdown.

| Phase | Milestones | Status |
|-------|------------|--------|
| **Phase 1: Foundations** | Storage, Topics, Consumers, Reliability | 🔜 Not Started |
| **Phase 2: Advanced Features** | Delays, Priority, Tracing, Schema | 📋 Planned |
| **Phase 3: Distribution** | Clustering, Replication, Rebalancing | 📋 Planned |
| **Phase 4: Operations** | APIs, CLI, Metrics, Kubernetes | 📋 Planned |

---

## Why Build This?

This project exists to deeply understand distributed systems by building one. But it's not just a learning exercise—it solves real frustrations:

1. **Kafka's operational pain** inspired zero-dependency clustering and cooperative rebalancing
2. **SQS's limitations** inspired the hybrid ACK model with replay capability  
3. **Missing delay support everywhere** inspired native timer wheel integration
4. **Debugging nightmares** inspired built-in message tracing

Every feature choice came from real pain points, not feature checklists.

---

## License

MIT License - see [LICENSE](LICENSE) for details.

---

<p align="center">
  <em>Building queues to understand queues.</em>
</p>
