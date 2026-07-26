<p align="center">
  <img src="docs/assets/logo.svg" alt="GoQueue Logo" width="180">
</p>

<h1 align="center">GoQueue</h1>

<p align="center">
  <a href="#the-problem">The Problem</a> •
  <a href="#the-solution">The Solution</a> •
  <a href="#unique-features">Unique Features</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#comparison">Comparison</a> •
  <a href="#architecture">Architecture</a>
</p>

---

A distributed message queue in Go: append-only log, its own coordination and
leader election with no ZooKeeper/etcd dependency, hierarchical timing wheel for
O(1) delayed delivery, priority lanes, and cooperative incremental rebalancing.
Single-cluster; see [Scope](#scope) for what it is not.

**If you only read four files:**

| File | Why |
|---|---|
| [`internal/broker/priority_index.go`](internal/broker/priority_index.go) | The default read path walks this index, not the log, and every lookup is a binary search — while the writer appended without keeping the slice offset-ordered. Two producers racing between offset allocation and indexing left `[…, 3, 2]`, and the search skipped 2 permanently: durably written, offset returned in a 200, unreadable forever. 74 of 500 offsets under 8 producers. [Details](#6--priority-lanes). |
| [`internal/broker/timer_wheel.go`](internal/broker/timer_wheel.go) | 4-level hierarchical wheel: 10ms to 7.76 days in 448 buckets where a flat wheel at the same range and precision needs 67,108,864. Draining a bucket has to release the wheel lock to run callbacks — and `container/list` nils an element's links on `Remove`, so a `Cancel()` in that window used to truncate the traversal and silently orphan the rest of the bucket. [Details](#2--native-delay--scheduled-messages). |
| [`internal/broker/cooperative_rebalance.go`](internal/broker/cooperative_rebalance.go) | Incremental partition handoff, so a consumer joining doesn't stop the group. Kafka's default is stop-the-world. |
| [`internal/cluster/replica_manager.go`](internal/cluster/replica_manager.go) | Leader election and ISR tracking. Shutdown is the interesting part: emitters aren't joinable, so the event channel is deliberately never closed. |

Three of those four are bugs whose defining property is that nothing reports
them — no panic, no error return, no failed assertion, and in two cases the race
detector is clean because every access really was under the right mutex.

Benchmarks are in [Performance](#performance), with the harness's limits stated
before its numbers — the sequential figure bounds a `urllib` client, not the broker.

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

**Design rationale:** Kafka gained incremental cooperative rebalancing in 2.4,
but it is opt-in — the default assignor still revokes every partition from every
member on each membership change. Here incremental handoff is the only path, so
there is no eager code path to fall back to and no configuration that turns
stop-the-world back on.

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

4 levels — 256×10ms, then 64 buckets each at 2.56s / 2.73min / 2.91h — covering
10ms to 7.76 days in 448 buckets. A flat wheel at the same precision and range
would need one bucket per tick: 671,088,640 ms / 10 ms = 67,108,864. Timers
cascade down a level at a time as their bucket's turn arrives, and each cascade
recomputes the remaining delay, so coarse placement at the top self-corrects
instead of accumulating error.

**Design rationale:** the hierarchical variant is the standard one — Linux
kernel timers, Netty's `HashedWheelTimer`, Kafka's purgatory — because it keeps
insert and cancel at O(1) across a range spanning six orders of magnitude,
which a heap does not and a flat wheel cannot afford the memory for.

**The part that was subtly wrong:** draining a bucket means walking a
`container/list` while releasing the wheel mutex to run each user callback.
`list.Remove` nils out an element's `next`/`prev`/`list` fields, and
`Element.Next()` returns nil once `list` is nil — so any element pointer held
across that unlock is a handle into memory another goroutine may unlink. A
`Cancel()` landing in the callback window on the *next* timer truncated the
traversal: the loop saw nil and stopped, silently abandoning every remaining
timer in the bucket until that level's cursor came around again — up to 7.76
days at level 3. The fix drains the bucket into a slice under the lock and
resolves fire-vs-reinsert before releasing it, so no list pointer survives the
unlock. See `drainBucketLocked` and the three regression tests in
[timer_wheel_cascade_test.go](internal/broker/timer_wheel_cascade_test.go),
which each fail against the previous implementation.

**A second one, in the placement arithmetic:** `tick()` advances the level-0
cursor and then drains the bucket it lands on, so the cursor always names the
bucket that was just processed. Bucket selection was
`(cursor + delayMs/10) % 256`, and integer division sends every delay under one
tick to offset 0 — into that just-drained bucket, which is not revisited until
the cursor completes a full 256-tick revolution. Anything with a sub-10ms
remaining delay therefore fired 2.56 seconds late: 256× the wheel's advertised
granularity, with no error and no lost message to point at it. The two paths
that hit it in practice are startup recovery, where every past-due timer has a
remaining delay of exactly 0, and any ordinary schedule where the persistent
delay-index write consumed the delay before `insertTimer` read the clock. The
offset is now clamped to at least one bucket — the wheel cannot be finer than
its 10ms tick, so firing one tick late is correct and firing a revolution late
is not. Tests in
[timer_wheel_subtick_test.go](internal/broker/timer_wheel_subtick_test.go).

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

**Design rationale:** per-message ACK and replay are usually presented as a
choice because queue systems delete on consume and log systems only track a
committed offset. They are not actually in tension — the log is the durable
record and stays append-only, while a separate in-memory visibility index
tracks which offsets are currently checked out and by whom. ACK mutates the
index, not the log, so replay from any earlier offset stays available.

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

**Design rationale:** the broker already knows every transition a message goes
through — it is the component performing them. Emitting that to an external
tracer means instrumenting each call site and then correlating spans back to a
message ID. Recording it against the message instead makes the history a
property of the message rather than of the instrumentation, so it survives with
no collector deployed. The cost is retention: trace records are bounded and
expire with the message.

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

**Design rationale:** an offset is the wrong unit for the question people
actually ask. "Replay everything after the incident started at 15:00" requires
first translating a timestamp into an offset per partition, which is a manual
lookup step in Kafka. Indexing the log by time as well as offset makes the
timestamp a first-class seek key, so a point-in-time query is one request rather
than a search followed by a request.

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

**The bug this cost:** the priority index is the *only* thing the default read
path walks — `Partition.Consume` asks
`PriorityIndex.GetNextAcrossPriorities(cursor)` for the next offset and reads
the log at whatever it returns. Every one of those lookups starts with a binary
search, and `AddMessage` appended without preserving offset order. Producers
allocate their offset inside `Log.Append`, under the log's lock, and index it
under a different lock, so two concurrent producers can allocate 2 and 3 in that
order and index 3 first. Binary search over `[..., 3, 2]` does not fail loudly:
it returns a plausible index, the scan runs forward from there, and offset 2 is
skipped. The reader then advances its cursor past 3, so 2 is never revisited.
The message was durably appended and its offset was returned to the publisher in
a 200 response, and no consumer could ever read it — no error, no log line, no
lost write at the storage layer. Under 8 concurrent producers and 500 messages,
74 offsets were unreachable.

It had been seen and misdiagnosed. An integration test asserting 100 published
messages were readable failed intermittently at 99 or 94, and carried the
comment *"the priority index may lag behind log appends under concurrency, so we
poll until all messages are visible"* with the poll deadline raised to 10
seconds. The index does not lag. Waiting longer cannot recover an entry that no
search will ever reach, which is why the deadline had to keep going up. The fix
keeps the slice sorted on insert — O(1) append in the ordinary in-order case, a
tail shift otherwise. Tests in
[priority_index_ordering_test.go](internal/broker/priority_index_ordering_test.go).

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

### What was measured

3-node EKS cluster, c5.xlarge per node (4 vCPU, 8 GB), ap-south-1, GoQueue
v0.4.1, February 2026. Load generator is a Python 3.12 `urllib` client running
as an in-cluster Job (1 vCPU limit), publishing ~10-byte payloads over the HTTP
API to a 6-partition topic. Publish path only — consume is not included.

| Producer mode | In flight | Throughput | Per-message cost |
|---|---|---|---|
| Sequential | 1 request | ~320 msgs/sec | ~3.1 ms |
| Concurrent | 8 threads | ~1,300 msgs/sec | ~770 µs |
| Batch | 100 msgs/request | ~30,000 msgs/sec | ~33 µs |
| Batch | 1,000 msgs/request | ~220,000 msgs/sec | ~4.5 µs |

### What the numbers actually bound

**The sequential row measures the harness, not the broker.** Python's `urllib`
does no connection pooling, so every message pays a fresh TCP handshake plus
HTTP parse, from a client pod capped at 1 vCPU. ~3.1 ms per round trip is what
that costs. It is a latency figure with a client-side bottleneck in it, and
reading it as "the broker tops out at 320 msgs/sec" is wrong.

**Concurrency hits a wall at 8 connections.** 4 threads → ~1,140/s, 8 → ~1,293/s,
16 → ~1,031/s, 32 → ~1,035/s. Adding connections past 8 makes it *worse*. That
shape points at per-request handling as the contended resource, which is
consistent with batching being the thing that fixes it.

**Batching is the only lever that moves the number by orders of magnitude.**
1,000-message batches reach ~220,000 msgs/sec — roughly 700x sequential — with
no change to the broker, only to how many messages share one request. The
per-message cost falls from ~3.1 ms to ~4.5 µs, which is the per-request
overhead being amortised away and nothing else.

Full runs, including the concurrency levels that regressed, are in
[docs/BENCHMARKS.md](docs/BENCHMARKS.md).

### Scope

This is a single-cluster implementation built to work through the mechanisms it
contains — the append-only log, the coordination and leader-election protocol,
the hierarchical timing wheel, cooperative rebalancing. It is not a Kafka
replacement and these numbers are not a claim against one. Kafka's I/O path has
had a decade of production tuning and a zero-copy fast path; a head-to-head
table would be measuring that history, not a design difference, and the two
numbers would not mean the same thing.

The measurements are here so the cost model is legible: where the time goes,
which lever moves it, and where the harness stops being the thing under test.

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

Every feature here started as a specific operational complaint, not a checklist item:

1. **Kafka's operational pain** drove zero-dependency clustering and cooperative rebalancing
2. **SQS's limitations** drove the hybrid ACK model with replay capability
3. **Missing delay support everywhere** drove native timer wheel integration
4. **Debugging nightmares** drove built-in message tracing

The parts worth reading are the ones where the naive implementation was wrong
and the code says why: the timer wheel's bucket-drain lock discipline
([internal/broker/timer_wheel.go](internal/broker/timer_wheel.go)), the
cooperative rebalance protocol
([internal/broker/cooperative_rebalance.go](internal/broker/cooperative_rebalance.go)),
and the replica manager's shutdown path
([internal/cluster/replica_manager.go](internal/cluster/replica_manager.go)),
where closing the event channel on Stop was a send-on-closed-channel panic
waiting for the right interleaving.

---

## License

MIT License - see [LICENSE](LICENSE) for details.

---

<p align="center">
  <em>Append-only log, own coordination layer, no external dependencies.</em>
</p>
