# GoQueue Development Progress

## Status: Phase 1 - Not Started

**Target Milestones**: 18
**Completed**: 0
**Current**: Milestone 1 (Storage Engine)

---

## Phase 1: Foundations (Milestones 1-4)

### Milestone 1: Storage Engine & Append-Only Log ⏳ CURRENT

**Goal:** Build the foundational append-only log that all message queues depend on.

**Learning Focus:**
- Why append-only logs are the foundation of Kafka, MySQL binlog, PostgreSQL WAL
- File I/O in Go (os.File, buffered I/O, fsync semantics)
- Binary encoding for compact wire formats
- Index structures for O(1) offset lookup
- Segment files for bounded file sizes

**Deliverables:**
- [ ] Message struct with binary encoding/decoding
- [ ] Append-only log writer with fsync modes
- [ ] Segment files with configurable size limits
- [ ] Sparse index for offset → file position
- [ ] Time index for timestamp → offset lookup
- [ ] Log reader (sequential and random access)
- [ ] Segment cleanup (retention by time/size)
- [ ] CRC32 checksums for corruption detection
- [ ] Benchmark: write throughput, read latency

**Key Concepts:**
```
Segment File Layout:
┌─────────────────────────────────────────────────────────────┐
│ Message 0 │ Message 1 │ Message 2 │ ... │ Message N │       │
└─────────────────────────────────────────────────────────────┘
                                                    ↑
                                              Active write position

Index File Layout (sparse):
┌────────────────────────────────────────┐
│ Offset 0    → Position 0               │
│ Offset 100  → Position 45678           │  (index every N bytes)
│ Offset 200  → Position 91234           │
└────────────────────────────────────────┘

Time Index Layout:
┌────────────────────────────────────────┐
│ Timestamp 1704067200 → Offset 0        │
│ Timestamp 1704070800 → Offset 523      │  (index every N seconds)
│ Timestamp 1704074400 → Offset 1247     │
└────────────────────────────────────────┘
```

**Tests:**
- [ ] Write 1M messages, verify all readable
- [ ] Segment rolls over at size limit
- [ ] Index allows sub-millisecond offset lookup
- [ ] Time index enables point-in-time queries
- [ ] Log survives process restart
- [ ] CRC detects single-bit corruption
- [ ] Retention cleanup removes old segments

---

### Milestone 2: Topics, Partitions & Producer API

**Goal:** Multi-partition topics with proper producer batching.

**Learning Focus:**
- Why partitions enable horizontal scaling
- Consistent hashing for partition assignment
- Producer batching for throughput vs latency trade-off
- Acknowledgment modes (fire-and-forget vs durable)

**Deliverables:**
- [ ] Topic abstraction (name → partitions mapping)
- [ ] Partition with dedicated log
- [ ] Partitioner interface (hash, round-robin, explicit)
- [ ] Consistent hash partitioner (murmur3)
- [ ] Producer with configurable batching
- [ ] Ack modes: none, leader, all
- [ ] Topic creation/deletion/listing API
- [ ] Partition count configuration
- [ ] HTTP API for publish
- [ ] Benchmark: throughput at various batch sizes

---

### Milestone 3: Consumer Groups & Offset Management

**Goal:** Multiple consumers sharing partition load with reliable offset tracking.

**Learning Focus:**
- Consumer group coordination problem
- Partition assignment strategies (range, round-robin, sticky)
- Offset commit patterns (auto vs manual)
- The rebalancing problem (and why Kafka's is painful)

**Deliverables:**
- [ ] Consumer group registry
- [ ] Partition assignment (range strategy first)
- [ ] Offset storage (file-based per group)
- [ ] Manual commit API
- [ ] Auto-commit with configurable interval
- [ ] Consumer heartbeat mechanism
- [ ] Long-polling message fetch
- [ ] Basic rebalancing on join/leave
- [ ] HTTP API for consumer operations
- [ ] Session timeout detection

---

### Milestone 4: Reliability - ACKs, Visibility & DLQ

**Goal:** At-least-once delivery with per-message acknowledgment.

**Learning Focus:**
- Delivery semantics (at-most-once, at-least-once, exactly-once)
- Visibility timeout pattern (SQS-style)
- Dead letter queue design
- Retry strategies with exponential backoff

**Deliverables:**
- [ ] Per-message ACK/NACK API
- [ ] Visibility timeout tracking
- [ ] In-flight message index
- [ ] Retry counter per message
- [ ] Dead letter queue routing
- [ ] DLQ topic auto-creation
- [ ] Backpressure (max unacked per consumer)
- [ ] Consumer lag calculation
- [ ] Message TTL (expire unprocessed)

---

## Phase 2: Advanced Features (Milestones 5-9)

### Milestone 5: Native Delay & Scheduled Messages ⭐ UNIQUE

**Goal:** First-class delayed message delivery.

**Learning Focus:**
- Timer wheel algorithm (Netty, Kafka purgatory)
- Hierarchical timing wheels for wide time ranges
- Persistent delay index design
- Trade-offs: memory vs disk for delay tracking

**Deliverables:**
- [ ] Hierarchical timer wheel implementation
- [ ] Delay index (persistent)
- [ ] Publish with delay parameter
- [ ] Publish with deliver-at timestamp
- [ ] Delay bucket optimization
- [ ] Timer tick goroutine
- [ ] Cancel delayed message
- [ ] Benchmark: 1M delayed messages

**Why This Matters:**
> "Timer wheels are used in Linux kernel scheduling, Netty, and Kafka's purgatory. I implemented the hierarchical variant for O(1) insert/delete across milliseconds to days."

---

### Milestone 6: Priority Lanes ⭐ UNIQUE

**Goal:** Fast-track high-priority messages within partitions.

**Learning Focus:**
- Priority queue data structures
- Fairness vs strict priority trade-offs
- Starvation prevention

**Deliverables:**
- [ ] Priority levels (high, normal, low)
- [ ] Per-partition priority queues
- [ ] Weighted fair queuing option
- [ ] Priority in message format
- [ ] Priority-aware consumer fetch
- [ ] Anti-starvation (low priority timeout)
- [ ] Priority metrics per lane

---

### Milestone 7: Message Tracing ⭐ UNIQUE

**Goal:** Track every message's journey through the system.

**Learning Focus:**
- Distributed tracing concepts
- Efficient trace storage
- Query patterns for traces

**Deliverables:**
- [ ] Trace ID generation
- [ ] Trace event types (published, replicated, delivered, acked)
- [ ] Trace storage (append-only trace log)
- [ ] Trace query API
- [ ] CLI: goqueue-cli trace <message-id>
- [ ] Trace retention policy
- [ ] Trace sampling for high throughput

---

### Milestone 8: Schema Registry

**Goal:** Message schema validation and evolution.

**Learning Focus:**
- Schema evolution compatibility (backward, forward, full)
- JSON Schema and Protocol Buffers
- Schema-on-read vs schema-on-write

**Deliverables:**
- [ ] Schema storage
- [ ] Schema registration API
- [ ] Compatibility checking
- [ ] Schema ID in message header
- [ ] Validation middleware
- [ ] JSON Schema support
- [ ] Protobuf support (optional)

---

### Milestone 9: Transactional Publish

**Goal:** Atomic multi-partition writes.

**Learning Focus:**
- Two-phase commit basics
- Transaction log design
- Idempotent producers

**Deliverables:**
- [ ] Producer ID assignment
- [ ] Sequence number tracking
- [ ] Transaction coordinator
- [ ] Begin/commit/abort API
- [ ] Idempotent deduplication
- [ ] Transaction timeout

---

## Phase 3: Distribution (Milestones 10-13)

### Milestone 10: Cluster Formation & Metadata

**Goal:** Multi-node cluster with shared metadata.

**Learning Focus:**
- Cluster membership protocols
- Metadata replication
- Failure detection

**Deliverables:**
- [ ] Node ID and addressing
- [ ] Peer discovery (static, DNS, multicast)
- [ ] Membership list with health
- [ ] Metadata store (embedded)
- [ ] Metadata replication
- [ ] Node failure detection
- [ ] Cluster bootstrap

---

### Milestone 11: Leader Election & Replication

**Goal:** Partition leaders with follower replication.

**Learning Focus:**
- Leader election algorithms
- Log replication (simplified Raft)
- In-sync replica (ISR) concept
- Consistency vs availability

**Deliverables:**
- [ ] Partition leader election
- [ ] Lease-based leadership
- [ ] Log replication protocol
- [ ] ISR management
- [ ] Min in-sync replicas config
- [ ] Leader failover
- [ ] Follower catch-up

---

### Milestone 12: Cooperative Rebalancing ⭐ UNIQUE

**Goal:** Zero-downtime consumer rebalancing.

**Learning Focus:**
- Kafka's stop-the-world problem
- Incremental cooperative protocol
- Partition handoff without stopping

**Deliverables:**
- [ ] Cooperative protocol implementation
- [ ] Incremental assignment
- [ ] Sticky assignment (minimize moves)
- [ ] Graceful partition handoff
- [ ] Rebalance metrics
- [ ] Rebalance timeout handling

**Why This Matters:**
> "Kafka's rebalancing stops all consumers even when one joins. I implemented Kafka's KIP-429 cooperative protocol as the default, making rebalances nearly invisible."

---

### Milestone 13: Online Partition Scaling

**Goal:** Add partitions without full rebalance.

**Learning Focus:**
- Online schema changes patterns
- Partition split strategies
- Key-based routing during splits

**Deliverables:**
- [ ] Add partition API
- [ ] Data redistribution (optional)
- [ ] Key routing during transition
- [ ] Minimal rebalance impact

---

## Phase 4: Operations (Milestones 14-18)

### Milestone 14: gRPC API & Go Client

**Goal:** High-performance gRPC API and idiomatic Go client.

**Deliverables:**
- [ ] Protocol buffer definitions
- [ ] gRPC server implementation
- [ ] Streaming produce/consume
- [ ] Go client library
- [ ] Connection pooling
- [ ] Retry with backoff

---

### Milestone 15: CLI Tool

**Goal:** Full-featured command-line tool.

**Deliverables:**
- [ ] Topic CRUD commands
- [ ] Produce/consume commands
- [ ] Consumer group management
- [ ] Offset reset commands
- [ ] Message trace lookup
- [ ] Cluster info commands
- [ ] Output formats (table, JSON, YAML)

---

### Milestone 16: Prometheus Metrics & Grafana

**Goal:** Full observability stack.

**Deliverables:**
- [ ] Prometheus metrics exporter
- [ ] Topic metrics (messages, bytes, partitions)
- [ ] Producer metrics (rate, latency, errors)
- [ ] Consumer metrics (lag, rate, rebalances)
- [ ] Cluster metrics (leaders, replicas, health)
- [ ] Grafana dashboard
- [ ] Alerting rules

---

### Milestone 17: Multi-Tenancy & Quotas

**Goal:** Resource isolation for multi-tenant deployments.

**Learning Focus:**
- Quota enforcement patterns
- Fair resource sharing
- Tenant isolation

**Deliverables:**
- [ ] Tenant ID in requests
- [ ] Per-tenant topic namespacing
- [ ] Producer quota (bytes/sec)
- [ ] Consumer quota (bytes/sec)
- [ ] Connection limits
- [ ] Storage limits

---

### Milestone 18: Kubernetes & Chaos Testing

**Goal:** Production-ready K8s deployment with chaos resilience.

**Deliverables:**
- [ ] Multi-stage Dockerfile
- [ ] docker-compose (3-node)
- [ ] Helm chart with StatefulSet
- [ ] PVC templates
- [ ] ServiceMonitor for Prometheus
- [ ] Chaos test scripts (node kill, network partition)
- [ ] Load test with k6
- [ ] Runbook documentation

---

## Key Metrics & Targets

| Metric | Target |
|--------|--------|
| Write throughput (single node) | 100K msg/s |
| Write throughput (3-node cluster) | 250K msg/s |
| p99 publish latency | < 10ms |
| p99 consume latency | < 5ms |
| Rebalance time (cooperative) | < 1 second |
| Leader failover time | < 5 seconds |
| Zero message loss | During any failover |

---

## Session Log

<!-- Track session-by-session progress -->

### Session 1
**Date:** TBD
**Focus:** Milestone 1 - Storage Engine
**Progress:**
- [ ] Started

---

## Design Decisions

<!-- Record key architectural choices and reasoning -->

---

## Questions to Research

<!-- Track questions that need deeper investigation -->
- [ ] Timer wheel vs heap for delay scheduling
- [ ] Raft vs simpler lease-based election
- [ ] mmap vs buffered I/O for log reads

---

## Key Learnings

<!-- Document important concepts discovered -->
