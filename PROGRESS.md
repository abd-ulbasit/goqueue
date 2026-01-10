# GoQueue Development Progress

## Status: Phase 3.5 - In Progress

**Target Milestones**: 18
**Completed**: 13
**Current**: Milestone 14 (Time Index, Snapshots & Log Compaction) - Partial Complete

---

## Phase 1: Foundations (Milestones 1-4)

### Milestone 1: Storage Engine & Append-Only Log ✅ COMPLETE

**Goal:** Build the foundational append-only log that all message queues depend on.

**Learning Focus:**
- Why append-only logs are the foundation of Kafka, MySQL binlog, PostgreSQL WAL
- File I/O in Go (os.File, buffered I/O, fsync semantics)
- Binary encoding for compact wire formats
- Index structures for O(1) offset lookup
- Segment files for bounded file sizes

**Deliverables:**
- [x] Message struct with binary encoding/decoding
- [x] Append-only log writer with fsync modes
- [x] Segment files with configurable size limits
- [x] Sparse index for offset → file position
- [ ] Time index for timestamp → offset lookup (deferred)
- [x] Log reader (sequential and random access)
- [ ] Segment cleanup (retention by time/size) (deferred)
- [x] CRC32 checksums for corruption detection
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
- [x] Write 1M messages, verify all readable
- [x] Segment rolls over at size limit
- [x] Index allows sub-millisecond offset lookup
- [ ] Time index enables point-in-time queries (deferred)
- [x] Log survives process restart
- [x] CRC detects single-bit corruption
- [ ] Retention cleanup removes old segments (deferred)

---

### Milestone 2: Topics, Partitions & Producer API ✅ COMPLETE

**Goal:** Multi-partition topics with proper producer batching.

**Learning Focus:**
- Why partitions enable horizontal scaling
- Consistent hashing for partition assignment
- Producer batching for throughput vs latency trade-off
- Acknowledgment modes (fire-and-forget vs durable)

**Deliverables:**
- [x] Topic abstraction (name → partitions mapping)
- [x] Partition with dedicated log
- [x] Partitioner interface (hash, round-robin, explicit)
- [x] Consistent hash partitioner (murmur3)
- [x] Producer with configurable batching
- [x] Ack modes: none, leader, all
- [x] Topic creation/deletion/listing API
- [x] Partition count configuration
- [x] HTTP API for publish
- [ ] Benchmark: throughput at various batch sizes

**Implementation Notes:**
```
Murmur3 Hash Partitioner:
┌─────────────┐     murmur3     ┌─────────────┐
│ Message Key │ ───────────────► │ Hash (32b)  │ ──► partition = hash % N
└─────────────┘                 └─────────────┘

Producer Batching (client-side):
┌──────────────────────────────────────────────────────────────┐
│                    Producer Accumulator                       │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ Partition 0: [msg1, msg4, msg7] → flush when:           │ │
│  │ Partition 1: [msg2, msg5]           - BatchSize=100     │ │
│  │ Partition 2: [msg3, msg6, msg8]     - LingerMs=5ms      │ │
│  │                                     - BatchBytes=64KB   │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘

AckMode Enum (forward-compatible):
  AckNone   = 0  (fire-and-forget, no wait)
  AckLeader = 1  (wait for broker acknowledgment)
  AckAll    = 2  (wait for all replicas - same as Leader until M11)
```

**Tests:**
- [x] Hash partitioner consistent for same key
- [x] Hash partitioner distributes evenly
- [x] Producer batches by size trigger
- [x] Producer batches by linger trigger
- [x] Producer flush drains all batches
- [x] Concurrent sends are thread-safe
- [x] HTTP API creates/lists/deletes topics
- [x] HTTP API publishes and consumes messages

---

### Milestone 3: Consumer Groups & Offset Management ✅ COMPLETE

**Goal:** Multiple consumers sharing partition load with reliable offset tracking.

**Learning Focus:**
- Consumer group coordination problem
- Partition assignment strategies (range, round-robin, sticky)
- Offset commit patterns (auto vs manual)
- The rebalancing problem (and why Kafka's is painful)

**Deliverables:**
- [x] Consumer group registry
- [x] Partition assignment (range strategy first)
- [x] Offset storage (file-based per group)
- [x] Manual commit API
- [x] Auto-commit with configurable interval
- [x] Consumer heartbeat mechanism
- [x] Long-polling message fetch
- [x] Basic rebalancing on join/leave
- [x] HTTP API for consumer operations
- [x] Session timeout detection

---

### Milestone 4: Reliability - ACKs, Visibility & DLQ ✅ COMPLETE

**Goal:** At-least-once delivery with per-message acknowledgment.

**Learning Focus:**
- Delivery semantics (at-most-once, at-least-once, exactly-once)
- Visibility timeout pattern (SQS-style)
- Dead letter queue design
- Retry strategies with exponential backoff

**Deliverables:**
- [x] Per-message ACK/NACK API
- [x] Visibility timeout tracking
- [x] In-flight message index
- [x] Retry counter per message
- [x] Dead letter queue routing
- [x] DLQ topic auto-creation
- [x] Backpressure (max unacked per consumer)
- [x] Consumer lag calculation
- [x] Message TTL (expire unprocessed)

---

## Phase 2: Advanced Features (Milestones 5-9)

### Milestone 5: Native Delay & Scheduled Messages ✅ COMPLETE ⭐ UNIQUE

**Goal:** First-class delayed message delivery.

**Learning Focus:**
- Timer wheel algorithm (Netty, Kafka purgatory)
- Hierarchical timing wheels for wide time ranges
- Persistent delay index design
- Trade-offs: memory vs disk for delay tracking

**Deliverables:**
- [x] Hierarchical timer wheel implementation
- [x] Delay index (persistent)
- [x] Publish with delay parameter
- [x] Publish with deliver-at timestamp
- [x] Delay bucket optimization
- [x] Timer tick goroutine
- [x] Cancel delayed message
- [ ] Benchmark: 1M delayed messages

**Why This Matters:**
> "Timer wheels are used in Linux kernel scheduling, Netty, and Kafka's purgatory. I implemented the hierarchical variant for O(1) insert/delete across milliseconds to days."

---

### Milestone 6: Priority Lanes ✅ COMPLETE ⭐ UNIQUE

**Goal:** Fast-track high-priority messages within partitions.

**Learning Focus:**
- Priority queue data structures
- Fairness vs strict priority trade-offs
- Starvation prevention
- Weighted Fair Queuing (WFQ) using Deficit Round Robin

**Deliverables:**
- [x] Priority levels (Critical, High, Normal, Low, Background)
- [x] Per-partition priority queues
- [x] Weighted fair queuing (Deficit Round Robin)
- [x] Priority in message format (32-byte header V2)
- [x] Priority-aware consumer fetch
- [x] Anti-starvation (configurable timeout, 30s default)
- [x] Priority metrics per lane (per-priority-per-partition)
- [x] HTTP API with `/priority/stats` endpoint

**Implementation Notes:**
```
Message Header V2 (32 bytes - Breaking Change):
┌───────┬────────┬───────┬───────┬────────┬──────────┬──────────┬──────────┬─────────┬──────────┐
│ Magic │Version │ Flags │ CRC32 │ Offset │Timestamp │ Priority │ Reserved │ KeyLen  │ ValueLen │
│  2B   │  1B    │  1B   │  4B   │   8B   │    8B    │    1B    │    1B    │   2B    │    4B    │
└───────┴────────┴───────┴───────┴────────┴──────────┴──────────┴──────────┴─────────┴──────────┘

Priority Levels:
  PriorityCritical   = 0  (emergencies, circuit breakers)
  PriorityHigh       = 1  (paid users, real-time)
  PriorityNormal     = 2  (default)
  PriorityLow        = 3  (batch jobs, reports)
  PriorityBackground = 4  (analytics, cleanup)

Weighted Fair Queuing (Deficit Round Robin):
  ┌─────────────────────────────────────────────────────────┐
  │          Priority Scheduler (DRR Algorithm)             │
  │                                                         │
  │  Priority    Weight   Quantum   Share                   │
  │  ─────────────────────────────────────                  │
  │  Critical    50       50        50%                     │
  │  High        25       25        25%                     │
  │  Normal      15       15        15%                     │
  │  Low          7        7         7%                     │
  │  Background   3        3         3%                     │
  │                                                         │
  │  Deficit Round Robin:                                   │
  │  1. Add quantum to deficit counter                      │
  │  2. Dequeue while deficit > 0 and queue not empty       │
  │  3. Move to next priority                               │
  │  4. Reset deficit when queue empties                    │
  └─────────────────────────────────────────────────────────┘

Starvation Prevention:
  ┌─────────────────────────────────────────────────────────┐
  │ If lower priority hasn't been served for 30s:           │
  │   → Temporarily boost to Critical priority              │
  │   → Serve boosted message                               │
  │   → Reset starvation timer                              │
  │   → Restore original priority tracking                  │
  └─────────────────────────────────────────────────────────┘

Per-Priority-Per-Partition Metrics (PPPP):
  GET /priority/stats
  {
    "topics": {
      "orders": {
        "partitions": {
          "0": {
            "ready": [100, 50, 200, 30, 10],  // per priority
            "in_flight": [5, 2, 10, 1, 0],
            "enqueue_rate": [10.5, 5.2, 20.0, 3.0, 1.0],
            "dequeue_rate": [10.0, 5.0, 19.5, 2.8, 0.9]
          }
        }
      }
    }
  }
```

**Tests:**
- [x] Basic enqueue/dequeue operations
- [x] Priority ordering (higher always first when available)
- [x] WFQ distribution follows weights
- [x] Critical priority always served first
- [x] Starvation prevention triggers after timeout
- [x] Single priority mode (no starvation timeout)
- [x] DequeueByPriority for priority-specific consumption
- [x] DequeueN batch operations
- [x] EnqueueBatch for bulk inserts
- [x] Stats collection and reporting

**Key Learnings:**
- WFQ provides fairness while respecting priority
- Deficit Round Robin is O(1) for dequeue, simple to implement
- uint8 types in Go cause infinite loops when decremented below 0 (wrap to 255)
- Message format versioning critical for backward compatibility

---

### Milestone 7: Message Tracing ⭐ UNIQUE ✅ COMPLETE

**Goal:** Track every message's journey through the system.

**Learning Focus:**
- Distributed tracing concepts (W3C Trace Context standard)
- Efficient trace storage (ring buffer + file export)
- Query patterns for traces (by ID, time range, topic)

**Deliverables:**
- [x] Trace ID generation (128-bit UUID, W3C compliant)
- [x] Trace event types (publish.received/persisted, consume.fetched/acked/nacked/rejected)
- [x] Trace storage (ring buffer + JSONL file export + OTLP/Jaeger exporters)
- [x] Trace query API (GetTrace, GetRecentTraces, SearchTraces)
- [x] HTTP API endpoints (/traces, /traces/{id}, /traces/search, /traces/stats)
- [x] Trace retention policy (ring buffer capacity + file rotation)
- [x] Trace sampling for high throughput (configurable sampling rate)

**Implementation Details:**
- `tracer.go` (~1700 lines): Complete tracing infrastructure
- `tracer_test.go`: 22 comprehensive tests
- 34-byte message header with headers support for trace context propagation
- Broker integration: automatic span recording for all message lifecycle events
- Multiple export formats: Stdout, File (JSONL with rotation), OTLP, Jaeger

---

### Milestone 8: Schema Registry ✅ COMPLETE

**Goal:** Message schema validation and evolution.

**Learning Focus:**
- Schema evolution compatibility (backward, forward, full)
- JSON Schema validation
- Central schema management
- Confluent Schema Registry API patterns

**Deliverables:**
- [x] Schema storage (file-based JSON)
- [x] Schema registration API
- [x] Compatibility checking (BACKWARD, FORWARD, FULL, NONE)
- [x] Schema ID in message header (`schema-id`)
- [x] Validation middleware in broker
- [x] JSON Schema support (Draft 7)
- [ ] Protobuf support (noted for future)

**Implementation Details:**
- **Schema Registry Core** (`internal/broker/schema_registry.go` ~1200 lines)
  - Subject management (TopicNameStrategy: subject = topic name)
  - Version management (sequential per subject)
  - Global unique IDs across all subjects
  - Compatibility checking before registration
  - Per-subject validation enable/disable
  - File persistence with crash recovery
  
- **JSON Schema Validator** (`internal/broker/json_schema_validator.go` ~500 lines)
  - Pure Go implementation (no external deps)
  - Type validation: string, integer, number, boolean, array, object, null
  - Constraints: minimum/maximum, minLength/maxLength, pattern, enum
  - Object validation: properties, required, additionalProperties
  - Array validation: items, minItems, maxItems, uniqueItems
  - Local $ref support for schema composition

- **Storage Structure:**
  ```
  data/schemas/
  ├── orders/
  │   ├── v1.json
  │   ├── v2.json
  │   └── config.json
  ├── _ids.json       (global ID mapping)
  └── _config.json    (global config)
  ```

- **HTTP API** (Confluent-compatible, 15 endpoints):
  - Schema registration and retrieval
  - Subject management
  - Compatibility testing
  - Global and per-subject config

**Tests:** 30+ tests covering registration, versioning, compatibility, validation, persistence

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

### Milestone 10: Cluster Formation & Metadata ✅ COMPLETE

**Goal:** Multi-node cluster with shared metadata.

**Learning Focus:**
- Cluster membership protocols
- Metadata replication
- Failure detection

**Deliverables:**
- [x] Node ID and addressing
- [x] Peer discovery (static, DNS)
- [x] Membership list with health
- [x] Metadata store (embedded)
- [x] Metadata replication
- [x] Node failure detection
- [x] Cluster bootstrap
- [x] Controller election (lease-based)
- [x] Cluster HTTP API endpoints

**Implementation Details:**
- **Coordinator** (`internal/cluster/coordinator.go`): Orchestrates bootstrap, membership, and failure detection
- **Node Identity** (`internal/cluster/node.go`): Node ID, addresses, version, tags
- **Membership** (`internal/cluster/membership.go`): Node registry, event system, quorum checking
- **Failure Detector** (`internal/cluster/failure_detector.go`): Heartbeat-based with 3s/6s/9s thresholds
- **Controller Elector** (`internal/cluster/controller_elector.go`): Lease-based leadership with 15s lease
- **Metadata Store** (`internal/cluster/metadata_store.go`): Topic metadata, partition assignments
- **Cluster Server** (`internal/cluster/cluster_server.go`): HTTP API with 7 endpoints

---

### Milestone 11: Leader Election & Replication ✅ COMPLETE

**Goal:** Partition leaders with follower replication.

**Learning Focus:**
- Leader election algorithms (partition-level)
- Log replication (pull-based, like Kafka)
- In-sync replica (ISR) concept
- High watermark for read consistency
- Consistency vs availability trade-offs

**Deliverables:**
- [x] Partition leader election (separate from controller)
- [x] Lease-based leadership for partitions
- [x] Log replication protocol (pull-based)
- [x] ISR management (combined time + offset criteria)
- [x] Min in-sync replicas config
- [x] Leader failover (clean and unclean election)
- [x] Follower catch-up (snapshot + log)
- [x] High watermark tracking
- [x] Replication HTTP endpoints

**Implementation Details:**
- **Replication Types** (`internal/cluster/replication_types.go` ~590 lines):
  - ReplicaRole (Leader/Follower), ReplicaState, FetchRequest/Response
  - ISRUpdate with shrink/expand types
  - ReplicationConfig with tunable parameters
  - Snapshot metadata and error codes
  
- **Replica Manager** (`internal/cluster/replica_manager.go` ~780 lines):
  - Local replica state management (BecomeLeader/BecomeFollower)
  - LEO/HW tracking for each replica
  - ISR manager integration for leaders
  - Pending ACK management for AckAll writes
  - Event emission for state changes
  
- **ISR Manager** (`internal/cluster/isr_manager.go` ~350 lines):
  - Combined criteria: time-based (10s) AND offset-based (1000 messages)
  - Per-follower progress tracking
  - Automatic shrink/expand detection
  - High watermark calculation

- **Follower Fetcher** (`internal/cluster/follower_fetcher.go` ~470 lines):
  - Background fetch loop (500ms interval)
  - Exponential backoff on errors
  - Snapshot triggering when too far behind
  - Statistics collection
  
- **Snapshot Manager** (`internal/cluster/snapshot_manager.go` ~540 lines):
  - Tar+gzip snapshot creation
  - SHA256 checksum verification
  - Snapshot loading and extraction
  - Cleanup of old snapshots
  
- **Partition Leader Elector** (`internal/cluster/partition_leader_elector.go` ~300 lines):
  - Clean election (ISR-only candidates)
  - Unclean election (configurable, default disabled)
  - Preferred leader election
  - Offset-start round-robin for initial assignment
  
- **Replication Server** (`internal/cluster/replication_server.go` ~650 lines):
  - POST /cluster/fetch - Follower fetch requests
  - POST /cluster/snapshot/create - Snapshot creation
  - GET /cluster/snapshot/{topic}/{partition}/{offset} - Download
  - POST /cluster/isr - ISR updates
  - POST /cluster/leader-election - Election requests
  - GET /cluster/partition/{topic}/{partition}/info - Partition info
  
- **Replication Client** (`internal/cluster/replication_client.go` ~430 lines):
  - HTTP client for followers
  - Fetch, snapshot, ISR reporting
  - Controller address resolution
  
- **Broker Integration** (`internal/broker/replication_integration.go` ~500 lines):
  - ReplicationCoordinator wrapper
  - Replica initialization
  - ISR checker background loop
  - Message application from fetches

**Key Concepts Implemented:**

```
PULL-BASED REPLICATION (like Kafka):
┌──────────────┐                    ┌──────────────┐
│    Leader    │◄───── Fetch ───────│   Follower   │
│              │                    │              │
│  LEO: 1000   │─── FetchResponse ─►│  LEO: 950    │
│  HW:  900    │   (msgs 950-999)   │  HW:  900    │
└──────────────┘                    └──────────────┘
          │
          │ Every 500ms, follower pulls
          │ Leader tracks follower progress
          │ ISR = followers within lag threshold
          ▼
     High Watermark = min(LEO of all ISR)

ISR SHRINK CRITERIA (Combined):
┌────────────────────────────────────────────────────────┐
│ Remove from ISR if BOTH conditions are true:           │
│                                                        │
│   1. Last fetch > 10 seconds ago (ISRLagTimeMaxMs)     │
│   2. Offset lag > 1000 messages (ISRLagMaxMessages)    │
│                                                        │
│ This prevents:                                         │
│   - Flapping ISR during bursty workloads               │
│   - Slow followers dragging down the group             │
└────────────────────────────────────────────────────────┘

SNAPSHOT-BASED CATCH-UP:
┌────────────────────────────────────────────────────────┐
│ If follower is > 10,000 messages behind:               │
│                                                        │
│   1. Request snapshot from leader                      │
│   2. Download tar.gz of log segments                   │
│   3. Verify SHA256 checksum                            │
│   4. Extract and replace local log                     │
│   5. Resume normal fetching                            │
│                                                        │
│ Much faster than fetching 10K+ messages one by one     │
└────────────────────────────────────────────────────────┘
```

**Configuration:**
```go
ReplicationConfig{
    FetchIntervalMs:           500,          // Follower fetch frequency
    FetchMaxBytes:             1024*1024,    // 1MB per fetch
    FetchTimeoutMs:            5000,         // HTTP timeout
    ISRLagTimeMaxMs:           10000,        // 10s time threshold
    ISRLagMaxMessages:         1000,         // Offset threshold
    MinInSyncReplicas:         1,            // Min ISR for writes
    UncleanLeaderElection:     false,        // Prefer consistency
    LeaderFollowerReadEnabled: false,        // Read from leader only
    SnapshotEnabled:           true,         // Fast catch-up
    SnapshotThresholdMessages: 10000,        // Trigger snapshot threshold
    AckTimeoutMs:              5000,         // AckAll wait time
}
```

---

### Milestone 12: Cooperative Rebalancing ✅ COMPLETE ⭐ UNIQUE

**Goal:** Zero-downtime consumer rebalancing using Kafka's KIP-429 cooperative protocol.

**Learning Focus:**
- Kafka's stop-the-world problem (why traditional rebalancing stops all consumers)
- Incremental cooperative protocol (two-phase revoke-then-assign)
- Sticky partition assignment (minimize partition movement)
- Partition handoff without stopping processing

**Deliverables:**
- [x] Core cooperative rebalancing types and state machine
- [x] Sticky partition assignor (minimizes partition movement)
- [x] Range and round-robin assignors
- [x] Two-phase rebalance protocol (revoke → assign)
- [x] Visibility timeout for revocation acknowledgment
- [x] Rebalance metrics (duration, partition moves, timeouts)
- [x] HTTP API for cooperative operations
- [x] Consumer group integration with cooperative mode
- [x] Assignment diff calculation
- [x] Graceful partition handoff

**Implementation Details:**
```
COOPERATIVE REBALANCE FLOW (KIP-429):

Traditional (Stop-the-World):
┌─────────────────────────────────────────────────────────────┐
│  Consumer-1: ████████████░░░░░░░░░░░░░░░████████████████    │
│  Consumer-2: ████████████░░░░░░░░░░░░░░░████████████████    │
│  Consumer-3:              NEW JOINS HERE                    │
│              ├─ ALL STOP ─┤             ├── RESUME ──────   │
│              (rebalance window - all processing halted)     │
└─────────────────────────────────────────────────────────────┘

Cooperative (Incremental):
┌─────────────────────────────────────────────────────────────┐
│  Consumer-1: ████████████│revoke│████████████████████████   │
│  Consumer-2: ██████████████████████████████████████████████ │
│  Consumer-3:                    │assign│████████████████    │
│              └── Only affected partitions stop briefly ──┘  │
└─────────────────────────────────────────────────────────────┘

STATE MACHINE:
  pending_revoke → pending_assign → complete
       ↑                              │
       └──────── (new trigger) ───────┘

STICKY ASSIGNMENT:
  - MaxImbalance: Allows 1 partition imbalance before forcing moves
  - Preserves existing assignments when members join/leave
  - Only moves partitions when absolutely necessary
```

**Key Files:**
- `cooperative_rebalance.go` - Core types, state machine, metrics
- `sticky_assignor.go` - Partition assignment strategies
- `cooperative_rebalancer.go` - Rebalance orchestration
- `cooperative_group.go` - Consumer group integration
- `cooperative_api.go` - HTTP API handlers

**Tests:**
- [x] Sticky assignor preserves assignments on member join
- [x] Range and round-robin assignors distribute evenly
- [x] Assignment diff calculation
- [x] Rebalancer state machine transitions
- [x] Metrics recording
- [x] Concurrent group rebalances
- [x] Generation tracking

**Why This Matters:**
> "Kafka's traditional rebalancing stops ALL consumers when ONE joins - a major pain point. I implemented Kafka's KIP-429 cooperative protocol, making rebalances nearly invisible. Consumers continue processing unaffected partitions during rebalance."

---

### Milestone 13: Online Partition Scaling

**Goal:** Add partitions without full rebalance.

**Learning Focus:**
- Online schema changes patterns
- Partition split strategies
- Key-based routing during splits

**Deliverables:**
- [x] Add partition API ✅
- [x] Partition reassignment with throttling ✅
- [x] Coordinator on internal topics ✅
- [x] Consumer group notification ✅

**Status:** ✅ COMPLETE

---

## Phase 3.5: Distribution Hardening

### Milestone 14: Time Index, Snapshots & Log Compaction ⭐

**Goal:** Optimize internal topic storage and enable time-based message queries.

**Learning Focus:**
- Time-based indices for efficient replay
- Snapshot strategies for fast recovery
- Log compaction for space efficiency
- Tombstone handling

**Deliverables:**
- [x] Time Index (Binary format, 4KB granularity, O(log n) lookup) ✅
  - `TimeIndex` with MaybeAppend, Lookup, LookupRange methods
  - Segment integration: ReadFromTimestamp, ReadTimeRange
  - Corruption recovery with automatic rebuild
  - Test coverage: 9 tests, all passing
- [x] Coordinator Snapshots (Binary format, CRC32 validation) ✅
  - SnapshotWriter, SnapshotReader, SnapshotManager
  - Trigger: 10K records OR 5 minutes
  - Keep last 3 snapshots, auto-cleanup
  - Test coverage: 9 tests, all passing
- [ ] Log Compaction (Infrastructure created, needs Log API integration) 🔄
  - Copy-on-compact strategy documented
  - Dirty ratio trigger (50% threshold)
  - Tombstone retention (24 hours)
  - Needs refactoring to work with Log abstraction

**Key Concepts:**

**Time Index:**
```
┌──────────────────────────────────────────────────────────────┐
│                   TIME INDEX (.timeindex)                    │
│                                                              │
│  Entry: 16 bytes (8B timestamp + 8B offset)                  │
│  Granularity: 4KB (same as offset index)                     │
│                                                              │
│  Timestamp    Offset       Timestamp    Offset               │
│  1640000000   0           1640000060   1000                  │
│                                                              │
│  Lookup: O(log n) binary search vs O(n) full scan            │
│  Use case: "Replay from 2 hours ago"                         │
└──────────────────────────────────────────────────────────────┘
```

**Coordinator Snapshots:**
```
┌──────────────────────────────────────────────────────────────┐
│                   SNAPSHOT LIFECYCLE                         │
│                                                              │
│  WITHOUT: Replay 1M records from offset 0 (~5 min)           │
│  WITH: Load snapshot @ 990K + replay 10K records (~3 sec)    │
│                                                              │
│  SPEEDUP: 100x faster recovery                               │
│                                                              │
│  Format: snapshot-{type}-{offset}-{timestamp}.bin            │
│  Header: 32 bytes (magic, version, type, CRC, offsets)       │
│  Entries: Type + KeyLen + Key + ValueLen + Value             │
│                                                              │
│  Trigger: 10K records OR 5 minutes (whichever first)         │
│  Cleanup: Keep last 3 snapshots                              │
└──────────────────────────────────────────────────────────────┘
```

**Log Compaction (Planned):**
```
┌──────────────────────────────────────────────────────────────┐
│                 COPY-ON-COMPACT STRATEGY                     │
│                                                              │
│  BEFORE: 1M records, 10K unique keys (990K duplicates)       │
│  AFTER:  10K records (one per key, 99% reduction)            │
│                                                              │
│  Strategy: Last value wins per key                           │
│  Trigger: Dirty ratio >= 50% (half are duplicates)           │
│  Tombstones: Null value = delete after 24h retention         │
│                                                              │
│  Use case: __consumer_offsets, __transaction_state           │
└──────────────────────────────────────────────────────────────┘
```

**Key Files:**
- `internal/storage/time_index.go` - Time index implementation
- `internal/storage/time_index_test.go` - Time index tests  
- `internal/storage/segment.go` - Updated with time index integration
- `internal/broker/coordinator_snapshot.go` - Snapshot infrastructure
- `internal/broker/coordinator_snapshot_test.go` - Snapshot tests
- `internal/broker/compactor.go` - Compaction (needs Log API work)
- `docs/ARCHITECTURE.md` - Updated with M14 documentation

**Why This Matters:**
> "Time-based queries enable debugging and replay use cases ('show me messages from last 2 hours'). Snapshots accelerate coordinator recovery from minutes to seconds. Log compaction keeps internal topics bounded."

**Status:** ⭐ PARTIAL COMPLETE (Time Index ✅, Snapshots ✅, Compaction 🔄)

---

## Phase 4: Operations (Milestones 15-18)

### Milestone 15: gRPC API & Go Client

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

### Milestone 19: Final Review & Documentation with Examples and Comparison to Alternatives

**Goal:** Comprehensive documentation and comparison to existing solutions.
**Deliverables:**
- [ ] User guide with examples
- [ ] Architecture overview
- [ ] API reference
- [ ] Performance benchmarks
- [ ] Comparison with Kafka, RabbitMQ, NATS, (any other alteirnatives)

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

 // TODO: read this in kafka's Paper .. can we do similar optimizations?
In addition we optimize the network access for consumers. Kafka
is a multi-subscriber system and a single message may be
consumed multiple times by different consumer applications. A
typical approach to sending bytes from a local file to a remote
socket involves the following steps: (1) read data from the storage
media to the page cache in an OS, (2) copy data in the page cache
to an application buffer, (3) copy application buffer to another
kernel buffer, (4) send the kernel buffer to the socket. This
includes 4 data copying and 2 system calls. On Linux and other
Unix operating systems, there exists a sendfile API [5] that can
directly transfer bytes from a file channel to a socket channel.
This typically avoids 2 of the copies and 1 system call introduced
in steps (2) and (3). Kafka exploits the sendfile API to efficiently
deliver bytes in a log segment file from a broker to a consumer