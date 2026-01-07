# GoQueue - Progress

## Overall Status

**Phase**: 2 of 4
**Milestones**: 9/18 complete
**Tests**: 220+ passing (storage: 50, broker: 145+, api: 24)
**Started**: Session 1

## Phase Progress

### Phase 1: Foundations (4/4) ✅
- [x] Milestone 1: Storage Engine & Append-Only Log ✅
- [x] Milestone 2: Topics, Partitions & Producer API ✅
- [x] Milestone 3: Consumer Groups & Offset Management ✅
- [x] Milestone 4: Reliability - ACKs, Visibility & DLQ ✅

### Phase 2: Advanced Features (5/5) ✅
- [x] Milestone 5: Native Delay & Scheduled Messages ✅
- [x] Milestone 6: Priority Lanes ✅ ⭐
- [x] Milestone 7: Message Tracing ✅ ⭐
- [x] Milestone 8: Schema Registry ✅
- [x] Milestone 9: Transactional Publish ✅ ⭐

### Phase 3: Distribution (0/4)
- [ ] Milestone 10: Cluster Formation & Metadata
- [ ] Milestone 11: Leader Election & Replication
- [ ] Milestone 12: Cooperative Rebalancing ⭐
- [ ] Milestone 13: Online Partition Scaling

### Phase 4: Operations (0/6)
- [ ] Milestone 14: gRPC API & Go Client (js/ts as well if time)
- [ ] Milestone 15: CLI Tool
- [ ] Milestone 16: Prometheus Metrics & Grafana
- [ ] Milestone 17: Multi-Tenancy & Quotas
- [ ] Milestone 18: Kubernetes & Chaos Testing
- [ ] Milestone 19: Final Review & Documentation with Examples and Comparison to Alternatives

## What Works

### Milestone 1 - Storage Engine ✅
- **Binary message encoding** with CRC32 Castagnoli checksums
- **Append-only log** with automatic segment rollover at 64MB
- **Segment files** (.log) with sealed/active states
- **Sparse index files** (.index) with 4KB granularity
- **Log reader** with offset-based consumption
- **Partition abstraction** wrapping the log
- **Topic management** with single partition support
- **Broker API** for publish/consume operations
- **Demo application** at cmd/goqueue/main.go

### Milestone 2 - Topics, Partitions & Producer API ✅
- **Murmur3 hash partitioner** for consistent key-based routing
- **Round-robin partitioner** for nil keys (even distribution)
- **Manual partitioner** for explicit partition selection
- **Multi-partition topics** with default 3 partitions
- **Client-side Producer** with background accumulator goroutine
- **Three batch triggers**: size (100), linger (5ms), bytes (64KB)
- **AckMode enum**: None/Leader/All (Leader/All same until M11)
- **HTTP REST API** with full CRUD for topics and publish/consume
- **Priority draining** in accumulator to prevent race conditions

### Milestone 3 - Consumer Groups & Offset Management ✅
- **Consumer group membership** with heartbeat-based session management
- **Range partition assignment** (Kafka-compatible strategy)
- **Generation ID tracking** for zombie consumer protection
- **Stop-the-world rebalancing** (eager protocol)
- **File-based offset storage** with JSON persistence
- **Auto-commit support** (5s interval) with manual commit API
- **Group coordinator** managing all consumer groups centrally
- **Session monitoring** with 30s timeout, 3s heartbeat interval
- **Chi router HTTP API** with middleware logging
- **Long-polling** for message consumption (30s default timeout)

### Milestone 4 - Reliability: ACKs, Visibility & DLQ ✅
- **Hybrid ACK model** - Per-message ACK combined with offset-based commits (best of SQS + Kafka)
- **Visibility timeout** - 30s default, configurable per-message, with heap-based tracking
- **Receipt handles** - Unique per-delivery (`topic:partition:offset:deliveryCount:nonce`)
- **Dead Letter Queue** - Per-topic DLQ with `.dlq` suffix, auto-creation, metadata preservation
- **Retry with exponential backoff** - Base 1s, multiplier 2x, max 60s
- **Max retries** - 3 attempts before DLQ routing
- **Extend visibility API** - For long-running processing tasks
- **ACK/NACK/Reject semantics**:
  - ACK: Success, message deleted, offset may advance
  - NACK: Transient failure, retry with backoff
  - Reject: Permanent failure, immediate DLQ
- **HTTP API endpoints**:
  - `POST /messages/ack` - Acknowledge successful processing
  - `POST /messages/nack` - Signal retry needed
  - `POST /messages/reject` - Send to DLQ (poison message)
  - `POST /messages/visibility` - Extend visibility timeout
  - `GET /reliability/stats` - ACK manager, visibility tracker, DLQ stats
- **Files created**:
  - `internal/broker/inflight.go` - InFlightMessage, ReceiptHandle, DLQMessage, ReliabilityConfig
  - `internal/broker/visibility_tracker.go` - Min-heap based timeout tracking
  - `internal/broker/ack_manager.go` - Per-message ACK state, retry queue
  - `internal/broker/dlq.go` - DLQ routing with auto-topic creation

### Milestone 5 - Native Delay & Scheduled Messages ✅
- **Hierarchical Timer Wheel** - 4-level wheel for O(1) timer operations
  - Level 0: 256 buckets × 10ms = 2.56 seconds
  - Level 1: 64 buckets × 2.56s = 2.73 minutes  
  - Level 2: 64 buckets × 2.73m = 2.91 hours
  - Level 3: 64 buckets × 2.91h = 7.76 days (max delay)
- **Delay Index** - Persistent storage for crash recovery
  - Binary format: 16-byte header + 32-byte entries
  - States: PENDING, DELIVERED, CANCELLED, EXPIRED
  - Per-topic index files in data/delay/{topic}/
- **Scheduler** - Coordinator connecting timer wheel, delay index, and broker
  - Schedule(topic, partition, offset, delay) - relative delay
  - ScheduleAt(topic, partition, offset, deliverAt) - absolute time
  - Cancel(topic, partition, offset) - cancel pending delivery
- **Broker Integration**:
  - `PublishWithDelay(topic, key, value, delay)` - relative delay
  - `PublishAt(topic, key, value, deliverAt)` - absolute timestamp
  - `CancelDelayed(topic, partition, offset)` - cancel pending
  - `GetDelayedMessages(topic, limit, skip)` - list pending
  - `DelayStats()` - scheduler statistics
- **HTTP API endpoints**:
  - `POST /topics/{name}/messages` - supports `delay` and `deliverAt` params
  - `GET /topics/{name}/delayed` - list pending delayed messages
  - `DELETE /topics/{name}/delayed/{partition}/{offset}` - cancel delayed
  - `GET /delay/stats` - scheduler and timer wheel statistics
- **Files created**:
  - `internal/broker/timer_wheel.go` - Hierarchical timer wheel implementation
  - `internal/broker/delay_index.go` - Persistent delay tracking
  - `internal/broker/scheduler.go` - Scheduler coordination
  - `internal/broker/timer_wheel_test.go` - Timer wheel tests
  - `internal/broker/delay_index_test.go` - Delay index tests
  - `internal/broker/scheduler_test.go` - Scheduler tests
- **Key Design Decisions**:
  - Messages written to log immediately (durability first)
  - Timer wheel tracks visibility, index tracks state
  - Zero/past delays fire immediately
  - Bucket position is authoritative (not DeliverAt time check)

### Milestone 6 - Priority Lanes ✅ ⭐
- **5 Priority Levels** - Critical(0), High(1), Normal(2), Low(3), Background(4)
  - Critical: Emergencies, circuit breakers (50% share)
  - High: Paid users, real-time updates (25% share)
  - Normal: Default traffic (15% share)
  - Low: Batch jobs, reports (7% share)
  - Background: Analytics, cleanup tasks (3% share)
- **32-byte Message Header** - Priority stored at position [24]
  - Format: Magic(2) + Version(1) + Flags(1) + CRC(4) + Offset(8) + Timestamp(8) + Priority(1) + Reserved(1) + KeyLen(2) + ValueLen(4)
  - Version: 1 (simplified, no V1/V2 branching during development)
- **Weighted Fair Queuing (WFQ)** - Deficit Round Robin algorithm
  - Weights: [50, 25, 15, 7, 3] for priorities 0-4
  - Each priority gets proportional share, not strict ordering
  - Critical always checked first each round
  - Deficit counter tracks fairness across rounds
- **Starvation Prevention** - 30s default timeout
  - If any priority hasn't been served for 30s, it gets boosted
  - Prevents low-priority messages from waiting forever
  - Configurable per scheduler
- **Per-Priority-Per-Partition Metrics (PPPP)**:
  - Ready count per priority
  - In-flight count per priority
  - Enqueue/dequeue rates per priority
  - Last served timestamps per priority
- **Broker Integration**:
  - `PublishWithPriority(topic, key, value, priority)` - publish with priority
  - Priority-aware consume respects WFQ ordering
  - `PriorityStats()` - per-priority-per-partition metrics
- **HTTP API endpoints**:
  - `POST /topics/{name}/messages` - supports `priority` param
  - `GET /topics/{name}/messages` - includes priority in response
  - `GET /priority/stats` - per-priority-per-partition metrics
- **Files created/modified**:
  - `internal/storage/priority.go` - Priority type, constants, validation
  - `internal/storage/message.go` - 32-byte header with Priority field
  - `internal/broker/priority_scheduler.go` - WFQ scheduler using DRR
  - `internal/broker/priority_index.go` - Per-partition priority tracking
  - `internal/broker/priority_scheduler_test.go` - 12 comprehensive tests
  - `internal/storage/segment.go` - Simplified reading (32-byte always)
- **Key Design Decisions**:
  - WFQ over strict priority (fairness > starvation risk)
  - DRR algorithm: O(1) dequeue, simple implementation
  - Priority persisted in message (survives restart)
  - Simplified versioning (V1 only during dev phase)
- **Bugs Fixed**:
  - uint8 underflow in loops (0 decrement → 255)
  - Segment reading always uses 32-byte header

### Milestone 7 - Message Tracing ✅ ⭐
- **W3C Trace Context Format** - Industry standard trace propagation
  - TraceID: 16 bytes (128 bits), hex encoded to 32 chars
  - SpanID: 8 bytes (64 bits), hex encoded to 16 chars
  - Traceparent format: `00-{trace_id}-{span_id}-{flags}`
  - Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
- **34-byte Message Header** - Added HeaderLen field for trace context
  - Format: Magic(2) + Version(1) + Flags(1) + CRC(4) + Offset(8) + Timestamp(8) + Priority(1) + Reserved(1) + KeyLen(2) + ValueLen(4) + HeaderLen(2)
  - Headers encoded as: Count(2) + [KeyLen(2) + Key + ValLen(2) + Val] × N
  - Enables trace context propagation via `traceparent` header
- **Span Event Types** - Complete message lifecycle coverage
  - Publish: `publish.received`, `publish.partitioned`, `publish.persisted`
  - Consume: `consume.fetched`, `consume.acked`, `consume.nacked`, `consume.rejected`
  - Delay: `delay.scheduled`, `delay.ready`, `delay.cancelled`, `delay.expired`
  - Visibility: `visibility.timeout`, `visibility.extended`
  - DLQ: `dlq.routed`
- **Ring Buffer Storage** - In-memory fast access
  - Configurable capacity (default 10,000 spans)
  - Thread-safe with RWMutex
  - Methods: Push(), Count(), GetRecent(), GetByTraceID(), GetByTimeRange()
  - Automatic eviction of oldest spans on overflow
- **File Exporter** - Persistent JSON traces
  - JSONL format (one JSON object per line)
  - File rotation by size (default 10MB)
  - Directory: data/traces/traces-{date}-{seq}.json
- **Stdout Exporter** - Real-time trace output for debugging
  - JSON format with pretty printing option
  - Configurable writer (default: os.Stdout)
- **OTLP Exporter** - OpenTelemetry Protocol support
  - HTTP transport with protobuf encoding
  - Batching for efficiency
  - Configurable endpoint and headers
- **Jaeger Exporter** - Jaeger Thrift UDP support
  - Direct UDP transport (agent mode)
  - Compact Thrift serialization
  - Configurable agent host/port
- **Query Interface**:
  - GetTrace(traceID) - Complete trace by ID
  - GetRecentTraces(limit) - Most recent traces
  - SearchTraces(query) - Filter by topic, partition, time range, status
  - Stats() - Buffer usage, exporter status, sampling rate
- **Trace Struct** - Complete trace view
  - TraceID, Topic, Partition, Offset
  - StartTime, EndTime, Duration
  - Status: completed, pending, error
  - Spans array sorted by timestamp
- **Broker Integration**:
  - Tracer initialized on broker startup
  - Publish operations record spans automatically
  - Consume operations record spans automatically
  - Ack/Nack/Reject operations record spans
  - Configurable per-topic tracing enable/disable
  - Sampling rate support (1.0 = 100%)
- **HTTP API endpoints**:
  - `GET /traces` - List recent traces (limit param)
  - `GET /traces/{traceID}` - Get specific trace with all spans
  - `GET /traces/search` - Search with filters (topic, partition, time, status)
  - `GET /traces/stats` - Tracer statistics
- **Files created**:
  - `internal/broker/tracer.go` - Complete tracing implementation (~1700 lines)
  - `internal/broker/tracer_test.go` - 22 comprehensive tests
  - `internal/storage/message.go` - Updated to 34-byte header with headers support
  - `internal/api/server.go` - Trace API endpoints
- **Key Design Decisions**:
  - W3C Trace Context for interoperability (not custom format)
  - Ring buffer for fast recent access (no DB dependency)
  - File export for persistence (simple, portable)
  - Sampling support for high-throughput scenarios
  - Span-per-event (not span-per-message) for granular visibility
  - Headers in message format (not out-of-band) for durability

### Milestone 8 - Schema Registry ✅
- **JSON Schema Validation** - Draft 7 compatible pure-Go validator
  - Type validation: string, integer, number, boolean, array, object, null
  - Property validation: properties, required, additionalProperties
  - Constraints: minimum/maximum, minLength/maxLength, pattern, enum
  - Array support: items, minItems, maxItems, uniqueItems
  - Nested objects with recursive validation
  - Local $ref support for schema composition
- **Compatibility Modes** - Safe schema evolution
  - BACKWARD (default): New schema reads old data (Confluent default)
  - FORWARD: Old schema reads new data
  - FULL: Both BACKWARD and FORWARD compatible
  - NONE: No compatibility checking (development/testing)
- **Subject Naming** - TopicNameStrategy
  - Subject = Topic name (1:1 mapping)
  - Simple and intuitive for most use cases
  - Future: RecordNameStrategy, TopicRecordNameStrategy
- **Versioning** - Sequential integers per subject
  - Global unique ID across all subjects
  - Version numbers sequential within each subject
  - Duplicate schema detection (returns existing)
- **Storage** - File-based JSON persistence
  - Directory: data/schemas/{subject}/
  - Files: v1.json, v2.json, config.json
  - Global: _ids.json (ID → subject:version mapping)
  - Survives restarts with full recovery
- **Caching** - In-memory validator cache
  - Compiled schemas cached for fast validation
  - Lazy loading on first use
  - Configurable cache size (default 1000)
- **Broker Integration**:
  - Schema validation in publish path
  - Per-subject validation enable/disable
  - Validation failure returns 400 error
  - Schema ID passed via `schema-id` header
- **HTTP API endpoints** (Confluent-compatible):
  - `POST /schemas/subjects/{subject}/versions` - Register schema
  - `GET /schemas/subjects/{subject}/versions` - List versions
  - `GET /schemas/subjects/{subject}/versions/latest` - Get latest
  - `GET /schemas/subjects/{subject}/versions/{version}` - Get specific
  - `DELETE /schemas/subjects/{subject}/versions/{version}` - Delete version
  - `GET /schemas/subjects` - List subjects
  - `POST /schemas/subjects/{subject}` - Check schema exists
  - `DELETE /schemas/subjects/{subject}` - Delete subject
  - `GET /schemas/ids/{id}` - Get by global ID
  - `POST /schemas/compatibility/subjects/{subject}/versions/{version}` - Test compatibility
  - `GET /schemas/config` - Get global config
  - `PUT /schemas/config` - Set global config
  - `GET /schemas/config/{subject}` - Get subject config
  - `PUT /schemas/config/{subject}` - Set subject config
  - `GET /schemas/stats` - Registry statistics
- **Files created**:
  - `internal/broker/schema_registry.go` - Core registry (~1200 lines)
  - `internal/broker/json_schema_validator.go` - JSON Schema validator (~500 lines)
  - `internal/broker/schema_registry_test.go` - 30+ comprehensive tests
  - `internal/api/server.go` - Schema API endpoints (15 handlers)
- **Key Design Decisions**:
  - JSON Schema only (Protobuf deferred, noted for future)
  - File-based storage matches existing patterns
  - Broker-side validation (not client-side)
  - Confluent-compatible API for familiarity
  - Per-subject config overrides global config
  - Soft delete for version safety

### Milestone 9 - Transactional Publish ✅ ⭐
- **Idempotent Producers** - Exactly-once publish semantics
  - ProducerId (int64) + Epoch (int16) for identity
  - Per-partition sequence numbers for ordering
  - Sequence validation: rejects out-of-order, detects duplicates
  - Sliding deduplication window (default 5 sequences)
  - Epoch-based zombie fencing (re-init bumps epoch)
- **Transaction Coordinator** - Two-phase commit lifecycle
  - States: Empty → Ongoing → PrepareCommit/Abort → CompleteCommit/Abort
  - Transaction ID generation with timestamp + random hex
  - Heartbeat support (same pattern as consumer groups)
  - Session timeout for dead producer detection
  - Transaction timeout for abandoned transactions
  - Background goroutines for timeout checking, snapshot taking
- **Transaction Log** - File-based WAL + Snapshots
  - WAL Record Types: init_producer, begin_txn, add_partition, prepare_commit/abort, complete_commit/abort, heartbeat, expire_producer, update_sequence
  - JSON-encoded WAL records (one per line)
  - Periodic snapshots for fast recovery
  - Storage: data/transactions/transactions.log + producer_state.json
- **Control Records** - Transaction markers in partition log
  - Flags byte in 34-byte message header (bits 3-4)
  - FlagControlRecord = 0x08 (bit 3)
  - FlagTransactionCommit = 0x10 (bit 4)
  - Commit: FlagControlRecord | FlagTransactionCommit
  - Abort: FlagControlRecord only
  - ControlRecordPayload: ProducerId, Epoch, TransactionalId
  - Written to ALL partitions in transaction
- **Broker Integration**:
  - TransactionCoordinator initialized on broker startup
  - WriteControlRecord method for commit/abort markers
  - PublishTransactional method with sequence validation
- **LSO (Last Stable Offset) - Read Committed Isolation** ⭐
  - **UncommittedTracker** - Tracks offsets in uncommitted transactions
    - Per-topic map: `topic:partition → Set[offsets]`
    - `TrackOffset(txnID, topic, partition, offset)` - Mark offset as uncommitted
    - `ClearTransaction(txnID)` - Returns `[]partitionOffset` for abort handling
    - Thread-safe with RWMutex
  - **AbortedTracker** - Permanently hides aborted message offsets
    - Per-partition map: `topic:partition → Set[offsets]`
    - `MarkAborted(offsets)` - Mark offsets from aborted transaction
    - `IsAborted(topic, partition, offset)` - O(1) lookup
    - Messages from aborted transactions never become visible
  - **Control Record Filtering** - Commit/abort markers invisible to consumers
    - `IsControlRecord()` method checks FlagControlRecord bit
    - Control records excluded from priority index
    - All 4 consume methods filter control records first
  - **Consume Filtering Pipeline** - Order matters for correctness:
    1. Filter control records (IsControlRecord)
    2. Filter delayed messages (deliverAt > now)
    3. Filter uncommitted transactions (UncommittedTracker.IsUncommitted)
    4. Filter aborted transactions (AbortedTracker.IsAborted)
  - **Transaction Completion Flow**:
    - Commit: `ClearTransaction()` removes from uncommitted → messages visible
    - Abort: `ClearTransaction()` returns offsets → `MarkTransactionAborted()` → messages permanently hidden
  - **Test Coverage**: 3 comprehensive tests
    - `TestBroker_UncommittedTransactionFiltering` - Uncommitted invisible, committed visible
    - `TestBroker_AbortedTransactionFiltering` - Aborted messages stay invisible forever
    - `TestBroker_MixedTransactionalAndNormalMessages` - Normal messages unaffected
  - **Known Limitation**: AbortedTracker is in-memory only; won't survive broker restart (acceptable for M9 scope)
  - GetTransactionCoordinator accessor for HTTP handlers
- **HTTP API endpoints** (9 endpoints):
  - `POST /producers/init` - Initialize producer (get PID + epoch)
  - `POST /producers/{producerId}/heartbeat` - Producer heartbeat
  - `POST /transactions/begin` - Begin transaction
  - `POST /transactions/publish` - Publish within transaction
  - `POST /transactions/add-partition` - Add partition to transaction
  - `POST /transactions/commit` - Commit transaction
  - `POST /transactions/abort` - Abort transaction
  - `GET /transactions` - List active transactions
  - `GET /transactions/stats` - Coordinator statistics
- **Files created**:
  - `internal/broker/idempotent_producer.go` - PID assignment, sequence tracking (~900 lines)
  - `internal/broker/transaction_log.go` - WAL + snapshot persistence (~650 lines)
  - `internal/broker/transaction_coordinator.go` - Transaction lifecycle (~850 lines)
  - `internal/broker/transaction_coordinator_test.go` - 22+ comprehensive tests
- **Files modified**:
  - `internal/storage/message.go` - Control record flags and payload
  - `internal/broker/broker.go` - TransactionCoordinator integration
  - `internal/api/server.go` - Transaction API endpoints (~600 lines)
- **Key Design Decisions**:
  - File-based transaction log (not internal topic like Kafka)
  - WAL + Snapshot pattern for efficient recovery
  - Epoch-based zombie fencing (not generation-based)
  - Heartbeat + timeout (same as consumer groups)
  - Flags byte for control records (not separate record type)
  - Per-partition sequence tracking (not per-topic)
  - Types match Kafka wire format (int64 PID, int16 epoch, int32 seq)

### Technical Decisions Made
| Decision | Choice | Rationale |
|----------|--------|-----------|
| Segment size | 64MB | Good balance for durability/performance |
| Index granularity | 4KB | Matches typical filesystem block size |
| Fsync interval | 1000ms | Good durability without excessive I/O |
| File I/O | Buffered (os.File) | Simpler, portable, good performance |
| Checksum | CRC32 Castagnoli | Hardware acceleration, widely used |
| Magic bytes | 0x47 0x51 ("GQ") | Identifies goqueue files |
| Hash algorithm | Murmur3 | Industry standard, Kafka compatible |
| Default partitions | 3 | Good parallelism, expandable in M13 |
| Batching location | Client-side | Reduces broker load, Kafka pattern |
| LingerMs=0 | Immediate flush | Kafka semantics for low latency |
| HTTP API style | REST-ish | Simple, curl-friendly, JSON |
| HTTP router | chi v5 | Lightweight, idiomatic, stdlib compatible |
| Session timeout | 30s | Kafka default, good balance |
| Heartbeat interval | 3s | ~10 heartbeats per session (margin) |
| Partition strategy | Range | Simple, deterministic, Kafka default |
| Rebalance protocol | Eager (stop-world) | Simpler; cooperative in M12 |
| Offset storage | File-based JSON | Simple, debuggable; Kafka uses topics |
| Auto-commit interval | 5s | Kafka default |
| MemberID format | clientID-randomHex | Unique, traceable |
| Poll timeout | 30s | Standard long-poll duration |
| ACK model | Hybrid (per-msg + offset) | Best of SQS (per-msg) + Kafka (offset) |
| Visibility timeout | 30s | SQS default, good for most workloads |
| Max retries | 3 | Industry standard before DLQ |
| Backoff strategy | Exponential (1s base, 2x) | Standard retry pattern |
| DLQ naming | `{topic}.dlq` | Clear, discoverable |
| Receipt handle format | `topic:partition:offset:deliveryCount:nonce` | Parseable, debuggable |
| Visibility heap | Min-heap | O(1) peek, O(log n) operations |
| Timer algorithm | Hierarchical wheel | O(1) operations, ~7.76 day max |
| Tick interval | 10ms | Good precision, low CPU overhead |
| Timer wheel levels | 4 (256/64/64/64) | Balance of granularity and range |
| Delay storage | Separate index file | Clean separation, efficient recovery |
| Delay API | Both relative and absolute | Maximum flexibility for producers |
| Max delay | ~7.76 days | Practical limit, fits 4-level wheel |
| Zero/past delay | Immediate fire | Intuitive behavior |
| Schema format | JSON Schema | Human readable, no codegen, simple |
| Schema storage | File-based JSON | Matches existing patterns, debuggable |
| Subject naming | TopicNameStrategy | Simple 1:1 mapping, intuitive |
| Compatibility default | BACKWARD | Confluent default, safest for teams |
| Schema ID location | Message header | Uses existing headers system |
| Validation point | Broker-side | Central enforcement, configurable |
| Versioning | Sequential integers | Simple, compact, industry standard |
| Validation failure | Reject with 400 | Fail fast, clear contract |
| Transaction log | File-based WAL + Snapshot | Matches goqueue patterns, simpler than topic |
| Zombie fencing | Epoch-based | Kafka-compatible, bumps on re-init |
| Producer ID type | int64 | Kafka wire format compatibility |
| Sequence tracking | Per-partition | Better parallelism than per-topic |
| Transaction timeout | 60s | Kafka default, allows slow consumers |
| Producer heartbeat | 3s | Same as consumer groups |
| Producer session | 30s | Same as consumer groups |
| Control records | Flags byte | Reuses existing header format |
| Dedup window | 5 sequences | Balance memory vs. retry coverage |
| LSO tracking | Offset set per partition | O(1) lookup, no position scanning like Kafka |
| Abort persistence | In-memory only | Acceptable for M9 scope; persistence in future |
| Control record hiding | All 4 consume methods | Consistent filtering regardless of consume style |

## What's Left to Build

### Core (Must Have)
- [x] Append-only log with segments ✅
- [x] Offset indexes ✅
- [ ] Time indexes (deferred - low priority)
- [x] Multi-partition topics ✅
- [x] Producer with batching ✅
- [x] HTTP API ✅
- [x] Consumer groups ✅ (M3)
- [x] Offset management ✅ (M3)
- [x] Per-message ACK ✅ (M4)
- [x] Visibility timeout ✅ (M4)
- [x] Dead letter queue ✅ (M4)
- [x] Transactional publish ✅ (M9)

### Differentiators (Key Features) ⭐
- [x] Native delay messages (timer wheel) ✅ (M5)
- [x] Priority lanes ✅ (M6)
- [x] Message tracing ✅ (M7)
- [x] Schema registry ✅ (M8)
- [x] Transactional producers ✅ (M9)
- [ ] Cooperative rebalancing (M12)

### Distribution (Multi-Node)
- [ ] Cluster membership
- [ ] Leader election
- [ ] Log replication

### Operations
- [ ] gRPC API
- [ ] CLI tool
- [ ] Prometheus metrics
- [ ] Kubernetes deployment

## Known Issues

### Visibility Tracking in Simple Consume (Deferred)

**Problem:** The simple consume endpoints (`GET /topics/{name}/partitions/{id}/messages/*`) don't track visibility timeout. They are read-only operations that simply fetch and filter messages from storage.

**Why This Matters:**
- Without visibility tracking, multiple consumers calling the simple consume API could get duplicate messages
- No receipt handles are generated (receipt handles are only for consumer groups)
- No ACK/NACK/Reject support (those operations require consumer group membership)

**Current Behavior:**
- Simple consume methods: Just read → filter → return (no state tracking)
- Consumer groups: Full lifecycle with ACK manager, visibility tracker, receipt handles

**Why Deferred:**
- Design decision: Keep simple consume truly simple for read-only use cases
- If you need reliability features (ACK, visibility, DLQ), use consumer groups
- Consumer groups already have full visibility tracking via ACK manager
- Simple consume is best suited for:
  - Debugging/inspection (manually looking at messages)
  - Replay/reprocessing (reading historical data)
  - Read-only analytics consumers
  - Testing and development

**When to Implement:**
- If users request visibility tracking for simple consume
- If we want to unify simple consume and consumer group consume patterns
- Consider: Would add complexity and state management to what's meant to be a simple API

**Related Code:**
- Consumer groups use: `internal/broker/ack_manager.go` + `visibility_tracker.go`
- Simple consume: `broker.Consume()`, `ConsumeByOffset()`, `ConsumeByPriority()`, `ConsumeByPriorityWFQ()`

## Lessons Learned

### Session 4 - Milestone 4
1. **Hybrid ACK model advantage** - Combining per-message ACK with offset commits gives both fine-grained control (retry single message) AND efficient progress tracking (offset-based recovery after crash).
2. **Min-heap for visibility tracking** - O(1) to check next expiring message, O(log n) for add/remove. Perfect fit for timeout-based tracking with potentially thousands of in-flight messages.
3. **Receipt handles encode location** - Including topic:partition:offset in receipt handle allows routing without database lookup. The nonce prevents replay attacks.
4. **DLQ auto-creation is essential** - Can't require users to pre-create DLQ topics. Auto-create with same partition count as original, with `.dlq` suffix for discoverability.
5. **Backpressure via MaxInFlightPerConsumer** - Without limits, a fast producer can exhaust memory. Default 1000 in-flight per consumer provides safe backpressure.

### Session 3 - Milestone 3
1. **Chi router URL params** - chi uses `chi.URLParam(r, "name")` to get URL params, not explicit arguments. Test code must call through `router.ServeHTTP()` not individual handlers.
2. **JSON partition keys as strings** - HTTP JSON APIs need partition IDs as strings (JSON keys), not ints. Convert with `strconv.Itoa()` for JSON serialization.
3. **Generation ID prevents zombies** - Stale consumers with old generation get rejected on heartbeat/commit. Critical for at-least-once guarantees.
4. **Stop-the-world rebalance tradeoff** - Simpler than incremental, but all consumers pause during rebalance. Good enough for v1; cooperative rebalancing in M12.

### Session 2 - Milestone 2
1. **Go's select is non-deterministic** - When multiple channels are ready, Go picks randomly. Fixed accumulator race condition by adding priority draining before flush/close.
2. **Pointer vs value semantics for channels** - `Send(record)` copies the struct, losing the `resultCh`. Fixed by creating `sendPtr(*ProducerRecord)` internal function.
3. **Test determinism with partitions** - Multi-partition defaults break tests that assume offset ordering. Solution: use single-partition configs in tests needing deterministic behavior.

## Performance Benchmarks

*No benchmarks yet - will add as milestones complete*

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| Write throughput | 100K msg/s | - | ⏳ |
| p99 publish latency | < 10ms | - | ⏳ |
| Rebalance time | < 1s | - | ⏳ |
