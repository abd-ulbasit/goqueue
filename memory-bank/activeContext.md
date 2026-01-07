# GoQueue - Active Context

## Current Focus

**Phase**: 2 - Advanced Features
**Milestone**: 9 - Transactional Publish ✅ COMPLETE (including LSO)
**Status**: Phase 2 Complete! Ready for Phase 3 (Distribution)

## What I'm Working On

### Just Completed (M9 - Transactional Publish + LSO)
- ✅ Idempotent Producers - ProducerId + Epoch for exactly-once
- ✅ Sequence Tracking - Per-partition deduplication window
- ✅ Transaction Coordinator - Two-phase commit lifecycle
- ✅ Transaction Log - File-based WAL + Snapshot persistence
- ✅ Control Records - Commit/abort markers in partition log
- ✅ **LSO (Last Stable Offset)** - Read committed isolation ⭐
  - UncommittedTracker - Tracks uncommitted transaction offsets
  - AbortedTracker - Permanently hides aborted message offsets
  - Control Record Filtering - All 4 consume methods filter markers
  - Consume Pipeline: control → delayed → uncommitted → aborted
- ✅ HTTP API - 9 transaction endpoints
- ✅ 22+ transaction coordinator tests
- ✅ 3 transaction filtering tests (LSO verification)

### Key Technical Decisions (M9)
- **Transaction Log**: File-based WAL + Snapshot (not internal topic like Kafka)
- **Zombie Fencing**: Epoch-based (bumps on re-init)
- **Producer ID Type**: int64 (Kafka wire format compatibility)
- **Sequence Tracking**: Per-partition (better parallelism than per-topic)
- **Transaction Timeout**: 60s (Kafka default)
- **Control Records**: Flags byte in 34-byte message header
- **Dedup Window**: 5 sequences (balance memory vs. retry coverage)
- **LSO Tracking**: Offset set per partition (O(1) lookup)
- **Abort Persistence**: In-memory only (acceptable for M9 scope)

### Files Created/Modified (M9)
- **Created**:
  - `internal/broker/idempotent_producer.go` - PID assignment, sequence tracking
  - `internal/broker/transaction_log.go` - WAL + snapshot persistence
  - `internal/broker/transaction_coordinator.go` - Transaction lifecycle
  - `internal/broker/transaction_coordinator_test.go` - Comprehensive tests
  - `internal/broker/uncommitted_tracker.go` - LSO tracking (UncommittedTracker + AbortedTracker)
- **Modified**:
  - `internal/storage/message.go` - Control record flags and payload
  - `internal/broker/broker.go` - TransactionCoordinator + LSO integration
  - `internal/broker/partition.go` - Control records excluded from priority index
  - `internal/broker/topic.go` - PublishMessageToPartition method
  - `internal/broker/broker_test.go` - 3 LSO filtering tests
  - `internal/api/server.go` - Transaction API endpoints

### Transaction API Endpoints
```
POST /producers/init                    Initialize producer (get PID + epoch)
POST /producers/{producerId}/heartbeat  Producer heartbeat
POST /transactions/begin                Begin transaction
POST /transactions/publish              Publish within transaction
POST /transactions/add-partition        Add partition to transaction
POST /transactions/commit               Commit transaction
POST /transactions/abort                Abort transaction
GET  /transactions                      List active transactions
GET  /transactions/stats                Coordinator statistics
```

### LSO Implementation Details
```
┌─────────────────────────────────────────────────────────────────┐
│                    CONSUME FILTERING PIPELINE                    │
├─────────────────────────────────────────────────────────────────┤
│  Message → IsControlRecord? → IsDelayed? → IsUncommitted?       │
│                                              ↓                   │
│                                         IsAborted? → Consumer   │
└─────────────────────────────────────────────────────────────────┘

UncommittedTracker:
  - TrackOffset(txnID, topic, partition, offset)
  - ClearTransaction(txnID) → []partitionOffset (for abort handling)
  - IsUncommitted(topic, partition, offset) → bool

AbortedTracker:
  - MarkAborted(offsets []partitionOffset)
  - IsAborted(topic, partition, offset) → bool
```

### Next Steps (Phase 3 - Distribution)
1. Milestone 10: Cluster Formation & Metadata
2. Milestone 11: Leader Election & Replication
3. Milestone 12: Cooperative Rebalancing ⭐
4. Milestone 13: Online Partition Scaling

## Recent Changes

### Session 6 - Milestone 6 Implementation
**Completed**:
- Created priority scheduling system
  - `priority.go` - Priority type with 5 levels, validation, string conversion
  - `priority_scheduler.go` - WFQ scheduler using DRR algorithm
  - `priority_index.go` - Per-partition priority tracking
  - `priority_scheduler_test.go` - 12 comprehensive tests
- Updated message format
  - 32-byte header (V1) with Priority at [24], Reserved at [25]
  - KeyLen shifted to [26:28], ValueLen to [28:32]
  - Simplified: no V1/V2 branching, just one format
- Updated storage layer
  - `segment.go` - Simplified readOneMessage, scanLogToEnd, rebuildSegment
  - Always reads 32-byte headers
- Updated broker integration
  - `partition.go` - Priority scheduler and index integration
  - `broker.go` - PriorityStats() method, priority metrics structures
- Implemented HTTP API endpoints
  - Modified `POST /topics/{name}/messages` - accepts `priority` param
  - Modified `GET /topics/{name}/messages` - includes priority in response
  - `GET /priority/stats` - per-priority-per-partition metrics
- Documentation updates
  - Added M6 section to ARCHITECTURE.md with diagrams
  - Updated PROGRESS.md with M6 completion
  - WFQ vs Strict Priority comparison

### Session 5 - Milestone 5 Implementation
**Completed**:
- Created delay scheduling system
  - `timer_wheel.go` - Hierarchical 4-level timer wheel
  - `delay_index.go` - Binary file format for persistence
  - `scheduler.go` - Coordinator connecting components
  - `timer_wheel_test.go` - Timer wheel unit tests
  - `delay_index_test.go` - Delay index unit tests
  - `scheduler_test.go` - Scheduler integration tests
- Updated broker integration
  - Added `scheduler *Scheduler` field to Broker
  - Added `PublishWithDelay()`, `PublishAt()` methods
  - Added `CancelDelayed()`, `IsDelayed()`, `GetDelayedMessages()`
  - Added `DelayStats()` for observability
  - Scheduler starts on broker startup, stops on shutdown
- Implemented HTTP API endpoints
  - Modified `POST /topics/{name}/messages` - accepts `delay`, `deliverAt` params
  - `GET /topics/{name}/delayed` - list pending delayed messages
  - `DELETE /topics/{name}/delayed/{partition}/{offset}` - cancel delayed
  - `GET /delay/stats` - scheduler and timer wheel statistics
- Fixed critical timer wheel bug
  - Was checking DeliverAt.Before(now) before firing
  - OS scheduler variance caused timers to miss
  - Solution: bucket position is authoritative
- Documentation updates
  - Added M5 section to ARCHITECTURE.md with diagrams
  - Updated progress.md with M5 completion
  - Updated comparison tables

### Session 4 - Milestone 4 Implementation
**Completed**:
- Created reliability layer
  - `inflight.go` - InFlightMessage, ReceiptHandle, DLQMessage, ReliabilityConfig
  - `visibility_tracker.go` - Min-heap based timeout tracking with background checker
  - `ack_manager.go` - Per-message ACK state, consumer state tracking, retry queue
  - `dlq.go` - DLQ routing with auto-topic creation, metadata preservation
- Updated broker integration
  - Added `AckManager()`, `ReliabilityConfig()` accessors
  - Added `ConsumeWithReceipts()` for tracked consumption
  - Added `Ack()`, `Nack()`, `Reject()`, `ExtendVisibility()` methods
  - Added `ReliabilityStats()` for observability
- Implemented HTTP API endpoints
  - `POST /messages/ack` - Acknowledge successful processing
  - `POST /messages/nack` - Signal retry needed (transient failure)
  - `POST /messages/reject` - Send to DLQ (poison message)
  - `POST /messages/visibility` - Extend visibility timeout
  - `GET /reliability/stats` - Combined reliability layer stats
- Technical decisions
  - Hybrid ACK model: Best of SQS (per-message) + Kafka (offset)
  - 30s default visibility timeout (SQS default)
  - 3 max retries before DLQ
  - Receipt handles encode location for debuggability

### Session 3 - Milestone 3 Implementation
**Completed**:
- Created consumer group system
  - `ConsumerGroup` struct with membership tracking
  - `Member` struct with heartbeat/assignment tracking
  - Range assignment for partition distribution
  - Generation ID increments on rebalance
- Built offset management
  - `OffsetManager` with file-based JSON storage
  - Auto-commit goroutine (configurable interval)
  - Atomic file writes for durability
  - Per-group, per-topic, per-partition offsets
- Implemented group coordinator
  - `GroupCoordinator` centralizes group management
  - Session monitoring goroutine evicts dead consumers
  - Topic registration for partition counting
- Refactored HTTP API
  - Replaced stdlib `ServeMux` with `chi` router
  - Added logging middleware
  - URL parameter support for `/groups/{groupID}`
- Added consumer group endpoints
  - `POST /groups/{group}/join` - join with client_id, topics
  - `POST /groups/{group}/heartbeat` - keep session alive
  - `POST /groups/{group}/leave` - graceful departure
  - `GET /groups/{group}/poll` - long-poll messages
  - `POST /groups/{group}/offsets` - commit offsets
  - `GET /groups/{group}/offsets` - fetch offsets
  - `GET /groups` - list all groups
  - `GET /groups/{group}` - group details
  - `DELETE /groups/{group}` - delete group

## Resolved Decisions (M3)

### Decision: Partition Assignment Strategy ✅
**Chosen**: Range assignment
**Reason**: Simple, deterministic, Kafka default

### Decision: Session Timeout ✅
**Chosen**: 30s session, 3s heartbeat
**Reason**: ~10 heartbeats per session, good margin

### Decision: Rebalance Protocol ✅
**Chosen**: Eager (stop-the-world)
**Reason**: Simpler; cooperative planned for M12

### Decision: Offset Storage ✅
**Chosen**: File-based JSON
**Reason**: Simple, debuggable; Kafka uses __consumer_offsets topic

### Decision: HTTP Router ✅
**Chosen**: chi v5
**Reason**: Lightweight, idiomatic Go, stdlib-compatible

### Decision: Poll Mechanism ✅
**Chosen**: Long-polling (30s timeout)
**Reason**: Simple, works with HTTP/1.1, no WebSocket complexity

### File Structure
```
data/logs/{topic}/{partition}/
├── 00000000000000000000.log    # Segment file
├── 00000000000000000000.index  # Sparse index
├── 00000000000000001000.log    # Next segment
└── 00000000000000001000.index
```

## Blockers

*None*

## Session Notes

### Session 1 (Milestone 1)
**Focus**: Complete storage engine implementation
**Completed**:
- All storage layer components (message, segment, index, log)
- Broker layer (partition, topic, broker)
- Comprehensive test coverage
- Demo application
- 

**Learned**:
- 

**Next**:
- 
```
