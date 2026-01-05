# GoQueue - Active Context

## Current Focus

**Phase**: 2 - Advanced Features
**Milestone**: 8 - Schema Registry ✅ COMPLETE
**Status**: Ready for Milestone 9 (Transactional Publish)

## What I'm Working On

### Just Completed (M8 - Schema Registry)
- ✅ Schema Registry Core - Centralized schema management (~1200 lines)
- ✅ JSON Schema Validator - Pure Go Draft 7 implementation (~500 lines)
- ✅ Compatibility Modes - BACKWARD (default), FORWARD, FULL, NONE
- ✅ Subject Naming - TopicNameStrategy (subject = topic name)
- ✅ Version Management - Sequential per subject, global unique IDs
- ✅ File Persistence - data/schemas/{subject}/v{N}.json
- ✅ Broker Integration - Validation in publish path, per-subject toggle
- ✅ HTTP API - Confluent-compatible endpoints (15 handlers)
- ✅ 30+ comprehensive schema registry tests

### Key Technical Decisions (M8)
- **Schema Format**: JSON Schema only (Protobuf deferred, noted for future)
- **Storage**: File-based JSON (matches existing patterns, debuggable)
- **Subject Naming**: TopicNameStrategy (subject = topic name, 1:1 mapping)
- **Compatibility Default**: BACKWARD (Confluent default, safest)
- **Schema ID Location**: Message header field `schema-id`
- **Validation Point**: Broker-side with per-subject toggle
- **Versioning**: Sequential integers, auto-incremented global IDs
- **Validation Failure**: Reject with 400 error (fail fast)

### Files Created/Modified (M8)
- **Created**:
  - `internal/broker/schema_registry.go` - Core registry implementation
  - `internal/broker/json_schema_validator.go` - JSON Schema validator
  - `internal/broker/schema_registry_test.go` - Comprehensive tests
- **Modified**:
  - `internal/broker/broker.go` - Schema registry integration
  - `internal/api/server.go` - Schema API endpoints (15 handlers)

### Schema Registry API Endpoints
```
POST   /schemas/subjects/{subject}/versions     Register new schema
GET    /schemas/subjects/{subject}/versions     List versions
GET    /schemas/subjects/{subject}/versions/latest  Get latest
GET    /schemas/subjects/{subject}/versions/{version}  Get specific
DELETE /schemas/subjects/{subject}/versions/{version}  Delete version
GET    /schemas/subjects                        List subjects
POST   /schemas/subjects/{subject}              Check schema exists
DELETE /schemas/subjects/{subject}              Delete subject
GET    /schemas/ids/{id}                        Get by global ID
POST   /schemas/compatibility/.../versions/{v}  Test compatibility
GET/PUT /schemas/config                         Global config
GET/PUT /schemas/config/{subject}               Subject config
GET    /schemas/stats                           Statistics
```

### Next Steps (Milestone 9 - Transactional Publish)
1. Producer ID assignment for idempotency
2. Sequence number tracking per partition
3. Transaction coordinator implementation
4. Begin/commit/abort transaction API
5. Idempotent message deduplication
6. Transaction timeout handling

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
