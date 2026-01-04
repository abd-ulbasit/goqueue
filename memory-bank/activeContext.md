# GoQueue - Active Context

## Current Focus

**Phase**: 2 - Advanced Features
**Milestone**: 7 - Message Tracing ✅ COMPLETE
**Status**: Ready for Milestone 8 (Schema Registry)

## What I'm Working On

### Just Completed (M7 - Message Tracing)
- ✅ W3C Trace Context Format - Industry standard for trace propagation
- ✅ 34-byte Message Header - Added HeaderLen field for trace headers
- ✅ Complete Span Event Types - publish, consume, ack, nack, reject, delay, dlq
- ✅ Ring Buffer Storage - In-memory fast access with 10,000 span capacity
- ✅ File Exporter - Persistent JSONL traces with rotation
- ✅ OTLP Exporter - OpenTelemetry Protocol support
- ✅ Jaeger Exporter - Jaeger Thrift UDP support
- ✅ Query Interface - GetTrace, GetRecentTraces, SearchTraces
- ✅ HTTP API - /traces, /traces/{id}, /traces/search, /traces/stats
- ✅ Broker Integration - Automatic span recording for all operations
- ✅ 22 comprehensive tracer tests

### Key Technical Decisions (M7)
- **Trace Format**: W3C Trace Context (interoperability with existing tools)
- **TraceID**: 16 bytes (128 bits), hex encoded to 32 chars
- **SpanID**: 8 bytes (64 bits), hex encoded to 16 chars
- **Header Size**: 34 bytes (added 2-byte HeaderLen at [32:34])
- **Headers Encoding**: Count(2) + [KeyLen(2) + Key + ValLen(2) + Val] × N
- **Storage**: Ring buffer (in-memory) + File export (persistent)
- **Sampling**: Configurable rate (1.0 = 100%)
- **Exporters**: Stdout, File (JSONL), OTLP, Jaeger

### Files Created/Modified (M7)
- **Created**:
  - `internal/broker/tracer.go` - Complete tracing implementation (~1700 lines)
  - `internal/broker/tracer_test.go` - 22 comprehensive tests
- **Modified**:
  - `internal/storage/message.go` - 34-byte header, Headers field, encode/decode
  - `internal/storage/message_test.go` - Header encoding tests
  - `internal/broker/broker.go` - Tracer integration, span recording
  - `internal/api/server.go` - Trace API endpoints

### Bug Fixes During M7
- **Message Size calculation**: Headers need 2-byte count prefix even when empty
- **API mismatches in tests**: RingBuffer uses Push/Count, not Add/Len

### Next Steps (Milestone 8 - Schema Registry)
1. Schema storage (Avro, Protobuf, JSON Schema)
2. Schema versioning and compatibility checking
3. Producer schema validation
4. Consumer schema evolution
5. HTTP API for schema management

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
