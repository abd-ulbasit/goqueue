# GoQueue - Active Context

## Current Focus

**Phase**: 3 - Distribution
**Milestone**: 10 - Cluster Formation & Metadata ✅ COMPLETE
**Status**: M10 Complete! Ready for M11 (Leader Election & Replication)

## What I'm Working On

### Just Completed (M10 - Cluster Formation & Metadata)
- ✅ **Node Identity** - NodeID with hostname fallback, NodeAddress parsing
- ✅ **Cluster Types** - NodeStatus (Unknown→Alive→Suspect→Dead→Leaving), NodeRole (Follower|Controller)
- ✅ **Membership Manager** - Thread-safe node registry with event listeners
- ✅ **Failure Detector** - Heartbeat-based health monitoring (3s/6s/9s timing)
- ✅ **Controller Election** - Lease-based with epoch tracking (15s lease, 5s renewal)
- ✅ **Metadata Store** - TopicMeta, PartitionAssignment with CRUD operations
- ✅ **Inter-Node HTTP API** - ClusterServer (7 endpoints) + ClusterClient
- ✅ **Bootstrap Coordinator** - Load state → Register self → Discover peers → Quorum → Election
- ✅ **Broker Integration** - ClusterModeConfig, 60s bootstrap timeout, 30s shutdown
- ✅ **Tests** - 20+ tests across types, membership, failure detection, election

### Key Technical Decisions (M10)
- **Peer Discovery**: Static config (gossip planned for later milestone)
- **Controller Election**: Lease-based (simpler than Raft consensus)
- **Cluster Heartbeat**: 3s interval, 6s suspect, 9s dead (conservative)
- **Cluster Metadata**: File-based JSON at dataDir/cluster/
- **Inter-node Comms**: HTTP/JSON (reuse existing infrastructure)
- **Node ID**: Configurable with hostname fallback
- **Quorum Size**: Configurable (default: majority of peers)
- **Controller Lease**: 15s timeout, 5s renewal interval
- **Epoch Tracking**: Per-elector monotonic (prevents split-brain)

### Files Created (M10)
- **Cluster Package** (8 files):
  - `internal/cluster/types.go` - Core data structures (~400 lines)
  - `internal/cluster/node.go` - Local node identity (~80 lines)
  - `internal/cluster/membership.go` - Membership management (~350 lines)
  - `internal/cluster/failure_detector.go` - Health monitoring (~200 lines)
  - `internal/cluster/controller_elector.go` - Leader election (~250 lines)
  - `internal/cluster/metadata_store.go` - Metadata storage (~350 lines)
  - `internal/cluster/cluster_server.go` - HTTP API (~400 lines)
  - `internal/cluster/coordinator.go` - Bootstrap orchestration (~450 lines)
- **Broker Integration**:
  - `internal/broker/cluster_integration.go` - Broker bridge (~200 lines)
- **Tests** (3 files):
  - `internal/cluster/types_test.go` - Types and parsing tests
  - `internal/cluster/membership_test.go` - Membership manager tests
  - `internal/cluster/failure_detector_test.go` - Failure detection + election tests
- **Modified**:
  - `internal/broker/broker.go` - ClusterEnabled, ClusterModeConfig (~50 lines)

### Cluster API Endpoints (M10)
```
POST /cluster/heartbeat   Record heartbeat from peer
POST /cluster/join        Handle join request from peer
POST /cluster/leave       Handle graceful leave request
GET  /cluster/state       Return cluster state snapshot
POST /cluster/vote        Handle vote request for election
GET  /cluster/metadata    Return cluster metadata
GET  /cluster/health      Health check endpoint
```

### Cluster Architecture (M10)
```
┌─────────────────────────────────────────────────────────────────┐
│                      CLUSTER COORDINATOR                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐  ┌─────────────────┐  ┌──────────────────┐   │
│  │     Node     │  │   Membership    │  │ FailureDetector  │   │
│  │   Identity   │  │    Manager      │  │   (Heartbeats)   │   │
│  └──────────────┘  └─────────────────┘  └──────────────────┘   │
│                                                                  │
│  ┌──────────────┐  ┌─────────────────┐  ┌──────────────────┐   │
│  │  Controller  │  │   Metadata      │  │  ClusterServer   │   │
│  │   Elector    │  │    Store        │  │   + Client       │   │
│  └──────────────┘  └─────────────────┘  └──────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         BROKER                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              clusterCoordinator                          │   │
│  │  - IsLeaderFor(topic, partition)                         │   │
│  │  - GetLeader(topic, partition)                           │   │
│  │  - CreateTopicMeta() / DeleteTopicMeta()                 │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Next Steps (M11 - Leader Election & Replication)
1. Log replication protocol (ISR concept)
2. Leader election for partitions (not just controller)
3. Follower fetch loop
4. Replica acknowledgment and high watermark
5. Partition reassignment on failure

## Recent Changes

### Session 10 - Milestone 10 Implementation
**Completed**:
- Implemented complete cluster formation system
  - Node identity with configurable ID and hostname fallback
  - Membership manager with event-driven notifications
  - Heartbeat-based failure detection (3s/6s/9s timing)
  - Lease-based controller election (15s lease)
  - Cluster metadata store for topic/partition assignments
  - HTTP API for inter-node communication (7 endpoints)
  - Bootstrap coordinator with quorum waiting
  - Broker integration with ClusterModeConfig
- Created comprehensive test suite (20+ tests)
- Updated memory bank with M10 documentation

### Session 9 - Milestone 9 Implementation
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
