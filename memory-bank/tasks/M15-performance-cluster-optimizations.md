# [M15] - Performance & Cluster Optimizations

**Status:** Completed  
**Added:** 2026-01-11  
**Updated:** 2026-01-11

## Original Request
Combined milestone addressing Category 2 (Performance/Production) and Category 3 (Cluster) issues identified during comprehensive goqueue review.

## Background
After completing 14 milestones, a system-wide review identified remaining TODOs and areas for improvement. These fall into two categories:

### Category 2: Performance & Production Concerns
Non-blocking but important optimizations for production deployments.

### Category 3: Cluster-Related
Items needed for distributed deployments with multiple nodes.

## Issues to Address

### Category 2: Performance/Production

| ID | Location | Issue | Priority |
|----|----------|-------|----------|
| P1 | delay_index.go:132 | O(n) scan for due messages - optimize with time index | HIGH |
| P2 | delay_index.go:188 | O(n) scan for removing by key - needs reverse index | MEDIUM |
| P3 | scheduler.go:360 | Memory optimization for scheduled messages | LOW |
| P4 | follower_fetcher.go:210 | Average latency calculation is simple | LOW |

### Category 3: Cluster-Related

| ID | Location | Issue | Priority |
|----|----------|-------|----------|
| C1 | controller_elector.go:247 | RPC stub - needs cluster communication | HIGH |
| C2 | controller_elector.go:254 | Vote tracking placeholder | HIGH |
| C3 | coordinator.go:186 | Snapshot loading incomplete | MEDIUM |
| C4 | coordinator.go:243 | Recovery process placeholder | MEDIUM |
| C5 | node.go:247 | Gossip protocol stub | LOW |

## Implementation Plan

### Phase 1: Performance Optimizations (P1, P2)

1. **Delay Index Time Optimization**
   - Replace linear scan with time-based index
   - Use heap or sorted tree structure for O(log n) lookups
   - Consider time-bucketing for batch operations

2. **Reverse Index for Delay Removal**
   - Add offset → entry mapping for O(1) removals
   - Balance memory vs speed tradeoff

### Phase 2: Cluster Communication (C1, C2)

1. **Controller RPC Implementation**
   - Define Raft-like RPC protocol
   - Implement RequestVote and AppendEntries
   - Add term tracking and vote persistence

2. **Vote Tracking**
   - Persistent vote storage
   - Election timeout management
   - Split vote handling

### Phase 3: Coordinator Improvements (C3, C4)

1. **Snapshot Loading**
   - Proper deserialization of coordinator state
   - Partition assignment recovery
   - Consumer group state restoration

2. **Recovery Process**
   - Full state reconstruction from snapshots + log
   - Membership synchronization
   - Leader election after recovery

### Phase 4: Optional Improvements (P3, P4, C5)

1. **Scheduler Memory Optimization**
   - Message pooling for scheduled items
   - Lazy loading of payload data

2. **Latency Calculation Enhancement**
   - Moving average or exponential smoothing
   - Percentile tracking (p50, p95, p99)

3. **Gossip Protocol**
   - Basic membership gossip
   - Failure detection integration

## Subtasks
| ID | Description | Status | Updated | Notes |
|----|-------------|--------|---------|-------|
| 1.1 | Implement time-bucketed delay index | Complete | 2026-01-11 | P1: O(n)→O(k) for GetReadyEntries |
| 1.2 | Add reverse index for O(1) removals | Complete | 2026-01-11 | P2: FilePosition field for O(1) state updates |
| 2.1 | Define cluster RPC protocol | Complete | 2026-01-11 | C1: Already had RPC, added vote tracking |
| 2.2 | Implement RequestVote RPC | Complete | 2026-01-11 | C2: Proper majority vote counting |
| 2.3 | Add vote persistence | Complete | 2026-01-11 | Votes tracked per election epoch |
| 3.1 | Implement snapshot deserialization | Complete | 2026-01-11 | C3: Already implemented in coordinator |
| 3.2 | Implement full recovery process | Complete | 2026-01-11 | C4: Already implemented in coordinator |
| 4.1 | Optimize scheduler memory | Complete | 2026-01-11 | P3: GetPendingEntriesPaginated |
| 4.2 | Improve latency calculations | Complete | 2026-01-11 | P4: EMA with α=0.2 |
| 4.3 | Basic gossip protocol | Complete | 2026-01-11 | C5: Heartbeat response merging |

## Progress Log
### 2026-01-11
- Created milestone combining Category 2 and Category 3 issues
- Organized into 4 implementation phases
- Prioritized HIGH items for cluster operation

### 2026-01-11 (Implementation Session)
**Phase 1 - Delay Index Performance (P1, P2):**
- Added time-bucketed index with 1-second granularity
- GetReadyEntries() now O(k) where k = due buckets, not O(n)
- Added FilePosition field to DelayEntry for O(1) state updates
- Added GetPendingEntriesPaginated() for memory-efficient pagination

**Phase 2 - Vote Tracking (C1, C2):**
- Added votesReceived map, votesNeeded, electionEpoch fields
- Fixed startElectionLocked() to calculate majority threshold
- Fixed handleVoteResult() to properly count votes
- Fixed requestVotes() for single-node cluster handling
- Clear vote state on becoming controller

**Phase 3 - Coordinator (C3, C4):**
- Already implemented: loadState() loads membership + metadata
- Already implemented: Recovery via bootstrap() sequence

**Phase 4 - Optional Improvements (P3, P4, C5):**
- P3: Updated GetDelayedMessages to use paginated retrieval
- P4: Implemented EMA latency calculation (α=0.2)
- C5: Enhanced BroadcastHeartbeats to merge response state (gossip)

All tests passing, build verified.

## Dependencies
- Requires cluster networking layer to be functional for C1-C5
- P1-P4 can be implemented independently

## Testing Strategy
- Unit tests for each optimization
- Benchmark tests to verify performance improvements
- Integration tests for cluster operations
- Chaos testing for failure scenarios

## Notes
- Category 1 issues (CRITICAL) have been fixed:
  - ✅ PublishAtWithPriority - M5+M6 integration complete
  - ✅ Transaction abort retry mechanism implemented
  - ✅ OTLP/Jaeger using official OpenTelemetry SDK
  - ✅ Delay filtering was already implemented (stale TODO removed)
