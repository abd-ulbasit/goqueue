# GoQueue - System Patterns

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GoQueue Node                                    │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                           API Layer                                     │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │ │
│  │  │  HTTP Server │  │  gRPC Server │  │ Admin Server │                  │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                          Broker Layer                                   │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │ │
│  │  │    Topics    │  │   Producer   │  │  Consumer    │                  │ │
│  │  │   Manager    │  │   Handler    │  │  Coordinator │                  │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                         Delivery Layer                                  │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │ │
│  │  │ Timer Wheel  │  │  Visibility  │  │   Priority   │                  │ │
│  │  │  (delays)    │  │   Tracker    │  │    Router    │                  │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                         Storage Layer                                   │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │ │
│  │  │  Log Writer  │  │   Segment    │  │    Index     │                  │ │
│  │  │              │  │   Manager    │  │   Manager    │                  │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Design Patterns

### 1. Append-Only Log

The foundation of the entire system. Every message is appended to an immutable log.

```
Log Structure:
┌────────────────────────────────────────────────────────────────────┐
│ Segment 0 (0.log)      │ Segment 1 (1000.log)   │ Segment 2 (2000.log) │
│ Offsets 0-999          │ Offsets 1000-1999      │ Offsets 2000-...     │
│ [msg][msg][msg]...     │ [msg][msg][msg]...     │ [msg][msg]...        │
└────────────────────────────────────────────────────────────────────┘
         │                        │                       │
         ▼                        ▼                       ▼
┌────────────────────────────────────────────────────────────────────┐
│ Index 0 (0.index)      │ Index 1 (1000.index)   │ Index 2 (2000.index) │
│ offset→position        │ offset→position        │ offset→position      │
└────────────────────────────────────────────────────────────────────┘
```

**Why append-only?**
- Sequential writes are 100x faster than random writes
- Immutability simplifies concurrency
- Natural audit trail
- Easy replication (just copy bytes)

### 2. Message Format

Binary format for compact storage:

```
┌──────────────────────────────────────────────────────────────────┐
│ Magic (2B) │ Version (1B) │ Flags (1B) │ CRC32 (4B)              │
├──────────────────────────────────────────────────────────────────┤
│ Offset (8B)           │ Timestamp (8B)                           │
├──────────────────────────────────────────────────────────────────┤
│ Key Length (2B)       │ Key (variable)                           │
├──────────────────────────────────────────────────────────────────┤
│ Value Length (4B)     │ Value (variable)                         │
├──────────────────────────────────────────────────────────────────┤
│ Headers Count (2B)    │ Headers (variable)                       │
└──────────────────────────────────────────────────────────────────┘

Flags byte:
  bit 0: compressed
  bit 1: has headers
  bit 2: has delay
  bit 3: has priority
  bits 4-7: reserved
```

### 3. Sparse Index

Index every N bytes (not every message) to balance memory vs lookup speed:

```go
type Index struct {
    entries []IndexEntry  // sorted by offset
}

type IndexEntry struct {
    Offset   int64  // logical offset
    Position int64  // byte position in segment file
}

// Lookup: binary search for largest offset <= target, then scan forward
```

### 4. Timer Wheel for Delays

Hierarchical timing wheel for O(1) delay scheduling:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Hierarchical Timer Wheel                      │
│                                                                  │
│  Level 0 (milliseconds): 256 buckets × 1ms = 256ms range        │
│  ┌─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┐                             │
│  │ │●│ │ │●│●│ │ │ │●│ │ │ │ │ │ │  ← current tick             │
│  └─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘                             │
│                                                                  │
│  Level 1 (seconds): 64 buckets × 256ms = 16.4s range            │
│  ┌─┬─┬─┬─┬─┬─┬─┬─┐                                              │
│  │●│ │ │●│ │ │ │ │                                              │
│  └─┴─┴─┴─┴─┴─┴─┴─┘                                              │
│                                                                  │
│  Level 2 (minutes): 64 buckets × 16.4s = 17.5min range          │
│  Level 3 (hours): 64 buckets × 17.5min = 18.6hr range           │
│  Level 4 (days): 64 buckets × 18.6hr = 49.7day range            │
└─────────────────────────────────────────────────────────────────┘
```

### 4.5 Priority Lanes with Weighted Fair Queuing

Per-partition priority scheduling using Deficit Round Robin:

```
┌─────────────────────────────────────────────────────────────────┐
│                 Priority Scheduler (per Partition)               │
│                                                                  │
│  Priority Queues:                                                │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Critical (0) │████████████████████████████████████████████│ │ Weight 50
│  │ High     (1) │████████████████████████                    │ │ Weight 25
│  │ Normal   (2) │███████████████                             │ │ Weight 15
│  │ Low      (3) │███████                                     │ │ Weight 7
│  │ Background(4)│███                                         │ │ Weight 3
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Deficit Round Robin Algorithm:                                  │
│  1. Add quantum (weight) to deficit counter                      │
│  2. Dequeue while deficit > 0 AND queue not empty                │
│  3. Subtract 1 from deficit for each dequeue                     │
│  4. Move to next priority (Critical always checked first)        │
│  5. Reset deficit when queue empties                             │
│                                                                  │
│  Starvation Prevention:                                          │
│  - Track last-served timestamp per priority                      │
│  - If wait > 30s, temporarily boost to Critical                  │
│  - Serve boosted message, reset timer                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Message Header Layout (32 bytes):
┌───────┬────────┬───────┬───────┬────────┬──────────┬──────────┬──────────┐
│ Magic │Version │ Flags │ CRC32 │ Offset │Timestamp │ Priority │ KeyLen+  │
│  2B   │  1B    │  1B   │  4B   │   8B   │    8B    │  1B+1B   │ ValueLen │
│ "GQ"  │  0x01  │       │       │        │          │ 0-4 + R  │  2B+4B   │
└───────┴────────┴───────┴───────┴────────┴──────────┴──────────┴──────────┘
```

### 5. Visibility Timeout State Machine

```
         ┌──────────────────────────────────────────┐
         │                                          │
         ▼                                          │
    ┌─────────┐     fetch     ┌─────────────┐      │
    │ VISIBLE │──────────────►│ IN_FLIGHT   │      │
    └─────────┘               └──────┬──────┘      │
         ▲                          │              │
         │                    ┌─────┴─────┐        │
         │                    ▼           ▼        │
         │              ┌─────────┐  ┌─────────┐   │
         │              │   ACK   │  │  NACK   │   │
         │              └────┬────┘  └────┬────┘   │
         │                   │            │        │
         │                   ▼            │        │
         │            ┌───────────┐       │        │
         │            │ PROCESSED │       │        │
         │            └───────────┘       │        │
         │                                │        │
         │          timeout expired       │        │
         └────────────────────────────────┴────────┘
                          │
                   retry_count++
                          │
                    retry >= max?
                          │
                          ▼
                    ┌─────────┐
                    │   DLQ   │
                    └─────────┘
```

### 6. Consumer Group Coordination

```
┌─────────────────────────────────────────────────────────────────┐
│                    Consumer Coordinator                          │
│                                                                  │
│  Consumer Groups:                                                │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ Group: "processors"                                         ││
│  │ Generation: 3                                               ││
│  │ Protocol: cooperative                                       ││
│  │                                                             ││
│  │ Members:                                                    ││
│  │ ┌─────────────┬────────────────┬─────────────────────────┐ ││
│  │ │ Member ID   │ Last Heartbeat │ Assigned Partitions     │ ││
│  │ ├─────────────┼────────────────┼─────────────────────────┤ ││
│  │ │ consumer-1  │ 2s ago         │ orders/0, orders/1      │ ││
│  │ │ consumer-2  │ 1s ago         │ orders/2                │ ││
│  │ └─────────────┴────────────────┴─────────────────────────┘ ││
│  │                                                             ││
│  │ Committed Offsets:                                          ││
│  │   orders/0: 12345                                          ││
│  │   orders/1: 67890                                          ││
│  │   orders/2: 11111                                          ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### 7. Cooperative Rebalancing Protocol

Unlike Kafka's eager rebalancing (stop-the-world), we use cooperative:

```
Eager Rebalancing (Kafka default):
──────────────────────────────────
1. Consumer B joins
2. Coordinator: "Everyone STOP and revoke ALL partitions"
3. All consumers stop processing
4. Coordinator computes new assignment
5. Coordinator: "Here are your new partitions"
6. All consumers resume
   
   Timeline:
   Consumer A: [P0,P1,P2]──STOP──────────────────────[P0,P1]───►
   Consumer B: ──────────────WAIT────────────────────[P2]──────►
                            ▲                      ▲
                            └──── ALL STOPPED ─────┘

Cooperative Rebalancing (GoQueue):
──────────────────────────────────
1. Consumer B joins
2. Coordinator: "Consumer A, please release P2 when ready"
3. Consumer A finishes current messages on P2, releases it
4. Consumer A continues processing P0, P1 uninterrupted
5. Coordinator assigns P2 to Consumer B
6. Consumer B starts processing P2
   
   Timeline:
   Consumer A: [P0,P1,P2]─────────[P0,P1]─────────────────────►
                         ↘ P2 handoff
   Consumer B: ────────────────────[P2]──────────────────────►
                                   ▲
                                   └── Only P2 paused briefly
```

### 8. Leader Election (Simplified)

Using lease-based election instead of full Raft:

```go
type Lease struct {
    PartitionID  string
    LeaderNodeID string
    ExpiresAt    time.Time
    Term         int64
}

// Leader renewal loop
for {
    if imLeader {
        // Extend lease
        lease.ExpiresAt = time.Now().Add(leaseDuration)
        broadcast(lease)
    } else {
        // Check if lease expired
        if time.Now().After(currentLease.ExpiresAt) {
            // Try to claim leadership
            tryClaimLeadership()
        }
    }
    time.Sleep(heartbeatInterval)
}
```

### 9. Reliability Layer - ACKs, Visibility & DLQ (M4)

The reliability layer provides per-message acknowledgment semantics on top of the Kafka-style offset-based model.

```
HYBRID ACK MODEL:
┌─────────────────────────────────────────────────────────────────┐
│                                                                  │
│  KAFKA-STYLE (offset only):                                     │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Offset:  0   1   2   3   4   5   6   7   8   9             │ │
│  │ Status:  ✓   ✓   ✓   ✓   ✓   │                             │ │
│  │                              │                             │ │
│  │                         committed=5                        │ │
│  │                                                            │ │
│  │ PROBLEM: If msg 3 fails, can't skip. Must reprocess 3,4,5  │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  GOQUEUE HYBRID:                                                │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Offset:  0   1   2   3   4   5   6   7   8   9             │ │
│  │ Status:  ✓   ✓   ✓   ✗   ✓   ✓   ?   ?   ?   ?             │ │
│  │                      │                                     │ │
│  │              msg 3 in retry queue                          │ │
│  │              (will be redelivered)                         │ │
│  │                                                            │ │
│  │ Committed offset = 2 (last contiguous ACKed)               │ │
│  │ On crash: Resume from offset 3 (natural recovery)          │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

#### Receipt Handle Format
```
{topic}:{partition}:{offset}:{deliveryCount}:{nonce}

Example: "orders:0:42:1:a1b2c3d4e5f67890"

WHY THIS FORMAT?
- Parseable: Extract message location without database lookup
- Debuggable: Human-readable for troubleshooting
- Unique: Nonce prevents replay attacks (new per delivery)
```

#### Visibility Tracker (Min-Heap Implementation)
```go
// Min-heap orders by deadline (earliest expiring at top)
type visibilityHeap []*visibilityItem

type visibilityItem struct {
    receiptHandle string
    deadline      time.Time    // When visibility expires
    message       *InFlightMessage
    index         int          // Heap index for O(log n) updates
}

// Complexity:
// - Track:    O(log n) heap push
// - Untrack:  O(log n) heap remove
// - Peek:     O(1) check earliest deadline
// - Extend:   O(log n) update + reheap
```

#### ACK Semantics
```
┌─────────────────────────────────────────────────────────────────┐
│ ACK (Success)                                                   │
│ ─────────────                                                   │
│ • Message is fully processed                                    │
│ • Will not be redelivered                                       │
│ • Committed offset may advance (if contiguous)                  │
│ • Use: Normal successful processing                             │
├─────────────────────────────────────────────────────────────────┤
│ NACK (Transient Failure)                                        │
│ ────────────────────────                                        │
│ • Processing failed, but may succeed on retry                   │
│ • Message scheduled for retry after backoff                     │
│ • DeliveryCount incremented                                     │
│ • After MaxRetries → automatic DLQ                              │
│ • Use: Database timeout, network error, temporary failure       │
├─────────────────────────────────────────────────────────────────┤
│ REJECT (Permanent Failure)                                      │
│ ─────────────────────────                                       │
│ • Message is "poison" (can never succeed)                       │
│ • Immediately routed to DLQ                                     │
│ • No retry attempts                                             │
│ • Use: Invalid format, schema error, business rule violation    │
└─────────────────────────────────────────────────────────────────┘
```

#### Exponential Backoff
```go
// Backoff calculation
backoff := baseMs * pow(multiplier, attempt-1)
if backoff > maxMs {
    backoff = maxMs
}

// With base=1s, multiplier=2, max=60s:
// Attempt 1: 1s
// Attempt 2: 2s
// Attempt 3: 4s
// Attempt 4: 8s
// Attempt 5: 16s
// Attempt 6: 32s
// Attempt 7: 60s (capped)
```

#### Dead Letter Queue Pattern
```
┌─────────────────┐       Route        ┌─────────────────┐
│   orders        │ ─────────────────► │   orders.dlq    │
└─────────────────┘                    └─────────────────┘
                                              │
DLQ Message Contains:                         │
├─ Original message key/value                 │
├─ Original topic/partition/offset            │
├─ Delivery attempts count                    │
├─ First delivery timestamp                   │
├─ Last delivery timestamp                    │
├─ Last consumer ID                           │
├─ DLQ reason (max_retries/rejected)         │
├─ Last error message                         │
└─ DLQ timestamp                              │
```

## Key Technical Decisions

### Decision 1: Embedded vs External Coordination
**Choice:** Embedded (no ZooKeeper/etcd dependency)
**Rationale:** Simplicity for single-node, lease-based for multi-node
**Trade-off:** Less battle-tested than Raft, but simpler to understand/debug

### Decision 2: Index Granularity
**Choice:** Sparse index (every 4KB)
**Rationale:** Balance memory usage vs lookup latency
**Trade-off:** Requires forward scan after binary search

### Decision 3: Visibility Timeout Storage
**Choice:** In-memory index with WAL
**Rationale:** Fast lookup, durable across restarts
**Trade-off:** Memory grows with in-flight messages

### Decision 4: Timer Wheel vs Heap
**Choice:** Hierarchical timer wheel
**Rationale:** O(1) insert/delete vs O(log n) for heap
**Trade-off:** Fixed time resolution, more complex implementation

## Error Handling Patterns

### Retry with Exponential Backoff
```go
func withRetry(op func() error, maxRetries int) error {
    backoff := 100 * time.Millisecond
    for i := 0; i < maxRetries; i++ {
        if err := op(); err == nil {
            return nil
        }
        time.Sleep(backoff)
        backoff *= 2
        if backoff > 30*time.Second {
            backoff = 30 * time.Second
        }
    }
    return ErrMaxRetriesExceeded
}
```

### Circuit Breaker for Node Communication
```go
// If node repeatedly fails, stop trying
type NodeCircuit struct {
    failures    int
    lastFailure time.Time
    state       CircuitState // closed, open, half-open
}
```

## Concurrency Patterns

### Per-Partition Locking
Each partition has its own lock. Operations on different partitions are independent.

```go
type Partition struct {
    mu  sync.RWMutex
    log *Log
    // ...
}

// Append - exclusive write lock
func (p *Partition) Append(msg *Message) error {
    p.mu.Lock()
    defer p.mu.Unlock()
    return p.log.Append(msg)
}

// Read - shared read lock
func (p *Partition) Read(offset int64) (*Message, error) {
    p.mu.RLock()
    defer p.mu.RUnlock()
    return p.log.Read(offset)
}
```

### Non-Blocking Channel Sends
For event notifications where dropped events are acceptable:

```go
select {
case eventCh <- event:
    // sent
default:
    // channel full, skip
}
```
