# GoQueue System Architecture

> Comprehensive architecture of GoQueue - a distributed message queue inspired by Kafka, SQS, and RabbitMQ.

---

## High-Level Overview

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                    GOQUEUE CLUSTER                                       │
│                                                                                          │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐│
│  │                              CONTROL PLANE (Milestone 10-11)                         ││
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                       ││
│  │  │ Cluster         │  │ Metadata        │  │ Leader          │                       ││
│  │  │ Coordinator     │  │ Store           │  │ Election        │                       ││
│  │  │                 │  │                 │  │ (Raft-like)     │                       ││
│  │  │ • Membership    │  │ • Topic configs │  │                 │                       ││
│  │  │ • Health checks │  │ • Partition map │  │ • Lease-based   │                       ││
│  │  │ • Failure detect│  │ • Consumer grps │  │ • ISR tracking  │                       ││
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘                       ││
│  └──────────────────────────────────────────────────────────────────────────────────────┘│
│                                                                                          │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐│
│  │                                  NODE 1 (Broker)                                     ││
│  │  ┌────────────────────────────────────────────────────────────────────────────────┐  ││
│  │  │                            API LAYER (Milestone 3, 14)                         │  ││
│  │  │                                                                                │  ││
│  │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │  ││
│  │  │  │   HTTP/REST │  │    gRPC     │  │  Admin API  │  │  Metrics    │            │  ││
│  │  │  │   (chi)     │  │  (future)   │  │             │  │  Endpoint   │            │  ││
│  │  │  │             │  │             │  │ • Topics    │  │ /metrics    │            │  ││
│  │  │  │ • Publish   │  │ • Streaming │  │ • Groups    │  │ Prometheus  │            │  ││
│  │  │  │ • Consume   │  │ • Bi-direct │  │ • Offsets   │  │             │            │  ││
│  │  │  │ • Long-poll │  │             │  │ • DLQ       │  │             │            │  ││
│  │  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘            │  ││
│  │  └────────────────────────────────────────────────────────────────────────────────┘  ││
│  │                                         │                                            ││
│  │                                         ▼                                            ││
│  │  ┌────────────────────────────────────────────────────────────────────────────────┐  ││
│  │  │                          BROKER LAYER (Milestone 2-4)                          │  ││
│  │  │                                                                                │  ││
│  │  │  ┌──────────────────────────────────────────────────────────────────────────┐  │  ││
│  │  │  │                        Topic Manager                                     │  │  ││
│  │  │  │  • Topic CRUD            • Partition routing      • Schema validation    │  │  ││
│  │  │  └──────────────────────────────────────────────────────────────────────────┘  │  ││
│  │  │                                    │                                           │  ││
│  │  │          ┌─────────────────────────┼─────────────────────────┐                 │  ││
│  │  │          ▼                         ▼                         ▼                 │  ││
│  │  │  ┌─────────────┐           ┌─────────────┐           ┌─────────────┐           │  ││
│  │  │  │ Partition 0 │           │ Partition 1 │           │ Partition 2 │           │  ││
│  │  │  │  (Leader)   │           │  (Follower) │           │  (Leader)   │           │  ││
│  │  │  │             │           │             │           │             │           │  ││
│  │  │  │ ┌─────────┐ │           │ ┌─────────┐ │           │ ┌─────────┐ │           │  ││
│  │  │  │ │   Log   │ │           │ │   Log   │ │           │ │   Log   │ │           │  ││
│  │  │  │ └─────────┘ │           │ └─────────┘ │           │ └─────────┘ │           │  ││
│  │  │  └─────────────┘           └─────────────┘           └─────────────┘           │  ││
│  │  │                                                                                │  ││
│  │  │  ┌──────────────────────────────────────────────────────────────────────────┐  │  ││
│  │  │  │                    Group Coordinator (Milestone 3)                       │  │  ││
│  │  │  │  • Consumer group membership    • Partition assignment (range/sticky)    │  │  ││
│  │  │  │  • Heartbeat monitoring         • Rebalance orchestration                │  │  ││
│  │  │  │  • Session timeout detection    • Generation ID tracking                 │  │  ││
│  │  │  └──────────────────────────────────────────────────────────────────────────┘  │  ││
│  │  │                                                                                │  ││
│  │  │  ┌──────────────────────────────────────────────────────────────────────────┐  │  ││
│  │  │  │                    Offset Manager (Milestone 3)                          │  │  ││
│  │  │  │  • Per-group offset storage     • Auto-commit (5s interval)              │  │  ││
│  │  │  │  • Manual commit API            • Offset reset (earliest/latest)         │  │  ││
│  │  │  │  • File-based persistence       • Consumer lag calculation               │  │  ││
│  │  │  └──────────────────────────────────────────────────────────────────────────┘  │  ││
│  │  │                                                                                │  ││
│  │  │  ┌──────────────────────────────────────────────────────────────────────────┐  │  ││
│  │  │  │                 ★ Reliability Layer (Milestone 4) ★                      │  │  ││
│  │  │  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                 │  │  ││
│  │  │  │  │ ACK Manager   │  │ Visibility    │  │ DLQ Router    │                 │  │  ││
│  │  │  │  │               │  │ Tracker       │  │               │                 │  │  ││
│  │  │  │  │ • Per-msg ACK │  │               │  │ • Max retries │                 │  │  ││
│  │  │  │  │ • NACK + retry│  │ • In-flight   │  │ • TTL expired │                 │  │  ││
│  │  │  │  │ • Batch ACK   │  │ • Timeout     │  │ • Poison msgs │                 │  │  ││
│  │  │  │  │               │  │ • Redeliver   │  │ • Auto-create │                 │  │  ││
│  │  │  │  └───────────────┘  └───────────────┘  └───────────────┘                 │  │  ││
│  │  │  └──────────────────────────────────────────────────────────────────────────┘  │  ││
│  │  │                                                                                │  ││
│  │  │  ┌──────────────────────────────────────────────────────────────────────────┐  │  ││
│  │  │  │              ★ Advanced Features (Milestone 5-7) ★                       │  │  ││
│  │  │  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                 │  │  ││
│  │  │  │  │ Timer Wheel   │  │ Priority      │  │ Message       │                 │  │  ││
│  │  │  │  │ (Delay Queue) │  │ Lanes         │  │ Tracer        │                 │  │  ││
│  │  │  │  │               │  │               │  │               │                 │  │  ││
│  │  │  │  │ • Hierarchical│  │ • High/Med/Lo │  │ • Trace ID    │                 │  │  ││
│  │  │  │  │ • O(1) insert │  │ • Fair queue  │  │ • Event log   │                 │  │  ││
│  │  │  │  │ • Persistent  │  │ • Anti-starve │  │ • Query API   │                 │  │  ││
│  │  │  │  └───────────────┘  └───────────────┘  └───────────────┘                 │  │  ││
│  │  │  └──────────────────────────────────────────────────────────────────────────┘  │  ││
│  │  └────────────────────────────────────────────────────────────────────────────────┘  ││
│  │                                         │                                            ││
│  │                                         ▼                                            ││
│  │  ┌────────────────────────────────────────────────────────────────────────────────┐  ││
│  │  │                         STORAGE LAYER (Milestone 1)                            │  ││
│  │  │                                                                                │  ││
│  │  │  ┌──────────────────────────────────────────────────────────────────────────┐  │  ││
│  │  │  │                          Append-Only Log                                 │  │  ││
│  │  │  │                                                                          │  │  ││
│  │  │  │   Segment 0 (sealed)    Segment 1 (sealed)    Segment 2 (active)         │  │  ││
│  │  │  │  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐         │  │  ││
│  │  │  │  │ 00000000.log    │   │ 00001000.log    │   │ 00002000.log    │         │  │  ││
│  │  │  │  │ [msg0..msg999]  │   │ [msg1000..1999] │   │ [msg2000..now]  │         │  │  ││
│  │  │  │  │                 │   │                 │   │                 │         │  │  ││
│  │  │  │  │ 00000000.index  │   │ 00001000.index  │   │ 00002000.index  │         │  │  ││
│  │  │  │  │ [sparse offset] │   │ [sparse offset] │   │ [sparse offset] │         │  │  ││
│  │  │  │  └─────────────────┘   └─────────────────┘   └─────────────────┘         │  │  ││
│  │  │  │                                                                          │  │  ││
│  │  │  │  Properties:                                                             │  │  ││
│  │  │  │  • 64MB segment size         • CRC32 checksums                           │  │  ││
│  │  │  │  • 4KB index granularity     • 1s fsync interval                         │  │  ││
│  │  │  │  • Binary encoding           • Buffered I/O                              │  │  ││
│  │  │  └──────────────────────────────────────────────────────────────────────────┘  │  ││
│  │  │                                                                                │  ││
│  │  │  ┌──────────────────────────────────────────────────────────────────────────┐  │  ││
│  │  │  │                         Metadata Storage                                 │  │  ││
│  │  │  │  • offsets/<group>.json      • inflight/<consumer>.json                  │  │  ││
│  │  │  │  • topics/<topic>/meta.json  • dlq/<topic>/meta.json                     │  │  ││
│  │  │  └──────────────────────────────────────────────────────────────────────────┘  │  ││
│  │  └────────────────────────────────────────────────────────────────────────────────┘  ││
│  └──────────────────────────────────────────────────────────────────────────────────────┘│
│                                                                                          │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐│
│  │                              NODE 2 & NODE 3 (Replicas)                              ││
│  │                                 (Same structure)                                     ││
│  │                                                                                      ││
│  │   Replication Flow (Milestone 11):                                                   ││
│  │   Leader ──────► Follower 1                                                          ││
│  │          └─────► Follower 2                                                          ││
│  │                                                                                      ││
│  │   ISR (In-Sync Replicas): Followers within lag threshold                             ││
│  └──────────────────────────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────────────────────┘
```
## Client Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                   CLIENT APPLICATIONS                                    │
│                                                                                          │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                              PRODUCER CLIENT                                       │  │
│  │                                                                                    │  │
│  │  ┌──────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                           Accumulator (Batching)                             │  │  │
│  │  │                                                                              │  │  │
│  │  │   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                          │  │  │
│  │  │   │ Partition 0 │  │ Partition 1 │  │ Partition 2 │                          │  │  │
│  │  │   │ Batch Queue │  │ Batch Queue │  │ Batch Queue │                          │  │  │
│  │  │   │             │  │             │  │             │                          │  │  │
│  │  │   │ [msg,msg,..]│  │ [msg,msg,..]│  │ [msg,msg,..]│                          │  │  │
│  │  │   └─────────────┘  └─────────────┘  └─────────────┘                          │  │  │
│  │  │                                                                              │  │  │
│  │  │   Flush Triggers:                                                            │  │  │
│  │  │   • BatchSize = 100 messages       • LingerMs = 5ms (or 0 for immediate)     │  │  │
│  │  │   • BatchBytes = 64KB              • Explicit Flush() call                   │  │  │
│  │  └──────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                         │                                          │  │
│  │                                         ▼                                          │  │
│  │  ┌──────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                            Partitioner                                       │  │  │
│  │  │                                                                              │  │  │
│  │  │   Key != nil ─────► Murmur3 Hash ─────► partition = hash % numPartitions     │  │  │
│  │  │   Key == nil ─────► Round Robin  ─────► partition = counter++ % N            │  │  │
│  │  │   Explicit   ─────► Manual       ─────► partition = specified                │  │  │
│  │  └──────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                         │                                          │  │
│  │                                         ▼                                          │  │
│  │  ┌──────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                         Acknowledgment Handler                               │  │  │
│  │  │                                                                              │  │  │
│  │  │   AckMode.None   ─────► Fire and forget (no wait)                            │  │  │
│  │  │   AckMode.Leader ─────► Wait for broker leader ACK                           │  │  │
│  │  │   AckMode.All    ─────► Wait for all ISR replicas (future)                   │  │  │
│  │  └──────────────────────────────────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                          │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                              CONSUMER CLIENT                                       │  │
│  │                                                                                    │  │
│  │  ┌──────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                         Consumer Group Member                                │  │  │
│  │  │                                                                              │  │  │
│  │  │   Member ID: client-abc123-f7e8d9                                            │  │  │
│  │  │   Group ID:  order-processors                                                │  │  │
│  │  │   Generation: 3                                                              │  │  │
│  │  │   Assigned:  [orders-0, orders-2]                                            │  │  │
│  │  └──────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                         │                                          │  │
│  │                     ┌───────────────────┴───────────────────┐                      │  │
│  │                     ▼                                       ▼                      │  │
│  │  ┌─────────────────────────────────┐  ┌──────────────────────────────────┐         │  │
│  │  │        Heartbeat Thread         │  │         Poll Thread              │         │  │
│  │  │                                 │  │                                  │         │  │
│  │  │  Every 3s ────► Coordinator     │  │  Long-poll (30s timeout)         │         │  │
│  │  │                                 │  │  Fetch from assigned partitions  │         │  │
│  │  │  Miss 10 heartbeats = dead      │  │  Return batch of messages        │         │  │
│  │  │  (30s session timeout)          │  │                                  │         │  │
│  │  └─────────────────────────────────┘  └──────────────────────────────────┘         │  │
│  │                                         │                                          │  │
│  │                                         ▼                                          │  │
│  │  ┌──────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                     ★ Message Processing (M4) ★                              │  │  │
│  │  │                                                                              │  │  │
│  │  │   ┌──────────────────────────────────────────────────────────────────────┐   │  │  │
│  │  │   │ for msg := range messages {                                          │   │  │  │
│  │  │   │     // Message is now IN-FLIGHT (invisible to others)                │   │  │  │
│  │  │   │     // Visibility timeout starts (default 30s)                       │   │  │  │
│  │  │   │                                                                      │   │  │  │
│  │  │   │     err := process(msg)                                              │   │  │  │
│  │  │   │                                                                      │   │  │  │
│  │  │   │     if err == nil {                                                  │   │  │  │
│  │  │   │         consumer.Ack(msg.ID)        // Remove from queue             │   │  │  │
│  │  │   │     } else if retryable(err) {                                       │   │  │  │
│  │  │   │         consumer.Nack(msg.ID)       // Redeliver after backoff       │   │  │  │
│  │  │   │     } else {                                                         │   │  │  │
│  │  │   │         consumer.Reject(msg.ID)     // Send to DLQ                   │   │  │  │
│  │  │   │     }                                                                │   │  │  │
│  │  │   │ }                                                                    │   │  │  │
│  │  │   └──────────────────────────────────────────────────────────────────────┘   │  │  │
│  │  │                                                                              │  │  │
│  │  │   If visibility timeout expires without ACK/NACK:                            │  │  │
│  │  │   • Message becomes visible again (redelivered)                              │  │  │
│  │  │   • Retry counter incremented                                                │  │  │
│  │  │   • After max retries → DLQ                                                  │  │  │
│  │  └──────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                         │                                          │  │
│  │                                         ▼                                          │  │
│  │  ┌──────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                          Offset Management                                   │  │  │
│  │  │                                                                              │  │  │
│  │  │   Auto-commit: Every 5s, commit offsets for processed messages               │  │  │
│  │  │   Manual:      consumer.Commit() after processing batch                      │  │  │
│  │  │                                                                              │  │  │
│  │  │   ⚠ ️️ ️With per-message ACK (M4), offset = last contiguously ACKed offset      │  │  │
│  │  └──────────────────────────────────────────────────────────────────────────── ─┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────── ───┘  │
└──────────────────────────────────────────────────────────────────────────────────── ─────┘
```

---

## Message Flow Diagrams

### 1. Publish Flow

```
Producer                     Broker                      Storage
   │                           │                            │
   │  1. Send(key, value)      │                            │
   │  ─────────────────────►   │                            │
   │                           │                            │
   │  2. Partition = hash(key) │                            │
   │     % numPartitions       │                            │
   │                           │                            │
   │                           │  3. Append to log          │
   │                           │  ─────────────────────►    │
   │                           │                            │
   │                           │  4. Write to segment       │
   │                           │     Update index           │
   │                           │  ◄─────────────────────    │
   │                           │                            │
   │  5. Return (partition,    │                            │
   │     offset) or error      │                            │
   │  ◄─────────────────────   │                            │
   │                           │                            │
```

### 2. Consume Flow (Current - Offset Based)

```
Consumer                   Group Coordinator              Partition Log
   │                              │                            │
   │  1. JoinGroup(groupId)       │                            │
   │  ────────────────────────►   │                            │
   │                              │                            │
   │  2. Assignment:              │                            │
   │     [partition-0, 2]         │                            │
   │  ◄────────────────────────   │                            │
   │                              │                            │
   │  3. Poll(timeout=30s)        │                            │
   │  ────────────────────────────┼───────────────────────►    │
   │                              │                            │
   │  4. Read from committed      │                            │
   │     offset                   │                            │
   │  ◄───────────────────────────┼────────────────────────    │
   │                              │                            │
   │  5. Process messages...      │                            │
   │                              │                            │
   │  6. CommitOffsets            │                            │
   │  ────────────────────────►   │                            │
   │                              │  7. Persist offset         │
   │                              │  ───────────────────────►  │
   │                              │                            │
```

### 3. Consume Flow (Milestone 4 - Per-Message ACK)

```
Consumer                    Broker                    In-Flight Index         DLQ
   │                          │                            │                   │
   │  1. Poll()               │                            │                   │
   │  ───────────────────►    │                            │                   │
   │                          │                            │                   │
   │  2. Messages + Receipt   │  3. Mark in-flight         │                   │
   │     Handles              │  ─────────────────────►    │                   │
   │  ◄───────────────────    │     (visibility timer      │                   │
   │                          │      starts)               │                   │
   │                          │                            │                   │
   │  4. Process msg[0]       │                            │                   │
   │     SUCCESS              │                            │                   │
   │                          │                            │                   │
   │  5. Ack(receipt[0])      │  6. Remove from            │                   │
   │  ───────────────────►    │     in-flight              │                   │
   │                          │  ─────────────────────►    │                   │
   │                          │                            │                   │
   │  7. Process msg[1]       │                            │                   │
   │     FAILED (retryable)   │                            │                   │
   │                          │                            │                   │
   │  8. Nack(receipt[1])     │  9. Schedule retry         │                   │
   │  ───────────────────►    │     (with backoff)         │                   │
   │                          │  ─────────────────────►    │                   │
   │                          │                            │                   │
   │  10. Process msg[2]      │                            │                   │
   │      FAILED (poison)     │                            │                   │
   │                          │                            │                   │
   │  11. Reject(receipt[2])  │  12. Route to DLQ          │                   │
   │  ───────────────────►    │  ───────────────────────────────────────────►  │
   │                          │                            │                   │
   │  --- TIMEOUT CASE ---    │                            │                   │
   │                          │                            │                   │
   │  (Consumer dies)         │  13. Visibility timeout    │                   │
   │                          │      expires               │                   │
   │                          │  ◄─────────────────────    │                   │
   │                          │                            │                   │
   │                          │  14. Increment retry       │                   │
   │                          │      count                 │                   │
   │                          │                            │                   │
   │                          │  15. Redeliver to          │                   │
   │                          │      another consumer      │                   │
   │                          │      (or same if alive)    │                   │
   │                          │                            │                   │
```

### 4. Consumer Group Rebalance Flow

```
                           Group Coordinator
                                  │
   Consumer 1        Consumer 2   │   Consumer 3
       │                 │        │        │
       │                 │        │        │  1. JoinGroup
       │                 │        │        │  ──────────►
       │                 │        │        │
       │                 │        │  2. Trigger Rebalance
       │                 │        │  ◄──────────────────
       │                 │        │
       │  3. "Rebalancing, stop fetching"
       │  ◄───────────────────────┤
       │                 │        │
       │                 │  ◄─────┤  (same notification)
       │                 │        │
       │  4. SyncGroup   │        │
       │  ──────────────────────► │
       │                 │        │
       │                 │  ────► │  (all members sync)
       │                 │        │
       │                 │        │  ◄──── (new member too)
       │                 │        │
       │  5. New assignment (Range Strategy):
       │     C1: [P0]             │
       │     C2: [P1]             │
       │     C3: [P2]             │
       │  ◄───────────────────────┤
       │                 │        │
       │                 │  ◄─────┤
       │                 │        │
       │                 │        │  ◄────
       │                 │        │
       │  6. Resume fetching from assigned partitions
       │                 │        │
```

---

## Data Structures

### Message Binary Format

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            MESSAGE BINARY FORMAT                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Offset  Size   Field           Description                                  │
│  ──────────────────────────────────────────────────────────────────────────  │
│  0       2      Magic           0x47 0x51 ("GQ" - GoQueue identifier)        │
│  2       1      Version         Format version (currently 1)                 │
│  3       1      Attributes      Compression, etc. (reserved)                 │
│  4       8      Offset          Message offset in partition (int64)          │
│  12      8      Timestamp       Unix timestamp in nanoseconds (int64)        │
│  20      4      KeyLength       Length of key (-1 if null)                   │
│  24      N      Key             Message key bytes                            │
│  24+N    4      ValueLength     Length of value                              │
│  28+N    M      Value           Message payload bytes                        │
│  28+N+M  4      CRC32           Castagnoli checksum of bytes [0, 28+N+M)     │
│                                                                              │
│  Total Size: 32 + KeyLength + ValueLength bytes (minimum 32 bytes)           │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

Example (key="order-123", value="{...json...}"):

  ┌────┬───┬───┬──────────┬──────────────────┬───┬───────────┬───┬─────────┬────────┐
  │ GQ │ 1 │ 0 │ 00000042 │ 1735500000000000 │ 9 │ order-123 │ 48│ {...}   │ CRC32  │
  └────┴───┴───┴──────────┴──────────────────┴───┴───────────┴───┴─────────┴────────┘
   2B   1B  1B     8B            8B            4B     9B       4B   48B       4B
```

### Index Entry Format

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           INDEX ENTRY FORMAT                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Offset  Size   Field           Description                                  │
│  ──────────────────────────────────────────────────────────────────────────  │
│  0       8      RelativeOffset  Offset relative to segment base (int64)      │
│  8       8      Position        Byte position in segment file (int64)        │
│                                                                              │
│  Total Size: 16 bytes per entry                                              │
│                                                                              │
│  Index is SPARSE: One entry every 4KB of log data                            │
│  Lookup: Binary search index → scan forward in log                           │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### In-Flight Message Entry (Milestone 4)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         IN-FLIGHT MESSAGE ENTRY                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Field              Type        Description                                  │
│  ──────────────────────────────────────────────────────────────────────────  │
│  ReceiptHandle      string      Unique identifier for this delivery attempt  │
│  MessageID          string      Original message identifier                  │
│  Topic              string      Source topic                                 │
│  Partition          int         Source partition                             │
│  Offset             int64       Message offset in partition                  │
│  ConsumerID         string      Consumer that received it                    │
│  GroupID            string      Consumer group                               │
│  DeliveryCount      int         Number of delivery attempts                  │
│  FirstDeliveryTime  time.Time   When first delivered                         │
│  LastDeliveryTime   time.Time   When last delivered (current attempt)        │
│  VisibilityDeadline time.Time   When message becomes visible again           │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
data/
├── topics/
│   ├── orders/
│   │   ├── partition-0/
│   │   │   ├── 00000000000000000000.log      # Segment file (messages)
│   │   │   ├── 00000000000000000000.index    # Offset index
│   │   │   ├── 00000000000000001000.log      # Next segment
│   │   │   └── 00000000000000001000.index
│   │   ├── partition-1/
│   │   │   └── ...
│   │   └── partition-2/
│   │       └── ...
│   │
│   └── orders.dlq/                           # Dead Letter Queue (auto-created)
│       └── partition-0/
│           └── ...
│
├── offsets/
│   ├── order-processors.json                 # Consumer group offsets
│   └── analytics-group.json
│
├── inflight/                                 # Milestone 4
│   ├── order-processors/
│   │   ├── member-abc123.json               # In-flight messages per consumer
│   │   └── member-def456.json
│   └── analytics-group/
│       └── ...
│
└── metadata/
    ├── topics.json                           # Topic configurations
    ├── groups.json                           # Consumer group metadata
    └── cluster.json                          # Cluster metadata (future)
```

---

## Configuration Reference

```yaml
# config.yaml
broker:
  id: "broker-1"
  host: "0.0.0.0"
  port: 8080
  data_dir: "./data"

storage:
  segment_size_bytes: 67108864      # 64MB
  index_interval_bytes: 4096        # 4KB (sparse index)
  fsync_interval_ms: 1000           # 1 second
  retention_hours: 168              # 7 days
  retention_bytes: -1               # Unlimited (-1)

producer:
  batch_size: 100                   # Messages per batch
  linger_ms: 5                      # Max wait before flush
  batch_bytes: 65536                # 64KB max batch
  ack_mode: "leader"                # none, leader, all
  retries: 3                        # Retry count
  retry_backoff_ms: 100             # Initial backoff

consumer:
  session_timeout_ms: 30000         # 30 seconds
  heartbeat_interval_ms: 3000       # 3 seconds
  max_poll_records: 500             # Messages per poll
  auto_commit: true
  auto_commit_interval_ms: 5000     # 5 seconds
  
  # Milestone 4
  visibility_timeout_ms: 30000      # 30 seconds
  max_retries: 3                    # Before DLQ
  backoff_multiplier: 2             # Exponential backoff

dlq:
  enabled: true
  suffix: ".dlq"                    # topic.dlq
  retention_hours: 336              # 14 days (longer than main)

# Future milestones
cluster:
  nodes:
    - "broker-1:8080"
    - "broker-2:8080"
    - "broker-3:8080"
  replication_factor: 3
  min_isr: 2
```

---

## Component Interaction Matrix

```
┌─────────────────────┬──────┬───────┬───────┬─────────┬───────┬───────┬─────┬───────┐
│                     │ HTTP │ Topic │ Part- │ Group   │Offset │ ACK   │ DLQ │ Timer │
│                     │ API  │ Mgr   │ ition │ Coord   │ Mgr   │ Mgr   │     │ Wheel │
├─────────────────────┼──────┼───────┼───────┼─────────┼───────┼───────┼─────┼───────┤
│ HTTP API            │  -   │   W   │   R   │    W    │   W   │   W   │  R  │   -   │
│ Topic Manager       │  -   │   -   │   W   │    -    │   -   │   -   │  W  │   -   │
│ Partition           │  -   │   R   │   -   │    -    │   -   │   -   │  -  │   -   │
│ Group Coordinator   │  -   │   R   │   R   │    -    │   W   │   R   │  -  │   -   │
│ Offset Manager      │  -   │   -   │   R   │    R    │   -   │   R   │  -  │   -   │
│ ACK Manager (M4)    │  -   │   R   │   R   │    R    │   W   │   -   │  W  │   R   │
│ DLQ Router (M4)     │  -   │   W   │   W   │    -    │   -   │   R   │  -  │   -   │
│ Timer Wheel (M5)    │  -   │   R   │   W   │    -    │   -   │   -   │  -  │   -   │
└─────────────────────┴──────┴───────┴───────┴─────────┴───────┴───────┴─────┴───────┘

Legend: W = Writes to, R = Reads from, - = No direct interaction
```

---

## Milestone 4 Component Detail

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                            RELIABILITY LAYER (Milestone 4)                              │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │                              ACK MANAGER                                          │  │
│  │                                                                                   │  │
│  │   Responsibilities:                                                               │  │
│  │   • Track per-message acknowledgment state                                        │  │
│  │   • Generate unique receipt handles for each delivery                             │  │
│  │   • Process ACK/NACK/REJECT commands                                              │  │
│  │   • Coordinate with Offset Manager for committed offset calculation               │  │
│  │                                                                                   │  │
│  │   State Machine (per message delivery):                                           │  │
│  │                                                                                   │  │
│  │      ┌──────────┐    Poll()    ┌───────────┐    Ack()    ┌───────────┐            │  │
│  │      │ PENDING  │ ───────────► │ IN_FLIGHT │ ──────────► │  ACKED    │            │  │
│  │      └──────────┘              └───────────┘             └───────────┘            │  │
│  │                                      │                                            │  │
│  │                                      │ Nack() or                                  │  │
│  │                                      │ Timeout                                    │  │
│  │                                      ▼                                            │  │
│  │                               ┌───────────┐                                       │  │
│  │                               │ SCHEDULED │ ──► (retry after backoff)             │  │
│  │                               │ FOR RETRY │                                       │  │
│  │                               └───────────┘                                       │  │
│  │                                      │                                            │  │
│  │                                      │ MaxRetries exceeded                        │  │
│  │                                      │ or Reject()                                │  │
│  │                                      ▼                                            │  │
│  │                               ┌───────────┐                                       │  │
│  │                               │   DLQ     │                                       │  │
│  │                               └───────────┘                                       │  │
│  │                                                                                   │  │
│  └───────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │                           VISIBILITY TRACKER                                      │  │
│  │                                                                                   │  │
│  │   Data Structure: Min-Heap by visibility deadline                                 │  │
│  │                                                                                   │  │
│  │   ┌─────────────────────────────────────────────────────────────────────┐         │  │
│  │   │ Heap: [(deadline=10:30:05, msg1), (deadline=10:30:08, msg2), ...]  │          │  │
│  │   └─────────────────────────────────────────────────────────────────────┘         │  │
│  │                                                                                   │  │
│  │   Background Goroutine:                                                           │  │
│  │   • Tick every 100ms                                                              │  │
│  │   • Pop expired entries from heap                                                 │  │
│  │   • For each expired: increment retry count, schedule redelivery                  │  │
│  │                                                                                   │  │
│  │   Operations:                                                                     │  │
│  │   • AddInFlight(msg, timeout) → insert into heap                                  │  │
│  │   • RemoveInFlight(receipt)   → mark as done (lazy delete)                        │  │
│  │   • ExtendVisibility(receipt, newTimeout) → update deadline                       │  │
│  │                                                                                   │  │
│  └───────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │                              DLQ ROUTER                                           │  │
│  │                                                                                   │  │
│  │   Trigger Conditions:                                                             │  │
│  │   • Explicit Reject() call (poison message)                                       │  │
│  │   • MaxRetries exceeded after repeated timeouts/NACKs                             │  │
│  │   • Message TTL expired (if configured)                                           │  │
│  │                                                                                   │  │
│  │   DLQ Message Format (preserves original + metadata):                             │  │
│  │   {                                                                               │  │
│  │     "originalTopic": "orders",                                                    │  │
│  │     "originalPartition": 2,                                                       │  │
│  │     "originalOffset": 12345,                                                      │  │
│  │     "originalTimestamp": "2025-01-15T10:30:00Z",                                  │  │
│  │     "originalKey": "order-123",                                                   │  │
│  │     "originalValue": "{...}",                                                     │  │
│  │     "deliveryAttempts": 4,                                                        │  │
│  │     "lastError": "processing timeout",                                            │  │
│  │     "dlqTimestamp": "2025-01-15T10:35:00Z",                                       │  │
│  │     "dlqReason": "MAX_RETRIES_EXCEEDED"                                           │  │
│  │   }                                                                               │  │
│  │                                                                                   │  │
│  │   Auto-Creation:                                                                  │  │
│  │   • DLQ topic: {original-topic}.dlq                                               │  │
│  │   • Same partition count as original                                              │  │
│  │   • Longer retention (14d default vs 7d)                                          │  │
│  │                                                                                   │  │
│  └───────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Comparison with Industry Systems

| Feature | GoQueue | Kafka | RabbitMQ | SQS |
|---------|---------|-------|----------|-----|
| **Message Model** | Log-based | Log-based | Broker-based | Queue-based |
| **Ordering** | Per-partition | Per-partition | Per-queue | FIFO optional |
| **ACK Model** | Per-message + offset | Offset-based | Per-message | Per-message |
| **Visibility Timeout** | ✅ (M4) | ❌ | ❌ | ✅ |
| **Dead Letter Queue** | ✅ (M4) | ❌ (external) | ✅ | ✅ |
| **Delay Messages** | ✅ Native (M5) | ❌ | Plugin | ✅ (15min max) |
| **Priority Lanes** | ✅ (M6) | ❌ | ✅ | ❌ |
| **Consumer Groups** | ✅ (M3) | ✅ | ❌ | ❌ |
| **Rebalance** | Cooperative (M12) | Cooperative | N/A | N/A |
| **Replication** | ISR (M11) | ISR | Mirroring | Managed |

---

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|-----------------|-------|
| Publish (single) | O(1) amortized | Append to active segment |
| Publish (batch) | O(n) | n = batch size |
| Index lookup | O(log n) | Binary search in sparse index |
| Sequential read | O(n) | n = messages to read |
| ACK (per-message) | O(log n) | Heap operations for visibility |
| Rebalance (eager) | O(p × c) | p = partitions, c = consumers |
| Rebalance (cooperative) | O(Δp) | Only changed assignments |

---

## Future Roadmap Visualization

```
Phase 1: Foundations           Phase 2: Advanced            Phase 3: Distribution
────────────────────           ─────────────────            ─────────────────────
                                                            
 [M1] Storage ✅                [M5] Delay Queue ⭐          [M10] Cluster
      │                              │                            │
      ▼                              ▼                            ▼
 [M2] Topics ✅                 [M6] Priority ⭐             [M11] Replication
      │                              │                            │
      ▼                              ▼                            ▼
 [M3] Consumer ✅               [M7] Tracing ⭐              [M12] Coop Rebalance ⭐
      │                              │                            │
      ▼                              ▼                            ▼
 [M4] Reliability              [M8] Schema                  [M13] Partition Scale
      (CURRENT)                      │                       
                                     ▼                       
                               [M9] Transactions             


Phase 4: Operations
───────────────────

 [M14] gRPC API ──► [M15] CLI ──► [M16] Metrics ──► [M17] Multi-tenant ──► [M18] K8s
```

---

*Last Updated: Milestone 3 Complete, Starting Milestone 4*
