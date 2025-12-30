# GoQueue - Progress

## Overall Status

**Phase**: 1 of 4
**Milestones**: 3/18 complete
**Tests**: 84 passing (storage: 42, broker: 18, api: 24)
**Started**: Session 1

## Phase Progress

### Phase 1: Foundations (3/4)
- [x] Milestone 1: Storage Engine & Append-Only Log ✅
- [x] Milestone 2: Topics, Partitions & Producer API ✅
- [x] Milestone 3: Consumer Groups & Offset Management ✅
- [ ] Milestone 4: Reliability - ACKs, Visibility & DLQ

### Phase 2: Advanced Features (0/5)
- [ ] Milestone 5: Native Delay & Scheduled Messages ⭐
- [ ] Milestone 6: Priority Lanes ⭐
- [ ] Milestone 7: Message Tracing ⭐
- [ ] Milestone 8: Schema Registry
- [ ] Milestone 9: Transactional Publish

### Phase 3: Distribution (0/4)
- [ ] Milestone 10: Cluster Formation & Metadata
- [ ] Milestone 11: Leader Election & Replication
- [ ] Milestone 12: Cooperative Rebalancing ⭐
- [ ] Milestone 13: Online Partition Scaling

### Phase 4: Operations (0/5)
- [ ] Milestone 14: gRPC API & Go Client (js/ts as well if time)
- [ ] Milestone 15: CLI Tool
- [ ] Milestone 16: Prometheus Metrics & Grafana
- [ ] Milestone 17: Multi-Tenancy & Quotas
- [ ] Milestone 18: Kubernetes & Chaos Testing

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
- [ ] Per-message ACK (Milestone 4)
- [ ] Visibility timeout (Milestone 4)
- [ ] Dead letter queue (Milestone 4)

### Differentiators (Key Features) ⭐
- [ ] Native delay messages (timer wheel)
- [ ] Priority lanes
- [ ] Message tracing
- [ ] Cooperative rebalancing

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

*None currently*

## Lessons Learned

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
