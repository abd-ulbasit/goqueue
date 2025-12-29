# GoQueue - Active Context

## Current Focus

**Phase**: 1 - Foundations
**Milestone**: 1 - Storage Engine & Append-Only Log
**Status**: ✅ COMPLETE

## What I'm Working On

### Completed
- ✅ Project structure (cmd, internal directories)
- ✅ Message struct with binary encoding (CRC32 Castagnoli)
- ✅ Append-only log writer with segment rollover
- ✅ Segment file management (64MB segments)
- ✅ Sparse index (4KB granularity) with binary search
- ✅ Partition abstraction over log
- ✅ Topic management with routing
- ✅ Broker with CRUD and publish/consume APIs
- ✅ Comprehensive test suite

### Next Steps (Milestone 2)
1. Add TCP server for remote connections
2. Implement wire protocol for producers/consumers
3. Add multi-partition topic support
4. Implement consumer groups

## Recent Changes

### Session 1 - Milestone 1 Implementation
**Completed**:
- Created binary message format with 30-byte header
  - Magic bytes: 0x47 0x51 ("GQ")
  - CRC32 Castagnoli for corruption detection
  - Supports keys up to 64KB, values up to 16MB
- Built segment file management
  - 64MB max segment size
  - Automatic sealing when full
  - Load/recovery from disk
- Created sparse index
  - 4KB byte granularity
  - Binary search for O(log n) lookups
  - Persists to .index files
- Implemented append-only log
  - Manages multiple segments
  - Automatic rollover
  - fsync every 1000ms (configurable)
- Added broker layer
  - Topic CRUD operations
  - Publish/Consume APIs
  - Statistics and metadata

## Resolved Decisions

### Decision: Binary Message Format ✅
**Chosen**: Custom binary with 30-byte header
**Reason**: Compact, fast parsing, CRC integrity

### Decision: Index Granularity ✅
**Chosen**: Every 4KB of data
**Reason**: Kafka-style balance, consistent index size

### Decision: Segment Size ✅
**Chosen**: 64MB
**Reason**: Good balance for most workloads

### Decision: fsync Strategy ✅
**Chosen**: Background sync every 1000ms
**Reason**: Balance durability and performance

### Decision: CRC Algorithm ✅
**Chosen**: CRC32 Castagnoli (SSE4.2 accelerated)
**Reason**: Fast on modern CPUs, good error detection

## Technical Details Implemented

### Message Binary Format
```
| Magic (2B) | Ver (1B) | Flags (1B) | CRC (4B) | Offset (8B) | 
| Timestamp (8B) | KeyLen (2B) | ValueLen (4B) | Key | Value |
```

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
