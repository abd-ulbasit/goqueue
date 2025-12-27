# GoQueue - Active Context

## Current Focus

**Phase**: 1 - Foundations
**Milestone**: 1 - Storage Engine & Append-Only Log
**Status**: Not Started

## What I'm Working On

### Immediate Next Steps
1. Set up project structure (cmd, internal, api directories)
2. Implement Message struct with binary encoding
3. Build append-only log writer
4. Add segment file management

## Recent Changes

*Project just initialized - no changes yet*

## Active Decisions

### Decision: Binary vs Text Encoding
**Context**: Message format for storage
**Options**:
1. Binary (custom) - Compact, fast parsing
2. JSON - Human readable, debugging friendly
3. Protocol Buffers - Schema evolution, cross-language

**Leaning toward**: Binary for storage, with JSON available for debugging
**Reason**: Message queues are throughput-sensitive, binary encoding is 3-5x faster

### Decision: Index Granularity
**Context**: How often to add entries to offset index
**Options**:
1. Every message - O(1) lookup, high memory
2. Every N messages - Balance
3. Every N bytes - Consistent index size

**Leaning toward**: Every 4KB of data
**Reason**: Kafka uses this, provides good balance

## Current Questions

- [ ] Should segments be memory-mapped for reads?
- [ ] How to handle partial writes (crash recovery)?
- [ ] When to fsync - every write, batched, or never?

## Blockers

*None currently*

## Session Notes

### Session Template
```
### Session N - [Date]
**Focus**: 
**Duration**: 
**Completed**:
- 

**Learned**:
- 

**Next**:
- 
```
