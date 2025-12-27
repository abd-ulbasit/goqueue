# GoQueue - Project Brief

## Project Overview

**GoQueue** is a distributed persistent message queue that combines the best features of Kafka (append-only log, partitions, consumer groups) with SQS (per-message ACKs, visibility timeout, DLQ) while adding unique capabilities neither has (native delay messages, cooperative rebalancing, message tracing).

## Core Problem Statement

Existing message queues force painful trade-offs:

1. **Kafka**: Powerful but operationally complex, no per-message ACKs, no native delays, disruptive rebalancing
2. **SQS**: Simple but no replay, no ordering, no consumer groups
3. **RabbitMQ**: Feature-rich but not designed for high-throughput log streaming

**GoQueue solves**: "I want Kafka's durability and replay with SQS's simplicity and per-message ACKs, plus features neither has."

## Goals

### Primary Goals
1. Build a production-capable distributed message queue from scratch
2. Deep understanding of distributed systems concepts through implementation
3. Create a portfolio project that solves real pain points, not just "Kafka lite"

### Learning Goals
- Append-only log design and implementation
- Binary encoding and file I/O optimization
- Consumer group coordination and rebalancing
- Leader election and log replication
- Timer wheel algorithm for delays
- Distributed tracing concepts

## Unique Selling Points (Differentiators)

| Feature | Why It Matters |
|---------|----------------|
| **Zero-downtime rebalancing** | Kafka's rebalancing stops ALL consumers. GoQueue does incremental handoff. |
| **Native delay messages** | Built-in timer wheel. No external scheduler needed. |
| **Per-message ACK + replay** | SQS-style acks but messages stay in log for replay. |
| **Built-in message tracing** | Track any message's journey without external tools. |
| **Priority lanes** | Fast-track important messages within same partition. |
| **Point-in-time replay** | Query by timestamp, not just offset. |

## Technical Scope

### In Scope
- Append-only log storage engine
- Multi-partition topics with configurable replication
- Consumer groups with offset management
- Per-message ACK with visibility timeout
- Dead letter queue
- Native delay/scheduled messages
- Priority message lanes
- Message tracing/lifecycle tracking
- Cooperative consumer rebalancing
- Leader election and log replication
- HTTP and gRPC APIs
- CLI tool
- Prometheus metrics
- Kubernetes deployment

### Out of Scope (for now)
- Multi-datacenter replication
- Exactly-once semantics (complex, diminishing returns)
- Full Raft implementation (using simplified leader election)
- GUI admin interface

## Success Criteria

1. **Functional**: All 18 milestones completed with passing tests
2. **Performance**: 100K msg/s single node, <10ms p99 latency
3. **Reliability**: Zero message loss during leader failover
4. **Presentable**: Clear README, working demo, interview-ready talking points

## Target Timeline

- **Phase 1 (Foundations)**: Milestones 1-4
- **Phase 2 (Advanced)**: Milestones 5-9  
- **Phase 3 (Distribution)**: Milestones 10-13
- **Phase 4 (Operations)**: Milestones 14-18

## Repository Structure

```
goqueue/
├── cmd/
│   ├── goqueue/           # Broker binary
│   └── goqueue-cli/       # CLI tool
├── internal/
│   ├── storage/           # Append-only log, segments, index
│   ├── broker/            # Topic, partition, producer handling
│   ├── consumer/          # Consumer groups, offsets, rebalancing
│   ├── delivery/          # ACKs, visibility, DLQ, delays
│   ├── cluster/           # Membership, metadata, election
│   ├── replication/       # Log replication
│   └── tracing/           # Message lifecycle tracking
├── api/
│   └── proto/             # gRPC definitions
├── deployments/
│   ├── docker/
│   └── helm/
├── docs/
└── memory-bank/
```

## Key References

- [The Log: What every software engineer should know](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)
- [Kafka: The Definitive Guide](https://www.confluent.io/resources/kafka-the-definitive-guide/)
- [Designing Data-Intensive Applications](https://dataintensive.net/) - Chapters 5-6
- [Hashed and Hierarchical Timing Wheels](http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf)
