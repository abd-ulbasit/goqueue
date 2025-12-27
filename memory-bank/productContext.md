# GoQueue - Product Context

## Why This Project Exists

### The Real Pain Points

**Pain Point 1: Kafka's Consumer Rebalancing**
When a consumer joins or leaves a Kafka consumer group, ALL consumers in the group stop processing while partitions are reassigned. This "stop-the-world" rebalancing causes:
- 30+ second processing pauses
- Latency spikes visible in monitoring
- Duplicate processing if commits weren't flushed
- Operational incidents during deployments

**Pain Point 2: No Per-Message Acknowledgment in Kafka**
Kafka uses batch offset commits. If message 5 fails but messages 1-4 and 6-10 succeed:
- You can't skip message 5
- You either commit all (losing 5) or commit none (reprocessing 1-4)
- One poison message blocks the entire partition

**Pain Point 3: No Native Delays in Kafka**
To delay a message in Kafka:
- Use Kafka Streams with windowing (complex)
- Run external scheduler polling queues (inefficient)
- Create multiple topics with naming conventions (hacky)

**Pain Point 4: SQS Can't Replay**
Once you delete a message from SQS, it's gone. You cannot:
- Reprocess historical messages
- Debug what happened yesterday
- Rebuild downstream systems from the queue

## Target Users

### Primary: Backend Engineers
- Building event-driven microservices
- Need reliable message passing between services
- Want operational simplicity without vendor lock-in

### Secondary: Platform/Infrastructure Teams
- Running message queue infrastructure
- Tired of Kafka's operational complexity
- Want self-hosted alternative to SQS

## User Experience Goals

### For Producers
1. **Simple publish API** - Just specify topic, key, value
2. **Optional delays** - First-class delay parameter
3. **Batching** - Automatic for throughput, configurable
4. **Acknowledgments** - Know when message is safe

### For Consumers
1. **Per-message ACK** - Process at your own pace
2. **Automatic redelivery** - Unacked messages come back
3. **Consumer groups** - Scale horizontally
4. **Replay** - Go back to any point in time

### For Operators
1. **Single binary** - No ZooKeeper, no external deps
2. **Prometheus metrics** - Built-in observability
3. **CLI tool** - Manage from command line
4. **Kubernetes-native** - Helm chart with StatefulSet

## How It Should Work

### Happy Path: Basic Pub/Sub
```
1. Producer publishes message to topic "orders"
2. Message lands in partition based on key hash
3. Consumer group "processors" has 3 consumers
4. Each consumer gets messages from assigned partitions
5. Consumer processes, calls ACK
6. Offset advances, message marked processed
7. Message remains in log for replay if needed
```

### Happy Path: Delayed Message
```
1. Producer publishes with delay: 30 minutes
2. Message stored but not visible to consumers
3. Timer wheel tracks delivery time
4. After 30 minutes, message becomes visible
5. Consumer receives, processes, ACKs as normal
```

### Failure Path: Unacked Message
```
1. Consumer receives message, starts processing
2. Consumer crashes before ACK
3. Visibility timeout expires (default 30s)
4. Message redelivered to another consumer
5. retry_count incremented
6. After 3 retries, sent to DLQ
```

### Failure Path: Leader Failover
```
1. Partition 0 leader is Node 1
2. Node 1 crashes
3. Followers detect via missed heartbeats
4. Node 2 (highest in ISR) becomes leader
5. Producers/consumers discover new leader
6. Processing continues with no message loss
```

## Key Metrics to Track

### Throughput
- Messages published per second
- Messages consumed per second
- Bytes in/out per second

### Latency
- Publish latency (p50, p95, p99)
- End-to-end latency (publish to consume)
- Commit latency

### Reliability
- Consumer lag (messages behind)
- Redelivery rate
- DLQ rate
- Replication lag

### Availability
- Leader election time
- Rebalance duration
- Node health

## Competitive Positioning

| | GoQueue | Kafka | SQS | RabbitMQ |
|---|---------|-------|-----|----------|
| **Complexity** | Low | High | Low | Medium |
| **Replay** | ✅ | ✅ | ❌ | ❌ |
| **Per-msg ACK** | ✅ | ❌ | ✅ | ✅ |
| **Delays** | ✅ Native | ❌ | ❌ | Plugin |
| **Rebalance** | Cooperative | Stop-world | N/A | N/A |
| **Self-hosted** | ✅ | ✅ | ❌ | ✅ |
| **Dependencies** | None | ZK/KRaft | AWS | Erlang |
