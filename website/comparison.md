---
layout: default
title: Comparison
description: How GoQueue compares to Kafka, RabbitMQ, SQS, and other message queues
---

# GoQueue vs Alternatives

How GoQueue compares to other message queues and when to use each.

---

## Feature Comparison Matrix

| Feature | GoQueue | Kafka | RabbitMQ | AWS SQS | NATS | Redis Streams |
|---------|---------|-------|----------|---------|------|---------------|
| **Architecture** |
| Log-based Storage | ✅ | ✅ | ❌ | ❌ | ✅ JetStream | ✅ |
| Partitioning | ✅ | ✅ | ❌ | FIFO only | ✅ | ✅ |
| Consumer Groups | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ |
| Cooperative Rebalance | ✅ | ✅ | ❌ | N/A | ❌ | ❌ |
| **Reliability** |
| At-least-once | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Exactly-once | ✅ | ✅ | ✅* | ❌ | ❌ | ❌ |
| ACK/NACK | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Visibility Timeout | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ |
| Dead Letter Queue | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ |
| **Features** |
| Priority Queues | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ |
| Delayed Messages | ✅ | ❌ | ✅ Plugin | ✅ | ❌ | ❌ |
| Schema Registry | ✅ Built-in | Separate | ❌ | ❌ | ❌ | ❌ |
| Transactions | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Message TTL | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Operations** |
| Deployment | Simple | Complex | Medium | Managed | Simple | Simple |
| Dependencies | None | ZK/KRaft | Erlang | AWS | None | Redis |
| Clustering | ✅ | ✅ | ✅ | Managed | ✅ | ✅ |
| Kubernetes Native | ✅ | ✅ | ✅ | N/A | ✅ | ✅ |
| **Observability** |
| Prometheus Metrics | ✅ | ✅ JMX | ✅ Plugin | CloudWatch | ✅ | ✅ |
| Distributed Tracing | ✅ | ❌ | ❌ | X-Ray | ❌ | ❌ |
| Health Endpoints | ✅ | ❌ | ✅ | N/A | ✅ | ❌ |

*RabbitMQ exactly-once requires specific configuration

---

## Detailed Comparisons

### GoQueue vs Apache Kafka

#### Architecture Philosophy

| Aspect | Kafka | GoQueue |
|--------|-------|---------|
| **Storage** | Append-only log | Append-only log |
| **Language** | Scala/Java (JVM) | Go (native binary) |
| **Coordination** | ZooKeeper or KRaft | Built in — no ZooKeeper, etcd or external store |
| **Memory Model** | Page cache | Direct I/O |

#### When to Choose Kafka

- ✅ **Massive scale** (millions of messages/sec)
- ✅ **Stream processing** with Kafka Streams or ksqlDB
- ✅ **Mature ecosystem** (connectors, tools)
- ✅ **Strong community** and commercial support

#### When to Choose GoQueue

- ✅ **Simpler deployment** (no JVM, no ZooKeeper)
- ✅ **Priority queues** (Kafka doesn't support)
- ✅ **Delayed messages** (native support)
- ✅ **SQS-style reliability** (visibility timeouts)
- ✅ **Built-in schema registry**
- ✅ **Lower memory footprint**

#### Configuration Comparison

**Kafka Producer:**
```properties
bootstrap.servers=kafka:9092
acks=all
batch.size=16384
linger.ms=5
max.in.flight.requests.per.connection=5
enable.idempotence=true
```

**GoQueue Producer:**
```yaml
producer:
  acks: all
  batchSize: 16384
  lingerMs: 5
  # Idempotence enabled by default
```

---

### GoQueue vs RabbitMQ

#### Architecture Philosophy

| Aspect | RabbitMQ | GoQueue |
|--------|----------|---------|
| **Model** | AMQP (exchanges, queues, bindings) | Topics & partitions |
| **Routing** | Flexible (direct, topic, fanout, headers) | Key-based partitioning |
| **Storage** | Queue-based (messages removed on consume) | Log-based (retained) |
| **Protocol** | AMQP 0-9-1 | HTTP/gRPC |

#### When to Choose RabbitMQ

- ✅ **Complex routing** (topic exchanges, headers)
- ✅ **AMQP compatibility** required
- ✅ **Message acknowledgment** at queue level
- ✅ **Existing RabbitMQ expertise**

#### When to Choose GoQueue

- ✅ **Message replay** (reprocess from any offset)
- ✅ **Consumer groups** with partition assignment
- ✅ **Batch publish** amortises per-request cost across a whole batch (see [Benchmarks](docs/operations/benchmarks/); no head-to-head against RabbitMQ has been run)
- ✅ **Simpler operations** (no Erlang)
- ✅ **Kafka-style semantics**

#### Feature Mapping

| RabbitMQ Concept | GoQueue Equivalent |
|------------------|-------------------|
| Exchange + Queue | Topic |
| Routing Key | Message Key |
| Consumer Tag | Member ID |
| Basic.Ack | /messages/ack |
| Basic.Nack | /messages/nack |
| Dead Letter Exchange | DLQ Topic |
| TTL | Retention + TTL |
| Priority Queue | Priority Levels |

---

### GoQueue vs AWS SQS

#### Architecture Philosophy

| Aspect | SQS | GoQueue |
|--------|-----|---------|
| **Deployment** | Managed service | Self-hosted |
| **Model** | Queue (messages deleted after processing) | Log (messages retained) |
| **Ordering** | FIFO queues only | All topics ordered within partition |
| **Scaling** | Automatic | Manual (partitions) |

#### When to Choose SQS

- ✅ **AWS native** applications
- ✅ **Zero operations** (fully managed)
- ✅ **Pay-per-use** pricing
- ✅ **Lambda integration**

#### When to Choose GoQueue

- ✅ **Multi-cloud/on-premise** deployment
- ✅ **Message replay** capability
- ✅ **Consumer groups**
- ✅ **No per-request API limits** (SQS caps batches at 10 messages; GoQueue's batch size is yours to choose)
- ✅ **Cost control** at scale
- ✅ **Priority queues**

#### API Comparison

**SQS Send:**
```python
sqs.send_message(
    QueueUrl=queue_url,
    MessageBody='{"order": "123"}',
    DelaySeconds=60
)
```

**GoQueue Publish:**
```bash
curl -X POST http://goqueue:8080/topics/orders/messages \
  -d '{"messages": [{"value": "{\"order\": \"123\"}", "delay": "60s"}]}'
```

---

### GoQueue vs NATS

#### Architecture Philosophy

| Aspect | NATS | GoQueue |
|--------|------|---------|
| **Core Model** | Pub/sub (ephemeral) | Log-based (persistent) |
| **JetStream** | Persistent streams | N/A |
| **Protocol** | NATS protocol | HTTP/gRPC |
| **Use Case** | Real-time messaging | Durable workloads |

#### When to Choose NATS

- ✅ **Real-time** pub/sub
- ✅ **Extremely low latency** (<1ms)
- ✅ **Request/reply** pattern
- ✅ **Service mesh** messaging

#### When to Choose GoQueue

- ✅ **Durability** is critical
- ✅ **Message replay** needed
- ✅ **Priority queues**
- ✅ **Schema validation**
- ✅ **Transactions**

---

### GoQueue vs Redis Streams

#### Architecture Philosophy

| Aspect | Redis Streams | GoQueue |
|--------|---------------|---------|
| **Storage** | In-memory + AOF/RDB | Disk-first |
| **Model** | Stream per key | Topics with partitions |
| **Consumer Groups** | Yes | Yes |
| **Clustering** | Redis Cluster | Native |

#### When to Choose Redis Streams

- ✅ **Already using Redis**
- ✅ **Simple setup**
- ✅ **Caching + messaging** in one system
- ✅ **Low latency** requirements

#### When to Choose GoQueue

- ✅ **Large message volumes** (disk-based)
- ✅ **Longer retention** (weeks/months)
- ✅ **Priority queues**
- ✅ **Schema registry**
- ✅ **Transactions**
- ✅ **Visibility timeouts**

---

## Use Case Recommendations

### Event Sourcing / CQRS

| Requirement | Best Choice |
|-------------|-------------|
| Millions of events | **Kafka** |
| Moderate scale, simpler ops | **GoQueue** |
| AWS native | **Kinesis** |

### Task Queues / Job Processing

| Requirement | Best Choice |
|-------------|-------------|
| Priority handling | **GoQueue** or **RabbitMQ** |
| AWS integration | **SQS** |
| Simple setup | **Redis Streams** |

### Real-time Messaging

| Requirement | Best Choice |
|-------------|-------------|
| Sub-millisecond latency | **NATS** |
| Durability required | **GoQueue** |
| Browser support | **RabbitMQ** (WebSocket) |

### Log Aggregation

| Requirement | Best Choice |
|-------------|-------------|
| Massive scale | **Kafka** |
| Simpler deployment | **GoQueue** |
| Existing ELK stack | **Kafka** |

### Microservices Communication

| Requirement | Best Choice |
|-------------|-------------|
| Request/reply | **NATS** or **RabbitMQ** |
| Event-driven | **GoQueue** or **Kafka** |
| AWS native | **SQS + SNS** |

---

## Migration Guides

### From Kafka to GoQueue

Key differences to handle:

1. **Configuration**: GoQueue uses YAML, not properties files
2. **Consumer Groups**: Similar concept, different API
3. **Offsets**: Same model, different commit API
4. **Transactions**: Similar semantics, different endpoints

```bash
# Kafka consumer to GoQueue
# Before (Kafka)
kafka-console-consumer --bootstrap-server kafka:9092 --topic orders --group my-group

# After (GoQueue)
curl -X POST http://goqueue:8080/groups/my-group/join -d '{"topics": ["orders"]}'
curl http://goqueue:8080/groups/my-group/poll?member_id=...
```

### From RabbitMQ to GoQueue

Key differences:

1. **No exchanges**: Route by message key to partitions
2. **Consumer groups** instead of individual consumers
3. **Log retention**: Messages aren't deleted after consumption

### From SQS to GoQueue

Key similarities (easier migration):

1. **Visibility timeouts**: Both support this pattern
2. **DLQ**: Both have dead letter queues
3. **Delayed messages**: Both support scheduling

---

## Performance Considerations

This page used to carry a throughput/latency table covering all six systems. It
has been removed. None of its rows were measured — including GoQueue's, which
claimed 500K+ msgs/sec at 5-20ms p99 against a best measured figure of ~220,000
msgs/sec with 1,000-message batches. Publishing a number 2.3× above anything
that was ever run is worse than publishing nothing.

What has actually been measured is on the
[Benchmarks](docs/operations/benchmarks/) page: one harness, one cluster shape,
stated before the numbers. The other five rows would have to be produced on the
same hardware with the same payload, durability settings and client quality
before they meant anything, and they have not been.

The feature comparison above is a different kind of claim — it is checkable from
each project's documentation, and it is what this page is for.

---

## Summary: When to Use GoQueue

✅ **Choose GoQueue when you need:**

- Kafka-style semantics with simpler operations
- Priority queues with ordering
- Delayed/scheduled messages
- SQS-style visibility timeouts
- Built-in schema registry
- Cloud-native deployment (Kubernetes)
- Lower operational complexity than Kafka

❌ **Consider alternatives when:**

- You need millions of messages/sec → **Kafka**
- You're all-in on AWS → **SQS**
- You need sub-millisecond latency → **NATS**
- You need complex routing → **RabbitMQ**
- You're already using Redis → **Redis Streams**
