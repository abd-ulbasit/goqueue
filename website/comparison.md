---
layout: default
title: Comparison
nav_order: 6
---

# GoQueue vs Alternatives
{: .no_toc }

How GoQueue compares to other message queues and when to use each.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

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
| **Coordination** | ZooKeeper or KRaft | etcd (optional) |
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
- ✅ **Higher throughput** for ordered streams
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
- ✅ **Higher throughput** needs
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

| System | Throughput (msgs/sec) | Latency (p99) | Memory |
|--------|----------------------|---------------|--------|
| Kafka | 1M+ | 10-50ms | High (JVM) |
| GoQueue | 500K+ | 5-20ms | Low |
| RabbitMQ | 100K+ | 1-10ms | Medium |
| SQS | Unlimited* | 20-100ms | N/A |
| NATS | 10M+ | <1ms | Low |
| Redis Streams | 500K+ | <1ms | High |

*SQS scales automatically but has per-request limits

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
