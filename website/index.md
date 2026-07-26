---
layout: default
title: Home
description: A distributed message queue in Go — append-only log, built-in coordination and leader election, hierarchical timing wheel for O(1) delayed delivery
---

<div class="hero">
  <h1>GoQueue</h1>
  <p class="hero-tagline">A distributed message queue in Go.<br>Append-only log, its own coordination and leader election with no ZooKeeper or etcd, hierarchical timing wheel for O(1) delayed delivery, priority lanes, cooperative incremental rebalancing.</p>
  <div class="hero-buttons">
    <a href="{{ '/docs/getting-started/quickstart' | relative_url }}" class="btn btn-primary">Get Started</a>
    <a href="https://github.com/abd-ulbasit/goqueue" class="btn btn-secondary">View on GitHub</a>
  </div>
</div>

---

## What is GoQueue?

GoQueue is a **distributed message queue** whose feature set borrows from three
different lineages:

- **Kafka-style** log-based storage with partitions for ordering and parallelism
- **SQS-style** visibility timeouts and dead letter queues for reliability  
- **RabbitMQ-style** priority queues and flexible routing

It is single-cluster and it is not a Kafka replacement. Kafka's I/O path has had
a decade of production tuning and a zero-copy fast path; no head-to-head has been
run here and none is claimed. What has been measured, along with the harness that
produced it, is on the [Benchmarks]({{ '/docs/operations/benchmarks' | relative_url }}) page.

---

## Key Features

<div class="features">
  <div class="feature">
    <div class="feature-icon">📦</div>
    <h3>Topics & Partitions</h3>
    <p>Kafka-style log-based storage with configurable partitions for parallelism and ordering guarantees.</p>
  </div>
  <div class="feature">
    <div class="feature-icon">👥</div>
    <h3>Consumer Groups</h3>
    <p>Automatic partition assignment, rebalancing, and cooperative rebalancing (KIP-429 style).</p>
  </div>
  <div class="feature">
    <div class="feature-icon">✅</div>
    <h3>Message Reliability</h3>
    <p>ACK/NACK, visibility timeouts, automatic retries, and dead letter queues.</p>
  </div>
  <div class="feature">
    <div class="feature-icon">⚡</div>
    <h3>Priority Queues</h3>
    <p>5 priority levels with weighted fair queuing to prevent starvation.</p>
  </div>
  <div class="feature">
    <div class="feature-icon">⏰</div>
    <h3>Delayed Messages</h3>
    <p>Schedule messages for future delivery with second-precision timing.</p>
  </div>
  <div class="feature">
    <div class="feature-icon">📋</div>
    <h3>Schema Registry</h3>
    <p>JSON Schema validation with compatibility checking (Confluent API compatible).</p>
  </div>
  <div class="feature">
    <div class="feature-icon">🔄</div>
    <h3>Transactions</h3>
    <p>Exactly-once semantics with idempotent producers and atomic commits.</p>
  </div>
  <div class="feature">
    <div class="feature-icon">📊</div>
    <h3>Observability</h3>
    <p>Prometheus metrics, distributed tracing, and comprehensive health checks.</p>
  </div>
</div>

---

## Quick Example

### Publish a Message

```bash
curl -X POST http://localhost:8080/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{
      "key": "user-123",
      "value": "{\"orderId\": \"12345\", \"amount\": 99.99}",
      "priority": "high"
    }]
  }'
```

### Consume via Consumer Group

```bash
# Join group
curl -X POST http://localhost:8080/groups/order-processors/join \
  -H "Content-Type: application/json" \
  -d '{"client_id": "consumer-1", "topics": ["orders"]}'

# Poll for messages
curl "http://localhost:8080/groups/order-processors/poll?member_id=<member_id>&timeout=30s"
```

### Using the Go Client

```go
package main

import (
    "context"
    "log"
    
    "goqueue/pkg/client"
)

func main() {
    // Create client
    c, err := client.New(client.DefaultConfig("localhost:9000"))
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Publish
    resp, err := c.Publish(context.Background(), "orders", 
        []byte(`{"orderId": "12345"}`))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Published to partition %d, offset %d", 
        resp.Partition, resp.Offset)
}
```

---

## Why GoQueue?

| Feature | Kafka | RabbitMQ | SQS | GoQueue |
|---------|-------|----------|-----|---------|
| Deployment | Complex (JVM + ZK) | Medium | Managed | **Simple (single binary)** |
| Priority Queues | ❌ | ✅ | ❌ | ✅ |
| Delayed Messages | ❌ | ✅ Plugin | ✅ | ✅ |
| Visibility Timeout | ❌ | ❌ | ✅ | ✅ |
| Partitioning | ✅ | ❌ | FIFO only | ✅ |
| Consumer Groups | ✅ | ❌ | ❌ | ✅ |
| Transactions | ✅ | ✅ | ❌ | ✅ |
| Schema Registry | Separate | ❌ | ❌ | **Built-in** |

[See full comparison →]({{ '/comparison' | relative_url }})

---

## Getting Started

Ready to try GoQueue? Follow our quickstart guide:

1. [Install GoQueue]({{ '/docs/getting-started/installation' | relative_url }})
2. [Create your first topic]({{ '/docs/getting-started/quickstart' | relative_url }})
3. [Set up consumer groups]({{ '/docs/concepts/consumer-groups' | relative_url }})
4. [Explore the API]({{ '/docs/api/' | relative_url }})
