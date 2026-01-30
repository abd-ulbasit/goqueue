---
layout: home
title: Home
nav_order: 1
description: "GoQueue is a high-performance distributed message queue built in Go, combining the best features of Kafka, SQS, and RabbitMQ."
permalink: /
---

# GoQueue
{: .fs-9 }

A high-performance distributed message queue built in Go.
{: .fs-6 .fw-300 }

[Get Started](/goqueue/docs/getting-started/quickstart){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/abd-ulbasit/goqueue){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## What is GoQueue?

GoQueue is a **distributed message queue** that combines the best features from multiple messaging systems:

- **Kafka-style** log-based storage with partitions for ordering and parallelism
- **SQS-style** visibility timeouts and dead letter queues for reliability
- **RabbitMQ-style** priority queues and flexible routing

Built from the ground up in Go for **performance**, **simplicity**, and **cloud-native** deployments.

---

## Key Features

<div class="feature-grid">

### 📦 Topics & Partitions
Kafka-style log-based storage with configurable partitions for parallelism and ordering guarantees.

### 👥 Consumer Groups
Automatic partition assignment, rebalancing, and cooperative rebalancing (KIP-429 style).

### ✅ Message Reliability
ACK/NACK, visibility timeouts, automatic retries, and dead letter queues.

### ⚡ Priority Queues
5 priority levels with weighted fair queuing to prevent starvation.

### ⏰ Delayed Messages
Schedule messages for future delivery with second-precision timing.

### 📋 Schema Registry
JSON Schema validation with compatibility checking (Confluent API compatible).

### 🔄 Transactions
Exactly-once semantics with idempotent producers and atomic commits.

### 📊 Observability
Prometheus metrics, distributed tracing, and comprehensive health checks.

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

| Aspect | Kafka | RabbitMQ | SQS | GoQueue |
|--------|-------|----------|-----|---------|
| **Deployment** | Complex (JVM + ZK) | Medium | Managed | Simple (single binary) |
| **Priority Queues** | ❌ | ✅ | ❌ | ✅ |
| **Delayed Messages** | ❌ | ✅ Plugin | ✅ | ✅ |
| **Visibility Timeout** | ❌ | ❌ | ✅ | ✅ |
| **Partitioning** | ✅ | ❌ | FIFO only | ✅ |
| **Consumer Groups** | ✅ | ❌ | ❌ | ✅ |
| **Transactions** | ✅ | ✅ | ❌ | ✅ |
| **Schema Registry** | Separate | ❌ | ❌ | ✅ Built-in |

[See full comparison →](/goqueue/comparison)

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                         GOQUEUE CLUSTER                        │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                    CONTROL PLANE                         │  │
│  │  Cluster Coordinator │ Metadata Store │ Leader Election  │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐           │
│  │   Node 1    │  │   Node 2    │  │   Node 3    │           │
│  │  (Broker)   │  │  (Broker)   │  │  (Broker)   │           │
│  │             │  │             │  │             │           │
│  │ Partitions: │  │ Partitions: │  │ Partitions: │           │
│  │  orders-0   │  │  orders-1   │  │  orders-2   │           │
│  │  events-1   │  │  events-2   │  │  events-0   │           │
│  └─────────────┘  └─────────────┘  └─────────────┘           │
└────────────────────────────────────────────────────────────────┘
```

[Learn more about architecture →](/goqueue/docs/concepts/architecture)

---

## Getting Started

Ready to try GoQueue? Follow our quickstart guide:

1. [Install GoQueue](/goqueue/docs/getting-started/installation)
2. [Create your first topic](/goqueue/docs/getting-started/quickstart)
3. [Publish and consume messages](/goqueue/docs/getting-started/quickstart#publish-messages)
4. [Set up consumer groups](/goqueue/docs/concepts/consumer-groups)

---

## Community

- [GitHub Discussions](https://github.com/abd-ulbasit/goqueue/discussions)
- [Report Issues](https://github.com/abd-ulbasit/goqueue/issues)
- [Contribute](https://github.com/abd-ulbasit/goqueue/blob/main/CONTRIBUTING.md)

---

<div class="footer-badges">
  <a href="https://github.com/abd-ulbasit/goqueue/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License">
  </a>
  <a href="https://github.com/abd-ulbasit/goqueue/releases">
    <img src="https://img.shields.io/github/v/release/abd-ulbasit/goqueue" alt="Release">
  </a>
  <a href="https://goreportcard.com/report/github.com/abd-ulbasit/goqueue">
    <img src="https://goreportcard.com/badge/github.com/abd-ulbasit/goqueue" alt="Go Report Card">
  </a>
</div>
