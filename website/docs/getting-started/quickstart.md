---
layout: default
title: Quickstart


---

# Quickstart


Get GoQueue running in 5 minutes.


## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Prerequisites

- GoQueue installed ([Installation Guide](installation))
- `curl` for API calls (or use Postman)

---

## Start the Broker

### Option 1: Run Directly

```bash
# Start with default configuration
goqueue

# Or with custom data directory
goqueue --data-dir ./data
```

You should see:

```
╔═══════════════════════════════════════════════════════════════╗
║                     GoQueue v1.0.0                            ║
╚═══════════════════════════════════════════════════════════════╝

📦 Starting broker...
   ✓ Broker started (NodeID: node-abc123)
   ✓ Data directory: ./data

📊 Initializing Prometheus metrics...
   ✓ Metrics initialized (endpoint: /metrics)

🌐 Starting HTTP API server on :8080
🔌 Starting gRPC server on :9000
```

### Option 2: Docker

```bash
docker run -d \
  --name goqueue \
  -p 8080:8080 \
  -p 9000:9000 \
  abdulbasit/goqueue:latest
```

---

## Verify the Broker is Running

```bash
# Health check
curl http://localhost:8080/health
```

Response:
```json
{
  "status": "ok",
  "timestamp": "2025-01-30T10:00:00Z"
}
```

---

## Create a Topic

```bash
curl -X POST http://localhost:8080/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders",
    "num_partitions": 3,
    "retention_hours": 168
  }'
```

Response:
```json
{
  "name": "orders",
  "partitions": 3,
  "created": true
}
```

{: .note }
Topics with more partitions allow more parallel consumers but may affect ordering. Use message keys to maintain order within a partition.

---

## Publish Messages

### Simple Message

```bash
curl -X POST http://localhost:8080/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{
      "value": "{\"orderId\": \"12345\", \"amount\": 99.99}"
    }]
  }'
```

Response:
```json
{
  "results": [{
    "partition": 1,
    "offset": 0,
    "priority": "normal"
  }]
}
```

### Message with Key (Ordering)

Messages with the same key go to the same partition, preserving order:

```bash
curl -X POST http://localhost:8080/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{
      "key": "user-123",
      "value": "{\"orderId\": \"12345\", \"userId\": \"user-123\"}"
    }]
  }'
```

### High Priority Message

```bash
curl -X POST http://localhost:8080/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{
      "value": "{\"alert\": \"payment failed\"}",
      "priority": "critical"
    }]
  }'
```

Priority levels: `critical` > `high` > `normal` > `low` > `background`

### Delayed Message

Schedule a message for future delivery:

```bash
curl -X POST http://localhost:8080/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{
      "value": "{\"reminder\": \"follow up\"}",
      "delay": "1h"
    }]
  }'
```

---

## Consume Messages

### Simple Consumption

Read directly from a partition (for testing/debugging):

```bash
curl "http://localhost:8080/topics/orders/partitions/0/messages?offset=0&limit=10"
```

Response:
```json
{
  "messages": [
    {
      "offset": 0,
      "timestamp": "2025-01-30T10:00:00Z",
      "key": "user-123",
      "value": "{\"orderId\": \"12345\"}",
      "priority": "normal"
    }
  ],
  "next_offset": 1
}
```

### Consumer Group (Recommended)

For production, use consumer groups for automatic partition assignment and failover:

#### 1. Join the Group

```bash
curl -X POST http://localhost:8080/groups/order-processors/join \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "consumer-1",
    "topics": ["orders"],
    "session_timeout": "30s"
  }'
```

Response:
```json
{
  "member_id": "consumer-1-abc123",
  "generation": 1,
  "leader": true,
  "assigned_partitions": ["orders-0", "orders-1", "orders-2"]
}
```

Save the `member_id` for subsequent calls.

#### 2. Poll for Messages

```bash
curl "http://localhost:8080/groups/order-processors/poll?member_id=consumer-1-abc123&max_messages=100&timeout=30s"
```

Response:
```json
{
  "messages": [
    {
      "topic": "orders",
      "partition": 0,
      "offset": 0,
      "key": "user-123",
      "value": "{\"orderId\": \"12345\"}",
      "timestamp": "2025-01-30T10:00:00Z",
      "receipt_handle": "rh-xyz789"
    }
  ]
}
```

#### 3. Acknowledge Messages

After processing, acknowledge the message:

```bash
curl -X POST http://localhost:8080/messages/ack \
  -H "Content-Type: application/json" \
  -d '{
    "receipt_handle": "rh-xyz789"
  }'
```

#### 4. Send Heartbeats

Keep your session alive (should be called periodically):

```bash
curl -X POST http://localhost:8080/groups/order-processors/heartbeat \
  -H "Content-Type: application/json" \
  -d '{
    "member_id": "consumer-1-abc123",
    "generation": 1
  }'
```

---

## Using the Go Client

For Go applications, use the native client:

```go
package main

import (
    "context"
    "log"
    "time"
    
    "goqueue/pkg/client"
)

func main() {
    // Create client
    c, err := client.New(client.DefaultConfig("localhost:9000"))
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    ctx := context.Background()

    // Create topic
    err = c.CreateTopic(ctx, "orders", 3)
    if err != nil {
        log.Fatal(err)
    }

    // Publish message
    resp, err := c.Publish(ctx, "orders", []byte(`{"orderId": "12345"}`))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Published to partition %d at offset %d", resp.Partition, resp.Offset)

    // Consume (simplified example)
    consumer, err := client.NewConsumer(c, client.ConsumerConfig{
        GroupID: "order-processors",
        Topics:  []string{"orders"},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    for {
        msgs, err := consumer.Poll(ctx, 100, 30*time.Second)
        if err != nil {
            log.Printf("Poll error: %v", err)
            continue
        }
        
        for _, msg := range msgs {
            log.Printf("Received: %s", string(msg.Value))
            consumer.Ack(ctx, msg.ReceiptHandle)
        }
    }
}
```

---

## What's Next?

Now that you have GoQueue running:

1. **[Concepts](../concepts/topics)** - Learn about topics, partitions, and consumer groups
2. **[Configuration](../configuration/reference)** - Tune GoQueue for your workload
3. **[API Reference](../api-reference/rest)** - Complete API documentation
4. **[Production Setup](../operations/production)** - Prepare for production deployment

---

## Troubleshooting

### Port Already in Use

```bash
# Check what's using port 8080
lsof -i :8080

# Use different ports
goqueue --http-port 8081 --grpc-port 9001
```

### Permission Denied

```bash
# Make sure data directory is writable
mkdir -p ./data
chmod 755 ./data
goqueue --data-dir ./data
```

### Connection Refused

Make sure the broker is running:

```bash
curl http://localhost:8080/healthz
```

See [Troubleshooting Guide](../operations/troubleshooting) for more solutions.
