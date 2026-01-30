---
layout: default
title: Reliability & Delivery


---

# Reliability & Delivery Guarantees


Understanding ACK, NACK, visibility timeouts, and dead letter queues.


## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Delivery Guarantees Overview

Message delivery guarantees define what happens when failures occur:

| Guarantee | Meaning | Trade-off |
|-----------|---------|-----------|
| **At-most-once** | Message delivered 0 or 1 times | Fastest, may lose messages |
| **At-least-once** | Message delivered 1 or more times | Safe, may have duplicates |
| **Exactly-once** | Message delivered exactly 1 time | Safest, most complex |

```
At-most-once (Fire and forget):
  Producer ──publish──► Broker ──deliver──► Consumer
                         │
                   If lost here,
                   gone forever

At-least-once (Default in GoQueue):
  Producer ──publish──► Broker ──deliver──► Consumer
                         │                     │
                         │                  ACK/NACK
                         │◄────────────────────┘
                         │
                   If no ACK, redeliver

Exactly-once (With transactions):
  Producer ──publish──► Broker ──deliver──► Consumer
       │                  │                    │
  Idempotent key     Deduplication          ACK + 
                                          idempotent
                                          processing
```

---

## Message Acknowledgment

### ACK (Acknowledge)

An **ACK** tells GoQueue: "I successfully processed this message, delete it."

```
Consumer                          Broker
   │                                │
   │◄───────── message M1 ──────────│
   │                                │
   │         (process M1)           │
   │                                │
   │─────────── ACK M1 ────────────►│
   │                                │
   │                           ┌────┴────┐
   │                           │ Delete  │
   │                           │   M1    │
   │                           └─────────┘
```

```bash
# HTTP API
curl -X POST http://localhost:8080/messages/ack \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "orders",
    "partition": 0,
    "offset": 42
  }'
```

### NACK (Negative Acknowledge)

A **NACK** tells GoQueue: "I couldn't process this, handle it based on policy."

```
Consumer                          Broker
   │                                │
   │◄───────── message M1 ──────────│
   │                                │
   │        (process fails)         │
   │                                │
   │─────────── NACK M1 ───────────►│
   │                                │
   │                           ┌────┴────┐
   │                           │ Retry?  │
   │                           │  DLQ?   │
   │                           └─────────┘
```

#### NACK Options

```bash
# NACK with retry (back of queue)
curl -X POST http://localhost:8080/messages/nack \
  -d '{
    "topic": "orders",
    "partition": 0,
    "offset": 42,
    "requeue": true
  }'

# NACK without retry (to DLQ immediately)
curl -X POST http://localhost:8080/messages/nack \
  -d '{
    "topic": "orders",
    "partition": 0,
    "offset": 42,
    "requeue": false
  }'
```

---

## Visibility Timeout

The **visibility timeout** is the period during which a message is invisible to other consumers after being received.

### How It Works

```
Timeline:
────────────────────────────────────────────────────────────────►

t=0         t=10s                 t=30s                   t=31s
 │            │                     │                       │
 │         Process                  │                       │
 ▼         completes                ▼                       │
┌─────────────────────────────────────────────────────────────┐
│                    Message INVISIBLE                        │
└─────────────────────────────────────────────────────────────┘
 │            │                     │                       │
Receive      ACK ──────────────────►│                       │
message      sent                   │                       │
                               If NO ACK               Message
                               by timeout              VISIBLE
                                                       again
                                                   (redelivered)
```

### Configuring Visibility Timeout

```yaml
# Per-topic default
defaults:
  topic:
    delivery:
      visibilityTimeout: "30s"  # Default

# When receiving messages
curl -X GET "http://localhost:8080/messages/orders/0?visibility_timeout=60s"
```

### Extending Visibility

If processing takes longer than expected, extend the timeout:

```bash
# Extend visibility by another 60 seconds
curl -X POST http://localhost:8080/messages/extend-visibility \
  -d '{
    "topic": "orders",
    "partition": 0,
    "offset": 42,
    "timeout_seconds": 60
  }'
```

{: .highlight }
**Pro Tip**: Implement heartbeat-based extension for long-running tasks. Every N seconds, extend visibility to prevent redelivery.

---

## Retry Behavior

When processing fails, GoQueue can automatically retry:

### Retry Flow

```
Attempt 1 ──► NACK ──► Delay ──► Attempt 2 ──► NACK ──► Delay ──► Attempt 3 ──► NACK ──► DLQ
                100ms                          200ms                          400ms
                      (exponential backoff)
```

### Configuration

```yaml
defaults:
  topic:
    delivery:
      maxRetries: 3           # Max delivery attempts
      initialRetryDelay: "100ms"
      maxRetryDelay: "30s"
      retryBackoffMultiplier: 2.0  # Exponential backoff
```

### Retry Headers

GoQueue adds headers to track retry state:

```json
{
  "headers": {
    "x-goqueue-delivery-count": "2",
    "x-goqueue-first-delivery": "2024-01-15T10:00:00Z",
    "x-goqueue-last-error": "timeout processing order"
  }
}
```

---

## Dead Letter Queue (DLQ)

Messages that fail after all retries go to a **Dead Letter Queue** (DLQ).

### DLQ Flow

```
                 ┌────────────────────────────────────────────┐
                 │              Topic: orders                 │
                 │                                            │
                 │  Message M1                                │
                 │     │                                      │
                 │     ▼                                      │
                 │  Consumer ──FAIL──► Retry 1 ──FAIL──►     │
                 │                     Retry 2 ──FAIL──►     │
                 │                     Retry 3 ──FAIL──►     │
                 │                         │                  │
                 │                         ▼                  │
                 │                   ┌─────────────┐          │
                 │                   │ Max retries │          │
                 │                   │  exceeded   │          │
                 │                   └──────┬──────┘          │
                 └─────────────────────────┼──────────────────┘
                                           │
                                           ▼
                 ┌────────────────────────────────────────────┐
                 │           Topic: orders-dlq                │
                 │                                            │
                 │  Message M1 (with failure metadata)        │
                 │                                            │
                 └────────────────────────────────────────────┘
```

### DLQ Message Enrichment

Messages in DLQ include failure context:

```json
{
  "payload": "original message content",
  "headers": {
    "x-goqueue-dlq-reason": "max_retries_exceeded",
    "x-goqueue-original-topic": "orders",
    "x-goqueue-original-partition": "0",
    "x-goqueue-delivery-attempts": "3",
    "x-goqueue-first-failure": "2024-01-15T10:00:00Z",
    "x-goqueue-last-failure": "2024-01-15T10:05:00Z",
    "x-goqueue-failure-errors": [
      "attempt 1: connection timeout",
      "attempt 2: service unavailable",
      "attempt 3: internal error"
    ]
  }
}
```

### Processing DLQ

```bash
# List DLQ topics
curl http://localhost:8080/topics | jq '.[] | select(.name | endswith("-dlq"))'

# Get DLQ message count
curl http://localhost:8080/topics/orders-dlq

# Consume from DLQ for investigation
curl http://localhost:8080/messages/orders-dlq/0

# Replay message back to original topic
curl -X POST http://localhost:8080/messages/dlq/replay \
  -d '{
    "dlq_topic": "orders-dlq",
    "partition": 0,
    "offset": 5,
    "target_topic": "orders"
  }'
```

---

## In-Flight Messages

**In-flight messages** are messages that have been delivered but not yet acknowledged.

### Tracking In-Flight

```
Partition State:
┌──────────────────────────────────────────────────────────┐
│  Offset:  0   1   2   3   4   5   6   7   8   9  10     │
│           █   █   █   ░   ░   ░   ▒   ▒   ▒   ○   ○     │
│           └─committed─┘   └─in-flight─┘   └─available─┘ │
│                                                          │
│  █ = Committed (processed)                              │
│  ░ = In-flight (being processed)                        │
│  ▒ = In-flight (visibility extending)                   │
│  ○ = Available (waiting for consumer)                   │
└──────────────────────────────────────────────────────────┘
```

### Monitoring In-Flight

```bash
# Get in-flight message count
curl http://localhost:8080/topics/orders/inflight

# Response
{
  "topic": "orders",
  "partitions": {
    "0": {
      "in_flight_count": 3,
      "oldest_in_flight_age_seconds": 25
    },
    "1": {
      "in_flight_count": 1,
      "oldest_in_flight_age_seconds": 10
    }
  }
}
```

---

## Exactly-Once Semantics

For exactly-once delivery, combine idempotent producers with transactional consumers:

### Idempotent Producer

```bash
# Enable idempotent producer
curl -X POST http://localhost:8080/producers/init \
  -d '{
    "producer_id": "order-service-1",
    "idempotent": true
  }'

# Response
{
  "producer_id": "order-service-1",
  "epoch": 1
}

# Publish with sequence number
curl -X POST http://localhost:8080/messages \
  -d '{
    "topic": "orders",
    "producer_id": "order-service-1",
    "epoch": 1,
    "sequence": 0,
    "payload": "order data"
  }'
```

### Transactional Processing

```bash
# Begin transaction
curl -X POST http://localhost:8080/transactions/begin \
  -d '{"producer_id": "order-service-1"}'

# Response
{"transaction_id": "txn-123456"}

# Publish multiple messages
curl -X POST http://localhost:8080/messages \
  -d '{
    "transaction_id": "txn-123456",
    "messages": [
      {"topic": "orders", "payload": "order1"},
      {"topic": "inventory", "payload": "reserve1"}
    ]
  }'

# Commit (all-or-nothing)
curl -X POST http://localhost:8080/transactions/commit \
  -d '{"transaction_id": "txn-123456"}'
```

---

## Best Practices

### For At-Least-Once (Recommended Default)

```go
// 1. Process message
err := processOrder(message)

// 2. Only ACK if processing succeeded
if err == nil {
    consumer.Ack(message)
} else {
    consumer.Nack(message, true)  // true = requeue
}

// 3. Make processing idempotent
func processOrder(msg Message) error {
    // Check if already processed (by order ID)
    if alreadyProcessed(msg.OrderID) {
        return nil  // Idempotent - safe to ACK
    }
    // Process...
}
```

### For Exactly-Once

1. Use idempotent producers with sequence numbers
2. Store processing results in same transaction as offset commit
3. Use deduplication on consumer side

### Choosing Visibility Timeout

| Processing Time | Recommended Timeout | Why |
|----------------|--------------------| --- |
| <1s (fast) | 30s | Room for retries |
| 1-10s (medium) | 60s | Processing + buffer |
| 10-60s (slow) | 120s + heartbeat extension | Prevent premature redelivery |
| >60s (very slow) | Use heartbeat extension | Can't predict duration |

---

## Failure Scenarios

### Consumer Crashes

```
t=0:  Consumer receives message, starts processing
t=5:  Consumer crashes (no ACK sent)
t=30: Visibility timeout expires
t=30: Message becomes visible
t=31: Different consumer receives message (redelivery)
```

### Network Partition

```
t=0:  Consumer receives message
t=5:  Network partition (consumer isolated)
t=25: Consumer sends ACK (but it's lost!)
t=30: Visibility timeout expires
t=30: Broker redelivers to another consumer
t=35: Network heals, original consumer finished
      → DUPLICATE if processing wasn't idempotent
```

**Solution**: Make processing idempotent, handle duplicates gracefully.

---

## Next Steps

- [Delayed Messages](delayed-messages) - Schedule messages for future delivery
- [Priority Queues](priority) - Process critical messages first
- [Transactions](transactions) - Atomic multi-message operations
