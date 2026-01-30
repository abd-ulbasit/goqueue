---
layout: default
title: Topics & Partitions


---

# Topics & Partitions


Understanding how messages are organized and stored in GoQueue.


## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## What is a Topic?

A **topic** is a named category or feed to which messages are published. Think of it as a logical channel for a specific type of message.

```
                    Topics
                       │
    ┌──────────────────┼──────────────────┐
    │                  │                  │
    ▼                  ▼                  ▼
┌────────┐        ┌────────┐        ┌────────┐
│ orders │        │ events │        │  logs  │
│        │        │        │        │        │
│ Order  │        │ User   │        │ System │
│ events │        │ events │        │ logs   │
└────────┘        └────────┘        └────────┘
```

**Real-world examples:**
- `orders` - Order created, updated, cancelled events
- `user-events` - User signup, login, profile changes
- `notifications` - Emails, SMS, push notifications to send
- `logs` - Application logs for processing

### Creating a Topic

```bash
# Using HTTP API
curl -X POST http://localhost:8080/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders",
    "partitions": 6,
    "retention_hours": 168
  }'

# Using CLI
goqueue-cli topic create orders --partitions 6
```

### Topic Naming Conventions

Good topic names are:
- Lowercase with hyphens: `user-events`, `order-updates`
- Descriptive of content: `payment-transactions` not `pt`
- Domain-scoped: `inventory.stock-updates`, `billing.invoices`

{: .warning }
Topic names cannot contain spaces or special characters except `-`, `_`, and `.`

---

## What is a Partition?

A **partition** is a unit of parallelism within a topic. Each partition is an independent, ordered log of messages.

```
Topic: orders (3 partitions)

Partition 0:  ┌─────┬─────┬─────┬─────┬─────┐
              │ O1  │ O4  │ O7  │ O10 │ ... │  ← Appended sequentially
              └─────┴─────┴─────┴─────┴─────┘
              offset: 0     1     2     3

Partition 1:  ┌─────┬─────┬─────┬─────┬─────┐
              │ O2  │ O5  │ O8  │ O11 │ ... │
              └─────┴─────┴─────┴─────┴─────┘
              offset: 0     1     2     3

Partition 2:  ┌─────┬─────┬─────┬─────┬─────┐
              │ O3  │ O6  │ O9  │ O12 │ ... │
              └─────┴─────┴─────┴─────┴─────┘
              offset: 0     1     2     3
```

### Why Partitions Matter

1. **Parallelism**: Each partition can be consumed by a different consumer
2. **Ordering**: Messages within a partition are strictly ordered
3. **Scalability**: More partitions = more throughput

```
1 Partition:                    3 Partitions:
                                
  ┌───────┐                       ┌─────┐
  │ Topic │                       │ P0  │──► Consumer 1
  └───┬───┘                       ├─────┤
      │                           │ P1  │──► Consumer 2
      ▼                           ├─────┤
  Consumer 1                      │ P2  │──► Consumer 3
  (processes ALL)                 └─────┘
                                  (3x throughput!)
```

### Choosing Partition Count

| Workload | Partitions | Reasoning |
|----------|------------|-----------|
| Low throughput (<1K msg/s) | 3 | Minimum for redundancy |
| Medium throughput (1K-10K) | 6-12 | Room for consumer scaling |
| High throughput (>10K) | 12-32 | Max parallel consumers |
| Critical ordering | 1 | Global ordering required |

{: .important }
You can increase partitions later, but you **cannot decrease** them. Start conservative.

---

## Message Routing to Partitions

When publishing, GoQueue determines which partition receives each message:

### Default: Round-Robin

Without a key, messages are distributed evenly:

```go
// No key - round-robin distribution
client.Publish("orders", nil, orderData)
// Message goes to P0, then P1, then P2, then P0...
```

### With Key: Hash-Based

With a key, messages with the same key always go to the same partition:

```go
// With key - consistent hashing
client.Publish("orders", []byte(orderID), orderData)
// Same orderID always goes to same partition
```

**Key selection strategies:**

| Entity | Recommended Key | Why |
|--------|----------------|-----|
| Orders | `order_id` | All events for an order stay together |
| Users | `user_id` | All user events in order |
| Inventory | `sku` | Stock changes for SKU stay ordered |
| Logs | None (round-robin) | No ordering requirement |

### Partition Selection Deep Dive

```
Message with key="order-123"
        │
        ▼
   ┌─────────────┐
   │  hash(key)  │  = 0x7A3B2C
   └─────────────┘
        │
        ▼
   ┌───────────────────────┐
   │ 0x7A3B2C % partitions │  = 0x7A3B2C % 3 = 1
   └───────────────────────┘
        │
        ▼
   Partition 1  ✓
```

{: .warning }
If you change the partition count, key→partition mapping changes! This can break ordering guarantees for in-flight messages.

---

## Ordering Guarantees

### Within a Partition: Strict Ordering

Messages in a single partition are **always** ordered:

```
Partition 0:
  Written: M1 → M2 → M3 → M4
  Read:    M1 → M2 → M3 → M4  ✓ Always this order
```

### Across Partitions: No Ordering

Messages across different partitions have **no ordering guarantee**:

```
P0: M1 → M4 → M7
P1: M2 → M5 → M8
P2: M3 → M6 → M9

Consumer might see: M2, M1, M3, M5, M4, M6...
```

### When You Need Global Ordering

If you need total ordering across all messages:

1. **Single partition** (limits throughput):
   ```json
   { "name": "audit-log", "partitions": 1 }
   ```

2. **Key-based ordering** (ordered within entity):
   ```go
   // All messages for same order stay ordered
   client.Publish("orders", []byte(orderID), event)
   ```

---

## Topic Configuration

### Retention

How long messages are kept:

```yaml
defaults:
  topic:
    retention:
      hours: 168     # 7 days (default)
      # OR
      bytes: 10737418240  # 10GB per partition
```

### Per-Topic Overrides

```bash
# Create topic with custom retention
curl -X POST http://localhost:8080/topics \
  -d '{
    "name": "metrics",
    "partitions": 12,
    "retention_hours": 24,
    "config": {
      "compression": "lz4"
    }
  }'
```

---

## Storage Layout

How topics/partitions map to files on disk:

```
/var/lib/goqueue/
├── logs/
│   ├── orders/
│   │   ├── 0/                    # Partition 0
│   │   │   ├── 00000000.log      # Segment file
│   │   │   ├── 00000000.idx      # Offset index
│   │   │   ├── 00000000.timeIdx  # Time index
│   │   │   └── 00000512.log      # Next segment
│   │   ├── 1/                    # Partition 1
│   │   └── 2/                    # Partition 2
│   └── events/
│       └── ...
├── offsets/                      # Consumer group offsets
└── schemas/                      # Schema registry
```

---

## Common Patterns

### Topic-per-Entity-Type

```
topics:
  - users           # User lifecycle events
  - orders          # Order events
  - inventory       # Stock changes
  - payments        # Payment events
```

### Topic-per-Event-Type

```
topics:
  - user-created
  - user-updated
  - order-created
  - order-shipped
```

### Single Topic with Routing

```
topic: domain-events
message.headers["type"] = "OrderCreated"
// Consumers filter by header
```

---

## Best Practices

1. **Start with 6 partitions** for most topics - gives room to grow
2. **Use keys consistently** - don't mix keyed and non-keyed messages
3. **Monitor partition lag** - uneven lag indicates hot partitions
4. **Plan for growth** - you can't reduce partitions later
5. **Separate by retention** - don't mix 24h logs with 30d transactions

---

## Next Steps

- [Consumer Groups](consumer-groups) - How to scale consumption
- [Messages](messages) - Understanding message structure
- [Reliability](reliability) - ACK/NACK patterns
