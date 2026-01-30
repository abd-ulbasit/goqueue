---
layout: default
title: Consumer Groups


---

# Consumer Groups


Scale message processing across multiple consumers while maintaining ordering guarantees.


## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## What is a Consumer Group?

A **consumer group** is a collection of consumers that work together to process messages from a topic. Each partition is assigned to exactly one consumer in the group.

```
                Topic: orders (3 partitions)
                ┌─────┬─────┬─────┐
                │ P0  │ P1  │ P2  │
                └──┬──┴──┬──┴──┬──┘
                   │     │     │
        ┌──────────┼─────┼─────┼───────────┐
        │          │     │     │           │
        │     Consumer Group: order-service │
        │          │     │     │           │
        │    ┌─────▼──┐ ┌▼─────▼┐          │
        │    │Consumer│ │Consumer│          │
        │    │   A    │ │   B   │          │
        │    │  (P0)  │ │(P1,P2)│          │
        │    └────────┘ └───────┘          │
        └──────────────────────────────────┘
```

### Key Properties

1. **Load balancing**: Partitions distributed across consumers
2. **Fault tolerance**: If a consumer dies, its partitions are reassigned
3. **Single delivery**: Each message delivered to exactly one consumer in the group
4. **Independent progress**: Different groups track progress independently

---

## Creating a Consumer Group

Consumer groups are created implicitly when a consumer joins:

```bash
# HTTP API
curl -X POST http://localhost:8080/groups/order-service/join \
  -H "Content-Type: application/json" \
  -d '{
    "member_id": "consumer-1",
    "topics": ["orders"],
    "session_timeout_ms": 30000
  }'

# Response
{
  "member_id": "consumer-1",
  "generation": 1,
  "assigned_partitions": {
    "orders": [0, 1]
  }
}
```

---

## Partition Assignment

### How Partitions are Assigned

When consumers join or leave, partitions are rebalanced:

```
Initial State (2 consumers, 6 partitions):
  Consumer A: P0, P1, P2
  Consumer B: P3, P4, P5

Consumer C joins:
  Consumer A: P0, P1
  Consumer B: P2, P3
  Consumer C: P4, P5

Consumer B crashes:
  Consumer A: P0, P1, P2
  Consumer C: P3, P4, P5
```

### Assignment Strategies

| Strategy | How it Works | Best For |
|----------|-------------|----------|
| Range | Assign contiguous partitions | Simple, predictable |
| RoundRobin | Distribute evenly | Balanced load |
| Sticky | Minimize reassignments | Reducing rebalance impact |
| Cooperative | Incremental changes | Zero-downtime rebalances |

GoQueue defaults to **Cooperative Sticky** - the most advanced strategy.

---

## Rebalancing

A **rebalance** occurs when the partition-to-consumer mapping changes.

### Triggers

1. Consumer joins the group
2. Consumer leaves (graceful shutdown)
3. Consumer crashes (session timeout)
4. Topic partitions change

### Cooperative Rebalancing (Default)

GoQueue uses **incremental cooperative rebalancing** (like Kafka's KIP-429):

```
Traditional "Stop-the-World":
────────────────────────────────────────────────►
      │                              │
   STOP ALL                      RESUME ALL
   REVOKE ALL                    ASSIGN NEW
      │                              │
      └──────── No processing ───────┘

Cooperative (GoQueue default):
────────────────────────────────────────────────►
 ┌─────┐ ┌─────┐ ┌─────┐
 │ P0  │ │ P1  │ │ P2  │  Consumer A continues P0, P1
 └─────┘ └──┬──┘ └──┬──┘
            │      │
            ▼      ▼
         Consumer B gets P1, P2 incrementally
```

**Benefits:**
- No full stop during rebalance
- Consumers keep processing unaffected partitions
- Faster, smoother scaling

---

## Group Coordination

### Heartbeats

Consumers send periodic heartbeats to prove they're alive:

```
Timeline:
──────────────────────────────────────────────►
  │         │         │         │
  HB        HB        HB        HB
  │         │         │         │
  └─────────┴─────────┴─────────┘
      10s       10s       10s

If no heartbeat for session_timeout (default 30s):
  → Consumer considered dead
  → Rebalance triggered
```

### Session Timeout vs Heartbeat Interval

```yaml
consumer:
  # How long before consumer is considered dead
  sessionTimeout: "30s"
  
  # How often to send heartbeats (should be < sessionTimeout/3)
  heartbeatInterval: "10s"
```

{: .warning }
Setting `sessionTimeout` too low causes unnecessary rebalances. Too high delays failure detection.

---

## Consumption Patterns

### Competing Consumers (Default)

Each message goes to **one** consumer:

```
                    Topic: orders
                    ┌─────────────────┐
                    │ M1, M2, M3, M4  │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
         Consumer A    Consumer B    Consumer C
         gets M1       gets M2       gets M3
```

**Use cases:**
- Task processing (orders, jobs)
- Parallel data processing
- Load distribution

### Fan-out (Multiple Groups)

Each message goes to **all** groups:

```
                    Topic: events
                    ┌─────────────────┐
                    │ M1, M2, M3, M4  │
                    └────────┬────────┘
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        ┌─────────┐    ┌─────────┐    ┌─────────┐
        │ Group:  │    │ Group:  │    │ Group:  │
        │analytics│    │ billing │    │  logs   │
        │         │    │         │    │         │
        │ All msgs│    │ All msgs│    │ All msgs│
        └─────────┘    └─────────┘    └─────────┘
```

**Use cases:**
- Multiple services need same events
- Analytics + real-time processing
- CQRS event distribution

---

## Offset Management

Each consumer group tracks its position independently:

```
Topic: orders, Partition 0
┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
│  0  │  1  │  2  │  3  │  4  │  5  │  6  │  7  │
└─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘
                    ▲                 ▲     ▲
                    │                 │     │
            Group: analytics    Group: billing
               Offset: 3          Offset: 6
                                            │
                                        Latest
                                       Offset: 7
```

### Commit Strategies

```go
// Auto-commit (periodic)
// Commits happen every autoCommitInterval (default 5s)
config.Consumer.AutoCommitInterval = 5 * time.Second

// Manual commit (recommended for at-least-once)
messages := consumer.Poll()
for _, msg := range messages {
    process(msg)
    consumer.Commit(msg.Offset)  // Commit after processing
}

// Batch commit
messages := consumer.Poll()
processAll(messages)
consumer.CommitAll()  // Commit all at once
```

### Offset Reset Policy

What happens when a group starts with no saved offset:

| Policy | Behavior | Use Case |
|--------|----------|----------|
| `earliest` | Start from beginning | Process all historical data |
| `latest` | Start from new messages only | Only care about new data |
| `none` | Error if no offset | Strict - require explicit offset |

---

## Scaling Consumer Groups

### Adding Consumers

```
Before (1 consumer, 6 partitions):
  Consumer A: P0, P1, P2, P3, P4, P5

Add Consumer B:
  → Rebalance triggered
  → Consumer A: P0, P1, P2
  → Consumer B: P3, P4, P5
```

### Maximum Consumers

```
⚠️ You cannot have MORE consumers than partitions!

6 partitions, 8 consumers:
  Consumer 1: P0
  Consumer 2: P1
  Consumer 3: P2
  Consumer 4: P3
  Consumer 5: P4
  Consumer 6: P5
  Consumer 7: (idle - no partitions!)
  Consumer 8: (idle - no partitions!)
```

{: .important }
Plan partition count based on expected max consumers.

---

## Monitoring Consumer Groups

### Key Metrics

| Metric | What It Means | Action If High |
|--------|--------------|----------------|
| Consumer Lag | Messages behind latest | Add consumers or optimize processing |
| Rebalance Frequency | How often rebalances occur | Check session timeout, consumer stability |
| Time in Rebalance | Duration of rebalances | Use cooperative protocol |

### Checking Group Status

```bash
# List all groups
curl http://localhost:8080/groups

# Get group details
curl http://localhost:8080/groups/order-service

# Response
{
  "group_id": "order-service",
  "state": "stable",
  "generation": 42,
  "members": [
    {
      "member_id": "consumer-1",
      "client_host": "10.0.0.5",
      "assigned_partitions": {"orders": [0, 1, 2]}
    },
    {
      "member_id": "consumer-2",
      "client_host": "10.0.0.6",
      "assigned_partitions": {"orders": [3, 4, 5]}
    }
  ]
}
```

---

## Best Practices

### Do's

1. **Name groups by service**: `order-service`, `analytics-pipeline`
2. **Use cooperative rebalancing**: Reduces processing gaps
3. **Commit after processing**: Prevents message loss
4. **Monitor lag**: Set alerts for growing lag
5. **Graceful shutdown**: Leave group cleanly

### Don'ts

1. **Don't use too many groups**: Each group adds coordination overhead
2. **Don't commit before processing**: Risk message loss on crash
3. **Don't set tiny session timeout**: Causes unnecessary rebalances
4. **Don't exceed partition count with consumers**: Wastes resources

---

## Example: Setting Up a Consumer Group

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    
    "github.com/abd-ulbasit/goqueue/pkg/client"
)

func main() {
    // Create consumer
    consumer, err := client.NewConsumer(&client.ConsumerConfig{
        Brokers:     []string{"localhost:8080"},
        GroupID:     "order-processor",
        Topics:      []string{"orders"},
        AutoCommit:  false,  // Manual commit for at-least-once
    })
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    // Handle shutdown
    ctx, cancel := context.WithCancel(context.Background())
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sigChan
        cancel()
    }()

    // Consume loop
    for {
        select {
        case <-ctx.Done():
            return
        default:
            messages, err := consumer.Poll(ctx, 100)
            if err != nil {
                log.Printf("Poll error: %v", err)
                continue
            }
            
            for _, msg := range messages {
                // Process message
                if err := processOrder(msg); err != nil {
                    log.Printf("Process error: %v", err)
                    consumer.Nack(msg)  // Will retry
                    continue
                }
                consumer.Ack(msg)  // Commit offset
            }
        }
    }
}

func processOrder(msg *client.Message) error {
    // Your processing logic
    return nil
}
```

---

## Next Steps

- [Reliability](reliability) - ACK, NACK, and DLQ patterns
- [Offsets](offsets) - Deep dive into offset management
- [Delivery Guarantees](delivery-guarantees) - At-least-once vs exactly-once
