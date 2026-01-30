---
layout: default
title: Concepts
nav_order: 3
has_children: true
---

# Core Concepts
{: .no_toc }

Understand the fundamental concepts behind GoQueue's message queue architecture.
{: .fs-6 .fw-300 }

---

GoQueue combines the best patterns from Kafka, RabbitMQ, and SQS into a unified message queue. This section explains the core concepts you need to understand to use GoQueue effectively.

## Architecture Overview

```
                                    ┌─────────────────────────────────────────┐
                                    │              GoQueue Broker             │
                                    │                                         │
 ┌──────────┐                       │  ┌─────────────────────────────────┐   │
 │ Producer │──publish──────────────┼─►│           Topic: orders         │   │
 │    A     │                       │  │  ┌─────────┬─────────┬────────┐ │   │
 └──────────┘                       │  │  │  P0     │   P1    │   P2   │ │   │
                                    │  │  │  ████   │   ██    │   █████│ │   │
 ┌──────────┐                       │  │  └─────────┴─────────┴────────┘ │   │
 │ Producer │──publish──────────────┼─►│                                 │   │
 │    B     │                       │  └─────────────────────────────────┘   │
 └──────────┘                       │                   │                    │
                                    │                   ▼                    │
                                    │  ┌─────────────────────────────────┐   │
                                    │  │      Consumer Group: order-svc  │   │
                                    │  └─────────────────────────────────┘   │
                                    │           │         │         │        │
                                    └───────────┼─────────┼─────────┼────────┘
                                                │         │         │
                                                ▼         ▼         ▼
                                          ┌─────────┐ ┌─────────┐ ┌─────────┐
                                          │Consumer │ │Consumer │ │Consumer │
                                          │   C1    │ │   C2    │ │   C3    │
                                          │  (P0)   │ │  (P1)   │ │  (P2)   │
                                          └─────────┘ └─────────┘ └─────────┘
```

## Key Concepts

| Concept | What It Is | Why It Matters |
|---------|-----------|----------------|
| [Topics](topics) | Logical channels for messages | Organize messages by type/purpose |
| [Partitions](partitions) | Shards within a topic | Enable parallel processing |
| [Consumer Groups](consumer-groups) | Coordinated consumers | Load balance work across instances |
| [Offsets](offsets) | Position tracking | Enable replay and exactly-once |
| [Messages](messages) | The data unit | What you publish and consume |

## GoQueue's Philosophy

### Pull-Based Consumption

Like Kafka, GoQueue uses **pull-based** consumption rather than push:

```
PUSH (RabbitMQ style):           PULL (GoQueue style):
                                
  Broker ──push──► Consumer        Consumer ◄──poll── Broker
     │                                 │
     ▼                                 ▼
  Broker controls rate             Consumer controls rate
  Consumer can be overwhelmed      Consumer takes what it can handle
```

**Benefits:**
- Consumer controls processing rate (natural backpressure)
- Consumer can batch messages for efficiency
- Simpler failure handling

### Log-Based Storage

Messages are stored in an **append-only log** (like Kafka):

```
Partition Log:
┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
│  0  │  1  │  2  │  3  │  4  │  5  │  6  │  7  │ ← Offsets
├─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
│ Msg │ Msg │ Msg │ Msg │ Msg │ Msg │ Msg │ Msg │
└─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘
                    ▲                       ▲
                    │                       │
              Consumer A              New messages
              reading here           appended here
```

**Benefits:**
- Fast writes (sequential I/O only)
- Multiple consumers can read same data
- Replay messages from any point
- Retention based on time or size

### Visibility Timeouts (SQS-Style)

GoQueue adds **visibility timeouts** for at-least-once delivery:

```
Timeline:
─────────────────────────────────────────────────────────────────►
    │                          │                          │
 Receive                   Timeout                     If no ACK,
 Message                   Period                      message
    │                          │                       redelivered
    ▼                          ▼                          │
┌───────────────────────────────────────────────────┐     │
│            Message INVISIBLE to others            │     ▼
└───────────────────────────────────────────────────┘ ┌───────┐
                                                      │Visible│
                                                      │ again │
                                                      └───────┘
```

This ensures messages aren't lost if a consumer crashes.

---

## Quick Comparison to Other Systems

| Feature | GoQueue | Kafka | RabbitMQ | SQS |
|---------|---------|-------|----------|-----|
| Storage | Log-based | Log-based | Queue | Queue |
| Consumption | Pull | Pull | Push/Pull | Pull |
| Ordering | Per-partition | Per-partition | Per-queue | FIFO optional |
| Replay | ✅ Yes | ✅ Yes | ❌ No | ❌ No |
| Visibility timeout | ✅ Yes | ❌ No | ✅ Yes | ✅ Yes |
| Dead letter queue | ✅ Yes | Manual | ✅ Yes | ✅ Yes |

---

## Learning Path

New to message queues? Start here:

1. **[Topics & Partitions](topics)** - Where messages live
2. **[Messages](messages)** - What you send and receive  
3. **[Consumer Groups](consumer-groups)** - How to scale consumption
4. **[Reliability](reliability)** - ACK, NACK, DLQ patterns
5. **[Delivery Guarantees](delivery-guarantees)** - At-least-once, exactly-once

Advanced topics:

- [Priority Queues](priority) - Process critical messages first
- [Delayed Messages](delayed-messages) - Schedule for future delivery
- [Transactions](transactions) - Atomic multi-message operations
- [Schema Registry](schemas) - Message validation
