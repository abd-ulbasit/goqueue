# GoQueue - Distributed Persistent Message Queue

A durable, distributed message queue with consumer groups, replication, and partition-based scaling. Think: simplified Kafka with the reliability of SQS.

## Architecture

```
                                    Producers
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
            ┌───────────────────────────────────────────────────┐
            │                    GoQueue Cluster                │
            │                                                   │
            │  ┌─────────────────────────────────────────────┐  │
            │  │              Topic: "orders"                │  │
            │  │                                             │  │
            │  │  ┌───────────┐ ┌───────────┐ ┌───────────┐  │  │
            │  │  │Partition 0│ │Partition 1│ │Partition 2│  │  │
            │  │  │  Leader:  │ │  Leader:  │ │  Leader:  │  │  │
            │  │  │  Node 1   │ │  Node 2   │ │  Node 3   │  │  │
            │  │  │           │ │           │ │           │  │  │
            │  │  │ Replicas: │ │ Replicas: │ │ Replicas: │  │  │
            │  │  │ Node 2,3  │ │ Node 1,3  │ │ Node 1,2  │  │  │
            │  │  └───────────┘ └───────────┘ └───────────┘  │  │
            │  │                                             │  │
            │  └─────────────────────────────────────────────┘  │
            │                                                   │
            │  ┌─────────────────────────────────────────────┐  │
            │  │           Consumer Group: "processors"      │  │
            │  │                                             │  │
            │  │  Consumer A ──► Partition 0                 │  │
            │  │  Consumer B ──► Partition 1                 │  │
            │  │  Consumer C ──► Partition 2                 │  │
            │  │                                             │  │
            │  │  Offsets: { P0: 1523, P1: 892, P2: 2341 }   │  │
            │  └─────────────────────────────────────────────┘  │
            │                                                   │
            └───────────────────────────────────────────────────┘
```

## Features

### Core Messaging
- [x] Topics with configurable partitions
- [x] Message publishing with partition key
- [x] Message ordering within partition
- [x] Configurable retention (time/size)

### Persistence
- [x] Append-only log (WAL) per partition
- [x] Survives broker restarts
- [x] Batched writes for performance
- [x] Segment files with cleanup

### Consumer Groups
- [x] Multiple consumers share partition load
- [x] Automatic partition assignment
- [x] Consumer rebalancing on join/leave
- [x] Offset tracking per consumer group
- [x] Resume from last committed offset

### Reliability
- [x] At-least-once delivery
- [x] Explicit acknowledgments
- [x] Dead letter queue (DLQ)
- [x] Retry with backoff
- [x] Backpressure handling

### Distribution (Multi-Node)
- [x] Partition distribution across nodes
- [x] Replication for fault tolerance
- [x] Leader election per partition
- [x] Automatic failover

### Operations
- [x] HTTP + gRPC APIs
- [x] Admin CLI tool
- [x] Prometheus metrics
- [x] Kubernetes StatefulSet deployment

---

## Milestone-by-Milestone Breakdown

### Milestone 1: Storage Engine & Basic Pub/Sub

**Goal:** Build the append-only log and basic messaging

**Learning Focus:**
- Append-only log design (why Kafka uses it)
- File I/O in Go (os.File, buffered I/O)
- Binary encoding for messages
- Index files for fast lookup

**Deliverables:**
- [ ] Message struct with encoding/decoding
- [ ] Append-only log writer
- [ ] Segment files (split log into chunks)
- [ ] Index file for offset→position lookup
- [ ] Log reader (read from offset)
- [ ] Basic topic management
- [ ] Simple producer (publish to topic)
- [ ] Simple consumer (read from offset)

**Code Structure:**
```
goqueue/
├── cmd/
│   └── goqueue/
│       └── main.go
├── internal/
│   ├── storage/
│   │   ├── log.go            # Append-only log
│   │   ├── segment.go        # Segment file management
│   │   ├── index.go          # Offset index
│   │   └── message.go        # Message encoding
│   └── broker/
│       ├── broker.go         # Main broker
│       ├── topic.go          # Topic management
│       └── partition.go      # Single partition
├── go.mod
└── README.md
```

**Message Format:**
```
┌──────────────────────────────────────────────────┐
│ Offset (8 bytes) │ Size (4 bytes) │ CRC (4 bytes)│
├──────────────────────────────────────────────────┤
│ Timestamp (8 bytes) │ Key Length (2 bytes)       │
├──────────────────────────────────────────────────┤
│ Key (variable) │ Value Length (4 bytes)          │
├──────────────────────────────────────────────────┤
│ Value (variable)                                 │
└──────────────────────────────────────────────────┘
```

**Tests:**
- [ ] Write and read messages correctly
- [ ] Segment rolls over at size limit
- [ ] Index allows fast offset lookup
- [ ] Log survives restart
- [ ] CRC detects corruption

---

### Milestone 2: Topics, Partitions & Producers

**Goal:** Multi-partition topics with proper producer API

**Learning Focus:**
- Partitioning strategies (hash, round-robin)
- Why partitions enable parallelism
- Producer batching for throughput
- Partition key consistency

**Deliverables:**
- [ ] Topic with multiple partitions
- [ ] Partition assignment by key hash
- [ ] Producer with batching
- [ ] Producer acknowledgment modes (fire-and-forget, wait-for-write)
- [ ] HTTP API for publishing
- [ ] Topic creation/deletion API
- [ ] Retention policy (time/size based cleanup)

**Code Structure:**
```
internal/
├── broker/
│   ├── topic.go              # Multi-partition topic
│   └── producer.go           # Producer handling
├── api/
│   ├── http/
│   │   ├── server.go
│   │   ├── publish.go        # POST /topics/{topic}/publish
│   │   └── admin.go          # Topic management
│   └── proto/
│       └── queue.proto       # gRPC definitions
└── partitioner/
    ├── hash.go               # Consistent hashing
    └── roundrobin.go
```

**HTTP API:**
```
# Create topic
POST /v1/topics
{
  "name": "orders",
  "partitions": 3,
  "replicationFactor": 2,
  "retention": {
    "hours": 168,
    "bytes": 10737418240
  }
}

# Publish message
POST /v1/topics/orders/publish
{
  "key": "user-123",      # Optional, for partition routing
  "value": "base64...",
  "headers": {
    "correlation-id": "abc123"
  }
}

Response:
{
  "partition": 1,
  "offset": 12345
}

# Batch publish
POST /v1/topics/orders/publish/batch
{
  "messages": [
    {"key": "user-123", "value": "..."},
    {"key": "user-456", "value": "..."}
  ]
}
```

**Tests:**
- [ ] Same key always goes to same partition
- [ ] Batching improves throughput
- [ ] Retention cleanup removes old segments
- [ ] Topic deletion cleans up files

**Benchmark:**
- [ ] Measure messages/second write throughput
- [ ] Measure latency at various batch sizes

---

### Milestone 3: Consumer Groups & Offset Management

**Goal:** Consumer groups with reliable offset tracking

**Learning Focus:**
- Consumer group coordination
- Partition assignment strategies
- Offset commit patterns
- Rebalancing protocol

**Deliverables:**
- [ ] Consumer group registration
- [ ] Partition assignment to consumers
- [ ] Offset tracking per group/partition
- [ ] Manual and auto offset commit
- [ ] Rebalancing on consumer join/leave
- [ ] Consumer heartbeat (liveness)
- [ ] Long-polling for messages

**Code Structure:**
```
internal/
├── consumer/
│   ├── group.go              # Consumer group management
│   ├── assignment.go         # Partition assignment
│   ├── coordinator.go        # Group coordinator
│   └── offset.go             # Offset storage
├── api/
│   └── http/
│       ├── subscribe.go      # Consumer endpoints
│       └── offset.go         # Offset commit endpoints
└── storage/
    └── offset_store.go       # Persistent offset storage
```

**Consumer API:**
```
# Join consumer group
POST /v1/groups/processors/join
{
  "consumerId": "consumer-1",
  "topics": ["orders"]
}

Response:
{
  "memberId": "consumer-1-abc123",
  "assignments": [
    {"topic": "orders", "partition": 0},
    {"topic": "orders", "partition": 1}
  ]
}

# Fetch messages
GET /v1/groups/processors/poll?timeout=30s

Response:
{
  "messages": [
    {
      "topic": "orders",
      "partition": 0,
      "offset": 12345,
      "key": "user-123",
      "value": "base64...",
      "timestamp": "2025-01-15T10:30:00Z"
    }
  ]
}

# Commit offsets
POST /v1/groups/processors/commit
{
  "offsets": [
    {"topic": "orders", "partition": 0, "offset": 12346}
  ]
}

# Heartbeat (keep membership alive)
POST /v1/groups/processors/heartbeat
{
  "memberId": "consumer-1-abc123"
}
```

**Rebalancing Flow:**
```
Consumer A joins → Assigned P0, P1, P2
Consumer B joins → Rebalance!
  Consumer A → P0, P1
  Consumer B → P2
Consumer A leaves → Rebalance!
  Consumer B → P0, P1, P2
```

**Tests:**
- [ ] Consumer receives messages from assigned partitions
- [ ] Offset commit persists across restarts
- [ ] Rebalancing redistributes partitions
- [ ] Dead consumer (no heartbeat) triggers rebalance
- [ ] New consumer joins, gets fair share

---

### Milestone 4: Reliability - ACKs, DLQ & Backpressure

**Goal:** At-least-once delivery guarantees

**Learning Focus:**
- Delivery semantics (at-most-once, at-least-once, exactly-once)
- Dead letter queue patterns
- Backpressure strategies
- Retry with exponential backoff

**Deliverables:**
- [ ] Per-message acknowledgment
- [ ] Visibility timeout (message redelivered if not ACKed)
- [ ] Retry count tracking
- [ ] Dead letter queue (after N retries)
- [ ] Backpressure (limit unacked messages per consumer)
- [ ] Consumer lag metrics
- [ ] Message TTL (expire unprocessed messages)

**Code Structure:**
```
internal/
├── consumer/
│   ├── ack.go                # Acknowledgment handling
│   ├── visibility.go         # Visibility timeout
│   └── dlq.go                # Dead letter queue
├── backpressure/
│   └── limiter.go            # Unacked message limiting
└── broker/
    └── retention.go          # TTL enforcement
```

**Acknowledgment Flow:**
```
┌──────────┐     fetch      ┌──────────┐
│ Consumer │◄───────────────│  Broker  │
└────┬─────┘                └────┬─────┘
     │                           │
     │    message in-flight      │
     │  (visibility timeout=30s) │
     │                           │
     ├────── ACK ───────────────►│  → Mark processed, advance offset
     │           OR              │
     ├────── NACK ──────────────►│  → Redeliver immediately
     │           OR              │
     │   (timeout expires)       │  → Redeliver automatically
     │                           │
     │   retry_count >= 3        │
     │           ↓               │
     │     Send to DLQ           │
```

**Extended API:**
```
# Acknowledge message
POST /v1/groups/processors/ack
{
  "topic": "orders",
  "partition": 0,
  "offset": 12345
}

# Negative acknowledge (redeliver)
POST /v1/groups/processors/nack
{
  "topic": "orders",
  "partition": 0,
  "offset": 12345,
  "delay": "5s"      # Optional delay before redelivery
}

# Read from DLQ
GET /v1/topics/orders.dlq/messages
```

**Configuration:**
```yaml
topics:
  orders:
    partitions: 3
    retention:
      hours: 168
    delivery:
      visibilityTimeout: 30s
      maxRetries: 3
      dlqTopic: "orders.dlq"
    backpressure:
      maxUnackedPerConsumer: 1000
```

**Tests:**
- [ ] Unacked message redelivered after timeout
- [ ] NACKed message redelivered immediately
- [ ] Message goes to DLQ after max retries
- [ ] Backpressure limits in-flight messages
- [ ] Consumer lag reflects unprocessed messages

---

### Milestone 5: Distribution - Replication & Leader Election

**Goal:** Multi-node deployment with fault tolerance

**Learning Focus:**
- Leader election (simplified, not full Raft)
- Log replication strategies
- Consistency vs availability trade-offs
- Cluster membership

**Deliverables:**
- [ ] Multi-node cluster formation
- [ ] Partition leader election (using etcd or simple protocol)
- [ ] Log replication to followers
- [ ] Configurable replication factor
- [ ] Leader failover
- [ ] Split-brain protection
- [ ] Cluster metadata store

**Code Structure:**
```
internal/
├── cluster/
│   ├── cluster.go            # Cluster management
│   ├── membership.go         # Node join/leave
│   ├── metadata.go           # Cluster metadata
│   └── election.go           # Leader election
├── replication/
│   ├── replicator.go         # Log replication
│   ├── follower.go           # Follower sync
│   └── acks.go               # Replication acknowledgment
└── api/
    └── internal/
        └── replication.go    # Inter-node API
```

**Replication Flow:**
```
Producer
    │
    ▼
┌─────────┐  replicate   ┌──────────┐
│ Leader  │─────────────►│ Follower │
│ (P0)    │              │   (P0)   │
│         │◄─────────────│          │
│         │    ack       │          │
└─────────┘              └──────────┘
    │
    │ ack (after replication)
    ▼
Producer
```

**Leader Election (Simplified):**
```go
// Using a simple lease-based approach
// - Each partition has one leader
// - Leader holds a lease (must renew periodically)
// - If lease expires, followers can claim leadership
// - Highest node ID wins tie-breaker (simple, deterministic)
```

**Cluster Configuration:**
```yaml
cluster:
  nodeId: "node-1"
  advertiseAddr: "10.0.1.1:7000"
  peers:
    - "10.0.1.2:7000"
    - "10.0.1.3:7000"
  
  # Using etcd for coordination (simpler than implementing our own)
  etcd:
    endpoints:
      - "10.0.2.1:2379"

replication:
  factor: 2                 # Each partition has 2 replicas
  ackMode: "leader"         # or "all" for stronger durability
  syncIntervalMs: 100
```

**Tests:**
- [ ] Partition leaders elected on startup
- [ ] Messages replicated to followers
- [ ] Leader failure triggers new election
- [ ] No data loss on leader failover
- [ ] Split cluster rejects writes (minority)

---

### Milestone 6: APIs, Operations & Production Readiness

**Goal:** Production-ready with full API, CLI, metrics, and K8s deployment

**Learning Focus:**
- gRPC API design
- CLI tool patterns in Go (cobra)
- Kubernetes StatefulSet for stateful apps
- Prometheus metrics for queues

**Deliverables:**
- [ ] gRPC API (in addition to HTTP)
- [ ] Admin CLI tool (goqueue-cli)
- [ ] Prometheus metrics
- [ ] Dockerfile (multi-stage)
- [ ] docker-compose for local cluster
- [ ] Helm chart with StatefulSet
- [ ] Grafana dashboard
- [ ] Load tests with results
- [ ] Comprehensive README
- [ ] Demo video

**CLI Tool:**
```bash
# Topic management
goqueue-cli topic create orders --partitions 3 --replication 2
goqueue-cli topic list
goqueue-cli topic describe orders
goqueue-cli topic delete orders

# Produce/consume (for testing)
echo "hello" | goqueue-cli produce orders
goqueue-cli consume orders --group test --from-beginning

# Consumer groups
goqueue-cli groups list
goqueue-cli groups describe processors
goqueue-cli groups reset-offsets processors --topic orders --to-earliest

# Cluster info
goqueue-cli cluster info
goqueue-cli cluster nodes
```

**Metrics:**
```
# Topics
goqueue_topic_messages_total{topic="orders", partition="0"}
goqueue_topic_bytes_total{topic="orders", partition="0"}

# Producers
goqueue_producer_messages_total{topic="orders"}
goqueue_producer_errors_total{topic="orders"}
goqueue_producer_latency_seconds{topic="orders"} (histogram)

# Consumers
goqueue_consumer_messages_total{group="processors", topic="orders"}
goqueue_consumer_lag{group="processors", topic="orders", partition="0"}
goqueue_consumer_commit_latency_seconds{group="processors"}

# Cluster
goqueue_partition_leader{topic="orders", partition="0", node="node-1"}
goqueue_replication_lag{topic="orders", partition="0", follower="node-2"}
goqueue_cluster_nodes{status="healthy|unhealthy"}
```

**Kubernetes Deployment:**
```yaml
# StatefulSet for stable network identities
# PVC for each pod's log storage
# Headless service for peer discovery
# Regular service for client access
```

**Code Structure:**
```
goqueue/
├── cmd/
│   ├── goqueue/              # Broker binary
│   │   └── main.go
│   └── goqueue-cli/          # CLI tool
│       └── main.go
├── internal/...
├── api/
│   └── proto/
│       └── queue.proto
├── deployments/
│   ├── docker/
│   │   └── Dockerfile
│   ├── docker-compose.yaml   # 3-node local cluster
│   └── helm/
│       └── goqueue/
│           ├── Chart.yaml
│           ├── values.yaml
│           └── templates/
│               ├── statefulset.yaml
│               ├── service.yaml
│               ├── headless-service.yaml
│               ├── configmap.yaml
│               └── pvc.yaml
├── scripts/
│   ├── loadtest.js
│   └── chaos.sh              # Kill nodes randomly
├── dashboards/
│   └── grafana.json
└── README.md
```

**Load Test Targets:**
- [ ] 100K messages/second write (3 nodes, 12 partitions)
- [ ] p99 publish latency < 10ms
- [ ] Zero message loss during leader failover
- [ ] Consumer group rebalance < 5 seconds

---

## Configuration Reference

```yaml
# Full broker configuration
broker:
  nodeId: "node-1"
  dataDir: "/var/lib/goqueue"
  
  listeners:
    http: ":8080"
    grpc: ":9000"
    internal: ":7000"      # Inter-node communication
    
  metrics:
    listen: ":9090"
    
logging:
  level: info
  format: json

storage:
  segmentSize: 1073741824  # 1GB per segment
  indexInterval: 4096      # Index every 4KB
  
cluster:
  etcd:
    endpoints:
      - "etcd:2379"
  replicationFactor: 2
  minInSyncReplicas: 1

defaults:
  topic:
    partitions: 3
    retention:
      hours: 168
      bytes: -1            # Unlimited
    delivery:
      visibilityTimeout: 30s
      maxRetries: 3
      
consumer:
  sessionTimeout: 30s
  heartbeatInterval: 10s
  maxPollRecords: 500
  
producer:
  batchSize: 16384         # 16KB
  lingerMs: 5              # Wait up to 5ms for batch
  acks: "leader"           # "leader" or "all"
```

---

## Development

```bash
# Run single node locally
go run ./cmd/goqueue -config config.yaml

# Run 3-node cluster with docker-compose
docker-compose -f deployments/docker-compose.yaml up

# Build
go build -o bin/goqueue ./cmd/goqueue
go build -o bin/goqueue-cli ./cmd/goqueue-cli

# Test
go test ./... -v

# Benchmark storage engine
go test ./internal/storage/... -bench=. -benchmem

# Load test
k6 run scripts/loadtest.js

# Chaos test (kill random nodes)
./scripts/chaos.sh

# Kubernetes
helm install goqueue ./deployments/helm/goqueue --set replicas=3
```

---

## Learning Resources

### Storage & Logs
- [The Log: What every software engineer should know](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) - Jay Kreps (Kafka creator)
- [Kafka: The Definitive Guide](https://www.confluent.io/resources/kafka-the-definitive-guide/) - Free ebook
- [How Kafka's Storage Internals Work](https://www.confluent.io/blog/apache-kafka-internals-broker-storage/)

### Distributed Systems
- [Designing Data-Intensive Applications](https://dataintensive.net/) - Chapter 5 (Replication), Chapter 6 (Partitioning)
- [Notes on Distributed Systems for Young Bloods](https://www.somethingsimilar.com/2013/01/14/notes-on-distributed-systems-for-young-bloods/)

### Leader Election
- [Raft Paper](https://raft.github.io/raft.pdf) - Just leader election section
- [etcd's use of Raft](https://etcd.io/docs/v3.5/learning/api/#lease-api)
- Using etcd leases for simple leader election

### Message Queue Design
- [RabbitMQ Internals](https://www.rabbitmq.com/tutorials/amqp-concepts.html)
- [SQS vs Kafka: Know the Differences](https://www.confluent.io/blog/kafka-vs-sqs-comparison/)
