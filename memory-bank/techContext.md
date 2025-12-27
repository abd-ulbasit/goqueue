# GoQueue - Technical Context

## Technology Stack

### Core Language
- **Go 1.21+** - Required for slog, improved generics

### Dependencies (Minimal)
| Dependency | Purpose | Why Not Stdlib |
|------------|---------|----------------|
| `gopkg.in/yaml.v3` | Config parsing | YAML is nicer than JSON for config |
| `google.golang.org/grpc` | gRPC server | Industry standard RPC |
| `google.golang.org/protobuf` | Proto encoding | Required for gRPC |
| `github.com/spf13/cobra` | CLI framework | De facto standard for Go CLIs |

### No External Runtime Dependencies
- No ZooKeeper
- No etcd (embedded coordination)
- No Kafka Connect
- No JVM

## Development Environment

### Required Tools
```bash
# Go
go version  # 1.21+

# Protocol Buffers
protoc --version  # 3.x

# gRPC tools
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Testing
go install github.com/rakyll/hey@latest  # HTTP load testing
go install go.k6.io/k6@latest           # Advanced load testing

# Development
go install github.com/air-verse/air@latest  # Hot reload
```

### Build Commands
```bash
# Build broker
go build -o bin/goqueue ./cmd/goqueue

# Build CLI
go build -o bin/goqueue-cli ./cmd/goqueue-cli

# Run tests
go test ./... -v -race

# Run benchmarks
go test ./internal/storage/... -bench=. -benchmem

# Generate protobuf
protoc --go_out=. --go-grpc_out=. api/proto/*.proto
```

## File System Layout

### Data Directory Structure
```
/var/lib/goqueue/
├── meta/
│   ├── topics.json           # Topic metadata
│   └── consumer_groups/
│       └── {group_id}.json   # Group state
├── logs/
│   └── {topic}/
│       └── {partition}/
│           ├── 00000000000000000000.log     # First segment
│           ├── 00000000000000000000.index   # Offset index
│           ├── 00000000000000000000.timeindex
│           ├── 00000000000000001000.log     # Second segment
│           ├── 00000000000000001000.index
│           └── ...
├── offsets/
│   └── {group_id}/
│       └── {topic}-{partition}.offset
├── delays/
│   ├── delay.wal             # Delay entries WAL
│   └── delay.index           # Timer wheel state
└── traces/
    └── {date}/
        └── traces.log        # Daily trace log
```

### File Naming Convention
- Segment files named by base offset (20 digits, zero-padded)
- Allows lexicographic sorting = offset sorting

## Configuration

### Environment Variables
```bash
GOQUEUE_NODE_ID=node-1
GOQUEUE_DATA_DIR=/var/lib/goqueue
GOQUEUE_HTTP_ADDR=:8080
GOQUEUE_GRPC_ADDR=:9000
GOQUEUE_METRICS_ADDR=:9090
GOQUEUE_LOG_LEVEL=info
GOQUEUE_LOG_FORMAT=json
```

### Config File (config.yaml)
```yaml
broker:
  nodeId: ${GOQUEUE_NODE_ID:node-1}
  dataDir: ${GOQUEUE_DATA_DIR:/var/lib/goqueue}
  
  listeners:
    http: ":8080"
    grpc: ":9000"
    internal: ":7000"    # Inter-node communication
    
storage:
  segmentSize: 1073741824    # 1GB
  indexIntervalBytes: 4096   # Index every 4KB
  timeIndexIntervalMs: 1000  # Index every second
  
  fsync:
    mode: "interval"         # none, every, interval
    intervalMs: 1000
    
defaults:
  topic:
    partitions: 6
    replicationFactor: 1     # Single node default
    retention:
      hours: 168             # 7 days
      bytes: -1              # Unlimited
      
  delivery:
    visibilityTimeout: 30s
    maxRetries: 3
    dlqEnabled: true
    
consumer:
  sessionTimeout: 30s
  heartbeatInterval: 10s
  maxPollRecords: 500
  rebalanceProtocol: cooperative
  
producer:
  batchSize: 16384           # 16KB
  lingerMs: 5
  acks: leader               # none, leader, all
  
metrics:
  enabled: true
  listen: ":9090"
  
logging:
  level: info
  format: json               # json, text
```

## API Design

### HTTP API (REST)
```
Topics:
  POST   /v1/topics                    # Create topic
  GET    /v1/topics                    # List topics
  GET    /v1/topics/{topic}            # Get topic info
  DELETE /v1/topics/{topic}            # Delete topic

Publish:
  POST   /v1/topics/{topic}/publish    # Publish single message
  POST   /v1/topics/{topic}/batch      # Publish batch

Consumer Groups:
  POST   /v1/groups/{group}/join       # Join group
  POST   /v1/groups/{group}/leave      # Leave group
  POST   /v1/groups/{group}/heartbeat  # Send heartbeat
  GET    /v1/groups/{group}/poll       # Long-poll for messages
  POST   /v1/groups/{group}/commit     # Commit offsets
  POST   /v1/groups/{group}/ack        # Ack single message
  POST   /v1/groups/{group}/nack       # Nack message

Admin:
  GET    /v1/admin/metrics             # Prometheus metrics
  GET    /v1/admin/health              # Health check
  GET    /v1/admin/cluster             # Cluster info
```

### gRPC API
```protobuf
service GoQueue {
  // Topics
  rpc CreateTopic(CreateTopicRequest) returns (Topic);
  rpc DeleteTopic(DeleteTopicRequest) returns (Empty);
  rpc ListTopics(Empty) returns (ListTopicsResponse);
  
  // Produce
  rpc Publish(PublishRequest) returns (PublishResponse);
  rpc PublishStream(stream PublishRequest) returns (stream PublishResponse);
  
  // Consume
  rpc Subscribe(SubscribeRequest) returns (stream Message);
  rpc Ack(AckRequest) returns (Empty);
  rpc Commit(CommitRequest) returns (Empty);
}
```

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Write throughput (1 node) | 100K msg/s | 1KB messages |
| Write throughput (3 nodes) | 250K msg/s | With replication |
| Read throughput | 500K msg/s | Sequential reads |
| p50 publish latency | < 1ms | acks=leader |
| p99 publish latency | < 10ms | acks=leader |
| p50 consume latency | < 1ms | From memory |
| p99 consume latency | < 5ms | Including disk |
| Rebalance duration | < 1 second | Cooperative |
| Leader failover | < 5 seconds | Detection + election |

## Testing Strategy

### Unit Tests
- Storage layer: message encoding, segment operations, index lookups
- Broker layer: topic management, partitioning, batching
- Consumer layer: offset management, rebalancing logic
- Delivery layer: visibility timeout, DLQ routing

### Integration Tests
- Produce-consume round trip
- Consumer group rebalancing
- Leader failover
- Delayed message delivery

### Chaos Tests
- Kill random nodes
- Network partitions (iptables)
- Disk full simulation
- Clock skew

### Load Tests
- Sustained throughput
- Spike handling
- Memory under load
- Consumer lag behavior

## Monitoring

### Key Prometheus Metrics
```
# Storage
goqueue_log_size_bytes{topic, partition}
goqueue_log_segments_total{topic, partition}
goqueue_log_append_latency_seconds{topic, partition}

# Producers
goqueue_messages_produced_total{topic}
goqueue_bytes_produced_total{topic}
goqueue_produce_errors_total{topic, error_type}
goqueue_produce_latency_seconds{topic}

# Consumers
goqueue_messages_consumed_total{group, topic}
goqueue_consumer_lag{group, topic, partition}
goqueue_visibility_timeout_total{group, topic}
goqueue_dlq_total{topic}

# Cluster
goqueue_partition_leader{topic, partition, node}
goqueue_replication_lag_messages{topic, partition, follower}
goqueue_rebalance_duration_seconds{group}
goqueue_nodes_total{status}
```

## Security Considerations (Future)

### Authentication
- API key authentication
- mTLS for inter-node communication

### Authorization
- Topic-level ACLs
- Consumer group ACLs

### Encryption
- TLS for client connections
- Encryption at rest (optional)
