---
layout: default
title: Configuration Reference
parent: Configuration
nav_order: 1
---

# Configuration Reference
{: .no_toc }

Complete reference for all GoQueue configuration options.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Configuration File Location

By default, GoQueue looks for configuration in:

1. `./config.yaml`
2. `/etc/goqueue/config.yaml`
3. `$HOME/.goqueue/config.yaml`

Specify a custom location:

```bash
goqueue --config /path/to/config.yaml
```

---

## Complete Configuration Example

```yaml
# GoQueue Configuration
# Copy this file to config.yaml and customize

# ==============================================================================
# BROKER SETTINGS
# ==============================================================================
broker:
  # Unique identifier for this node in a cluster
  # Default: auto-generated UUID
  nodeId: "node-1"
  
  # Directory for storing data (logs, indexes, offsets)
  # Default: /var/lib/goqueue
  dataDir: "/var/lib/goqueue"

# ==============================================================================
# NETWORK LISTENERS
# ==============================================================================
listeners:
  # HTTP REST API endpoint
  # Used by: producers, consumers, admin operations
  # Default: :8080
  http: ":8080"
  
  # gRPC API endpoint
  # Used by: Go/Java clients for streaming
  # Default: :9000
  grpc: ":9000"
  
  # Internal cluster communication
  # Used by: replication, cluster coordination
  # Default: :7000
  internal: ":7000"

# ==============================================================================
# METRICS CONFIGURATION
# ==============================================================================
metrics:
  # Prometheus metrics endpoint
  # Default: :9090
  listen: ":9090"
  
  # Path for metrics scraping
  # Default: /metrics
  path: "/metrics"
  
  # Include Go runtime metrics (goroutines, memory, GC)
  # Default: true
  includeGoCollector: true
  
  # Include process metrics (CPU, file descriptors)
  # Default: true
  includeProcessCollector: true

# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================
logging:
  # Log level: debug, info, warn, error
  # Default: info
  level: "info"
  
  # Log format: json or text
  # Default: json (recommended for production)
  format: "json"
  
  # Output destination: stdout, stderr, or file path
  # Default: stdout
  output: "stdout"

# ==============================================================================
# STORAGE ENGINE SETTINGS
# ==============================================================================
storage:
  # Maximum size per segment file before rolling to new segment
  # Larger segments = fewer files, slower recovery
  # Smaller segments = more files, faster recovery
  # Default: 1073741824 (1GB)
  segmentSize: 1073741824
  
  # Bytes between sparse index entries
  # Smaller = more precise seeks, larger index files
  # Larger = less precise seeks, smaller index files
  # Default: 4096 (4KB)
  indexInterval: 4096
  
  # Sync writes to disk immediately
  # true = safer but slower (fsync on every write)
  # false = faster but risk of data loss on crash
  # Default: false
  syncOnWrite: false
  
  # If syncOnWrite is false, sync interval
  # How often to flush writes to disk
  # Default: 1s
  syncInterval: "1s"
  
  # Compression algorithm for messages
  # Options: none, snappy, lz4, zstd
  # Default: none
  compression: "none"

# ==============================================================================
# CLUSTER CONFIGURATION
# ==============================================================================
cluster:
  # Enable clustering (requires etcd)
  # Default: false
  enabled: false
  
  # etcd endpoints for cluster coordination
  etcd:
    endpoints:
      - "localhost:2379"
    # Optional authentication
    username: ""
    password: ""
    # TLS settings
    tls:
      enabled: false
      certFile: ""
      keyFile: ""
      caFile: ""
  
  # Default replication factor for new topics
  # Minimum: 1, Recommended: 3
  # Default: 2
  replicationFactor: 2
  
  # Minimum replicas that must acknowledge a write
  # Must be <= replicationFactor
  # Default: 1
  minInSyncReplicas: 1
  
  # Timeout for replication requests
  # Default: 5s
  replicationTimeout: "5s"

# ==============================================================================
# DEFAULT TOPIC SETTINGS
# ==============================================================================
# These apply to newly created topics unless overridden
defaults:
  topic:
    # Number of partitions for new topics
    # More partitions = more parallelism
    # Default: 3
    partitions: 3
    
    # Message retention settings
    retention:
      # Delete messages older than this (0 = infinite retention)
      # Default: 168 (7 days)
      hours: 168
      
      # Delete oldest messages when topic exceeds this size
      # -1 = unlimited
      # Default: -1
      bytes: -1
    
    # Delivery settings
    delivery:
      # How long a message is invisible after being received
      # Gives consumer time to process before redelivery
      # Default: 30s
      visibilityTimeout: "30s"
      
      # Maximum delivery attempts before sending to DLQ
      # Default: 3
      maxRetries: 3
      
      # Dead letter queue topic suffix
      # Default: -dlq
      dlqSuffix: "-dlq"

# ==============================================================================
# CONSUMER SETTINGS
# ==============================================================================
consumer:
  # Session timeout: how long before consumer is considered dead
  # If no heartbeat received within this time, consumer is removed
  # Default: 30s
  sessionTimeout: "30s"
  
  # Heartbeat interval: how often consumer should send heartbeat
  # Should be < sessionTimeout/3
  # Default: 10s
  heartbeatInterval: "10s"
  
  # Maximum messages returned per poll request
  # Default: 500
  maxPollRecords: 500
  
  # Maximum time to wait for messages in long-poll
  # Default: 30s
  maxPollTimeout: "30s"
  
  # Auto-commit offset interval
  # 0 = manual commit only (recommended for at-least-once)
  # Default: 5s
  autoCommitInterval: "5s"
  
  # Rebalance settings
  rebalance:
    # Rebalance protocol: eager or cooperative
    # eager = all partitions revoked during rebalance
    # cooperative = incremental rebalance (KIP-429)
    # Default: cooperative
    protocol: "cooperative"
    
    # Maximum time to wait for rebalance to complete
    # Default: 60s
    timeout: "60s"

# ==============================================================================
# PRODUCER SETTINGS
# ==============================================================================
producer:
  # Batch messages up to this size before sending
  # Larger = higher throughput, higher latency
  # Default: 16384 (16KB)
  batchSize: 16384
  
  # Maximum time to wait for batch to fill
  # 0 = send immediately (no batching)
  # Default: 5ms
  lingerMs: 5
  
  # Acknowledgment mode
  # none = fire and forget (fastest, may lose messages)
  # leader = wait for leader to write (balanced)
  # all = wait for all in-sync replicas (safest)
  # Default: leader
  acks: "leader"
  
  # Retry settings for failed publishes
  retries: 3
  retryBackoff: "100ms"
  
  # Idempotent producer (exactly-once semantics)
  # Requires acks=all in cluster mode
  # Default: true
  idempotent: true
  
  # Transaction timeout
  # How long a transaction can be open before auto-abort
  # Default: 60s
  transactionTimeout: "60s"

# ==============================================================================
# PRIORITY QUEUE SETTINGS
# ==============================================================================
priority:
  # Enable priority queues
  # Default: true
  enabled: true
  
  # Scheduling algorithm
  # wfq = Weighted Fair Queuing (prevents starvation)
  # strict = Strict priority (higher always first)
  # Default: wfq
  scheduler: "wfq"
  
  # Weights for WFQ scheduling (higher = more resources)
  weights:
    critical: 100
    high: 50
    normal: 25
    low: 10
    background: 5

# ==============================================================================
# DELAY QUEUE SETTINGS
# ==============================================================================
delay:
  # Enable delayed messages
  # Default: true
  enabled: true
  
  # Timer wheel configuration
  timerWheel:
    # Tick duration (resolution of delay queue)
    # Smaller = more precise, more CPU
    # Default: 100ms
    tickDuration: "100ms"
    
    # Number of ticks per wheel
    # Default: 512
    ticksPerWheel: 512
  
  # Maximum delay allowed
  # Default: 168h (7 days)
  maxDelay: "168h"

# ==============================================================================
# SCHEMA REGISTRY SETTINGS
# ==============================================================================
schema:
  # Enable schema registry
  # Default: true
  enabled: true
  
  # Global compatibility mode
  # NONE, BACKWARD, FORWARD, FULL, BACKWARD_TRANSITIVE, FORWARD_TRANSITIVE, FULL_TRANSITIVE
  # Default: BACKWARD
  compatibility: "BACKWARD"
  
  # Validate messages against schema on publish
  # Default: false (opt-in per topic)
  validateOnPublish: false

# ==============================================================================
# MULTI-TENANCY SETTINGS
# ==============================================================================
tenancy:
  # Enable multi-tenancy
  # Default: false
  enabled: false
  
  # Default quotas for new tenants
  defaultQuotas:
    maxTopics: 100
    maxPartitionsPerTopic: 12
    maxMessageSizeBytes: 1048576  # 1MB
    maxMessagesPerSecond: 10000
    maxBytesPerSecond: 104857600  # 100MB/s
    maxRetentionHours: 720  # 30 days

# ==============================================================================
# TRACING SETTINGS
# ==============================================================================
tracing:
  # Enable message tracing
  # Default: true
  enabled: true
  
  # Maximum traces to keep in memory
  # Default: 10000
  maxTraces: 10000
  
  # Trace retention
  # Default: 24h
  retention: "24h"

# ==============================================================================
# TOPIC-SPECIFIC OVERRIDES
# ==============================================================================
# Override default settings for specific topics
topics:
  # High-throughput event topic
  events:
    partitions: 12
    retention:
      hours: 24
    delivery:
      visibilityTimeout: "10s"
      maxRetries: 1
  
  # Critical order processing topic
  orders:
    partitions: 6
    retention:
      hours: 720  # 30 days
    delivery:
      visibilityTimeout: "60s"
      maxRetries: 5
```

---

## Environment Variables

All configuration options can be set via environment variables with the `GOQUEUE_` prefix:

| Config Path | Environment Variable |
|-------------|---------------------|
| `broker.nodeId` | `GOQUEUE_BROKER_NODEID` |
| `broker.dataDir` | `GOQUEUE_BROKER_DATADIR` |
| `listeners.http` | `GOQUEUE_LISTENERS_HTTP` |
| `listeners.grpc` | `GOQUEUE_LISTENERS_GRPC` |
| `logging.level` | `GOQUEUE_LOGGING_LEVEL` |
| `storage.syncOnWrite` | `GOQUEUE_STORAGE_SYNCONWRITE` |
| `cluster.enabled` | `GOQUEUE_CLUSTER_ENABLED` |

Example:

```bash
export GOQUEUE_BROKER_DATADIR=/data/goqueue
export GOQUEUE_LISTENERS_HTTP=:8080
export GOQUEUE_LOGGING_LEVEL=debug
goqueue
```

---

## Command-Line Flags

Common flags:

```bash
goqueue [flags]

Flags:
  --config string       Path to configuration file
  --data-dir string     Data directory (overrides config)
  --http-port string    HTTP API port (default ":8080")
  --grpc-port string    gRPC API port (default ":9000")
  --log-level string    Log level: debug, info, warn, error
  --log-format string   Log format: json, text
  --node-id string      Node identifier for clustering
  --help                Show help
  --version             Show version
```

Example:

```bash
goqueue --config /etc/goqueue/config.yaml \
        --log-level debug \
        --http-port :8081
```

---

## Configuration by Use Case

### Development

```yaml
broker:
  dataDir: "./data"

listeners:
  http: ":8080"
  grpc: ":9000"

logging:
  level: "debug"
  format: "text"

storage:
  syncOnWrite: false
  segmentSize: 67108864  # 64MB (smaller for dev)
```

### Production (Single Node)

```yaml
broker:
  dataDir: "/var/lib/goqueue"

listeners:
  http: ":8080"
  grpc: ":9000"

logging:
  level: "info"
  format: "json"

storage:
  syncOnWrite: false
  syncInterval: "1s"
  segmentSize: 1073741824  # 1GB

defaults:
  topic:
    partitions: 6
    retention:
      hours: 168
```

### Production (Cluster)

```yaml
broker:
  nodeId: "node-1"
  dataDir: "/var/lib/goqueue"

cluster:
  enabled: true
  etcd:
    endpoints:
      - "etcd-1:2379"
      - "etcd-2:2379"
      - "etcd-3:2379"
  replicationFactor: 3
  minInSyncReplicas: 2

producer:
  acks: "all"
  idempotent: true
```

---

## Validation

Validate your configuration:

```bash
goqueue validate --config /path/to/config.yaml
```

This checks for:
- Syntax errors
- Invalid values
- Incompatible settings
- Security warnings

---

## Next Steps

- [Production Configuration](production) - Best practices for production
- [Performance Tuning](tuning) - Optimize for your workload
- [Cluster Setup](../operations/clustering) - Multi-node deployment
