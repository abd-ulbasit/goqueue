---
layout: default
title: Operations
nav_order: 5
has_children: true
---

# Operations Guide
{: .no_toc }

Deploy, monitor, and operate GoQueue in production.
{: .fs-6 .fw-300 }

---

## Deployment Options

| Environment | Recommended Setup | Guide |
|-------------|------------------|-------|
| Development | Single binary, local filesystem | [Local Setup](local) |
| Docker | Docker Compose with persistence | [Docker](docker) |
| Kubernetes | Helm chart with StatefulSet | [Kubernetes](kubernetes) |
| Production | 3+ node cluster with etcd | [Clustering](clustering) |

---

## Quick Deployment

### Binary

```bash
# Download latest release
curl -L https://github.com/abd-ulbasit/goqueue/releases/latest/download/goqueue-linux-amd64 -o goqueue
chmod +x goqueue

# Start with defaults
./goqueue

# Start with custom config
./goqueue --config /etc/goqueue/config.yaml
```

### Docker

```bash
# Single container
docker run -d \
  --name goqueue \
  -p 8080:8080 \
  -p 9000:9000 \
  -v goqueue-data:/var/lib/goqueue \
  ghcr.io/abd-ulbasit/goqueue:latest

# With docker-compose
docker-compose up -d
```

### Kubernetes

```bash
# Add Helm repo
helm repo add goqueue https://abd-ulbasit.github.io/goqueue/charts
helm repo update

# Install
helm install goqueue goqueue/goqueue \
  --namespace goqueue \
  --create-namespace
```

---

## Monitoring

### Prometheus Metrics

GoQueue exposes Prometheus metrics on port 9090:

```bash
curl http://localhost:9090/metrics
```

Key metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `goqueue_messages_published_total` | Counter | Total messages published |
| `goqueue_messages_consumed_total` | Counter | Total messages consumed |
| `goqueue_consumer_lag` | Gauge | Messages behind latest |
| `goqueue_partition_size_bytes` | Gauge | Partition data size |
| `goqueue_rebalances_total` | Counter | Consumer group rebalances |

### Grafana Dashboard

Import our pre-built dashboard:

```bash
# Dashboard ID: 12345
# Or download from: https://github.com/abd-ulbasit/goqueue/tree/main/deploy/grafana/dashboards
```

### Health Checks

```bash
# Kubernetes liveness probe
GET /livez
# Returns 200 if process is alive

# Kubernetes readiness probe  
GET /readyz
# Returns 200 if ready to serve traffic

# Combined health check
GET /healthz
# Returns 200 if healthy, 503 if degraded
```

---

## Alerting

Recommended alerts:

| Alert | Condition | Severity |
|-------|-----------|----------|
| High Consumer Lag | lag > 10000 for 5m | Warning |
| Consumer Group Stuck | lag increasing for 15m | Critical |
| Disk Space Low | partition disk > 80% | Warning |
| Disk Space Critical | partition disk > 95% | Critical |
| No Leader | partition has no leader for 1m | Critical |
| Replication Lag | ISR < replication factor for 5m | Warning |

See [Alert Rules](alerting) for Prometheus alertmanager configurations.

---

## Backup & Recovery

### Backup

```bash
# Backup all data
goqueue-admin backup \
  --output /backup/goqueue-$(date +%Y%m%d).tar.gz

# Backup specific topics
goqueue-admin backup \
  --topics orders,events \
  --output /backup/orders-events.tar.gz
```

### Restore

```bash
# Full restore (stops broker)
goqueue-admin restore \
  --input /backup/goqueue-20240115.tar.gz

# Topic restore (online)
goqueue-admin restore \
  --input /backup/orders-events.tar.gz \
  --topics orders
```

---

## Common Operations

### Adding Partitions

```bash
curl -X PATCH http://localhost:8080/topics/orders \
  -d '{"partitions": 12}'
```

{: .warning }
You can only increase partitions, never decrease.

### Resetting Consumer Offset

```bash
# Reset to earliest
curl -X POST http://localhost:8080/groups/order-service/offsets/reset \
  -d '{"topic": "orders", "to": "earliest"}'

# Reset to specific offset
curl -X POST http://localhost:8080/groups/order-service/offsets/reset \
  -d '{"topic": "orders", "partition": 0, "offset": 1000}'

# Reset to timestamp
curl -X POST http://localhost:8080/groups/order-service/offsets/reset \
  -d '{"topic": "orders", "to_timestamp": "2024-01-15T00:00:00Z"}'
```

### Draining a Consumer

```bash
# Stop consumer from receiving new messages
curl -X POST http://localhost:8080/groups/order-service/members/consumer-1/drain

# Wait for in-flight to complete, then remove
```

---

## Troubleshooting

### Consumer Lag Growing

1. Check consumer processing rate
2. Add more consumers (up to partition count)
3. Check for slow consumers (increase timeout or optimize)
4. Consider adding partitions

### Rebalances Too Frequent

1. Increase `sessionTimeout` (default 30s)
2. Check consumer stability (crashes, network)
3. Check GC pauses (tune Java/Go GC if needed)

### Disk Space Issues

1. Reduce retention period
2. Enable compression
3. Add storage / expand volumes
4. Archive old data

See [Troubleshooting Guide](troubleshooting) for more.

---

## Security

### TLS/SSL

```yaml
listeners:
  http: ":8080"
  grpc: ":9000"
  
tls:
  enabled: true
  certFile: "/etc/goqueue/tls/server.crt"
  keyFile: "/etc/goqueue/tls/server.key"
  caFile: "/etc/goqueue/tls/ca.crt"
  clientAuth: "require"  # none, request, require
```

### Authentication

```yaml
auth:
  enabled: true
  
  # API Key authentication
  apiKeys:
    - key: "your-api-key"
      name: "admin"
      roles: ["admin"]
    - key: "producer-key"
      name: "producer"
      roles: ["producer"]
  
  # JWT authentication
  jwt:
    enabled: true
    issuer: "https://auth.example.com"
    jwksUrl: "https://auth.example.com/.well-known/jwks.json"
```

### Authorization

```yaml
authorization:
  enabled: true
  
  # Role definitions
  roles:
    admin:
      permissions: ["*"]
    producer:
      permissions: ["topic:read", "topic:write"]
    consumer:
      permissions: ["topic:read", "group:*"]
    readonly:
      permissions: ["topic:read"]
  
  # ACLs
  acls:
    - principal: "producer-service"
      resource: "topic:orders"
      operations: ["write"]
    - principal: "analytics"
      resource: "topic:*"
      operations: ["read"]
```

---

## Next Steps

- [Docker Deployment](docker) - Container deployment guide
- [Kubernetes Deployment](kubernetes) - Helm chart and manifests
- [Clustering](clustering) - Multi-node setup
- [Monitoring](monitoring) - Detailed observability guide
