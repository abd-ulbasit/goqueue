---
layout: default
title: Operations


---

# Operations Guide


Deploy, monitor, and operate GoQueue in production.


---

## Deployment Options

| Environment | Recommended Setup |
|-------------|------------------|
| Development | Single binary, local filesystem |
| Docker | Docker Compose with persistence |
| Kubernetes | Helm chart with StatefulSet |
| Production | 3+ node cluster, `GOQUEUE_CLUSTER_ENABLED=true` |

Coordination is built in: gossip membership and controller election run inside
the broker, so a production cluster is three or more GoQueue processes and
nothing else. There is no ZooKeeper, etcd or other external coordination
service to deploy.

---

## Quick Deployment

### Binary

```bash
# Download latest release
curl -L https://github.com/abd-ulbasit/goqueue/releases/latest/download/goqueue-linux-amd64 -o goqueue
chmod +x goqueue

# Start with defaults (binds to loopback, data in ./data)
./goqueue

# Configure through the environment; there is no config file or flag
GOQUEUE_BROKER_DATADIR=/var/lib/goqueue \
GOQUEUE_LISTENERS_HTTP=:8080 \
GOQUEUE_LISTENERS_GRPC=:9000 \
./goqueue
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

GoQueue exposes Prometheus metrics at `/metrics` on the HTTP API listener.
There is no separate metrics port:

```bash
curl http://localhost:8080/metrics
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

Security is configured through the environment, like everything else. The full
variable list is in the [Configuration Reference]({{ '/docs/configuration/reference' | relative_url }}).

### TLS

```bash
export GOQUEUE_TLS_ENABLED=true
export GOQUEUE_TLS_CERT_FILE=/etc/goqueue/tls/server.crt
export GOQUEUE_TLS_KEY_FILE=/etc/goqueue/tls/server.key
export GOQUEUE_TLS_CA_FILE=/etc/goqueue/tls/ca.crt
export GOQUEUE_TLS_CLIENT_AUTH=require-verify
export GOQUEUE_TLS_MIN_VERSION=1.3
```

The certificate files are watched and reloaded in place, so renewal does not
need a restart. The same suffixes under `GOQUEUE_CLUSTER_TLS_` configure mTLS
between cluster nodes.

### Authentication

```bash
export GOQUEUE_AUTH_ENABLED=true
export GOQUEUE_API_ROOT_KEY=...          # keep in a Secret, not in a manifest
export GOQUEUE_AUTH_ALLOW_HEALTH=true    # false breaks Kubernetes probes
```

The root key is registered at startup with the `admin` role. It is the only key
that comes from the environment; further keys are minted through the admin API
using it, and are returned once at creation time:

```bash
curl -H "X-API-Key: $GOQUEUE_API_ROOT_KEY" -H "Content-Type: application/json" \
  -d '{"name":"producer-svc","roles":["producer"]}' \
  http://localhost:8080/admin/keys
{"id":"key_3444ca357e1abf51","key":"gq_22a8294e...","prefix":"gq_22a8294e","name":"producer-svc","roles":["producer"]}
```

`GET /admin/keys` lists keys by hash and prefix, `DELETE /admin/keys/{keyID}`
revokes one. Roles are `admin`, `producer`, `consumer` and `readonly`.

### Authorization

```bash
export GOQUEUE_ACL_ENABLED=true
```

Enables per-key ACL enforcement on top of authentication. Rules are managed at
`/admin/acls` and scoped to a topic, consumer group or the cluster:

```bash
curl -H "X-API-Key: $GOQUEUE_API_ROOT_KEY" -H "Content-Type: application/json" \
  -d '{"principal":"producer-svc","resource_type":"topic","resource_name":"orders","operation":"write","effect":"allow"}' \
  http://localhost:8080/admin/acls
```

`resource_type` is `topic`, `group` or `cluster`; `operation` is `read`,
`write`, `create`, `delete` or `all`; `effect` is `allow` or `deny`. Rules are
held in memory, so they need reapplying after a restart.

---

## Next Steps

- [Configuration Reference]({{ '/docs/configuration/reference' | relative_url }}) - Every variable the broker reads
- [Benchmarks]({{ '/docs/operations/benchmarks' | relative_url }}) - What has been measured, and how
