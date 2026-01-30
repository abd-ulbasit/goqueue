---
layout: default
title: API Reference


---

# API Reference


Complete reference for GoQueue's REST and gRPC APIs.


---

## Overview

GoQueue provides two APIs:

| API | Port | Use Case | Protocol |
|-----|------|----------|----------|
| **REST API** | 8080 | CRUD operations, admin, simple clients | HTTP/JSON |
| **gRPC API** | 9000 | High-performance streaming, Go/Java clients | Protocol Buffers |

## REST API

The REST API is available on port 8080 by default. All endpoints return JSON.

### Base URL

```
http://localhost:8080
```

### Authentication

GoQueue supports optional authentication via API keys or JWT tokens:

```bash
# API Key (header)
curl -H "X-API-Key: your-api-key" http://localhost:8080/topics

# Bearer Token
curl -H "Authorization: Bearer your-jwt-token" http://localhost:8080/topics
```

### Common Response Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 201 | Created |
| 400 | Bad request (check request body) |
| 404 | Resource not found |
| 409 | Conflict (e.g., topic already exists) |
| 429 | Rate limited |
| 500 | Internal server error |

### Error Response Format

```json
{
  "error": {
    "code": "TOPIC_NOT_FOUND",
    "message": "Topic 'orders' does not exist",
    "details": {
      "topic": "orders"
    }
  }
}
```

---

## API Categories

### Core Operations

| Endpoint | Description |
|----------|-------------|
| [Topics](topics-api) | Create, list, delete topics |
| [Messages](messages-api) | Publish and consume messages |
| [Consumer Groups](consumer-groups-api) | Group management and coordination |
| [Offsets](offsets-api) | Commit and retrieve offsets |

### Advanced Features

| Endpoint | Description |
|----------|-------------|
| [Reliability](reliability-api) | ACK, NACK, DLQ operations |
| [Priority](priority-api) | Priority queue management |
| [Delayed Messages](delayed-api) | Scheduled message delivery |
| [Transactions](transactions-api) | Atomic multi-message operations |
| [Schemas](schemas-api) | Schema registry operations |

### Operations

| Endpoint | Description |
|----------|-------------|
| [Health](health-api) | Health check endpoints |
| [Admin](admin-api) | Cluster and broker management |
| [Metrics](metrics-api) | Prometheus metrics |

---

## Quick Reference

### Topics

```bash
# List topics
GET /topics

# Create topic
POST /topics
{"name": "orders", "partitions": 6}

# Get topic details
GET /topics/{name}

# Delete topic
DELETE /topics/{name}
```

### Messages

```bash
# Publish message
POST /topics/{name}/messages
{"payload": "...", "key": "order-123"}

# Batch publish
POST /topics/{name}/messages/batch
{"messages": [{"payload": "..."}, ...]}

# Consume messages
GET /topics/{name}/partitions/{partition}/messages?offset=0&max=100

# Long-poll consume
GET /topics/{name}/partitions/{partition}/messages?wait_ms=30000
```

### Consumer Groups

```bash
# Join group
POST /groups/{group}/join
{"member_id": "consumer-1", "topics": ["orders"]}

# Heartbeat
POST /groups/{group}/heartbeat
{"member_id": "consumer-1", "generation": 1}

# Leave group
POST /groups/{group}/leave
{"member_id": "consumer-1"}

# Get group info
GET /groups/{group}
```

### Acknowledgments

```bash
# Acknowledge message
POST /messages/ack
{"topic": "orders", "partition": 0, "offset": 42}

# Negative acknowledge
POST /messages/nack
{"topic": "orders", "partition": 0, "offset": 42, "requeue": true}
```

### Health

```bash
# Kubernetes liveness
GET /livez

# Kubernetes readiness
GET /readyz

# Combined health
GET /healthz

# Version info
GET /version
```

---

## OpenAPI Specification

The complete OpenAPI 3.1 specification is available at:

- **Interactive Docs**: `http://localhost:8080/docs`
- **OpenAPI JSON**: `http://localhost:8080/openapi.json`
- **Raw Spec**: [openapi.yaml](https://github.com/abd-ulbasit/goqueue/blob/main/api/openapi/openapi.yaml)

---

## gRPC API

The gRPC API is available on port 9000 and provides streaming capabilities.

### Proto Definition

```protobuf
service GoQueue {
  // Streaming publish
  rpc PublishStream(stream PublishRequest) returns (stream PublishResponse);
  
  // Streaming consume  
  rpc ConsumeStream(ConsumeRequest) returns (stream Message);
  
  // Bidirectional streaming for consumer groups
  rpc GroupStream(stream GroupRequest) returns (stream GroupResponse);
}
```

### Go Client Example

```go
import "github.com/abd-ulbasit/goqueue/pkg/client"

// Create client
client, err := client.NewGRPCClient("localhost:9000")
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Streaming publish
stream, err := client.PublishStream(ctx)
for _, msg := range messages {
    stream.Send(&pb.PublishRequest{
        Topic:   "orders",
        Payload: msg,
    })
}
responses, err := stream.CloseAndRecv()
```

### Full Proto Definition

See [goqueue.proto](https://github.com/abd-ulbasit/goqueue/blob/main/api/proto/goqueue.proto)

---

## Rate Limiting

GoQueue implements rate limiting per client:

| Limit | Default | Configurable |
|-------|---------|--------------|
| Requests per second | 1000 | Yes |
| Burst | 100 | Yes |
| Publish bytes/second | 100MB | Yes |

Rate limit headers:

```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 950
X-RateLimit-Reset: 1705320000
```

---

## Pagination

List endpoints support pagination:

```bash
# Page through topics
GET /topics?page=1&per_page=50

# Response includes pagination info
{
  "data": [...],
  "pagination": {
    "page": 1,
    "per_page": 50,
    "total": 120,
    "total_pages": 3
  }
}
```

---

## Next Steps

- [Topics API](topics-api) - Full topic endpoint reference
- [Messages API](messages-api) - Publishing and consuming
- [Go Client Library](../client/go) - Using the Go SDK
