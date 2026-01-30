# GoQueue Go Client

Go SDK for [GoQueue](https://github.com/abd-ulbasit/goqueue) - A high-performance distributed message queue that combines the best features of Kafka, SQS, and RabbitMQ.

## Features

- **Zero dependencies** - Pure Go HTTP client
- **Context support** - All operations accept context for cancellation
- **Automatic retry** - Exponential backoff on transient failures
- **Type-safe** - Full Go types for all requests/responses
- **Comprehensive docs** - Godoc comments for IDE support

## Installation

```bash
go get github.com/abd-ulbasit/goqueue-client-go
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    goqueue "github.com/abd-ulbasit/goqueue-client-go"
)

func main() {
    ctx := context.Background()
    
    // Create a client
    client := goqueue.NewClient("http://localhost:8080")
    defer client.Close()

    // Create a topic
    _, err := client.Topics.Create(ctx, &goqueue.CreateTopicRequest{
        Name:          "orders",
        NumPartitions: 3,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Publish messages
    resp, err := client.Messages.Publish(ctx, "orders", []*goqueue.PublishMessage{
        {Value: `{"orderId": "12345", "amount": 99.99}`},
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Published to partition %d, offset %d\n", 
        resp.Results[0].Partition, resp.Results[0].Offset)

    // Simple consume
    consumeResp, err := client.Messages.Consume(ctx, "orders", 0, 0, 100)
    if err != nil {
        log.Fatal(err)
    }
    for _, msg := range consumeResp.Messages {
        fmt.Printf("Received: %s\n", msg.Value)
    }
}
```

## Consumer Groups

For production workloads, use consumer groups:

```go
func worker(ctx context.Context, client *goqueue.Client, groupID, workerID string) error {
    // Join the consumer group
    join, err := client.Groups.Join(ctx, groupID, &goqueue.JoinGroupRequest{
        ClientID:       workerID,
        Topics:         []string{"orders"},
        SessionTimeout: "30s",
    })
    if err != nil {
        return err
    }
    memberID := join.MemberID
    generation := join.Generation

    fmt.Printf("Worker %s joined with partitions: %v\n", workerID, join.AssignedPartitions)

    defer func() {
        client.Groups.Leave(ctx, groupID, &goqueue.LeaveGroupRequest{
            MemberID: memberID,
        })
    }()

    for {
        // Poll for messages
        resp, err := client.Groups.Poll(ctx, groupID, memberID, &goqueue.PollOptions{
            MaxMessages: 10,
            Timeout:     "10s",
        })
        if err != nil {
            return err
        }

        // Process messages
        for _, msg := range resp.Messages {
            fmt.Printf("Worker %s processing: %s\n", workerID, msg.Value)
            
            // Acknowledge
            if err := client.Messages.Ack(ctx, msg.ReceiptHandle, nil); err != nil {
                return err
            }
        }

        // Send heartbeat
        heartbeat, err := client.Groups.Heartbeat(ctx, groupID, &goqueue.HeartbeatRequest{
            MemberID:   memberID,
            Generation: generation,
        })
        if err != nil {
            return err
        }

        if heartbeat.RebalanceRequired {
            fmt.Printf("Worker %s needs to rejoin\n", workerID)
            break
        }
    }
    return nil
}
```

## Message Options

### With Keys (Ordering)

```go
_, err := client.Messages.Publish(ctx, "orders", []*goqueue.PublishMessage{
    {Key: "user-123", Value: `{"event": "order_created"}`},
    {Key: "user-123", Value: `{"event": "payment_received"}`}, // Same partition
})
```

### Priority Messages

```go
_, err := client.Messages.Publish(ctx, "alerts", []*goqueue.PublishMessage{
    {Value: "Normal alert", Priority: "normal"},
    {Value: "Critical alert!", Priority: "critical"}, // Delivered first
})
```

### Delayed Messages

```go
_, err := client.Messages.Publish(ctx, "reminders", []*goqueue.PublishMessage{
    {Value: "Follow up in 1 hour", Delay: "1h"},
    {Value: "Daily report", Delay: "24h"},
})
```

## Reliability Patterns

### ACK/NACK/Reject

```go
func processMessage(ctx context.Context, client *goqueue.Client, msg *goqueue.Message) error {
    err := doWork(msg.Value)
    
    switch {
    case err == nil:
        // Success - acknowledge
        return client.Messages.Ack(ctx, msg.ReceiptHandle, nil)
        
    case isTemporaryError(err):
        // Temporary failure - NACK for redelivery
        delay := "30s"
        return client.Messages.Nack(ctx, msg.ReceiptHandle, &delay)
        
    default:
        // Permanent failure - send to DLQ
        reason := err.Error()
        return client.Messages.Reject(ctx, msg.ReceiptHandle, &reason)
    }
}
```

### Extending Visibility Timeout

```go
func longProcess(ctx context.Context, client *goqueue.Client, msg *goqueue.Message) error {
    // Periodically extend visibility in background
    done := make(chan struct{})
    defer close(done)
    
    go func() {
        ticker := time.NewTicker(20 * time.Second)
        defer ticker.Stop()
        
        for {
            select {
            case <-done:
                return
            case <-ticker.C:
                client.Messages.ExtendVisibility(ctx, msg.ReceiptHandle, "60s")
            }
        }
    }()

    // Do long work
    return doLongWork(msg.Value)
}
```

## Transactions (Exactly-Once)

```go
func transferFunds(ctx context.Context, client *goqueue.Client, from, to string, amount float64) error {
    // Initialize producer
    producer, err := client.Transactions.InitProducer(ctx, &goqueue.InitProducerRequest{
        TransactionalID: fmt.Sprintf("transfer-%s-%s", from, to),
    })
    if err != nil {
        return err
    }

    // Begin transaction
    _, err = client.Transactions.Begin(ctx, &goqueue.BeginTransactionRequest{
        ProducerID:      producer.ProducerID,
        Epoch:           producer.Epoch,
        TransactionalID: fmt.Sprintf("transfer-%s-%s", from, to),
    })
    if err != nil {
        return err
    }

    // Publish debit
    _, err = client.Transactions.Publish(ctx, &goqueue.TransactionalPublishRequest{
        ProducerID: producer.ProducerID,
        Epoch:      producer.Epoch,
        Topic:      "account-debits",
        Value:      fmt.Sprintf(`{"account": "%s", "amount": %f}`, from, -amount),
        Sequence:   1,
    })
    if err != nil {
        client.Transactions.Abort(ctx, &goqueue.AbortTransactionRequest{
            ProducerID:      producer.ProducerID,
            Epoch:           producer.Epoch,
            TransactionalID: fmt.Sprintf("transfer-%s-%s", from, to),
        })
        return err
    }

    // Publish credit
    _, err = client.Transactions.Publish(ctx, &goqueue.TransactionalPublishRequest{
        ProducerID: producer.ProducerID,
        Epoch:      producer.Epoch,
        Topic:      "account-credits",
        Value:      fmt.Sprintf(`{"account": "%s", "amount": %f}`, to, amount),
        Sequence:   2,
    })
    if err != nil {
        client.Transactions.Abort(ctx, &goqueue.AbortTransactionRequest{
            ProducerID:      producer.ProducerID,
            Epoch:           producer.Epoch,
            TransactionalID: fmt.Sprintf("transfer-%s-%s", from, to),
        })
        return err
    }

    // Commit
    return client.Transactions.Commit(ctx, &goqueue.CommitTransactionRequest{
        ProducerID:      producer.ProducerID,
        Epoch:           producer.Epoch,
        TransactionalID: fmt.Sprintf("transfer-%s-%s", from, to),
    })
}
```

## Schema Registry

```go
import "encoding/json"

// Register a schema
schema := map[string]any{
    "type": "object",
    "properties": map[string]any{
        "orderId": map[string]any{"type": "string"},
        "amount":  map[string]any{"type": "number"},
    },
    "required": []string{"orderId", "amount"},
}
schemaJSON, _ := json.Marshal(schema)

resp, err := client.Schemas.Register(ctx, "orders-value", &goqueue.RegisterSchemaRequest{
    Schema: string(schemaJSON),
})
fmt.Printf("Schema ID: %d\n", resp.ID)

// Get latest schema
version, err := client.Schemas.GetVersion(ctx, "orders-value", "latest")
fmt.Printf("Version: %d\n", version.Version)

// Set compatibility
_, err = client.Schemas.SetConfig(ctx, &goqueue.CompatibilityConfig{
    CompatibilityLevel: "BACKWARD",
})
```

## Error Handling

```go
import "errors"

_, err := client.Topics.Get(ctx, "non-existent")
if err != nil {
    var qErr *goqueue.Error
    if errors.As(err, &qErr) {
        fmt.Printf("Status: %d\n", qErr.Status)     // 404
        fmt.Printf("Message: %s\n", qErr.Message)   // "topic not found"
    }
}
```

## Configuration

```go
client := goqueue.NewClient("http://localhost:8080",
    goqueue.WithTimeout(60*time.Second),
    goqueue.WithHeaders(map[string]string{
        "X-Tenant-ID":   "my-tenant",
        "Authorization": "Bearer token",
    }),
    goqueue.WithMaxRetries(5),
    goqueue.WithRetryDelays(100*time.Millisecond, 10*time.Second),
    goqueue.WithHTTPClient(customHTTPClient),
)
```

## API Reference

### Services

| Service | Description |
|---------|-------------|
| `client.Health` | Health checks and probes |
| `client.Topics` | Topic management |
| `client.Messages` | Message publish/consume/ack |
| `client.Delayed` | Delayed message operations |
| `client.Groups` | Consumer group operations |
| `client.Priority` | Priority queue statistics |
| `client.Schemas` | Schema registry operations |
| `client.Transactions` | Transaction operations |
| `client.Tracing` | Message tracing |
| `client.Admin` | Administrative operations |

## Requirements

- Go 1.21+
- No external dependencies

## License

MIT License - see [LICENSE](LICENSE) for details.
