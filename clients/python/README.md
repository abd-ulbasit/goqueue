# GoQueue Python Client

Python SDK for [GoQueue](https://github.com/abd-ulbasit/goqueue) - A high-performance distributed message queue that combines the best features of Kafka, SQS, and RabbitMQ.

## Features

- **Async-first design** - Built on `httpx` for efficient async I/O
- **Full type hints** - Complete type coverage for IDE support
- **Rich docstrings** - Documentation available in your IDE
- **Auto-retry** - Automatic retry with exponential backoff
- **Context manager** - Clean resource management

## Installation

```bash
pip install goqueue-client
```

## Quick Start

```python
import asyncio
from goqueue_client import GoQueueClient

async def main():
    # Create a client (async context manager)
    async with GoQueueClient("http://localhost:8080") as client:
        # Create a topic
        await client.topics.create({
            "name": "orders",
            "num_partitions": 3
        })
        
        # Publish messages
        await client.messages.publish("orders", [
            {"value": '{"orderId": "12345", "amount": 99.99}'}
        ])
        
        # Simple consume
        response = await client.messages.consume("orders", partition=0)
        for msg in response["messages"]:
            print(f"Received: {msg['value']}")

asyncio.run(main())
```

## Consumer Groups

For production workloads, use consumer groups for parallel processing:

```python
import asyncio
from goqueue_client import GoQueueClient

async def worker(client: GoQueueClient, group_id: str, worker_id: str):
    """Worker that processes messages from a consumer group."""
    
    # Join the consumer group
    join_response = await client.groups.join(group_id, {
        "client_id": worker_id,
        "topics": ["orders"],
        "session_timeout": "30s"
    })
    member_id = join_response["member_id"]
    generation = join_response["generation"]
    
    print(f"Worker {worker_id} joined with partitions: {join_response['assigned_partitions']}")
    
    try:
        while True:
            # Poll for messages
            response = await client.groups.poll(
                group_id,
                member_id,
                max_messages=10,
                timeout="10s"
            )
            
            # Process messages
            for msg in response["messages"]:
                print(f"Worker {worker_id} processing: {msg['value']}")
                
                # Acknowledge successful processing
                await client.messages.ack(msg["receipt_handle"])
            
            # Send heartbeat
            heartbeat = await client.groups.heartbeat(group_id, {
                "member_id": member_id,
                "generation": generation
            })
            
            # Handle rebalances
            if heartbeat.get("rebalance_required"):
                print(f"Worker {worker_id} needs to rejoin")
                break
    finally:
        # Leave the group
        await client.groups.leave(group_id, {"member_id": member_id})

async def main():
    async with GoQueueClient("http://localhost:8080") as client:
        # Create topic
        await client.topics.create({
            "name": "orders",
            "num_partitions": 6
        })
        
        # Start multiple workers
        await asyncio.gather(
            worker(client, "order-processors", "worker-1"),
            worker(client, "order-processors", "worker-2"),
            worker(client, "order-processors", "worker-3"),
        )

asyncio.run(main())
```

## Message Options

### With Keys (Ordering)

Messages with the same key go to the same partition:

```python
await client.messages.publish("orders", [
    {"key": "user-123", "value": '{"event": "order_created"}'},
    {"key": "user-123", "value": '{"event": "payment_received"}'},  # Same partition
])
```

### Priority Messages

Higher priority messages are delivered first:

```python
await client.messages.publish("alerts", [
    {"value": "Normal alert", "priority": "normal"},
    {"value": "Critical alert!", "priority": "critical"},  # Delivered first
])
```

### Delayed Messages

Schedule messages for future delivery:

```python
await client.messages.publish("reminders", [
    {"value": "Follow up in 1 hour", "delay": "1h"},
    {"value": "Daily report", "delay": "24h"},
])
```

## Reliability Patterns

### ACK/NACK/Reject

```python
async def process_message(client: GoQueueClient, msg: dict):
    """Process a message with proper acknowledgment."""
    try:
        # Process the message
        result = await do_work(msg["value"])
        
        # Success - acknowledge
        await client.messages.ack(msg["receipt_handle"])
        
    except TemporaryError:
        # Temporary failure - NACK for redelivery
        await client.messages.nack(
            msg["receipt_handle"],
            delay="30s"  # Retry after 30 seconds
        )
        
    except PermanentError as e:
        # Permanent failure - send to DLQ
        await client.messages.reject(
            msg["receipt_handle"],
            reason=str(e)
        )
```

### Extending Visibility Timeout

For long-running operations:

```python
async def long_process(client: GoQueueClient, msg: dict):
    """Process that takes longer than visibility timeout."""
    
    # Start an async task to extend visibility periodically
    async def extend_visibility():
        while True:
            await asyncio.sleep(20)  # Extend every 20 seconds
            await client.messages.extend_visibility(
                msg["receipt_handle"],
                timeout="60s"
            )
    
    extend_task = asyncio.create_task(extend_visibility())
    
    try:
        await do_long_work(msg["value"])
        await client.messages.ack(msg["receipt_handle"])
    finally:
        extend_task.cancel()
```

## Transactions (Exactly-Once)

```python
async def transfer_funds(client: GoQueueClient, from_account: str, to_account: str, amount: float):
    """Transfer funds with exactly-once semantics."""
    
    # Initialize producer
    producer = await client.transactions.init_producer({
        "transactional_id": f"transfer-{from_account}-{to_account}"
    })
    producer_id = producer["producer_id"]
    epoch = producer["epoch"]
    
    try:
        # Begin transaction
        await client.transactions.begin({
            "producer_id": producer_id,
            "epoch": epoch,
            "transactional_id": f"transfer-{from_account}-{to_account}"
        })
        
        # Publish debit message
        await client.transactions.publish({
            "producer_id": producer_id,
            "epoch": epoch,
            "topic": "account-debits",
            "value": json.dumps({
                "account": from_account,
                "amount": -amount
            }),
            "sequence": 1
        })
        
        # Publish credit message
        await client.transactions.publish({
            "producer_id": producer_id,
            "epoch": epoch,
            "topic": "account-credits",
            "value": json.dumps({
                "account": to_account,
                "amount": amount
            }),
            "sequence": 2
        })
        
        # Commit transaction - both or neither
        await client.transactions.commit({
            "producer_id": producer_id,
            "epoch": epoch,
            "transactional_id": f"transfer-{from_account}-{to_account}"
        })
        
    except Exception:
        # Abort transaction on any error
        await client.transactions.abort({
            "producer_id": producer_id,
            "epoch": epoch,
            "transactional_id": f"transfer-{from_account}-{to_account}"
        })
        raise
```

## Schema Registry

```python
import json

# Register a schema
await client.schemas.register("orders-value", {
    "schema": json.dumps({
        "type": "object",
        "properties": {
            "orderId": {"type": "string"},
            "amount": {"type": "number"},
            "items": {
                "type": "array",
                "items": {"type": "object"}
            }
        },
        "required": ["orderId", "amount"]
    })
})

# Get latest schema
schema = await client.schemas.get_version("orders-value", "latest")
print(f"Schema ID: {schema['id']}")

# Configure compatibility (BACKWARD, FORWARD, FULL, NONE)
await client.schemas.set_config({"compatibilityLevel": "BACKWARD"})
```

## Health Checks

```python
# Simple health check
health = await client.health.check()
print(f"Status: {health['status']}")

# Kubernetes probes
liveness = await client.health.liveness()   # /healthz
readiness = await client.health.readiness()  # /readyz

# Detailed readiness with checks
readiness = await client.health.readiness(verbose=True)
for check_name, result in readiness.get("checks", {}).items():
    print(f"{check_name}: {result['status']}")

# Get statistics
stats = await client.health.stats()
print(f"Topics: {stats['topics']}")
print(f"Uptime: {stats['uptime']}")
```

## Error Handling

```python
from goqueue_client import GoQueueClient, GoQueueError

async with GoQueueClient("http://localhost:8080") as client:
    try:
        await client.topics.get("non-existent-topic")
    except GoQueueError as e:
        print(f"Error: {e.message}")
        print(f"Status: {e.status}")  # 404
        print(f"Body: {e.body}")
```

## Configuration

```python
client = GoQueueClient(
    "http://localhost:8080",
    timeout=30.0,           # Request timeout in seconds
    headers={               # Additional headers
        "X-Tenant-ID": "my-tenant",
        "Authorization": "Bearer token"
    },
    max_retries=3,          # Retry attempts for failed requests
    initial_delay=0.1,      # Initial retry delay (seconds)
    max_delay=5.0,          # Maximum retry delay (seconds)
)
```

## API Reference

### Services

| Service | Description |
|---------|-------------|
| `client.health` | Health checks and probes |
| `client.topics` | Topic management |
| `client.messages` | Message publish/consume/ack |
| `client.delayed` | Delayed message operations |
| `client.groups` | Consumer group operations |
| `client.priority` | Priority queue statistics |
| `client.schemas` | Schema registry operations |
| `client.transactions` | Transaction operations |
| `client.tracing` | Message tracing |
| `client.admin` | Administrative operations |

## Requirements

- Python 3.9+
- httpx

## License

MIT License - see [LICENSE](LICENSE) for details.
