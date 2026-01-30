# GoQueue JavaScript/TypeScript Client

Official JavaScript/TypeScript client for [GoQueue](https://github.com/abd-ulbasit/goqueue) - a high-performance distributed message queue.

## Features

- 🎯 **Full TypeScript Support** - Complete type definitions with JSDoc documentation
- 🔄 **Automatic Retries** - Configurable retry with exponential backoff
- ⏱️ **Timeout Handling** - Per-request timeout configuration
- 📦 **Zero Dependencies** - Uses native `fetch` API
- 🌐 **Universal** - Works in Node.js, browsers, and edge runtimes

## Installation

```bash
npm install goqueue-client
# or
yarn add goqueue-client
# or
pnpm add goqueue-client
```

## Quick Start

```typescript
import { GoQueueClient } from 'goqueue-client';

// Create a client
const client = new GoQueueClient({
  baseUrl: 'http://localhost:8080',
});

// Create a topic
await client.topics.create({
  name: 'orders',
  num_partitions: 6,
  retention_hours: 168, // 7 days
});

// Publish messages
await client.messages.publish('orders', [
  { key: 'user-123', value: JSON.stringify({ orderId: 'abc', amount: 99.99 }) },
]);

// Consume with consumer groups
const { member_id, generation } = await client.groups.join('order-processors', {
  client_id: 'worker-1',
  topics: ['orders'],
});

// Poll for messages
const { messages } = await client.groups.poll('order-processors', member_id, {
  max_messages: 10,
  timeout: '30s',
});

for (const msg of messages) {
  const order = JSON.parse(msg.value);
  console.log(`Processing order: ${order.orderId}`);

  // Acknowledge successful processing
  await client.messages.ack(msg.receipt_handle);
}
```

## Configuration

```typescript
const client = new GoQueueClient({
  // Required: GoQueue server URL
  baseUrl: 'http://localhost:8080',

  // Optional: Request timeout (default: 30000ms)
  timeout: 30000,

  // Optional: Custom headers for all requests
  headers: {
    'X-Tenant-ID': 'my-tenant',
    'Authorization': 'Bearer token',
  },

  // Optional: Retry configuration
  retry: {
    maxRetries: 3,      // Maximum retry attempts
    initialDelay: 100,  // Initial delay in ms
    maxDelay: 5000,     // Maximum delay in ms
  },

  // Optional: Custom fetch implementation
  fetch: customFetch,
});
```

## API Reference

### Health Checks

```typescript
// Basic health check
const health = await client.health.check();
// { status: 'ok', timestamp: '2025-01-30T10:30:00Z' }

// Kubernetes probes
await client.health.liveness();   // for livenessProbe
await client.health.readiness();  // for readinessProbe
await client.health.startup();    // for startupProbe

// Server info
const version = await client.health.version();
const stats = await client.health.stats();
```

### Topics

```typescript
// List all topics
const { topics } = await client.topics.list();

// Create a topic
await client.topics.create({
  name: 'orders',
  num_partitions: 6,
  retention_hours: 168,
});

// Get topic details
const details = await client.topics.get('orders');
console.log(`Total messages: ${details.total_messages}`);

// Delete a topic
await client.topics.delete('old-topic');
```

### Publishing Messages

```typescript
// Simple message
await client.messages.publish('orders', [
  { value: '{"orderId": "123"}' },
]);

// Message with key (for ordering)
await client.messages.publish('orders', [
  { key: 'user-123', value: '{"event": "purchase"}' },
]);

// Priority message
await client.messages.publish('alerts', [
  { value: 'Critical alert!', priority: 'critical' },
]);

// Delayed message (delivered in 1 hour)
await client.messages.publish('reminders', [
  { value: 'Follow up', delay: '1h' },
]);

// Scheduled message (specific time)
await client.messages.publish('scheduled', [
  { value: 'Morning report', deliverAt: '2025-01-31T09:00:00Z' },
]);

// Batch publish
await client.messages.publish('events', [
  { key: 'user-1', value: '{"event": "login"}' },
  { key: 'user-2', value: '{"event": "purchase"}', priority: 'high' },
  { key: 'user-3', value: '{"event": "logout"}' },
]);
```

### Consumer Groups

```typescript
// Join a consumer group
const { member_id, generation, assigned_partitions } = await client.groups.join(
  'order-processors',
  {
    client_id: 'worker-1',
    topics: ['orders'],
    session_timeout: '30s',
  }
);

// Poll for messages
const { messages } = await client.groups.poll('order-processors', member_id, {
  max_messages: 100,
  timeout: '30s',
});

// Process and acknowledge
for (const msg of messages) {
  try {
    await processMessage(msg.value);
    await client.messages.ack(msg.receipt_handle);
  } catch (error) {
    // Temporary failure - will be redelivered
    await client.messages.nack(msg.receipt_handle);
  }
}

// Send heartbeat
const { rebalance_required } = await client.groups.heartbeat('order-processors', {
  member_id,
  generation,
});

if (rebalance_required) {
  // Rejoin to get new partition assignments
}

// Leave the group
await client.groups.leave('order-processors', { member_id });
```

### Message Acknowledgment

```typescript
// ACK - Message processed successfully
await client.messages.ack(receiptHandle);

// NACK - Temporary failure, redeliver later
await client.messages.nack(receiptHandle, '30s');

// Reject - Permanent failure, send to DLQ
await client.messages.reject(receiptHandle, 'Invalid JSON format');

// Extend visibility timeout (processing taking longer)
await client.messages.extendVisibility(receiptHandle, '60s');
```

### Delayed Messages

```typescript
// List pending delayed messages
const delayed = await client.delayed.list('orders', { limit: 50 });
for (const msg of delayed.messages) {
  console.log(`Will deliver at: ${msg.deliver_at}`);
}

// Cancel a delayed message
await client.delayed.cancel('orders', 0, 12345);

// Get delay statistics
const stats = await client.delayed.stats();
console.log(`Pending: ${stats.pending_count}`);
```

### Schema Registry

```typescript
// Register a schema
const { id } = await client.schemas.register('orders-value', {
  schema: JSON.stringify({
    type: 'object',
    properties: {
      orderId: { type: 'string' },
      amount: { type: 'number' },
    },
    required: ['orderId', 'amount'],
  }),
});

// Get latest schema
const schema = await client.schemas.getVersion('orders-value', 'latest');

// Set compatibility mode
await client.schemas.setConfig({ compatibilityLevel: 'BACKWARD' });
```

### Transactions (Exactly-Once)

```typescript
// Initialize producer
const { producer_id, epoch } = await client.transactions.initProducer({
  transactional_id: 'order-processor-1',
});

// Begin transaction
await client.transactions.begin({
  producer_id,
  epoch,
  transactional_id: 'order-processor-1',
});

// Publish within transaction
await client.transactions.publish({
  producer_id,
  epoch,
  topic: 'orders',
  value: JSON.stringify({ orderId: '123' }),
  sequence: 1,
});

// Commit transaction
await client.transactions.commit({
  producer_id,
  epoch,
  transactional_id: 'order-processor-1',
});
```

### Tracing

```typescript
// List recent traces
const { traces } = await client.tracing.list({ limit: 100 });

// Search traces
const results = await client.tracing.search({
  topic: 'orders',
  status: 'error',
  start: '2025-01-30T00:00:00Z',
});

// Get trace details
const trace = await client.tracing.get('trace-123');
for (const event of trace.events ?? []) {
  console.log(`${event.timestamp}: ${event.event_type}`);
}
```

### Administration

```typescript
// Add partitions to a topic
await client.admin.addPartitions('orders', { count: 12 });

// Create a tenant
await client.admin.createTenant({
  name: 'acme-corp',
  quotas: {
    max_topics: 100,
    max_messages_per_second: 10000,
  },
});
```

## Error Handling

```typescript
import { GoQueueError } from 'goqueue-client';

try {
  await client.topics.get('non-existent');
} catch (error) {
  if (error instanceof GoQueueError) {
    console.log('Status:', error.status);   // 404
    console.log('Message:', error.message); // "Topic not found"
    console.log('Body:', error.body);       // Original response body
  }
}
```

## TypeScript Support

All types are exported and can be imported:

```typescript
import type {
  PublishMessage,
  MessagePriority,
  ConsumeMessage,
  GroupDetails,
  // ... all other types
} from 'goqueue-client';

const message: PublishMessage = {
  key: 'user-123',
  value: JSON.stringify({ event: 'purchase' }),
  priority: 'high',
};
```

## Requirements

- Node.js 18+ (for native `fetch`)
- Or any runtime with `fetch` support (browsers, Deno, Bun, Cloudflare Workers)

## License

MIT © GoQueue Team

## Links

- [GoQueue GitHub](https://github.com/abd-ulbasit/goqueue)
- [Documentation](https://abd-ulbasit.github.io/goqueue)
- [Report Issues](https://github.com/abd-ulbasit/goqueue/issues)
