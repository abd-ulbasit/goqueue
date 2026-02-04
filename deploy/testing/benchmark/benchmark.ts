// =============================================================================
// GOQUEUE TYPESCRIPT BENCHMARK SUITE
// =============================================================================
//
// This script tests GoQueue throughput and latency using the TypeScript client.
//
// USAGE:
//   # Install dependencies
//   cd deploy/testing/benchmark
//   npm install
//
//   # Run benchmark against local GoQueue
//   GOQUEUE_URL=http://localhost:8080 npx tsx benchmark.ts
//
//   # Run against EKS cluster
//   GOQUEUE_URL=http://<load-balancer-url>:8080 npx tsx benchmark.ts
//
// =============================================================================

interface BenchmarkResult {
  name: string;
  totalMessages: number;
  durationMs: number;
  throughput: number;
  avgLatencyMs: number;
  p50LatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
}

// =============================================================================
// CONFIGURATION
// =============================================================================

const GOQUEUE_URL = process.env.GOQUEUE_URL || 'http://localhost:8080';

// =============================================================================
// HTTP CLIENT
// =============================================================================

async function createTopic(name: string, partitions: number): Promise<void> {
  // GoQueue API uses /topics (not /api/v1/topics)
  const response = await fetch(`${GOQUEUE_URL}/topics`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name,
      num_partitions: partitions,
    }),
  });

  if (!response.ok && response.status !== 409) {
    throw new Error(`Failed to create topic: ${response.status} ${await response.text()}`);
  }
}

async function deleteTopic(name: string): Promise<void> {
  await fetch(`${GOQUEUE_URL}/topics/${name}`, {
    method: 'DELETE',
  });
}

async function publish(
  topic: string,
  messages: Array<{ value: string; priority?: number }>
): Promise<void> {
  const response = await fetch(`${GOQUEUE_URL}/topics/${topic}/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ messages }),
  });

  if (!response.ok) {
    throw new Error(`Publish failed: ${response.status}`);
  }
}

async function joinGroup(
  groupId: string,
  clientId: string,
  topics: string[]
): Promise<{ member_id: string }> {
  const response = await fetch(`${GOQUEUE_URL}/groups/${groupId}/join`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: clientId,
      topics,
    }),
  });

  if (!response.ok) {
    throw new Error(`Join group failed: ${response.status}`);
  }

  return response.json();
}

async function poll(groupId: string, memberId: string): Promise<any> {
  // Poll uses GET /groups/{group}/poll?member_id=xxx
  const response = await fetch(
    `${GOQUEUE_URL}/groups/${groupId}/poll?member_id=${memberId}&timeout=5s`,
    { method: 'GET' }
  );

  if (!response.ok) {
    throw new Error(`Poll failed: ${response.status}`);
  }

  return response.json();
}

// =============================================================================
// HELPERS
// =============================================================================

function generateMessage(size: number): { value: string } {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let value = '';
  for (let i = 0; i < size; i++) {
    value += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return { value };
}

function generateMessages(count: number, size: number): Array<{ value: string }> {
  return Array.from({ length: count }, () => generateMessage(size));
}

function percentile(arr: number[], p: number): number {
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function formatNumber(n: number): string {
  return n.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

// =============================================================================
// BENCHMARKS
// =============================================================================

async function benchmarkPublishThroughput(): Promise<BenchmarkResult> {
  console.log('\n📊 Running: Publish Throughput Benchmark');
  console.log('   Testing batch publish with 100 messages x 1KB each...');

  const topicName = `bench-ts-${Date.now()}`;
  await createTopic(topicName, 6);

  const batchSize = 100;
  const messageSize = 1024;
  const iterations = 100;
  const messages = generateMessages(batchSize, messageSize);
  const latencies: number[] = [];

  const startTime = Date.now();

  for (let i = 0; i < iterations; i++) {
    const batchStart = Date.now();
    await publish(topicName, messages);
    latencies.push(Date.now() - batchStart);
  }

  const durationMs = Date.now() - startTime;
  const totalMessages = iterations * batchSize;

  await deleteTopic(topicName);

  return {
    name: 'Publish Throughput (batch=100, size=1KB)',
    totalMessages,
    durationMs,
    throughput: (totalMessages / durationMs) * 1000,
    avgLatencyMs: latencies.reduce((a, b) => a + b, 0) / latencies.length,
    p50LatencyMs: percentile(latencies, 50),
    p95LatencyMs: percentile(latencies, 95),
    p99LatencyMs: percentile(latencies, 99),
  };
}

async function benchmarkConcurrentPublish(): Promise<BenchmarkResult> {
  console.log('\n📊 Running: Concurrent Publish Benchmark');
  console.log('   Testing 8 concurrent producers...');

  const topicName = `bench-concurrent-${Date.now()}`;
  await createTopic(topicName, 6);

  const numProducers = 8;
  const batchSize = 100;
  const iterationsPerProducer = 50;
  const messageSize = 1024;
  const messages = generateMessages(batchSize, messageSize);
  const latencies: number[] = [];

  const startTime = Date.now();

  const producers = Array.from({ length: numProducers }, async () => {
    for (let i = 0; i < iterationsPerProducer; i++) {
      const batchStart = Date.now();
      await publish(topicName, messages);
      latencies.push(Date.now() - batchStart);
    }
  });

  await Promise.all(producers);

  const durationMs = Date.now() - startTime;
  const totalMessages = numProducers * iterationsPerProducer * batchSize;

  await deleteTopic(topicName);

  return {
    name: `Concurrent Publish (producers=${numProducers})`,
    totalMessages,
    durationMs,
    throughput: (totalMessages / durationMs) * 1000,
    avgLatencyMs: latencies.reduce((a, b) => a + b, 0) / latencies.length,
    p50LatencyMs: percentile(latencies, 50),
    p95LatencyMs: percentile(latencies, 95),
    p99LatencyMs: percentile(latencies, 99),
  };
}

async function benchmarkEndToEndLatency(): Promise<BenchmarkResult> {
  console.log('\n📊 Running: End-to-End Latency Benchmark');
  console.log('   Measuring publish → consume latency...');

  const topicName = `bench-e2e-${Date.now()}`;
  const groupName = `bench-group-${Date.now()}`;
  await createTopic(topicName, 1);

  const { member_id } = await joinGroup(groupName, 'latency-tester', [topicName]);

  const iterations = 200;
  const latencies: number[] = [];

  for (let i = 0; i < iterations; i++) {
    const message = { value: JSON.stringify({ ts: Date.now(), seq: i }) };
    
    const startTime = Date.now();
    await publish(topicName, [message]);
    await poll(groupName, member_id);
    latencies.push(Date.now() - startTime);
  }

  await deleteTopic(topicName);

  const totalDuration = latencies.reduce((a, b) => a + b, 0);

  return {
    name: 'End-to-End Latency',
    totalMessages: iterations,
    durationMs: totalDuration,
    throughput: (iterations / totalDuration) * 1000,
    avgLatencyMs: totalDuration / iterations,
    p50LatencyMs: percentile(latencies, 50),
    p95LatencyMs: percentile(latencies, 95),
    p99LatencyMs: percentile(latencies, 99),
  };
}

async function benchmarkSustainedThroughput(): Promise<BenchmarkResult> {
  console.log('\n📊 Running: Sustained Throughput Benchmark (30s)');
  console.log('   Running sustained load with 4 producers...');

  const topicName = `bench-sustained-${Date.now()}`;
  await createTopic(topicName, 6);

  const durationMs = 30000; // 30 seconds
  const numProducers = 4;
  const batchSize = 100;
  const messageSize = 1024;
  const messages = generateMessages(batchSize, messageSize);

  let totalMessages = 0;
  let running = true;
  const latencies: number[] = [];

  const startTime = Date.now();

  const producers = Array.from({ length: numProducers }, async () => {
    while (running) {
      const batchStart = Date.now();
      try {
        await publish(topicName, messages);
        totalMessages += batchSize;
        latencies.push(Date.now() - batchStart);
      } catch (e) {
        // Ignore errors during benchmark
      }
    }
  });

  await new Promise((resolve) => setTimeout(resolve, durationMs));
  running = false;

  await Promise.allSettled(producers);
  await deleteTopic(topicName);

  const actualDuration = Date.now() - startTime;

  return {
    name: `Sustained Throughput (${durationMs / 1000}s, ${numProducers} producers)`,
    totalMessages,
    durationMs: actualDuration,
    throughput: (totalMessages / actualDuration) * 1000,
    avgLatencyMs: latencies.length > 0 ? latencies.reduce((a, b) => a + b, 0) / latencies.length : 0,
    p50LatencyMs: latencies.length > 0 ? percentile(latencies, 50) : 0,
    p95LatencyMs: latencies.length > 0 ? percentile(latencies, 95) : 0,
    p99LatencyMs: latencies.length > 0 ? percentile(latencies, 99) : 0,
  };
}

// =============================================================================
// MAIN
// =============================================================================

function printResults(results: BenchmarkResult[]): void {
  console.log('\n' + '═'.repeat(100));
  console.log('                              GOQUEUE BENCHMARK RESULTS');
  console.log('═'.repeat(100));
  console.log(`Target: ${GOQUEUE_URL}`);
  console.log(`Date: ${new Date().toISOString()}`);
  console.log('─'.repeat(100));

  console.log('\n┌────────────────────────────────────────────────────────┬────────────┬────────────┬─────────────┐');
  console.log('│ Benchmark                                              │ Throughput │ Avg Latency│ P99 Latency │');
  console.log('│                                                        │ (msgs/sec) │   (ms)     │    (ms)     │');
  console.log('├────────────────────────────────────────────────────────┼────────────┼────────────┼─────────────┤');

  for (const result of results) {
    const name = result.name.padEnd(54);
    const throughput = formatNumber(result.throughput).padStart(10);
    const avgLatency = formatNumber(result.avgLatencyMs).padStart(10);
    const p99Latency = formatNumber(result.p99LatencyMs).padStart(11);
    console.log(`│ ${name} │ ${throughput} │ ${avgLatency} │ ${p99Latency} │`);
  }

  console.log('└────────────────────────────────────────────────────────┴────────────┴────────────┴─────────────┘');

  // Comparison table
  console.log('\n' + '─'.repeat(100));
  console.log('                              INDUSTRY COMPARISON (Reference)');
  console.log('─'.repeat(100));
  console.log(`
┌──────────────┬─────────────────────┬───────────────┬───────────────────────────────────────────────────┐
│ System       │ Throughput (msg/s)  │ P99 Latency   │ Notes                                             │
├──────────────┼─────────────────────┼───────────────┼───────────────────────────────────────────────────┤
│ Apache Kafka │ 1,000,000+          │ 5-50ms        │ With batching, linger.ms, compression             │
│ RabbitMQ     │ 10,000-100,000      │ 1-10ms        │ Persistent messages, prefetch tuned               │
│ AWS SQS      │ 300,000 (3K FIFO)   │ 50-500ms      │ Managed service, HTTP API overhead                │
│ Redis Streams│ 500,000+            │ <1ms          │ In-memory, optional persistence                   │
│ NATS         │ 10,000,000+         │ <1ms          │ At-most-once default, JetStream for durability    │
├──────────────┼─────────────────────┼───────────────┼───────────────────────────────────────────────────┤
│ GoQueue      │ ${formatNumber(results[results.length - 1]?.throughput || 0).padStart(17)} │ ${formatNumber(results[results.length - 1]?.p99LatencyMs || 0).padStart(11)}ms │ This benchmark run                                │
└──────────────┴─────────────────────┴───────────────┴───────────────────────────────────────────────────┘
`);
}

async function main(): Promise<void> {
  console.log('╔════════════════════════════════════════════════════════════════════════════════════════════════╗');
  console.log('║                           GOQUEUE BENCHMARK SUITE (TypeScript)                                  ║');
  console.log('╠════════════════════════════════════════════════════════════════════════════════════════════════╣');
  console.log(`║ Target URL: ${GOQUEUE_URL.padEnd(83)}║`);
  console.log('╚════════════════════════════════════════════════════════════════════════════════════════════════╝');

  // Verify connection
  try {
    const healthResponse = await fetch(`${GOQUEUE_URL}/health`);
    if (!healthResponse.ok) {
      throw new Error(`Health check failed: ${healthResponse.status}`);
    }
    console.log('\n✅ Connected to GoQueue server');
  } catch (error) {
    console.error(`\n❌ Failed to connect to GoQueue at ${GOQUEUE_URL}`);
    console.error('   Make sure the server is running and accessible.');
    process.exit(1);
  }

  const results: BenchmarkResult[] = [];

  try {
    results.push(await benchmarkPublishThroughput());
    results.push(await benchmarkConcurrentPublish());
    // NOTE: End-to-end test requires consumer group heartbeats
    // which are not implemented in this simple benchmark. Skipping for now.
    // results.push(await benchmarkEndToEndLatency());
    results.push(await benchmarkSustainedThroughput());
  } catch (error) {
    console.error('\n❌ Benchmark failed:', error);
    process.exit(1);
  }

  printResults(results);
}

main().catch(console.error);
