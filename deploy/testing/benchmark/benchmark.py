#!/usr/bin/env python3
"""
=============================================================================
GOQUEUE PYTHON BENCHMARK SUITE
=============================================================================

This script tests GoQueue throughput and latency using Python.

USAGE:
    # Run benchmark against local GoQueue
    GOQUEUE_URL=http://localhost:8080 python3 benchmark.py

    # Run against EKS cluster
    GOQUEUE_URL=http://<load-balancer-url>:8080 python3 benchmark.py

REQUIREMENTS:
    pip install requests

=============================================================================
"""

import json
import os
import random
import statistics
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Any
import requests

# =============================================================================
# CONFIGURATION
# =============================================================================

GOQUEUE_URL = os.environ.get("GOQUEUE_URL", "http://localhost:8080")

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class BenchmarkResult:
    name: str
    total_messages: int
    duration_ms: float
    throughput: float  # msgs/sec
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float


# =============================================================================
# HTTP CLIENT
# =============================================================================

class GoQueueClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        # Connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def create_topic(self, name: str, partitions: int) -> None:
        # GoQueue API uses /topics (not /api/v1/topics)
        response = self.session.post(
            f"{self.base_url}/topics",
            json={"name": name, "num_partitions": partitions},
        )
        if response.status_code not in (201, 409):
            raise Exception(f"Failed to create topic: {response.status_code}")

    def delete_topic(self, name: str) -> None:
        self.session.delete(f"{self.base_url}/topics/{name}")

    def publish(self, topic: str, messages: List[Dict[str, Any]]) -> None:
        response = self.session.post(
            f"{self.base_url}/topics/{topic}/messages",
            json={"messages": messages},
        )
        if response.status_code not in (200, 201):
            raise Exception(f"Publish failed: {response.status_code}")

    def join_group(self, group_id: str, client_id: str, topics: List[str]) -> str:
        response = self.session.post(
            f"{self.base_url}/groups/{group_id}/join",
            json={"client_id": client_id, "topics": topics},
        )
        if response.status_code not in (200, 201):
            raise Exception(f"Join group failed: {response.status_code}")
        return response.json()["member_id"]

    def poll(self, group_id: str, member_id: str) -> Dict[str, Any]:
        # Poll uses GET /groups/{group}/poll?member_id=xxx
        response = self.session.get(
            f"{self.base_url}/groups/{group_id}/poll",
            params={"member_id": member_id, "timeout": "5s"}
        )
        if response.status_code != 200:
            raise Exception(f"Poll failed: {response.status_code} - {response.text}")
        return response.json()

    def health_check(self) -> bool:
        try:
            response = self.session.get(f"{self.base_url}/health")
            return response.status_code == 200
        except:
            return False


# =============================================================================
# HELPERS
# =============================================================================

def generate_message(size: int) -> Dict[str, str]:
    """Generate a random message of specified size."""
    value = "".join(random.choices(string.ascii_letters + string.digits, k=size))
    return {"value": value}


def generate_messages(count: int, size: int) -> List[Dict[str, str]]:
    """Generate a batch of messages."""
    return [generate_message(size) for _ in range(count)]


def percentile(values: List[float], p: float) -> float:
    """Calculate the p-th percentile of values."""
    if not values:
        return 0.0
    sorted_values = sorted(values)
    idx = int((p / 100) * len(sorted_values))
    return sorted_values[min(idx, len(sorted_values) - 1)]


# =============================================================================
# BENCHMARKS
# =============================================================================

def benchmark_publish_throughput(client: GoQueueClient) -> BenchmarkResult:
    """Benchmark batch publish throughput."""
    print("\n📊 Running: Publish Throughput Benchmark")
    print("   Testing batch publish with 100 messages x 1KB each...")

    topic_name = f"bench-py-{int(time.time() * 1000)}"
    client.create_topic(topic_name, 6)

    batch_size = 100
    message_size = 1024
    iterations = 100
    messages = generate_messages(batch_size, message_size)
    latencies = []

    start_time = time.time()

    for _ in range(iterations):
        batch_start = time.time()
        client.publish(topic_name, messages)
        latencies.append((time.time() - batch_start) * 1000)

    duration_ms = (time.time() - start_time) * 1000
    total_messages = iterations * batch_size

    client.delete_topic(topic_name)

    return BenchmarkResult(
        name="Publish Throughput (batch=100, size=1KB)",
        total_messages=total_messages,
        duration_ms=duration_ms,
        throughput=(total_messages / duration_ms) * 1000,
        avg_latency_ms=statistics.mean(latencies),
        p50_latency_ms=percentile(latencies, 50),
        p95_latency_ms=percentile(latencies, 95),
        p99_latency_ms=percentile(latencies, 99),
    )


def benchmark_concurrent_publish(client: GoQueueClient) -> BenchmarkResult:
    """Benchmark concurrent publish throughput."""
    print("\n📊 Running: Concurrent Publish Benchmark")
    print("   Testing 8 concurrent producers...")

    topic_name = f"bench-concurrent-{int(time.time() * 1000)}"
    client.create_topic(topic_name, 6)

    num_producers = 8
    batch_size = 100
    iterations_per_producer = 50
    message_size = 1024
    messages = generate_messages(batch_size, message_size)
    latencies = []

    def producer_task():
        local_latencies = []
        for _ in range(iterations_per_producer):
            batch_start = time.time()
            client.publish(topic_name, messages)
            local_latencies.append((time.time() - batch_start) * 1000)
        return local_latencies

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_producers) as executor:
        futures = [executor.submit(producer_task) for _ in range(num_producers)]
        for future in as_completed(futures):
            latencies.extend(future.result())

    duration_ms = (time.time() - start_time) * 1000
    total_messages = num_producers * iterations_per_producer * batch_size

    client.delete_topic(topic_name)

    return BenchmarkResult(
        name=f"Concurrent Publish (producers={num_producers})",
        total_messages=total_messages,
        duration_ms=duration_ms,
        throughput=(total_messages / duration_ms) * 1000,
        avg_latency_ms=statistics.mean(latencies),
        p50_latency_ms=percentile(latencies, 50),
        p95_latency_ms=percentile(latencies, 95),
        p99_latency_ms=percentile(latencies, 99),
    )


def benchmark_end_to_end_latency(client: GoQueueClient) -> BenchmarkResult:
    """Benchmark end-to-end latency."""
    print("\n📊 Running: End-to-End Latency Benchmark")
    print("   Measuring publish → consume latency...")

    topic_name = f"bench-e2e-{int(time.time() * 1000)}"
    group_name = f"bench-group-{int(time.time() * 1000)}"
    client.create_topic(topic_name, 1)

    member_id = client.join_group(group_name, "latency-tester", [topic_name])

    iterations = 200
    latencies = []

    for i in range(iterations):
        message = {"value": json.dumps({"ts": time.time(), "seq": i})}
        
        start_time = time.time()
        client.publish(topic_name, [message])
        client.poll(group_name, member_id)
        latencies.append((time.time() - start_time) * 1000)

    client.delete_topic(topic_name)

    total_duration = sum(latencies)

    return BenchmarkResult(
        name="End-to-End Latency",
        total_messages=iterations,
        duration_ms=total_duration,
        throughput=(iterations / total_duration) * 1000,
        avg_latency_ms=statistics.mean(latencies),
        p50_latency_ms=percentile(latencies, 50),
        p95_latency_ms=percentile(latencies, 95),
        p99_latency_ms=percentile(latencies, 99),
    )


def benchmark_sustained_throughput(client: GoQueueClient) -> BenchmarkResult:
    """Benchmark sustained throughput over time."""
    print("\n📊 Running: Sustained Throughput Benchmark (30s)")
    print("   Running sustained load with 4 producers...")

    topic_name = f"bench-sustained-{int(time.time() * 1000)}"
    client.create_topic(topic_name, 6)

    duration_secs = 30
    num_producers = 4
    batch_size = 100
    message_size = 1024
    messages = generate_messages(batch_size, message_size)

    total_messages = 0
    running = True
    latencies = []
    end_time = time.time() + duration_secs

    def producer_task():
        nonlocal total_messages
        local_latencies = []
        while time.time() < end_time:
            try:
                batch_start = time.time()
                client.publish(topic_name, messages)
                local_latencies.append((time.time() - batch_start) * 1000)
                total_messages += batch_size
            except:
                pass
        return local_latencies

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_producers) as executor:
        futures = [executor.submit(producer_task) for _ in range(num_producers)]
        for future in as_completed(futures):
            latencies.extend(future.result())

    actual_duration = (time.time() - start_time) * 1000

    client.delete_topic(topic_name)

    return BenchmarkResult(
        name=f"Sustained Throughput ({duration_secs}s, {num_producers} producers)",
        total_messages=total_messages,
        duration_ms=actual_duration,
        throughput=(total_messages / actual_duration) * 1000 if actual_duration > 0 else 0,
        avg_latency_ms=statistics.mean(latencies) if latencies else 0,
        p50_latency_ms=percentile(latencies, 50),
        p95_latency_ms=percentile(latencies, 95),
        p99_latency_ms=percentile(latencies, 99),
    )


# =============================================================================
# OUTPUT
# =============================================================================

def print_results(results: List[BenchmarkResult]) -> None:
    """Print benchmark results in a formatted table."""
    print("\n" + "═" * 100)
    print("                              GOQUEUE BENCHMARK RESULTS (Python)")
    print("═" * 100)
    print(f"Target: {GOQUEUE_URL}")
    print(f"Date: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
    print("─" * 100)

    print("\n┌" + "─" * 56 + "┬" + "─" * 12 + "┬" + "─" * 12 + "┬" + "─" * 13 + "┐")
    print("│ Benchmark                                              │ Throughput │ Avg Latency│ P99 Latency │")
    print("│                                                        │ (msgs/sec) │   (ms)     │    (ms)     │")
    print("├" + "─" * 56 + "┼" + "─" * 12 + "┼" + "─" * 12 + "┼" + "─" * 13 + "┤")

    for result in results:
        name = result.name[:54].ljust(54)
        throughput = f"{result.throughput:,.0f}".rjust(10)
        avg_latency = f"{result.avg_latency_ms:,.2f}".rjust(10)
        p99_latency = f"{result.p99_latency_ms:,.2f}".rjust(11)
        print(f"│ {name} │ {throughput} │ {avg_latency} │ {p99_latency} │")

    print("└" + "─" * 56 + "┴" + "─" * 12 + "┴" + "─" * 12 + "┴" + "─" * 13 + "┘")

    # Comparison table
    print("\n" + "─" * 100)
    print("                              INDUSTRY COMPARISON (Reference)")
    print("─" * 100)
    
    last_throughput = f"{results[-1].throughput:,.0f}" if results else "N/A"
    last_p99 = f"{results[-1].p99_latency_ms:,.2f}" if results else "N/A"
    
    print(f"""
┌──────────────┬─────────────────────┬───────────────┬───────────────────────────────────────────────────┐
│ System       │ Throughput (msg/s)  │ P99 Latency   │ Notes                                             │
├──────────────┼─────────────────────┼───────────────┼───────────────────────────────────────────────────┤
│ Apache Kafka │ 1,000,000+          │ 5-50ms        │ With batching, linger.ms, compression             │
│ RabbitMQ     │ 10,000-100,000      │ 1-10ms        │ Persistent messages, prefetch tuned               │
│ AWS SQS      │ 300,000 (3K FIFO)   │ 50-500ms      │ Managed service, HTTP API overhead                │
│ Redis Streams│ 500,000+            │ <1ms          │ In-memory, optional persistence                   │
│ NATS         │ 10,000,000+         │ <1ms          │ At-most-once default, JetStream for durability    │
├──────────────┼─────────────────────┼───────────────┼───────────────────────────────────────────────────┤
│ GoQueue      │ {last_throughput:>17} │ {last_p99:>11}ms │ This benchmark run                                │
└──────────────┴─────────────────────┴───────────────┴───────────────────────────────────────────────────┘
""")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("╔" + "═" * 100 + "╗")
    print("║" + "                           GOQUEUE BENCHMARK SUITE (Python)                                   ".ljust(100) + "║")
    print("╠" + "═" * 100 + "╣")
    print(f"║ Target URL: {GOQUEUE_URL}".ljust(101) + "║")
    print("╚" + "═" * 100 + "╝")

    client = GoQueueClient(GOQUEUE_URL)

    # Verify connection
    if not client.health_check():
        print(f"\n❌ Failed to connect to GoQueue at {GOQUEUE_URL}")
        print("   Make sure the server is running and accessible.")
        exit(1)
    
    print("\n✅ Connected to GoQueue server")

    results = []

    try:
        results.append(benchmark_publish_throughput(client))
        results.append(benchmark_concurrent_publish(client))
        # NOTE: End-to-end and sustained tests require consumer group heartbeats
        # which are not implemented in this simple benchmark. Skipping for now.
        # results.append(benchmark_end_to_end_latency(client))
        results.append(benchmark_sustained_throughput(client))
    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        exit(1)

    print_results(results)


if __name__ == "__main__":
    main()
