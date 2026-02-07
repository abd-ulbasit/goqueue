// =============================================================================
// GOQUEUE BENCHMARK TEST SUITE
// =============================================================================
//
// This file contains comprehensive benchmarks to test GoQueue throughput,
// latency, and compare performance characteristics against industry standards.
//
// USAGE:
//   # Run all benchmarks against local GoQueue
//   GOQUEUE_URL=http://localhost:8080 go test -bench=. -benchmem -v
//
//   # Run against EKS cluster
//   GOQUEUE_URL=http://<load-balancer-url>:8080 go test -bench=. -benchmem -v
//
//   # Run specific benchmark
//   GOQUEUE_URL=http://localhost:8080 go test -bench=BenchmarkPublishThroughput -benchmem -v
//
// WHAT WE MEASURE:
//   - Publish throughput (messages/sec)
//   - Consume throughput (messages/sec)
//   - End-to-end latency (publish → consume)
//   - Batch publish performance
//   - Consumer group scaling
//   - Concurrent producer performance
//
// =============================================================================

package benchmark

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// CONFIGURATION
// =============================================================================

var (
	// baseURL is the GoQueue server URL (from environment)
	baseURL = getEnv("GOQUEUE_URL", "http://localhost:8080")

	// Message sizes for testing
	mediumMessage = 1024 // 1 KB
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// =============================================================================
// TEST HELPERS
// =============================================================================

// HTTPClient wraps http.Client with common operations
type HTTPClient struct {
	client  *http.Client
	baseURL string
}

func newClient() *HTTPClient {
	return &HTTPClient{
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		baseURL: baseURL,
	}
}

// CreateTopic creates a topic for testing
//
// NOTE: GoQueue API uses /topics and expects "num_partitions" field (not "partitions")
func (c *HTTPClient) CreateTopic(name string, partitions int) error {
	payload := map[string]interface{}{
		"name":           name,
		"num_partitions": partitions, // API expects "num_partitions"
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", c.baseURL+"/topics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 201 Created or 409 Conflict (already exists) are both OK
	if resp.StatusCode != 201 && resp.StatusCode != 409 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create topic failed: %d - %s", resp.StatusCode, string(body))
	}
	return nil
}

// DeleteTopic removes a topic
func (c *HTTPClient) DeleteTopic(name string) error {
	req, _ := http.NewRequest("DELETE", c.baseURL+"/topics/"+name, nil)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// Publish sends messages to a topic
func (c *HTTPClient) Publish(topic string, messages []map[string]interface{}) error {
	payload := map[string]interface{}{
		"messages": messages,
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", c.baseURL+"/topics/"+topic+"/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("publish failed: %d - %s", resp.StatusCode, string(body))
	}
	return nil
}

// generateMessage creates a random message of the specified size
func generateMessage(size int) map[string]interface{} {
	data := make([]byte, size)
	rand.Read(data)
	return map[string]interface{}{
		"value": string(data),
	}
}

// generateMessages creates a batch of messages
func generateMessages(count, size int) []map[string]interface{} {
	messages := make([]map[string]interface{}, count)
	for i := 0; i < count; i++ {
		messages[i] = generateMessage(size)
	}
	return messages
}

// =============================================================================
// PUBLISH THROUGHPUT BENCHMARKS
// =============================================================================

// BenchmarkPublishSingleMessage measures single message publish latency
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ WHAT THIS TESTS:                                                           │
// │   - Latency of publishing one message at a time                            │
// │   - Baseline for understanding GoQueue's minimum latency                   │
// │                                                                             │
// │ COMPARISON:                                                                 │
// │   - Kafka: ~1-5ms for single message with acks=1                           │
// │   - RabbitMQ: ~1-3ms for persistent message                                │
// │   - SQS: ~10-50ms (network + API overhead)                                 │
// └─────────────────────────────────────────────────────────────────────────────┘
func BenchmarkPublishSingleMessage(b *testing.B) {
	client := newClient()
	topicName := fmt.Sprintf("bench-single-%d", time.Now().UnixNano())

	if err := client.CreateTopic(topicName, 1); err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}
	defer client.DeleteTopic(topicName)

	message := []map[string]interface{}{generateMessage(mediumMessage)}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := client.Publish(topicName, message); err != nil {
			b.Fatalf("Publish failed: %v", err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
}

// BenchmarkPublishBatch measures batch publish throughput
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ WHAT THIS TESTS:                                                           │
// │   - Throughput when publishing messages in batches                         │
// │   - Amortized network overhead per message                                 │
// │                                                                             │
// │ WHY BATCHING MATTERS:                                                       │
// │   - Network round-trip is often the bottleneck                             │
// │   - Batching amortizes connection overhead across many messages            │
// │   - Kafka achieves millions/sec primarily through batching                 │
// └─────────────────────────────────────────────────────────────────────────────┘
func BenchmarkPublishBatch(b *testing.B) {
	batchSizes := []int{10, 100, 500, 1000}
	messageSizes := []int{100, 1024, 10240}

	for _, batchSize := range batchSizes {
		for _, msgSize := range messageSizes {
			name := fmt.Sprintf("batch=%d/msgSize=%d", batchSize, msgSize)
			b.Run(name, func(b *testing.B) {
				client := newClient()
				topicName := fmt.Sprintf("bench-batch-%d-%d-%d", batchSize, msgSize, time.Now().UnixNano())

				if err := client.CreateTopic(topicName, 6); err != nil {
					b.Fatalf("Failed to create topic: %v", err)
				}
				defer client.DeleteTopic(topicName)

				messages := generateMessages(batchSize, msgSize)

				b.ResetTimer()
				b.ReportAllocs()

				totalMessages := int64(0)
				for i := 0; i < b.N; i++ {
					if err := client.Publish(topicName, messages); err != nil {
						b.Fatalf("Publish failed: %v", err)
					}
					totalMessages += int64(batchSize)
				}

				b.StopTimer()
				b.ReportMetric(float64(totalMessages)/b.Elapsed().Seconds(), "msgs/sec")
				b.ReportMetric(float64(totalMessages*int64(msgSize))/b.Elapsed().Seconds()/1024/1024, "MB/sec")
			})
		}
	}
}

// BenchmarkPublishConcurrent measures throughput with multiple producers
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ WHAT THIS TESTS:                                                           │
// │   - How well GoQueue scales with concurrent producers                      │
// │   - Lock contention and parallel write performance                         │
// │                                                                             │
// │ REAL-WORLD SCENARIO:                                                        │
// │   - Multiple microservices publishing to the same topic                    │
// │   - High-throughput event streaming                                        │
// └─────────────────────────────────────────────────────────────────────────────┘
func BenchmarkPublishConcurrent(b *testing.B) {
	concurrencies := []int{1, 2, 4, 8, 16, 32}

	for _, concurrency := range concurrencies {
		b.Run(fmt.Sprintf("producers=%d", concurrency), func(b *testing.B) {
			client := newClient()
			topicName := fmt.Sprintf("bench-concurrent-%d-%d", concurrency, time.Now().UnixNano())

			if err := client.CreateTopic(topicName, 6); err != nil {
				b.Fatalf("Failed to create topic: %v", err)
			}
			defer client.DeleteTopic(topicName)

			messages := generateMessages(100, mediumMessage) // Batch of 100

			b.ResetTimer()
			b.ReportAllocs()

			var totalMessages int64
			var wg sync.WaitGroup

			opsPerGoroutine := b.N / concurrency
			if opsPerGoroutine < 1 {
				opsPerGoroutine = 1
			}

			for c := 0; c < concurrency; c++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < opsPerGoroutine; i++ {
						if err := client.Publish(topicName, messages); err != nil {
							return
						}
						atomic.AddInt64(&totalMessages, 100)
					}
				}()
			}

			wg.Wait()
			b.StopTimer()

			b.ReportMetric(float64(totalMessages)/b.Elapsed().Seconds(), "msgs/sec")
		})
	}
}

// =============================================================================
// LATENCY BENCHMARKS
// =============================================================================

// TestEndToEndLatency measures the time from publish to consume
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ WHAT THIS TESTS:                                                           │
// │   - Total time for a message to be available to consumers                  │
// │   - Critical for real-time applications                                    │
// │                                                                             │
// │ LATENCY EXPECTATIONS (p99):                                                 │
// │   - Kafka: 5-50ms depending on configuration                               │
// │   - RabbitMQ: 1-10ms                                                       │
// │   - SQS: 50-500ms                                                          │
// │   - GoQueue target: <10ms for 99th percentile                              │
// └─────────────────────────────────────────────────────────────────────────────┘
func TestEndToEndLatency(t *testing.T) {
	client := newClient()
	topicName := fmt.Sprintf("latency-test-%d", time.Now().UnixNano())
	groupName := fmt.Sprintf("latency-group-%d", time.Now().UnixNano())

	if err := client.CreateTopic(topicName, 1); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer client.DeleteTopic(topicName)

	// Join consumer group
	joinPayload := map[string]interface{}{
		"client_id": "latency-tester",
		"topics":    []string{topicName},
	}
	joinBody, _ := json.Marshal(joinPayload)

	joinReq, _ := http.NewRequest("POST", baseURL+"/groups/"+groupName+"/join", bytes.NewReader(joinBody))
	joinReq.Header.Set("Content-Type", "application/json")

	joinResp, err := client.client.Do(joinReq)
	if err != nil {
		t.Fatalf("Failed to join group: %v", err)
	}
	defer joinResp.Body.Close()

	var joinResult struct {
		MemberID string `json:"member_id"`
	}
	json.NewDecoder(joinResp.Body).Decode(&joinResult)

	// Measure latencies
	numSamples := 1000
	latencies := make([]time.Duration, 0, numSamples)

	for i := 0; i < numSamples; i++ {
		timestamp := time.Now().UnixNano()
		message := []map[string]interface{}{
			{
				"value": fmt.Sprintf(`{"timestamp":%d}`, timestamp),
			},
		}

		publishStart := time.Now()

		// Publish
		if err := client.Publish(topicName, message); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		// Poll for the message
		// Note: GoQueue poll endpoint is GET /groups/{groupID}/poll (no member ID in path)
		pollReq, _ := http.NewRequest("GET", fmt.Sprintf("%s/groups/%s/poll?timeout=5000", baseURL, groupName), nil)
		pollReq.Header.Set("X-Member-ID", joinResult.MemberID)
		pollResp, err := client.client.Do(pollReq)
		if err != nil {
			t.Fatalf("Poll failed: %v", err)
		}
		pollResp.Body.Close()

		latency := time.Since(publishStart)
		latencies = append(latencies, latency)
	}

	// Calculate statistics
	var total time.Duration
	var min, max time.Duration = latencies[0], latencies[0]

	for _, l := range latencies {
		total += l
		if l < min {
			min = l
		}
		if l > max {
			max = l
		}
	}

	avg := total / time.Duration(len(latencies))

	// Calculate percentiles (simple approach)
	// Sort latencies
	for i := 0; i < len(latencies); i++ {
		for j := i + 1; j < len(latencies); j++ {
			if latencies[j] < latencies[i] {
				latencies[i], latencies[j] = latencies[j], latencies[i]
			}
		}
	}

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	t.Logf("End-to-End Latency Results (n=%d):", numSamples)
	t.Logf("  Min:  %v", min)
	t.Logf("  Max:  %v", max)
	t.Logf("  Avg:  %v", avg)
	t.Logf("  P50:  %v", p50)
	t.Logf("  P95:  %v", p95)
	t.Logf("  P99:  %v", p99)
}

// =============================================================================
// THROUGHPUT TEST (Non-Benchmark)
// =============================================================================

// TestSustainedThroughput runs a sustained load test
func TestSustainedThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping sustained throughput test in short mode")
	}

	client := newClient()
	topicName := fmt.Sprintf("throughput-test-%d", time.Now().UnixNano())

	if err := client.CreateTopic(topicName, 6); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer client.DeleteTopic(topicName)

	duration := 30 * time.Second
	batchSize := 100
	numProducers := 8

	messages := generateMessages(batchSize, mediumMessage)

	var totalMessages int64
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	start := time.Now()

	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if err := client.Publish(topicName, messages); err != nil {
						return
					}
					atomic.AddInt64(&totalMessages, int64(batchSize))
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	throughput := float64(totalMessages) / elapsed.Seconds()
	mbPerSec := (float64(totalMessages) * float64(mediumMessage)) / elapsed.Seconds() / 1024 / 1024

	t.Logf("Sustained Throughput Test Results:")
	t.Logf("  Duration:     %v", elapsed)
	t.Logf("  Producers:    %d", numProducers)
	t.Logf("  Batch Size:   %d", batchSize)
	t.Logf("  Total Msgs:   %d", totalMessages)
	t.Logf("  Throughput:   %.2f msgs/sec", throughput)
	t.Logf("  Data Rate:    %.2f MB/sec", mbPerSec)
}

// =============================================================================
// COMPARISON REFERENCE
// =============================================================================

// PrintComparisonTable prints a reference table of queue system performance
func TestPrintComparisonTable(t *testing.T) {
	t.Log(`
╔═══════════════════════════════════════════════════════════════════════════════════════╗
║                     MESSAGE QUEUE PERFORMANCE COMPARISON                               ║
╠═══════════════════════════════════════════════════════════════════════════════════════╣
║ System      │ Throughput (msg/s) │ Latency (p99) │ Durability        │ Best For       ║
╠═════════════╪════════════════════╪═══════════════╪═══════════════════╪════════════════╣
║ Kafka       │ 1M+ (batched)      │ 5-50ms        │ Replicated, disk  │ Event streams  ║
║ RabbitMQ    │ 10K-100K           │ 1-10ms        │ Optional persist  │ Task queues    ║
║ AWS SQS     │ 300K (FIFO: 3K)    │ 50-500ms      │ Managed, durable  │ Cloud native   ║
║ Redis Pub   │ 500K+              │ <1ms          │ None (in-memory)  │ Real-time      ║
║ NATS        │ 10M+               │ <1ms          │ Optional JetStr   │ Microservices  ║
╠═════════════╪════════════════════╪═══════════════╪═══════════════════╪════════════════╣
║ GoQueue     │ 100K-500K (est)    │ 5-20ms (est)  │ Replicated WAL    │ K8s native     ║
╚═══════════════════════════════════════════════════════════════════════════════════════╝

Notes:
- Kafka throughput requires proper batching (linger.ms, batch.size)
- RabbitMQ with prefetch + acks = 1 can achieve higher throughput
- SQS FIFO limited by deduplication (300 msg/s per group)
- NATS achieves high throughput with at-most-once delivery
- GoQueue targets Kafka-like features with SQS-like simplicity
`)
}
