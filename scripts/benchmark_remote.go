// =============================================================================
// REMOTE BENCHMARK CLIENT FOR GOQUEUE
// =============================================================================
//
// This benchmark tests GoQueue's throughput via HTTP API, specifically targeting
// the batch publish endpoint with the new write batching optimizations.
//
// USAGE:
//   go run benchmark_remote.go -url http://YOUR-LB-URL:8080 -topic benchmark-perf-8p
//
// =============================================================================

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Message represents a message to publish
type Message struct {
	Key     string            `json:"key"`
	Value   string            `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`
}

// BatchPublishRequest is the request body for batch publish
type BatchPublishRequest struct {
	Messages []Message `json:"messages"`
}

// PublishResult holds statistics from a benchmark run
type PublishResult struct {
	TotalMessages  int64
	TotalDuration  time.Duration
	SuccessCount   int64
	ErrorCount     int64
	ThroughputMsgS float64
	ThroughputMBs  float64
	AvgLatencyMs   float64
	P99LatencyMs   float64
}

var (
	baseURL     = flag.String("url", "http://localhost:8080", "GoQueue base URL")
	topicName   = flag.String("topic", "benchmark-perf-8p", "Topic name")
	batchSize   = flag.Int("batch", 1000, "Messages per batch")
	numBatches  = flag.Int("batches", 100, "Number of batches to send")
	concurrency = flag.Int("concurrency", 4, "Number of parallel workers")
	messageSize = flag.Int("size", 256, "Message payload size in bytes")
)

func main() {
	flag.Parse()

	fmt.Println("=" + string(repeat('=', 78)))
	fmt.Println("GOQUEUE REMOTE BENCHMARK")
	fmt.Println("=" + string(repeat('=', 78)))
	fmt.Printf("URL:         %s\n", *baseURL)
	fmt.Printf("Topic:       %s\n", *topicName)
	fmt.Printf("Batch Size:  %d messages\n", *batchSize)
	fmt.Printf("Batches:     %d\n", *numBatches)
	fmt.Printf("Concurrency: %d workers\n", *concurrency)
	fmt.Printf("Msg Size:    %d bytes\n", *messageSize)
	fmt.Println("-" + string(repeat('-', 78)))

	// Test connectivity
	resp, err := http.Get(*baseURL + "/health")
	if err != nil {
		fmt.Printf("ERROR: Cannot connect to %s: %v\n", *baseURL, err)
		return
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		fmt.Printf("ERROR: Health check failed with status %d\n", resp.StatusCode)
		return
	}
	fmt.Println("✓ Health check passed")

	// Create HTTP client with connection pooling
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: *concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 30 * time.Second,
	}

	// Prepare message payload
	payload := make([]byte, *messageSize)
	for i := range payload {
		payload[i] = byte('A' + (i % 26))
	}
	payloadStr := string(payload)

	// Run benchmark
	fmt.Println("\nStarting benchmark...")
	result := runBenchmark(client, payloadStr)

	// Print results
	fmt.Println("\n" + "=" + string(repeat('=', 78)))
	fmt.Println("BENCHMARK RESULTS")
	fmt.Println("=" + string(repeat('=', 78)))
	fmt.Printf("Total Messages:     %d\n", result.TotalMessages)
	fmt.Printf("Total Duration:     %.2fs\n", result.TotalDuration.Seconds())
	fmt.Printf("Success Count:      %d\n", result.SuccessCount)
	fmt.Printf("Error Count:        %d\n", result.ErrorCount)
	fmt.Println("-" + string(repeat('-', 78)))
	fmt.Printf("THROUGHPUT:         %.0f msg/sec\n", result.ThroughputMsgS)
	fmt.Printf("THROUGHPUT:         %.2f MB/sec\n", result.ThroughputMBs)
	fmt.Printf("Avg Latency:        %.2f ms\n", result.AvgLatencyMs)
	fmt.Println("=" + string(repeat('=', 78)))
}

func runBenchmark(client *http.Client, payload string) PublishResult {
	var (
		wg           sync.WaitGroup
		successCount int64
		errorCount   int64
		totalLatency int64 // in microseconds
	)

	batchesPerWorker := *numBatches / *concurrency
	endpoint := fmt.Sprintf("%s/topics/%s/messages", *baseURL, *topicName)

	start := time.Now()

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Create batch payload once, reuse
			messages := make([]Message, *batchSize)
			for i := range messages {
				messages[i] = Message{
					Key:   fmt.Sprintf("key-%d-%d", workerID, i),
					Value: payload,
				}
			}

			reqBody := BatchPublishRequest{Messages: messages}
			jsonData, _ := json.Marshal(reqBody)

			for b := 0; b < batchesPerWorker; b++ {
				batchStart := time.Now()

				req, err := http.NewRequest("POST", endpoint, bytes.NewReader(jsonData))
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					atomic.AddInt64(&successCount, int64(*batchSize))
				} else {
					atomic.AddInt64(&errorCount, int64(*batchSize))
				}

				latency := time.Since(batchStart).Microseconds()
				atomic.AddInt64(&totalLatency, latency)

				// Progress indicator every 10 batches
				if (b+1)%10 == 0 {
					fmt.Printf("Worker %d: %d/%d batches complete\n", workerID, b+1, batchesPerWorker)
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)

	totalMessages := int64(*batchSize) * int64(*numBatches)
	totalDataMB := float64(totalMessages) * float64(*messageSize) / (1024 * 1024)

	return PublishResult{
		TotalMessages:  totalMessages,
		TotalDuration:  duration,
		SuccessCount:   successCount,
		ErrorCount:     errorCount,
		ThroughputMsgS: float64(successCount) / duration.Seconds(),
		ThroughputMBs:  totalDataMB / duration.Seconds(),
		AvgLatencyMs:   float64(totalLatency) / float64(*numBatches) / 1000,
	}
}

func repeat(char byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = char
	}
	return b
}
