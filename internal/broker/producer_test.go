package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// PRODUCER TESTS
// =============================================================================
//
// These tests verify:
// 1. Basic send/receive functionality
// 2. Batching triggers (size, time, bytes)
// 3. Synchronous sends
// 4. Flush behavior
// 5. Concurrent sends
// 6. Graceful shutdown
//

// setupTestBrokerAndTopic creates a test broker with a topic.
func setupTestBrokerAndTopic(t *testing.T, numPartitions int) (*Broker, func()) {
	t.Helper()

	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	broker, err := NewBroker(config)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	err = broker.CreateTopic(TopicConfig{
		Name:          "test-topic",
		NumPartitions: numPartitions,
	})
	if err != nil {
		broker.Close()
		t.Fatalf("Failed to create topic: %v", err)
	}

	cleanup := func() {
		broker.Close()
	}

	return broker, cleanup
}

// TestProducerSendSync tests synchronous message sending.
func TestProducerSendSync(t *testing.T) {
	broker, cleanup := setupTestBrokerAndTopic(t, 3)
	defer cleanup()

	config := DefaultProducerConfig("test-topic")
	config.LingerMs = 0 // Disable linger for immediate flush

	producer, err := NewProducer(broker, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send some messages
	for i := 0; i < 10; i++ {
		result := producer.SendSync(ctx, ProducerRecord{
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		})

		if result.Error != nil {
			t.Errorf("SendSync failed: %v", result.Error)
		}
		if result.Topic != "test-topic" {
			t.Errorf("Expected topic 'test-topic', got %s", result.Topic)
		}
		if result.Offset < 0 {
			t.Errorf("Expected non-negative offset, got %d", result.Offset)
		}
	}
}

// TestProducerBatchBySize tests that batch flushes when BatchSize is reached.
func TestProducerBatchBySize(t *testing.T) {
	broker, cleanup := setupTestBrokerAndTopic(t, 1)
	defer cleanup()

	config := DefaultProducerConfig("test-topic")
	config.BatchSize = 5            // Small batch for testing
	config.LingerMs = 5000          // High linger so only size triggers (not time)
	config.BatchBytes = 1024 * 1024 // High byte limit

	producer, err := NewProducer(broker, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Use async Send with result channels to verify batch flush by size
	results := make([]chan ProducerResult, 5)
	for i := 0; i < 5; i++ {
		results[i] = make(chan ProducerResult, 1)
		err := producer.Send(ProducerRecord{
			Key:      []byte("key"),
			Value:    []byte("value"),
			resultCh: results[i],
		})
		if err != nil {
			t.Fatalf("Send %d failed: %v", i, err)
		}
	}

	// All 5 messages should trigger batch flush
	// Wait for all results with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for i, ch := range results {
		select {
		case result := <-ch:
			if result.Error != nil {
				t.Errorf("Message %d error: %v", i, result.Error)
			}
		case <-ctx.Done():
			t.Fatalf("Message %d result timeout - batch not flushed by size", i)
		}
	}

	// Check stats - batch should have been flushed
	stats := producer.Stats()
	if stats.BatchesSent != 1 {
		t.Errorf("Expected at least 1 batch sent, got %d", stats.BatchesSent)
	}
}

// TestProducerBatchByLinger tests that batch flushes after LingerMs.
func TestProducerBatchByLinger(t *testing.T) {
	broker, cleanup := setupTestBrokerAndTopic(t, 1)
	defer cleanup()

	config := DefaultProducerConfig("test-topic")
	config.BatchSize = 1000 // High size so linger triggers first
	config.LingerMs = 50    // 50ms linger
	config.BatchBytes = 1024 * 1024

	producer, err := NewProducer(broker, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Send a few messages (less than BatchSize)
	for i := 0; i < 3; i++ {
		err := producer.Send(ProducerRecord{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		if err != nil {
			t.Errorf("Send failed: %v", err)
		}
	}

	// Wait for linger to trigger
	time.Sleep(100 * time.Millisecond)

	// Check stats
	stats := producer.Stats()
	if stats.BatchesSent < 1 {
		t.Errorf("Expected linger to trigger flush, batches sent: %d", stats.BatchesSent)
	}
}

// TestProducerFlush tests explicit flush.
func TestProducerFlush(t *testing.T) {
	broker, cleanup := setupTestBrokerAndTopic(t, 1)
	defer cleanup()

	config := DefaultProducerConfig("test-topic")
	config.BatchSize = 1000 // High size
	config.LingerMs = 10000 // High linger (10s)
	config.BatchBytes = 1024 * 1024

	producer, err := NewProducer(broker, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Send messages
	for i := 0; i < 5; i++ {
		err := producer.Send(ProducerRecord{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		if err != nil {
			t.Errorf("Send failed: %v", err)
		}
	}

	// Explicit flush
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = producer.Flush(ctx)
	if err != nil {
		t.Errorf("Flush failed: %v", err)
	}

	// Check stats
	stats := producer.Stats()
	if stats.MessagesAcked != 5 {
		t.Errorf("Expected 5 messages acked, got %d", stats.MessagesAcked)
	}
}

// TestProducerPartitioning tests that messages route to correct partitions.
func TestProducerPartitioning(t *testing.T) {
	broker, cleanup := setupTestBrokerAndTopic(t, 5)
	defer cleanup()

	config := DefaultProducerConfig("test-topic")
	config.LingerMs = 0 // Immediate flush

	producer, err := NewProducer(broker, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send messages with same key - should go to same partition
	key := []byte("user-12345")
	var firstPartition int

	for i := 0; i < 10; i++ {
		result := producer.SendSync(ctx, ProducerRecord{
			Key:   key,
			Value: []byte("value"),
		})
		if result.Error != nil {
			t.Fatalf("Send failed: %v", result.Error)
		}

		if i == 0 {
			firstPartition = result.Partition
		} else if result.Partition != firstPartition {
			t.Errorf("Same key routed to different partitions: %d vs %d",
				firstPartition, result.Partition)
		}
	}
}

// TestProducerConcurrentSends tests thread safety.
func TestProducerConcurrentSends(t *testing.T) {
	broker, cleanup := setupTestBrokerAndTopic(t, 3)
	defer cleanup()

	config := DefaultProducerConfig("test-topic")
	config.LingerMs = 10
	config.BatchSize = 50

	producer, err := NewProducer(broker, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Launch multiple goroutines sending concurrently
	numGoroutines := 10
	messagesPerGoroutine := 100
	var wg sync.WaitGroup

	errors := make(chan error, numGoroutines*messagesPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < messagesPerGoroutine; i++ {
				result := producer.SendSync(ctx, ProducerRecord{
					Key:   []byte("key"),
					Value: []byte("value"),
				})
				if result.Error != nil {
					errors <- result.Error
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errCount := 0
	for err := range errors {
		t.Logf("Error: %v", err)
		errCount++
	}
	if errCount > 0 {
		t.Errorf("Got %d errors during concurrent sends", errCount)
	}

	// Verify all messages were sent
	stats := producer.Stats()
	expected := int64(numGoroutines * messagesPerGoroutine)
	if stats.MessagesAcked != expected {
		t.Errorf("Expected %d messages acked, got %d", expected, stats.MessagesAcked)
	}
}

// TestProducerClose tests graceful shutdown.
func TestProducerClose(t *testing.T) {
	broker, cleanup := setupTestBrokerAndTopic(t, 1)
	defer cleanup()

	config := DefaultProducerConfig("test-topic")
	config.BatchSize = 1000 // High batch size
	config.LingerMs = 10000 // High linger

	producer, err := NewProducer(broker, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}

	// Send some messages
	for i := 0; i < 10; i++ {
		err := producer.Send(ProducerRecord{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		if err != nil {
			t.Errorf("Send failed: %v", err)
		}
	}

	// Close should flush pending messages
	err = producer.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify messages were sent before close
	stats := producer.Stats()
	if stats.MessagesAcked != 10 {
		t.Errorf("Expected 10 messages acked on close, got %d", stats.MessagesAcked)
	}

	// Subsequent sends should fail
	err = producer.Send(ProducerRecord{Key: []byte("key"), Value: []byte("value")})
	if !errors.Is(err, ErrProducerClosed) {
		t.Errorf("Expected ErrProducerClosed, got %v", err)
	}
}

// TestProducerStats tests statistics tracking.
func TestProducerStats(t *testing.T) {
	broker, cleanup := setupTestBrokerAndTopic(t, 1)
	defer cleanup()

	config := DefaultProducerConfig("test-topic")
	config.LingerMs = 0

	producer, err := NewProducer(broker, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send messages
	numMessages := 25
	for i := 0; i < numMessages; i++ {
		producer.SendSync(ctx, ProducerRecord{
			Key:   []byte("key"),
			Value: []byte("value-with-some-bytes"),
		})
	}

	stats := producer.Stats()

	if stats.MessagesSent != int64(numMessages) {
		t.Errorf("MessagesSent: expected %d, got %d", numMessages, stats.MessagesSent)
	}
	if stats.MessagesAcked != int64(numMessages) {
		t.Errorf("MessagesAcked: expected %d, got %d", numMessages, stats.MessagesAcked)
	}
	if stats.BytesSent == 0 {
		t.Error("BytesSent should be > 0")
	}
	if stats.BatchesSent == 0 {
		t.Error("BatchesSent should be > 0")
	}
}

// BenchmarkProducerSendSync benchmarks synchronous sends.
func BenchmarkProducerSendSync(b *testing.B) {
	dir := b.TempDir()
	broker, _ := NewBroker(BrokerConfig{DataDir: dir, NodeID: "bench"})
	defer broker.Close()

	broker.CreateTopic(TopicConfig{Name: "bench-topic", NumPartitions: 3})

	config := DefaultProducerConfig("bench-topic")
	producer, _ := NewProducer(broker, config)
	defer producer.Close()

	ctx := context.Background()
	record := ProducerRecord{
		Key:   []byte("benchmark-key"),
		Value: []byte("benchmark-value-with-some-payload-data"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		producer.SendSync(ctx, record)
	}
}

// BenchmarkProducerSendAsync benchmarks async sends with batching.
func BenchmarkProducerSendAsync(b *testing.B) {
	dir := b.TempDir()
	broker, _ := NewBroker(BrokerConfig{DataDir: dir, NodeID: "bench"})
	defer broker.Close()

	broker.CreateTopic(TopicConfig{Name: "bench-topic", NumPartitions: 3})

	config := DefaultProducerConfig("bench-topic")
	config.BatchSize = 100
	config.LingerMs = 5
	producer, _ := NewProducer(broker, config)
	defer producer.Close()

	record := ProducerRecord{
		Key:   []byte("benchmark-key"),
		Value: []byte("benchmark-value-with-some-payload-data"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		producer.Send(record)
	}

	// Flush remaining
	producer.Flush(context.Background())
}
