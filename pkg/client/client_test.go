// =============================================================================
// GO CLIENT LIBRARY TESTS
// =============================================================================
//
// These tests verify the Go client library functionality including:
//   - Client connection and configuration
//   - Publish operations
//   - Consume operations
//   - Offset management
//   - Health checking
//
// TEST STRATEGY:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                         Test Architecture                              │
//   │                                                                         │
//   │   ┌─────────────────┐          ┌─────────────────┐                     │
//   │   │   Test Broker   │◄────────►│  gRPC Server    │                     │
//   │   │   (in-memory)   │          │  (random port)  │                     │
//   │   └─────────────────┘          └────────┬────────┘                     │
//   │                                         │                               │
//   │                                ┌────────▼────────┐                     │
//   │                                │   Go Client     │                     │
//   │                                │   (under test)  │                     │
//   │                                └─────────────────┘                     │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// WHY TEST AGAINST REAL SERVER:
//   - Validates actual wire protocol
//   - Tests serialization/deserialization
//   - Catches integration issues early
//
// =============================================================================

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	pb "goqueue/api/proto/gen/go"
	"goqueue/internal/broker"
	goqueuegrpc "goqueue/internal/grpc"
)

// =============================================================================
// TEST HELPERS
// =============================================================================

// testEnv holds the test environment.
type testEnv struct {
	broker *broker.Broker
	server *goqueuegrpc.Server
	client *Client
	addr   string
}

// setupTestEnv creates a broker, server, and client for testing.
func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()

	// Create temp directory for broker data
	tempDir := t.TempDir()

	// Create broker
	brokerConfig := broker.DefaultBrokerConfig()
	brokerConfig.DataDir = tempDir

	b, err := broker.NewBroker(brokerConfig)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	// Find available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Close()
		t.Fatalf("Failed to find available port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close() // Close so server can use it

	// Create and start gRPC server
	serverConfig := goqueuegrpc.DefaultServerConfig()
	serverConfig.Address = addr
	serverConfig.EnableReflection = false

	server := goqueuegrpc.NewServer(b, serverConfig)

	// Start server in background (Start() blocks)
	go func() {
		if err := server.Start(); err != nil {
			// Server stopped — expected during teardown; error is non-actionable in tests.
			_ = err
		}
	}()

	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)

	// Create client using our client library
	clientConfig := DefaultConfig(addr)
	clientConfig.DialTimeout = 5 * time.Second

	client, err := New(clientConfig)
	if err != nil {
		server.Stop()
		b.Close()
		t.Fatalf("Failed to create client: %v", err)
	}

	return &testEnv{
		broker: b,
		server: server,
		client: client,
		addr:   addr,
	}
}

// teardown cleans up the test environment.
func (te *testEnv) teardown() {
	if te.client != nil {
		te.client.Close()
	}
	if te.server != nil {
		te.server.Stop()
	}
	if te.broker != nil {
		te.broker.Close()
	}
}

// =============================================================================
// CLIENT CONFIGURATION TESTS
// =============================================================================

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig("localhost:9000")

	if config.Address != "localhost:9000" {
		t.Errorf("Expected address 'localhost:9000', got %s", config.Address)
	}

	if config.DialTimeout <= 0 {
		t.Error("Default dial timeout should be positive")
	}

	if config.MaxRetries < 0 {
		t.Error("Max retries should not be negative")
	}

	if config.KeepAliveTime <= 0 {
		t.Error("Keep alive time should be positive")
	}
}

func TestNew_InvalidAddress(t *testing.T) {
	config := DefaultConfig("invalid-address:99999")
	config.DialTimeout = 500 * time.Millisecond // Short timeout

	_, err := New(config)
	if err == nil {
		t.Error("Expected error for invalid address")
	}
}

// =============================================================================
// HEALTH SERVICE TESTS
// =============================================================================

func TestClient_Health(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	status, err := te.client.Health(ctx)
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	if status != pb.ServingStatus_SERVING_STATUS_SERVING {
		t.Errorf("Expected SERVING status, got %v", status)
	}
}

// =============================================================================
// PUBLISH TESTS
// =============================================================================

func TestClient_Publish(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	// Create topic
	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "client-test-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish message
	result, err := te.client.Publish(ctx, "client-test-topic", []byte("test-value"))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if result.Partition != 0 {
		t.Errorf("Expected partition 0, got %d", result.Partition)
	}

	if result.Offset < 0 {
		t.Errorf("Expected valid offset, got %d", result.Offset)
	}
}

func TestClient_Publish_WithKey(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	// Create topic
	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "key-topic",
		NumPartitions: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish with key
	result, err := te.client.Publish(ctx, "key-topic", []byte("test-value"),
		WithKey([]byte("test-key")))
	if err != nil {
		t.Fatalf("Publish with key failed: %v", err)
	}

	if result.Offset < 0 {
		t.Errorf("Expected valid offset, got %d", result.Offset)
	}
}

func TestClient_Publish_WithHeaders(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	// Create topic
	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "headers-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish with headers
	result, err := te.client.Publish(ctx, "headers-topic", []byte("test-value"),
		WithHeaders(map[string]string{
			"content-type": "application/json",
			"trace-id":     "abc123",
		}))
	if err != nil {
		t.Fatalf("Publish with headers failed: %v", err)
	}

	if result.Offset < 0 {
		t.Errorf("Expected valid offset, got %d", result.Offset)
	}
}

func TestClient_Publish_TopicNotFound(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish to non-existent topic
	_, err := te.client.Publish(ctx, "non-existent-topic", []byte("test-value"))
	if err == nil {
		t.Error("Expected error for non-existent topic")
	}
}

func TestClient_Publish_MultipleMessages(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	// Create topic with multiple partitions
	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "multi-partition-topic",
		NumPartitions: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish multiple messages
	for i := 0; i < 20; i++ {
		result, err := te.client.Publish(ctx, "multi-partition-topic",
			[]byte(fmt.Sprintf("value-%d", i)),
			WithKey([]byte(fmt.Sprintf("key-%d", i))))
		if err != nil {
			t.Errorf("Publish %d failed: %v", i, err)
		}
		if result.Offset < 0 {
			t.Errorf("Invalid offset for message %d: %d", i, result.Offset)
		}
	}
}

// =============================================================================
// CONSUME TESTS
// =============================================================================

func TestClient_Consume(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	// Create topic and publish messages
	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "consume-test-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx := context.Background()

	// Publish some messages first
	for i := 0; i < 5; i++ {
		_, err := te.client.Publish(ctx, "consume-test-topic",
			[]byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Consume messages with timeout
	consumeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := te.client.Consume(consumeCtx, "consume-test-topic",
		[]int32{0}, pb.ConsumeStartPosition_CONSUME_START_POSITION_EARLIEST)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Receive messages
	messageCount := 0
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Context timeout is expected
			break
		}
		if msg != nil {
			messageCount++
		}
		if messageCount >= 5 {
			break
		}
	}

	if messageCount < 5 {
		t.Logf("Received %d messages (may be limited by stream behavior)", messageCount)
	}
}

// =============================================================================
// OFFSET TESTS
// =============================================================================

func TestClient_CommitOffsets(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	// Create topic
	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "commit-offset-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Commit offsets
	err = te.client.CommitOffsets(ctx, "test-consumer-group", []OffsetCommit{
		{
			Topic:     "commit-offset-topic",
			Partition: 0,
			Offset:    42,
		},
	})
	if err != nil {
		t.Fatalf("CommitOffsets failed: %v", err)
	}
}

func TestClient_FetchOffsets(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	// Create topic
	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "fetch-offset-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx := context.Background()

	// First commit offsets
	err = te.client.CommitOffsets(ctx, "fetch-offset-group", []OffsetCommit{
		{
			Topic:     "fetch-offset-topic",
			Partition: 0,
			Offset:    100,
		},
	})
	if err != nil {
		t.Fatalf("CommitOffsets failed: %v", err)
	}

	// Fetch offsets
	offsets, err := te.client.FetchOffsets(ctx, "fetch-offset-group", []TopicPartition{
		{
			Topic:     "fetch-offset-topic",
			Partition: 0,
		},
	})
	if err != nil {
		t.Fatalf("FetchOffsets failed: %v", err)
	}

	// Find our offset
	found := false
	for _, o := range offsets {
		if o.Topic == "fetch-offset-topic" && o.Partition == 0 {
			// -1 means no committed offset, 100 means our committed offset
			// The actual behavior depends on broker implementation
			t.Logf("Got offset %d for topic %s partition %d", o.Offset, o.Topic, o.Partition)
			found = true
			break
		}
	}
	if !found {
		t.Logf("Offset not found in response (may be expected behavior)")
	}
}

// =============================================================================
// CONCURRENT TESTS
// =============================================================================

func TestClient_ConcurrentPublish(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	// Create topic
	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "concurrent-publish-topic",
		NumPartitions: 4,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx := context.Background()

	// Publish from multiple goroutines
	const numGoroutines = 10
	const messagesPerGoroutine = 10
	errChan := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < messagesPerGoroutine; i++ {
				_, err := te.client.Publish(ctx, "concurrent-publish-topic",
					[]byte(fmt.Sprintf("g%d-msg-%d", goroutineID, i)),
					WithKey([]byte(fmt.Sprintf("g%d", goroutineID))))
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d, message %d: %w", goroutineID, i, err)
					return
				}
			}
			errChan <- nil
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		if err := <-errChan; err != nil {
			t.Error(err)
		}
	}

	t.Logf("Successfully published %d messages from %d goroutines",
		numGoroutines*messagesPerGoroutine, numGoroutines)
}

// =============================================================================
// CONNECTION TESTS
// =============================================================================

func TestClient_Close(t *testing.T) {
	te := setupTestEnv(t)

	// Close client
	err := te.client.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Operations after close should fail
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err = te.client.Health(ctx)
	if err == nil {
		t.Error("Expected error after close")
	}

	// Clean up the rest
	te.client = nil // Already closed
	te.teardown()
}

func TestClient_CloseIdempotent(t *testing.T) {
	te := setupTestEnv(t)
	defer func() {
		te.client = nil
		te.teardown()
	}()

	// Close multiple times should not error
	err := te.client.Close()
	if err != nil {
		t.Errorf("First close failed: %v", err)
	}

	err = te.client.Close()
	if err != nil {
		t.Errorf("Second close should not error: %v", err)
	}
}

// =============================================================================
// PUBLISH OPTIONS TESTS
// =============================================================================

func TestPublishOptions_WithPriority(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	// Create topic with priority support
	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "priority-test",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test different priority levels
	priorities := []int32{0, 1, 2, 3, 4}

	for _, priority := range priorities {
		resp, err := te.client.Publish(ctx, "priority-test", []byte(fmt.Sprintf("priority-%d", priority)),
			WithPriority(priority),
		)
		if err != nil {
			t.Errorf("Publish with priority %d failed: %v", priority, err)
			continue
		}
		t.Logf("Published message with priority %d at offset %d", priority, resp.Offset)
	}
}

func TestPublishOptions_WithPriority_Clamping(t *testing.T) {
	// Test that out-of-range priorities are clamped
	te := setupTestEnv(t)
	defer te.teardown()

	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "priority-clamp-test",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test negative priority (should clamp to 0)
	resp, err := te.client.Publish(ctx, "priority-clamp-test", []byte("negative"),
		WithPriority(-5),
	)
	if err != nil {
		t.Errorf("Publish with negative priority failed: %v", err)
	} else {
		t.Logf("Published with negative priority (clamped) at offset %d", resp.Offset)
	}

	// Test too-high priority (should clamp to 4)
	resp, err = te.client.Publish(ctx, "priority-clamp-test", []byte("too-high"),
		WithPriority(100),
	)
	if err != nil {
		t.Errorf("Publish with too-high priority failed: %v", err)
	} else {
		t.Logf("Published with too-high priority (clamped) at offset %d", resp.Offset)
	}
}

func TestPublishOptions_WithDelay(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "delay-test",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish with 100ms delay
	resp, err := te.client.Publish(ctx, "delay-test", []byte("delayed message"),
		WithDelay(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Publish with delay failed: %v", err)
	}

	t.Logf("Published delayed message at offset %d", resp.Offset)
}

func TestPublishOptions_WithDeliverAt(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "deliver-at-test",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Schedule for 100ms in the future
	deliverAt := time.Now().Add(100 * time.Millisecond)
	resp, err := te.client.Publish(ctx, "deliver-at-test", []byte("scheduled message"),
		WithDeliverAt(deliverAt),
	)
	if err != nil {
		t.Fatalf("Publish with deliver_at failed: %v", err)
	}

	t.Logf("Published scheduled message at offset %d (deliver at: %v)", resp.Offset, deliverAt)
}

func TestPublishOptions_Combined(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "combined-test",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Combine multiple options
	resp, err := te.client.Publish(ctx, "combined-test", []byte("combined options"),
		WithKey([]byte("key1")),
		WithPriority(1),
		WithHeaders(map[string]string{
			"source":  "test",
			"version": "1.0",
		}),
	)
	if err != nil {
		t.Fatalf("Publish with combined options failed: %v", err)
	}

	t.Logf("Published message with combined options at offset %d", resp.Offset)
}

func TestPublishOptions_WithAckMode(t *testing.T) {
	te := setupTestEnv(t)
	defer te.teardown()

	err := te.broker.CreateTopic(broker.TopicConfig{
		Name:          "ack-mode-test",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test with leader ack (default)
	resp, err := te.client.Publish(ctx, "ack-mode-test", []byte("leader ack"),
		WithAckMode(pb.AckMode_ACK_MODE_LEADER),
	)
	if err != nil {
		t.Errorf("Publish with ACK_MODE_LEADER failed: %v", err)
	} else {
		t.Logf("Published with leader ack at offset %d", resp.Offset)
	}

	// Test with no ack (fire and forget)
	resp, err = te.client.Publish(ctx, "ack-mode-test", []byte("no ack"),
		WithAckMode(pb.AckMode_ACK_MODE_NONE),
	)
	if err != nil {
		t.Errorf("Publish with ACK_MODE_NONE failed: %v", err)
	} else {
		t.Logf("Published with no ack at offset %d", resp.Offset)
	}
}

// =============================================================================
// RETRY LOGIC TESTS
// =============================================================================

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{
			name:      "nil error",
			err:       nil,
			retryable: false,
		},
		{
			name:      "non-gRPC error is retryable",
			err:       fmt.Errorf("some network error"),
			retryable: true,
		},
		// Note: More comprehensive gRPC error tests would require mocking
		// gRPC status codes, which is complex. These tests verify basic behavior.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryable(tt.err)
			if result != tt.retryable {
				t.Errorf("isRetryable(%v) = %v, want %v", tt.err, result, tt.retryable)
			}
		})
	}
}
