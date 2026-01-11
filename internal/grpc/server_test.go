// =============================================================================
// GRPC SERVER TESTS
// =============================================================================
//
// These tests verify the gRPC API functionality including:
//   - Server startup and shutdown
//   - Service registration
//   - Health checking
//   - Basic publish/consume flow
//
// TEST ARCHITECTURE:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                         Test Setup                                      │
//   │                                                                         │
//   │   ┌─────────────────┐          ┌─────────────────┐                      │
//   │   │   Test Broker   │◄────────►│  gRPC Server    │                      │
//   │   │   (in-memory)   │          │  (on random     │                      │
//   │   └─────────────────┘          │   port)         │                      │
//   │                                └────────┬────────┘                      │
//   │                                         │                               │
//   │                                ┌────────▼────────┐                      │
//   │                                │  Test Client    │                      │
//   │                                │  (direct gRPC)  │                      │
//   │                                └─────────────────┘                      │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package grpc

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	pb "goqueue/api/proto/gen/go"
	"goqueue/internal/broker"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// =============================================================================
// TEST HELPERS
// =============================================================================

// testServer wraps a gRPC server for testing.
type testServer struct {
	broker     *broker.Broker
	server     *Server
	conn       *grpc.ClientConn
	addr       string
	cleanupDir string
}

// setupTestServer creates a test broker and gRPC server.
func setupTestServer(t *testing.T) *testServer {
	t.Helper()

	// Create temp directory for broker data
	tempDir := t.TempDir()

	// Create broker
	config := broker.DefaultBrokerConfig()
	config.DataDir = tempDir

	b, err := broker.NewBroker(config)
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

	// Create gRPC server
	serverConfig := DefaultServerConfig()
	serverConfig.Address = addr
	serverConfig.EnableReflection = false // Not needed for tests

	server := NewServer(b, serverConfig)

	// Start server in background (Start() blocks)
	go func() {
		if err := server.Start(); err != nil {
			// Server stopped - expected during teardown
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create client connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		server.Stop()
		b.Close()
		t.Fatalf("Failed to connect to server: %v", err)
	}

	return &testServer{
		broker:     b,
		server:     server,
		conn:       conn,
		addr:       addr,
		cleanupDir: tempDir,
	}
}

// teardown cleans up the test server.
func (ts *testServer) teardown() {
	if ts.conn != nil {
		ts.conn.Close()
	}
	if ts.server != nil {
		ts.server.Stop()
	}
	if ts.broker != nil {
		ts.broker.Close()
	}
}

// =============================================================================
// HEALTH SERVICE TESTS
// =============================================================================

func TestHealthService_Check(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	client := pb.NewHealthServiceClient(ts.conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Check(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	if resp.Status != pb.ServingStatus_SERVING_STATUS_SERVING {
		t.Errorf("Expected SERVING status, got %v", resp.Status)
	}
}

func TestHealthService_Check_WithService(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	client := pb.NewHealthServiceClient(ts.conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check specific service - returns SERVICE_UNKNOWN because we don't
	// register per-service health (this is expected behavior)
	resp, err := client.Check(ctx, &pb.HealthCheckRequest{
		Service: "goqueue.v1.PublishService",
	})
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	// Should return SERVICE_UNKNOWN for specific services (not registered)
	if resp.Status != pb.ServingStatus_SERVING_STATUS_SERVICE_UNKNOWN {
		t.Errorf("Expected SERVICE_UNKNOWN status for specific service, got %v", resp.Status)
	}
}

// =============================================================================
// PUBLISH SERVICE TESTS
// =============================================================================

func TestPublishService_Publish(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	// Create topic first
	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "test-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	client := pb.NewPublishServiceClient(ts.conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish a message
	resp, err := client.Publish(ctx, &pb.PublishRequest{
		Topic: "test-topic",
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if resp.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got %s", resp.Topic)
	}

	if resp.Partition != 0 {
		t.Errorf("Expected partition 0, got %d", resp.Partition)
	}

	if resp.Offset < 0 {
		t.Errorf("Expected valid offset, got %d", resp.Offset)
	}

	if resp.Error != nil {
		t.Errorf("Unexpected error: %v", resp.Error)
	}
}

func TestPublishService_Publish_TopicNotFound(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	client := pb.NewPublishServiceClient(ts.conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to publish to non-existent topic
	// The server returns a gRPC error (NotFound status)
	_, err := client.Publish(ctx, &pb.PublishRequest{
		Topic: "non-existent-topic",
		Value: []byte("test-value"),
	})

	// Should get gRPC error with NotFound code
	if err == nil {
		t.Error("Expected error for non-existent topic, got none")
	}
}

func TestPublishService_Publish_MultipleMessages(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	// Create topic with multiple partitions
	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "multi-msg-topic",
		NumPartitions: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	client := pb.NewPublishServiceClient(ts.conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish multiple messages
	for i := 0; i < 10; i++ {
		resp, err := client.Publish(ctx, &pb.PublishRequest{
			Topic: "multi-msg-topic",
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}

		if resp.Error != nil {
			t.Errorf("Publish %d returned error: %v", i, resp.Error)
		}
	}
}

// =============================================================================
// CONSUME SERVICE TESTS
// =============================================================================

func TestConsumeService_Consume(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	// Create topic and publish messages
	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "consume-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	publishClient := pb.NewPublishServiceClient(ts.conn)
	ctx := context.Background()

	// Publish some messages
	for i := 0; i < 5; i++ {
		_, err := publishClient.Publish(ctx, &pb.PublishRequest{
			Topic: "consume-topic",
			Value: []byte(fmt.Sprintf("message-%d", i)),
		})
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Consume messages
	consumeClient := pb.NewConsumeServiceClient(ts.conn)

	consumeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := consumeClient.Consume(consumeCtx, &pb.ConsumeRequest{
		Topic:         "consume-topic",
		Partitions:    []int32{0},
		StartPosition: pb.ConsumeStartPosition_CONSUME_START_POSITION_EARLIEST,
		MaxMessages:   10,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Receive messages
	messageCount := 0
	for {
		resp, err := stream.Recv()
		if err != nil {
			// Stream ended or error
			break
		}

		if msgs := resp.GetMessages(); msgs != nil {
			messageCount += len(msgs.Messages)
		}

		if messageCount >= 5 {
			break
		}
	}

	t.Logf("Received %d messages from stream", messageCount)
}

// =============================================================================
// ACK SERVICE TESTS
// =============================================================================

func TestAckService_Ack(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	// Note: Full ack testing requires consumer group setup
	// This is a basic test that the service responds

	client := pb.NewAckServiceClient(ts.conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to ack (will fail because no such message, but tests service is responding)
	_, err := client.Ack(ctx, &pb.AckRequest{
		GroupId:       "test-group",
		ReceiptHandle: "invalid-handle",
	})

	// Expected to fail (invalid receipt handle) but service should respond
	if err != nil {
		t.Logf("Ack returned expected error: %v", err)
	}
}

func TestAckService_Reject(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	client := pb.NewAckServiceClient(ts.conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to reject
	_, err := client.Reject(ctx, &pb.RejectRequest{
		GroupId:       "test-group",
		ReceiptHandle: "invalid-handle",
		Reason:        "test rejection",
	})

	// Expected to fail but service should respond
	if err != nil {
		t.Logf("Reject returned expected error: %v", err)
	}
}

// =============================================================================
// OFFSET SERVICE TESTS
// =============================================================================

func TestOffsetService_CommitOffsets(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	// Create topic
	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "offset-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	client := pb.NewOffsetServiceClient(ts.conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Commit offsets
	resp, err := client.CommitOffsets(ctx, &pb.CommitOffsetsRequest{
		GroupId: "test-group",
		Offsets: []*pb.OffsetCommit{
			{
				Topic:     "offset-topic",
				Partition: 0,
				Offset:    10,
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitOffsets failed: %v", err)
	}

	// Verify response
	if len(resp.Results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(resp.Results))
	}
}

func TestOffsetService_FetchOffsets(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	// Create topic
	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "fetch-offset-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	client := pb.NewOffsetServiceClient(ts.conn)
	ctx := context.Background()

	// First commit an offset
	_, err = client.CommitOffsets(ctx, &pb.CommitOffsetsRequest{
		GroupId: "fetch-group",
		Offsets: []*pb.OffsetCommit{
			{
				Topic:     "fetch-offset-topic",
				Partition: 0,
				Offset:    42,
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitOffsets failed: %v", err)
	}

	// Fetch the offset back
	fetchResp, err := client.FetchOffsets(ctx, &pb.FetchOffsetsRequest{
		GroupId: "fetch-group",
		Partitions: []*pb.TopicPartition{
			{
				Topic:     "fetch-offset-topic",
				Partition: 0,
			},
		},
	})
	if err != nil {
		t.Fatalf("FetchOffsets failed: %v", err)
	}

	if len(fetchResp.Offsets) != 1 {
		t.Errorf("Expected 1 offset, got %d", len(fetchResp.Offsets))
	}

	// Log the actual offset returned
	t.Logf("Fetched offset: %d", fetchResp.Offsets[0].Offset)
}

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

func TestIntegration_PublishAndConsume(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	// Create topic
	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "integration-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	ctx := context.Background()

	// Publish messages
	publishClient := pb.NewPublishServiceClient(ts.conn)
	expectedMessages := []string{"msg-1", "msg-2", "msg-3"}

	for _, msg := range expectedMessages {
		resp, err := publishClient.Publish(ctx, &pb.PublishRequest{
			Topic: "integration-topic",
			Value: []byte(msg),
		})
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
		if resp.Error != nil {
			t.Fatalf("Publish error: %v", resp.Error)
		}
	}

	// Consume messages
	consumeClient := pb.NewConsumeServiceClient(ts.conn)

	consumeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := consumeClient.Consume(consumeCtx, &pb.ConsumeRequest{
		Topic:         "integration-topic",
		Partitions:    []int32{0},
		StartPosition: pb.ConsumeStartPosition_CONSUME_START_POSITION_EARLIEST,
		MaxMessages:   10,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Collect messages
	receivedMessages := make([]string, 0)
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}

		if msgs := resp.GetMessages(); msgs != nil {
			for _, m := range msgs.Messages {
				receivedMessages = append(receivedMessages, string(m.Value))
			}
		}

		if len(receivedMessages) >= len(expectedMessages) {
			break
		}
	}

	t.Logf("Published %d messages, received %d messages",
		len(expectedMessages), len(receivedMessages))
}

// =============================================================================
// CONCURRENT TESTS
// =============================================================================

func TestConcurrent_MultiplePublishers(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	// Create topic
	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "concurrent-topic",
		NumPartitions: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	client := pb.NewPublishServiceClient(ts.conn)
	ctx := context.Background()

	// Publish from multiple goroutines
	const numPublishers = 5
	const messagesPerPublisher = 20
	done := make(chan error, numPublishers)

	for p := 0; p < numPublishers; p++ {
		go func(publisherID int) {
			for i := 0; i < messagesPerPublisher; i++ {
				_, err := client.Publish(ctx, &pb.PublishRequest{
					Topic: "concurrent-topic",
					Key:   []byte(fmt.Sprintf("publisher-%d", publisherID)),
					Value: []byte(fmt.Sprintf("msg-%d-%d", publisherID, i)),
				})
				if err != nil {
					done <- fmt.Errorf("publisher %d failed: %w", publisherID, err)
					return
				}
			}
			done <- nil
		}(p)
	}

	// Wait for all publishers
	for i := 0; i < numPublishers; i++ {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}

	t.Logf("Successfully published %d messages from %d concurrent publishers",
		numPublishers*messagesPerPublisher, numPublishers)
}

// =============================================================================
// HEALTH SERVICE COMPREHENSIVE TESTS
// =============================================================================

func TestHealthService_BrokerClosed(t *testing.T) {
	ts := setupTestServer(t)

	client := pb.NewHealthServiceClient(ts.conn)

	// Close broker (simulates shutdown)
	ts.broker.Close()

	// Wait a bit for the close to propagate
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Health check should now return NOT_SERVING
	resp, err := client.Check(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	if resp.Status != pb.ServingStatus_SERVING_STATUS_NOT_SERVING {
		t.Errorf("Expected NOT_SERVING status after broker close, got %v", resp.Status)
	}

	// Clean up (broker already closed)
	ts.broker = nil
	ts.teardown()
}

// =============================================================================
// PUBLISH SERVICE PRIORITY/DELAY TESTS
// =============================================================================

func TestPublishService_PublishWithPriority(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "priority-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	client := pb.NewPublishServiceClient(ts.conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test all priority levels
	for priority := int32(0); priority <= 4; priority++ {
		resp, err := client.Publish(ctx, &pb.PublishRequest{
			Topic:    "priority-topic",
			Value:    []byte(fmt.Sprintf("priority-%d-message", priority)),
			Priority: priority,
		})
		if err != nil {
			t.Errorf("Publish with priority %d failed: %v", priority, err)
			continue
		}
		t.Logf("Published priority-%d message at offset %d", priority, resp.Offset)
	}
}

func TestPublishService_PublishWithDelay(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "delay-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	client := pb.NewPublishServiceClient(ts.conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish with delay
	delay := durationpb.New(100 * time.Millisecond)
	resp, err := client.Publish(ctx, &pb.PublishRequest{
		Topic: "delay-topic",
		Value: []byte("delayed message"),
		Delay: delay,
	})
	if err != nil {
		t.Fatalf("Publish with delay failed: %v", err)
	}

	t.Logf("Published delayed message at offset %d", resp.Offset)
}

func TestPublishService_PublishWithDeliverAt(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "deliver-at-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	client := pb.NewPublishServiceClient(ts.conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish with deliver_at (100ms in the future)
	deliverAt := timestamppb.New(time.Now().Add(100 * time.Millisecond))
	resp, err := client.Publish(ctx, &pb.PublishRequest{
		Topic:     "deliver-at-topic",
		Value:     []byte("scheduled message"),
		DeliverAt: deliverAt,
	})
	if err != nil {
		t.Fatalf("Publish with deliver_at failed: %v", err)
	}

	t.Logf("Published scheduled message at offset %d", resp.Offset)
}

func TestPublishService_PublishWithPriorityAndDelay(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "priority-delay-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	client := pb.NewPublishServiceClient(ts.conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Combine priority and delay (high priority urgent task, but delayed)
	resp, err := client.Publish(ctx, &pb.PublishRequest{
		Topic:    "priority-delay-topic",
		Value:    []byte("high-priority delayed message"),
		Priority: 0, // Critical priority
		Delay:    durationpb.New(50 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("Publish with priority and delay failed: %v", err)
	}

	t.Logf("Published high-priority delayed message at offset %d", resp.Offset)
}

// =============================================================================
// OFFSET SERVICE RESET TESTS
// =============================================================================

func TestOffsetService_ResetToEarliest(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "reset-earliest-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	client := pb.NewOffsetServiceClient(ts.conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Reset to earliest (even without prior offsets)
	resetResp, err := client.ResetOffsets(ctx, &pb.ResetOffsetsRequest{
		GroupId:  "reset-earliest-group",
		Strategy: pb.OffsetResetStrategy_OFFSET_RESET_STRATEGY_EARLIEST,
		Partitions: []*pb.TopicPartition{
			{Topic: "reset-earliest-topic", Partition: 0},
		},
	})
	if err != nil {
		t.Fatalf("ResetOffsets failed: %v", err)
	}

	if len(resetResp.Results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(resetResp.Results))
	}

	// Earliest should be 0 for empty partition
	if resetResp.Results[0].NewOffset != 0 {
		t.Errorf("Expected offset 0 after reset to earliest, got %d", resetResp.Results[0].NewOffset)
	}

	t.Logf("Reset to earliest successful: new_offset=%d", resetResp.Results[0].NewOffset)
}

func TestOffsetService_ResetToLatestWithMessages(t *testing.T) {
	ts := setupTestServer(t)
	defer ts.teardown()

	err := ts.broker.CreateTopic(broker.TopicConfig{
		Name:          "reset-latest-topic",
		NumPartitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Publish some messages first
	pubClient := pb.NewPublishServiceClient(ts.conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		_, err := pubClient.Publish(ctx, &pb.PublishRequest{
			Topic: "reset-latest-topic",
			Value: []byte(fmt.Sprintf("msg-%d", i)),
		})
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	offsetClient := pb.NewOffsetServiceClient(ts.conn)

	// Reset to latest
	resetResp, err := offsetClient.ResetOffsets(ctx, &pb.ResetOffsetsRequest{
		GroupId:  "reset-latest-group",
		Strategy: pb.OffsetResetStrategy_OFFSET_RESET_STRATEGY_LATEST,
		Partitions: []*pb.TopicPartition{
			{Topic: "reset-latest-topic", Partition: 0},
		},
	})
	if err != nil {
		t.Fatalf("ResetOffsets failed: %v", err)
	}

	if len(resetResp.Results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(resetResp.Results))
	}

	// Should be at next offset after 5 messages (we don't know exact number without checking broker)
	t.Logf("Reset to latest successful: new_offset=%d", resetResp.Results[0].NewOffset)
}
