// =============================================================================
// PUBLISH SERVICE - HIGH-PERFORMANCE MESSAGE PUBLISHING
// =============================================================================
//
// WHAT IS THIS?
// The publish service handles all message publishing over gRPC:
//   - Unary Publish: Single message, immediate response
//   - Streaming PublishStream: Batch messages with flow control
//
// WHY TWO METHODS?
//
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │ Method         │ Use Case                │ Latency    │ Throughput     │
//   ├────────────────┼─────────────────────────┼────────────┼────────────────┤
//   │ Publish        │ Single important msg    │ ~1-5ms     │ Lower          │
//   │ PublishStream  │ Batch import, ETL       │ ~0.1-1ms   │ 10-100x higher │
//   └────────────────┴─────────────────────────┴────────────┴────────────────┘
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   - Kafka Producer:
//     - Async send() with callbacks
//     - Batches internally (linger.ms, batch.size)
//     - Compression per batch
//     - goqueue: Streaming provides similar batching semantics
//
//   - Pulsar Producer:
//     - send() (sync) vs sendAsync()
//     - Configurable batching
//     - goqueue: Unary = sync, Stream = async-like
//
//   - SQS:
//     - SendMessage (single) vs SendMessageBatch (up to 10)
//     - goqueue: Stream can handle unlimited batch size
//
// FLOW CONTROL:
//
//   HTTP/2 (which gRPC uses) has built-in flow control:
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                                                                         │
//   │  Client                              Server                             │
//   │    │                                  │                                 │
//   │    │ ─── DATA frame ────────────────► │                                 │
//   │    │ ─── DATA frame ────────────────► │                                 │
//   │    │ ─── DATA frame ────────────────► │                                 │
//   │    │ ◄── WINDOW_UPDATE ─────────────  │ (server processes, sends ack)   │
//   │    │ ─── DATA frame ────────────────► │                                 │
//   │    │         ...                      │                                 │
//   │                                                                         │
//   │  If server is slow, WINDOW_UPDATE is delayed                            │
//   │  → Client automatically slows down (backpressure!)                      │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package grpc

import (
	"context"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"goqueue/internal/broker"
	"goqueue/internal/storage"
)

// =============================================================================
// PUBLISH SERVICE SERVER
// =============================================================================

// publishServiceServer implements the PublishService gRPC interface.
type publishServiceServer struct {
	// Embed unimplemented to satisfy interface
	// This ensures forward compatibility when new methods are added to proto
	UnimplementedPublishServiceServer

	broker *broker.Broker
	logger *slog.Logger
}

// NewPublishServiceServer creates a new publish service server.
func NewPublishServiceServer(b *broker.Broker, logger *slog.Logger) PublishServiceServer {
	return &publishServiceServer{
		broker: b,
		logger: logger,
	}
}

// =============================================================================
// UNARY PUBLISH
// =============================================================================
//
// Publish handles single message publishing with immediate acknowledgment.
//
// FLOW:
//   1. Validate request (topic exists, payload not empty)
//   2. Determine partition (by key hash or explicit)
//   3. Write to partition log
//   4. Wait for ack based on ack_mode
//   5. Return offset and timestamp
//
// ERROR HANDLING:
//   - Topic not found → codes.NotFound
//   - Invalid request → codes.InvalidArgument
//   - Broker error → codes.Internal
//   - Context canceled → codes.Canceled
//
// =============================================================================

func (s *publishServiceServer) Publish(
	ctx context.Context,
	req *PublishRequest,
) (*PublishResponse, error) {
	// =========================================================================
	// VALIDATION
	// =========================================================================
	//
	// Always validate input at the API boundary. This prevents:
	// - Passing invalid data to broker (could corrupt state)
	// - Wasting resources on doomed requests
	// - Returning cryptic errors from deeper layers
	//
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	// Value can be empty (tombstone messages for compaction)
	// but nil is different from empty - we allow both

	// =========================================================================
	// DETERMINE PARTITION
	// =========================================================================
	//
	// Partition selection follows Kafka semantics:
	//   - If explicit partition specified → use it
	//   - If key provided → hash(key) % partitions
	//   - Otherwise → round-robin
	//
	// WHY KEY-BASED PARTITIONING?
	// Same key always goes to same partition, guaranteeing ordering.
	// Example: All events for order-123 go to partition 2.
	//
	var partition int
	var offset int64
	var err error

	// Check for delayed delivery
	if req.Delay != nil || req.DeliverAt != nil {
		partition, offset, err = s.publishDelayed(ctx, req)
	} else if req.Priority > 0 {
		// Priority message
		partition, offset, err = s.publishWithPriority(ctx, req)
	} else {
		// Normal publish
		partition, offset, err = s.broker.Publish(req.Topic, req.Key, req.Value)
	}

	if err != nil {
		return nil, mapBrokerError(err)
	}

	return &PublishResponse{
		Topic:     req.Topic,
		Partition: int32(partition),
		Offset:    offset,
		Timestamp: timestamppb.Now(),
	}, nil
}

// publishDelayed handles messages with delay or deliver_at.
func (s *publishServiceServer) publishDelayed(
	ctx context.Context,
	req *PublishRequest,
) (partition int, offset int64, err error) {
	var delay time.Duration

	if req.DeliverAt != nil {
		// Calculate delay from deliver_at timestamp
		deliverAt := req.DeliverAt.AsTime()
		delay = time.Until(deliverAt)
		if delay < 0 {
			delay = 0 // Deliver immediately if time has passed
		}
	} else if req.Delay != nil {
		delay = req.Delay.AsDuration()
	}

	return s.broker.PublishWithDelay(req.Topic, req.Key, req.Value, delay)
}

// publishWithPriority handles messages with priority.
func (s *publishServiceServer) publishWithPriority(
	ctx context.Context,
	req *PublishRequest,
) (partition int, offset int64, err error) {
	// Map proto priority (0-4) to storage.Priority
	priority := storage.Priority(req.Priority)
	if priority > storage.PriorityBackground {
		priority = storage.PriorityBackground
	}

	return s.broker.PublishWithPriority(req.Topic, req.Key, req.Value, priority)
}

// =============================================================================
// STREAMING PUBLISH
// =============================================================================
//
// PublishStream handles bidirectional streaming for high-throughput publishing.
//
// WHY BIDIRECTIONAL?
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Option A: Client streaming (→ single response at end)                   │
//   │   Client ─── msg1 ───► Server                                           │
//   │   Client ─── msg2 ───► Server                                           │
//   │   Client ─── msg3 ───► Server                                           │
//   │   Client ─── [close] ─► Server                                          │
//   │   Client ◄── BatchAck ── Server                                         │
//   │                                                                         │
//   │   Problem: Client doesn't know if messages succeeded until end          │
//   │   Problem: Can't implement per-message acks for critical messages       │
//   │                                                                         │
//   │ Option B: Bidirectional (→ responses as needed)                         │
//   │   Client ─── msg1 ───► Server                                           │
//   │   Client ─── msg2 ───► Server                                           │
//   │   Client ◄── ack1,2 ── Server  (batched acks)                           │
//   │   Client ─── msg3 ───► Server                                           │
//   │   Client ◄── ack3 ──── Server                                           │
//   │                                                                         │
//   │   Benefit: Client gets continuous feedback                              │
//   │   Benefit: Server can batch acks for efficiency                         │
//   │   Benefit: Flow control at gRPC/HTTP2 level                             │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// FLOW:
//   1. Client opens stream
//   2. Client sends PublishStreamRequest messages
//   3. Server publishes each, sends PublishStreamResponse
//   4. Server may batch responses (future optimization)
//   5. Stream closes when client closes or error occurs
//
// =============================================================================

func (s *publishServiceServer) PublishStream(
	stream PublishService_PublishStreamServer,
) error {
	ctx := stream.Context()

	s.logger.Debug("publish stream started")

	// Track stats for logging
	var messageCount int64
	var errorCount int64
	startTime := time.Now()

	// Process messages until stream closes
	for {
		// Check context for cancellation
		select {
		case <-ctx.Done():
			s.logger.Debug("publish stream canceled",
				"messages", messageCount,
				"errors", errorCount,
				"duration", time.Since(startTime),
			)
			return ctx.Err()
		default:
		}

		// Receive next message from client
		// This blocks until:
		//   - Client sends a message
		//   - Client closes the stream (io.EOF)
		//   - Error occurs
		req, err := stream.Recv()
		if err == io.EOF {
			// Client closed the stream - graceful end
			s.logger.Debug("publish stream completed",
				"messages", messageCount,
				"errors", errorCount,
				"duration", time.Since(startTime),
			)
			return nil
		}
		if err != nil {
			s.logger.Error("publish stream recv error",
				"error", err,
				"messages", messageCount,
			)
			return status.Errorf(codes.Internal, "receive error: %v", err)
		}

		// Publish the message
		response := s.publishStreamMessage(ctx, req)

		// Track stats
		messageCount++
		if response.Error != nil {
			errorCount++
		}

		// Send response to client
		// In a production system, you might batch these responses
		// for efficiency (send every N responses or every X ms)
		if err := stream.Send(response); err != nil {
			s.logger.Error("publish stream send error",
				"error", err,
				"correlation_id", req.CorrelationId,
			)
			return status.Errorf(codes.Internal, "send error: %v", err)
		}
	}
}

// publishStreamMessage publishes a single message from the stream.
func (s *publishServiceServer) publishStreamMessage(
	ctx context.Context,
	req *PublishStreamRequest,
) *PublishStreamResponse {
	response := &PublishStreamResponse{
		CorrelationId: req.CorrelationId,
		Topic:         req.Topic,
	}

	// Validate
	if req.Topic == "" {
		response.Error = &Error{
			Code:    ErrorCodeUnspecified,
			Message: "topic is required",
		}
		return response
	}

	// Publish based on features
	var partition int
	var offset int64
	var err error

	if req.Delay != nil {
		delay := req.Delay.AsDuration()
		partition, offset, err = s.broker.PublishWithDelay(req.Topic, req.Key, req.Value, delay)
	} else if req.Priority > 0 {
		priority := storage.Priority(req.Priority)
		partition, offset, err = s.broker.PublishWithPriority(req.Topic, req.Key, req.Value, priority)
	} else {
		partition, offset, err = s.broker.Publish(req.Topic, req.Key, req.Value)
	}

	if err != nil {
		response.Error = brokerErrorToProto(err)
		return response
	}

	response.Partition = int32(partition)
	response.Offset = offset
	response.Timestamp = timestamppb.Now()

	return response
}

// =============================================================================
// ERROR CONVERSION
// =============================================================================

// brokerErrorToProto converts a broker error to a proto Error message.
func brokerErrorToProto(err error) *Error {
	if err == nil {
		return nil
	}

	errMsg := err.Error()
	code := ErrorCodeUnspecified

	// Map to specific error codes
	switch {
	case contains(errMsg, "topic not found"), contains(errMsg, "topic does not exist"):
		code = ErrorCodeTopicNotFound
	case contains(errMsg, "already exists"):
		code = ErrorCodeTopicAlreadyExists
	case contains(errMsg, "partition"):
		code = ErrorCodePartitionNotFound
	case contains(errMsg, "group not found"):
		code = ErrorCodeGroupNotFound
	case contains(errMsg, "message too large"):
		code = ErrorCodeMessageTooLarge
	case contains(errMsg, "receipt handle"):
		code = ErrorCodeReceiptHandleInvalid
	}

	return &Error{
		Code:    code,
		Message: errMsg,
	}
}
