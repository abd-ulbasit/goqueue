// =============================================================================
// ACK SERVICE - MESSAGE ACKNOWLEDGMENT
// =============================================================================
//
// WHAT IS THIS?
// The ack service handles message acknowledgment after consumption:
//   - Ack: "I processed this successfully"
//   - Nack: "I failed, retry later"
//   - Reject: "This message is bad, send to DLQ"
//   - ExtendVisibility: "I need more time"
//
// WHY SEPARATE FROM CONSUME?
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Design Option           │ Pros                │ Cons                    │
//   ├─────────────────────────┼─────────────────────┼─────────────────────────┤
//   │ A: ACKs on consume      │ Single stream       │ Complex protocol        │
//   │    stream               │ Lower latency       │ Harder to debug         │
//   │                         │                     │ Mixed concerns          │
//   │                         │                     │                         │
//   │ B: Separate ACK         │ Clear separation    │ Extra RPC call          │
//   │    service (chosen)     │ Easier debugging    │ Slightly higher latency │
//   │                         │ Batch ACKs          │                         │
//   │                         │ Simpler consume     │                         │
//   └─────────────────────────┴─────────────────────┴─────────────────────────┘
//
// We chose Option B because:
//   1. ACKs are less frequent than messages (process 100, ACK once)
//   2. Clearer separation of concerns
//   3. Easier to add batch ACK optimization
//   4. Matches SQS and RabbitMQ patterns
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   - Kafka:
//     - No per-message ACK (offset commit only)
//     - "Auto-commit" or manual commitSync/commitAsync
//     - Message loss possible between commit and crash
//
//   - SQS:
//     - DeleteMessage (ACK)
//     - ChangeMessageVisibility (extend)
//     - No explicit NACK (just let visibility timeout expire)
//     - DLQ after maxReceiveCount
//
//   - RabbitMQ:
//     - basic.ack, basic.nack, basic.reject
//     - requeue option on nack/reject
//     - Publisher confirms for producers
//
//   - goqueue:
//     - Per-message ACK/NACK/REJECT
//     - Receipt handles (like SQS)
//     - Visibility timeout
//     - DLQ with reason tracking
//
// =============================================================================

package grpc

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"goqueue/internal/broker"
)

// =============================================================================
// ACK SERVICE SERVER
// =============================================================================

// ackServiceServer implements the AckService gRPC interface.
type ackServiceServer struct {
	UnimplementedAckServiceServer

	broker *broker.Broker
	logger *slog.Logger
}

// NewAckServiceServer creates a new ack service server.
func NewAckServiceServer(b *broker.Broker, logger *slog.Logger) AckServiceServer {
	return &ackServiceServer{
		broker: b,
		logger: logger,
	}
}

// =============================================================================
// ACK - SUCCESSFUL PROCESSING
// =============================================================================
//
// Ack indicates the consumer successfully processed the message.
// The message is permanently removed from the queue.
//
// FLOW:
//   1. Consumer receives message with receipt_handle
//   2. Consumer processes message
//   3. Consumer calls Ack(receipt_handle)
//   4. Broker marks message as complete
//   5. Message is not redelivered
//
// RECEIPT HANDLE:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ What is a receipt handle?                                               │
//   │                                                                         │
//   │ A receipt handle is a unique, opaque token that identifies a specific   │
//   │ delivery of a message. It's NOT the same as offset because:             │
//   │                                                                         │
//   │ - Same message can have different receipt handles on redelivery         │
//   │ - Receipt handle encodes: topic, partition, offset, consumer, time      │
//   │ - Receipt handle expires with visibility timeout                        │
//   │                                                                         │
//   │ FORMAT (internal):                                                      │
//   │   {topic}:{partition}:{offset}:{consumer_id}:{timestamp}                │
//   │                                                                         │
//   │ WHY NOT JUST OFFSET?                                                    │
//   │ - Security: Can't ACK arbitrary messages                                │
//   │ - Tracking: Know which consumer is ACKing                               │
//   │ - Expiration: Can invalidate old handles                                │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

func (s *ackServiceServer) Ack(
	ctx context.Context,
	req *AckRequest,
) (*AckResponse, error) {
	// Validation
	if req.ReceiptHandle == "" && (req.Topic == "" || req.Partition < 0) {
		return nil, status.Error(codes.InvalidArgument,
			"either receipt_handle or topic/partition/offset is required")
	}

	// Get the ack manager
	ackManager := s.broker.GetAckManager()
	if ackManager == nil {
		// Fall back to offset-based ACK if no ack manager
		return s.ackByOffset(ctx, req)
	}

	// ACK by receipt handle
	if req.ReceiptHandle != "" {
		result, err := ackManager.Ack(req.ReceiptHandle)
		if err != nil {
			return &AckResponse{
				Success: false,
				Error:   brokerErrorToProto(err),
			}, nil
		}

		_ = result // Could return more info from result
		return &AckResponse{Success: true}, nil
	}

	// ACK by offset (legacy path)
	return s.ackByOffset(ctx, req)
}

// ackByOffset handles ACK when receipt handle is not available.
func (s *ackServiceServer) ackByOffset(
	ctx context.Context,
	req *AckRequest,
) (*AckResponse, error) {
	// This is a simplified path for when we don't have receipt handles
	// Just mark the offset as committed

	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id required for offset-based ack")
	}

	coordinator := s.broker.GetGroupCoordinator()
	if coordinator == nil {
		return nil, status.Error(codes.Internal, "group coordinator not available")
	}

	// Commit the offset
	// Note: memberID is empty string since we don't have consumer ID in AckRequest
	// The coordinator will accept this for basic offset commits
	err := coordinator.CommitOffset(req.GroupId, req.Topic, int(req.Partition), req.Offset+1, "")
	if err != nil {
		return &AckResponse{
			Success: false,
			Error:   brokerErrorToProto(err),
		}, nil
	}

	return &AckResponse{Success: true}, nil
}

// =============================================================================
// NACK - RETRY LATER
// =============================================================================
//
// Nack indicates the consumer failed to process the message but wants to retry.
// The message is made visible again after a delay.
//
// USE CASES:
//   - Transient error (database timeout, network glitch)
//   - Dependency unavailable (downstream service down)
//   - Rate limiting (too many requests)
//
// FLOW:
//   1. Consumer receives message
//   2. Consumer fails to process (transient error)
//   3. Consumer calls Nack(receipt_handle, visibility_timeout)
//   4. Message becomes invisible for visibility_timeout
//   5. After timeout, message is redelivered
//   6. Retry count is incremented
//   7. If retry_count > max_retries, message goes to DLQ
//
// COMPARISON:
//   - RabbitMQ basic.nack: requeue=true puts message at head of queue
//   - SQS: No explicit NACK, let visibility timeout expire
//   - goqueue: Explicit NACK with custom delay
//
// =============================================================================

func (s *ackServiceServer) Nack(
	ctx context.Context,
	req *NackRequest,
) (*NackResponse, error) {
	// Validation
	if req.ReceiptHandle == "" && (req.Topic == "" || req.Partition < 0) {
		return nil, status.Error(codes.InvalidArgument,
			"either receipt_handle or topic/partition/offset is required")
	}

	// Get visibility timeout
	visibilityTimeout := 30 * time.Second // Default
	if req.VisibilityTimeout != nil {
		visibilityTimeout = req.VisibilityTimeout.AsDuration()
	}

	// Get the ack manager
	ackManager := s.broker.GetAckManager()
	if ackManager == nil {
		return nil, status.Error(codes.Internal, "ack manager not available")
	}

	// NACK by receipt handle
	if req.ReceiptHandle != "" {
		result, err := ackManager.Nack(req.ReceiptHandle, "gRPC NACK")
		if err != nil {
			return &NackResponse{
				Success: false,
				Error:   brokerErrorToProto(err),
			}, nil
		}

		// Calculate when message will be visible again
		nextVisible := time.Now().Add(visibilityTimeout)

		_ = result // Could use result for more info
		return &NackResponse{
			Success:       true,
			NextVisibleAt: timestamppb.New(nextVisible),
		}, nil
	}

	// NACK without receipt handle not supported
	return nil, status.Error(codes.InvalidArgument, "receipt_handle required for NACK")
}

// =============================================================================
// REJECT - SEND TO DLQ
// =============================================================================
//
// Reject indicates the message is "poison" and should not be retried.
// The message is moved to the Dead Letter Queue (DLQ).
//
// USE CASES:
//   - Invalid message format (can't parse)
//   - Business logic rejection (order already canceled)
//   - Schema mismatch (unexpected fields)
//   - Permanent downstream error (record not found)
//
// DLQ TOPIC:
//   Named: {original_topic}.dlq
//   Example: orders → orders.dlq
//
// DLQ MESSAGE HEADERS:
//   - x-original-topic: Original topic name
//   - x-original-partition: Original partition
//   - x-original-offset: Original offset
//   - x-reject-reason: Why message was rejected
//   - x-rejected-at: Timestamp of rejection
//   - x-retry-count: How many times message was retried
//
// =============================================================================

func (s *ackServiceServer) Reject(
	ctx context.Context,
	req *RejectRequest,
) (*RejectResponse, error) {
	// Validation
	if req.ReceiptHandle == "" && (req.Topic == "" || req.Partition < 0) {
		return nil, status.Error(codes.InvalidArgument,
			"either receipt_handle or topic/partition/offset is required")
	}

	// Get the ack manager
	ackManager := s.broker.GetAckManager()
	if ackManager == nil {
		return nil, status.Error(codes.Internal, "ack manager not available")
	}

	// Reject by receipt handle
	if req.ReceiptHandle != "" {
		reason := req.Reason
		if reason == "" {
			reason = "rejected via gRPC"
		}

		result, err := ackManager.Reject(req.ReceiptHandle, reason)
		if err != nil {
			return &RejectResponse{
				Success: false,
				Error:   brokerErrorToProto(err),
			}, nil
		}

		// Return DLQ info if available
		response := &RejectResponse{Success: true}
		if result != nil && result.DLQTopic != "" {
			response.DlqTopic = result.DLQTopic
			// Partition and offset are not returned by ack manager
			// since DLQ publishing is async
		}

		return response, nil
	}

	return nil, status.Error(codes.InvalidArgument, "receipt_handle required for reject")
}

// =============================================================================
// EXTEND VISIBILITY - REQUEST MORE TIME
// =============================================================================
//
// ExtendVisibility gives the consumer more time to process a message.
// Useful for long-running tasks.
//
// USE CASES:
//   - Video transcoding (takes 5+ minutes)
//   - Large file processing
//   - Complex calculations
//   - Waiting for external callback
//
// FLOW:
//   1. Consumer receives message (30s visibility timeout)
//   2. Consumer starts processing
//   3. After 20s, consumer realizes it needs more time
//   4. Consumer calls ExtendVisibility(receipt_handle, 60s)
//   5. Visibility timeout extended to 60s from now
//   6. Consumer finishes, calls Ack
//
// WITHOUT EXTEND:
//   - Consumer takes 45s to process
//   - After 30s, message becomes visible again
//   - Another consumer picks it up → duplicate processing!
//
// COMPARISON:
//   - SQS: ChangeMessageVisibility
//   - RabbitMQ: No equivalent (use longer timeout or manual requeueing)
//   - Kafka: No visibility timeout concept
//
// =============================================================================

func (s *ackServiceServer) ExtendVisibility(
	ctx context.Context,
	req *ExtendVisibilityRequest,
) (*ExtendVisibilityResponse, error) {
	// Validation
	if req.ReceiptHandle == "" {
		return nil, status.Error(codes.InvalidArgument, "receipt_handle is required")
	}

	if req.VisibilityTimeout == nil {
		return nil, status.Error(codes.InvalidArgument, "visibility_timeout is required")
	}

	extension := req.VisibilityTimeout.AsDuration()
	if extension <= 0 {
		return nil, status.Error(codes.InvalidArgument, "visibility_timeout must be positive")
	}

	// Get the ack manager
	ackManager := s.broker.GetAckManager()
	if ackManager == nil {
		return nil, status.Error(codes.Internal, "ack manager not available")
	}

	// Extend visibility
	newDeadline, err := ackManager.ExtendVisibility(req.ReceiptHandle, extension)
	if err != nil {
		return &ExtendVisibilityResponse{
			Success: false,
			Error:   brokerErrorToProto(err),
		}, nil
	}

	return &ExtendVisibilityResponse{
		Success:     true,
		NewDeadline: timestamppb.New(newDeadline),
	}, nil
}

// =============================================================================
// BATCH ACK - ACKNOWLEDGE MULTIPLE MESSAGES
// =============================================================================
//
// BatchAck acknowledges multiple messages in a single RPC call.
// Much more efficient than individual ACKs.
//
// EFFICIENCY:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Individual ACKs (100 messages):                                         │
//   │   100 RPC calls × ~1ms = 100ms total                                    │
//   │   100 context switches                                                  │
//   │   100 log entries                                                       │
//   │                                                                         │
//   │ Batch ACK (100 messages):                                               │
//   │   1 RPC call × ~5ms = 5ms total                                         │
//   │   1 context switch                                                      │
//   │   1 log entry (batched)                                                 │
//   │                                                                         │
//   │ Result: 20x faster!                                                     │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// PARTIAL FAILURE:
//   BatchAck can succeed for some messages and fail for others.
//   The response includes:
//   - success_count: How many succeeded
//   - failures: List of failed ACKs with individual errors
//
// =============================================================================

func (s *ackServiceServer) BatchAck(
	ctx context.Context,
	req *BatchAckRequest,
) (*BatchAckResponse, error) {
	// Validation
	if len(req.ReceiptHandles) == 0 && len(req.Offsets) == 0 {
		return nil, status.Error(codes.InvalidArgument,
			"either receipt_handles or offsets is required")
	}

	response := &BatchAckResponse{
		Failures: make([]*AckError, 0),
	}

	// Get the ack manager
	ackManager := s.broker.GetAckManager()

	// Process receipt handles
	if len(req.ReceiptHandles) > 0 && ackManager != nil {
		for _, handle := range req.ReceiptHandles {
			_, err := ackManager.Ack(handle)
			if err != nil {
				response.Failures = append(response.Failures, &AckError{
					ReceiptHandle: handle,
					Error:         brokerErrorToProto(err),
				})
			} else {
				response.SuccessCount++
			}
		}
	}

	// Process offsets
	if len(req.Offsets) > 0 {
		coordinator := s.broker.GetGroupCoordinator()
		if coordinator == nil && len(req.ReceiptHandles) == 0 {
			return nil, status.Error(codes.Internal, "group coordinator not available")
		}

		if coordinator != nil {
			for _, offsetAck := range req.Offsets {
				err := coordinator.CommitOffset(
					req.GroupId,
					offsetAck.Topic,
					int(offsetAck.Partition),
					offsetAck.Offset+1,
					"", // memberID not available in batch request
				)
				if err != nil {
					response.Failures = append(response.Failures, &AckError{
						Topic:     offsetAck.Topic,
						Partition: offsetAck.Partition,
						Offset:    offsetAck.Offset,
						Error:     brokerErrorToProto(err),
					})
				} else {
					response.SuccessCount++
				}
			}
		}
	}

	return response, nil
}
