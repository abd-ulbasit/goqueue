// =============================================================================
// ACK MANAGER - PER-MESSAGE ACKNOWLEDGMENT
// =============================================================================
//
// WHAT IS THE ACK MANAGER?
// The ACK Manager provides per-message acknowledgment (ACK/NACK/Reject) on top
// of the Kafka-style offset-based consumption model. This hybrid approach gives
// us the best of both worlds:
//
//   KAFKA-STYLE (offset only):
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │ Offset:  0   1   2   3   4   5   6   7   8   9                       │
//   │ Status:  ✓   ✓   ✓   ✓   ✓   │                                       │
//   │                              │                                       │
//   │                         committed                                    │
//   │                         offset=5                                     │
//   │                                                                      │
//   │ PROBLEM: If msg 3 fails, can't skip it. Must reprocess 3,4,5...      │
//   └──────────────────────────────────────────────────────────────────────┘
//
//   SQS-STYLE (per-message only):
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │ Message:  A   B   C   D   E   F   G   H   I   J                      │
//   │ Status:   ✓   ✓   ✗   ✓   ✓   ?   ?   ?   ?   ?                      │
//   │                   │                                                  │
//   │              message C                                               │
//   │              retried                                                 │
//   │                                                                      │
//   │ PRO: Can retry individual messages without reprocessing others       │
//   │ CON: No offset concept, harder to track overall progress             │
//   └──────────────────────────────────────────────────────────────────────┘
//
//   GOQUEUE HYBRID:
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │ Offset:  0   1   2   3   4   5   6   7   8   9                       │
//   │ Status:  ✓   ✓   ✓   ✗   ✓   ✓   ?   ?   ?   ?                       │
//   │                      │                                               │
//   │              msg 3 in retry                                          │
//   │              queue (will be                                          │
//   │              redelivered)                                            │
//   │                                                                      │
//   │ Committed offset = 2 (last contiguous ACKed)                         │
//   │                                                                      │
//   │ On restart: Resume from offset 3 (msg 3 redelivered naturally)       │
//   └──────────────────────────────────────────────────────────────────────┘
//
// WHY HYBRID?
//   - Per-message ACK: Fine-grained control, can retry single message
//   - Offset tracking: Efficient storage, simple recovery after crash
//   - Best of both: Individual message handling + overall progress tracking
//
// ACK SEMANTICS:
//   - ACK:    "Message processed successfully, delete it"
//   - NACK:   "Message processing failed, retry it later"
//   - REJECT: "Message is poison, send to DLQ immediately"
//
// =============================================================================

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrAckManagerClosed means the ACK manager has been shut down
	ErrAckManagerClosed = errors.New("ack manager is closed")

	// ErrConsumerNotFound means the consumer is not registered
	ErrConsumerNotFound = errors.New("consumer not found")

	// ErrBackpressure means the consumer has too many in-flight messages
	ErrBackpressure = errors.New("backpressure: too many in-flight messages")

	// ErrMessageNotFound means the message is not found (already acked, expired, or invalid)
	ErrMessageNotFound = errors.New("message not found")

	// Note: ErrInvalidReceiptHandle is defined in inflight.go
)

// =============================================================================
// ACK RESULT
// =============================================================================

// AckResult represents the result of an ACK/NACK/REJECT operation.
//
// FIELD USAGE:
//
//	ACK:
//	  - Topic, Partition, Offset: Which message was ACKed
//	  - NewCommittedOffset: The new committed offset (may have advanced)
//	  - OffsetAdvanced: True if committed offset moved forward
//	  - RemainingInFlight: Messages still being processed
//
//	NACK:
//	  - Topic, Partition, Offset: Which message was NACKed
//	  - Action: "requeued" or "dlq"
//	  - DeliveryCount: How many times this message has been delivered
//	  - NextVisibleAt: When the message will be available again
//	  - DLQTopic: If sent to DLQ, which topic
//
//	REJECT:
//	  - Topic, Partition, Offset: Which message was rejected
//	  - Action: "rejected"
//	  - DLQTopic: The DLQ topic where message was sent

type AckResult struct {
	// Success indicates if the operation was successful
	Success bool

	// Action taken (for logging/debugging): "acked", "requeued", "dlq", "rejected"
	Action string

	// Topic the message belongs to
	Topic string

	// Partition the message belongs to
	Partition int

	// Offset of the message
	Offset int64

	// NewCommittedOffset is the committed offset after this ACK (may have advanced)
	NewCommittedOffset int64

	// OffsetAdvanced is true if the committed offset moved forward
	OffsetAdvanced bool

	// RemainingInFlight is the number of messages still in-flight for this consumer
	RemainingInFlight int

	// DeliveryCount is how many times this message has been delivered (for NACK)
	DeliveryCount int

	// NextVisibleAt is when a NACKed message will become visible again
	NextVisibleAt time.Time

	// DLQTopic is the dead letter queue topic (for NACK->DLQ or REJECT)
	DLQTopic string

	// Error if operation failed
	Error error
}

// =============================================================================
// CONSUMER STATE
// =============================================================================

// ConsumerAckState tracks ACK state for a single consumer within a partition.
//
// IMPORTANT CONCEPT: COMMITTED OFFSET CALCULATION
//
// The committed offset is the highest offset where ALL messages up to and
// including that offset have been ACKed. This is calculated by finding the
// lowest un-ACKed message.
//
// Example:
//
//	Offset:      0   1   2   3   4   5   6   7   8   9
//	ACKed:       ✓   ✓   ✓   ?   ✓   ✓   ?   ?   ?   ?
//	                     │
//	                committed = 2 (offset 3 not ACKed yet)
//
// When message 3 is ACKed:
//
//	Offset:      0   1   2   3   4   5   6   7   8   9
//	ACKed:       ✓   ✓   ✓   ✓   ✓   ✓   ?   ?   ?   ?
//	                             │
//	                        committed = 5 (jumps forward)
type ConsumerAckState struct {
	// ConsumerID identifies this consumer
	ConsumerID string

	// GroupID is the consumer group
	GroupID string

	// Topic is the topic being consumed
	Topic string

	// Partition is the partition number
	Partition int

	// CommittedOffset is the highest contiguously ACKed offset
	CommittedOffset int64

	// PendingAcks maps offset to in-flight state (true = ACKed, false = pending)
	// Only tracks messages between CommittedOffset and HighWaterMark
	PendingAcks map[int64]bool

	// HighWaterMark is the highest offset delivered to this consumer
	HighWaterMark int64

	// InFlightCount is the number of messages currently in-flight
	InFlightCount int

	// mu protects all fields
	mu sync.Mutex
}

// NewConsumerAckState creates a new consumer ACK state tracker.
func NewConsumerAckState(consumerID, groupID, topic string, partition int, startOffset int64) *ConsumerAckState {
	return &ConsumerAckState{
		ConsumerID:      consumerID,
		GroupID:         groupID,
		Topic:           topic,
		Partition:       partition,
		CommittedOffset: startOffset - 1, // Nothing committed yet
		PendingAcks:     make(map[int64]bool),
		HighWaterMark:   startOffset - 1,
	}
}

// MarkDelivered marks an offset as delivered (in-flight).
func (s *ConsumerAckState) MarkDelivered(offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.PendingAcks[offset] = false // false = pending (not yet ACKed)
	if offset > s.HighWaterMark {
		s.HighWaterMark = offset
	}
	s.InFlightCount++
}

// MarkAcked marks an offset as successfully ACKed.
// Returns the new committed offset after updating.
func (s *ConsumerAckState) MarkAcked(offset int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Mark as ACKed
	s.PendingAcks[offset] = true
	s.InFlightCount--

	// Update committed offset
	return s.updateCommittedOffset()
}

// updateCommittedOffset recalculates the committed offset.
// Called after any ACK to potentially advance the commit point.
//
// ALGORITHM:
//  1. Start from current committed offset + 1
//  2. While next offset is ACKed, advance committed offset
//  3. Clean up entries we've passed
func (s *ConsumerAckState) updateCommittedOffset() int64 {
	// Find the new committed offset by scanning forward
	newCommitted := s.CommittedOffset

	for offset := s.CommittedOffset + 1; offset <= s.HighWaterMark; offset++ {
		acked, exists := s.PendingAcks[offset]
		if !exists {
			// Gap in delivery (shouldn't happen normally)
			break
		}
		if !acked {
			// Not yet ACKed, stop here
			break
		}
		// This offset is ACKed, advance
		newCommitted = offset
		// Clean up (no longer need to track)
		delete(s.PendingAcks, offset)
	}

	s.CommittedOffset = newCommitted
	return newCommitted
}

// MarkNacked marks an offset as NACKed (will be retried).
// The offset remains in pending state for redelivery.
func (s *ConsumerAckState) MarkNacked(offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove from pending (will be redelivered)
	delete(s.PendingAcks, offset)
	s.InFlightCount--
}

// GetCommittedOffset returns the current committed offset.
func (s *ConsumerAckState) GetCommittedOffset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.CommittedOffset
}

// GetInFlightCount returns the number of in-flight messages.
func (s *ConsumerAckState) GetInFlightCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.InFlightCount
}

// =============================================================================
// ACK MANAGER
// =============================================================================

// AckManager coordinates per-message acknowledgment across all consumers.
//
// RESPONSIBILITIES:
//   - Track in-flight messages per consumer
//   - Process ACK/NACK/REJECT requests
//   - Calculate committed offsets
//   - Coordinate with VisibilityTracker for timeout handling
//   - Route rejected messages to DLQ
//
// ARCHITECTURE:
//
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│                           ACK MANAGER                                   │
//	│                                                                         │
//	│   ┌─────────────────────┐      ┌─────────────────────┐                  │
//	│   │  Consumer States    │      │  Visibility         │                  │
//	│   │  (per consumer/     │◄────►│  Tracker            │                  │
//	│   │   partition)        │      │  (timeout heap)     │                  │
//	│   └─────────────────────┘      └─────────────────────┘                  │
//	│              │                          │                               │
//	│              │                          │                               │
//	│              ▼                          ▼                               │
//	│   ┌─────────────────────┐      ┌─────────────────────┐                  │
//	│   │  Offset Manager     │      │  Retry Queue        │                  │
//	│   │  (commits)          │      │  (backoff schedule) │                  │
//	│   └─────────────────────┘      └─────────────────────┘                  │
//	│                                         │                               │
//	│                                         ▼                               │
//	│                                ┌─────────────────────┐                  │
//	│                                │  DLQ Router         │                  │
//	│                                │  (max retries)      │                  │
//	│                                └─────────────────────┘                  │
//	│                                                                         │
//	└─────────────────────────────────────────────────────────────────────────┘
type AckManager struct {
	// visibilityTracker tracks visibility timeouts
	visibilityTracker *VisibilityTracker

	// dlqRouter handles dead letter queue routing
	dlqRouter *DLQRouter

	// consumerStates maps "{groupID}:{consumerID}:{topic}:{partition}" to state
	consumerStates map[string]*ConsumerAckState

	// retryQueue holds messages scheduled for retry
	retryQueue chan *InFlightMessage

	// config holds reliability configuration
	config ReliabilityConfig

	// broker reference for offset commits and DLQ
	broker *Broker

	// mu protects consumerStates
	mu sync.RWMutex

	// ctx for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc

	// wg for waiting on goroutines
	wg sync.WaitGroup

	// logger for ACK operations
	logger *slog.Logger

	// closed flag
	closed bool

	// stats for monitoring
	stats AckManagerStats
}

// AckManagerStats tracks ACK manager metrics.
type AckManagerStats struct {
	TotalAcks     int64
	TotalNacks    int64
	TotalRejects  int64
	TotalExpired  int64
	TotalRetries  int64
	TotalDLQ      int64
	CurrentQueued int
}

// NewAckManager creates a new ACK manager.
func NewAckManager(broker *Broker, config ReliabilityConfig) *AckManager {
	ctx, cancel := context.WithCancel(context.Background())

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	am := &AckManager{
		consumerStates: make(map[string]*ConsumerAckState),
		// TODO:should this be configurable?
		retryQueue: make(chan *InFlightMessage, 10000),
		config:     config,
		broker:     broker,
		ctx:        ctx,
		cancel:     cancel,
		logger:     logger,
	}

	// Create visibility tracker with expiry callback
	am.visibilityTracker = NewVisibilityTracker(config, am.onVisibilityExpired)

	// Create DLQ router
	am.dlqRouter = NewDLQRouter(broker, config)

	// Start retry processor
	am.wg.Add(1)
	go am.retryProcessor()

	return am
}

// =============================================================================
// CONSUMER STATE MANAGEMENT
// =============================================================================

// consumerKey generates a unique key for consumer state lookup.
func consumerKey(groupID, consumerID, topic string, partition int) string {
	return fmt.Sprintf("%s:%s:%s:%d", groupID, consumerID, topic, partition)
}

// GetOrCreateConsumerState gets or creates ACK state for a consumer/partition.
func (am *AckManager) GetOrCreateConsumerState(
	consumerID, groupID, topic string,
	partition int,
	startOffset int64,
) *ConsumerAckState {
	am.mu.Lock()
	defer am.mu.Unlock()

	key := consumerKey(groupID, consumerID, topic, partition)
	state, exists := am.consumerStates[key]
	if !exists {
		state = NewConsumerAckState(consumerID, groupID, topic, partition, startOffset)
		am.consumerStates[key] = state
	}
	return state
}

// RemoveConsumerState removes ACK state for a consumer (on leave/eviction).
func (am *AckManager) RemoveConsumerState(consumerID, groupID, topic string, partition int) {
	am.mu.Lock()
	defer am.mu.Unlock()

	key := consumerKey(groupID, consumerID, topic, partition)
	delete(am.consumerStates, key)
}

// =============================================================================
// DELIVERY TRACKING
// =============================================================================

// TrackDelivery registers a message as delivered to a consumer.
//
// PARAMETERS:
//   - msg: The message being delivered
//   - consumerID, groupID: Consumer identification
//   - visibilityTimeout: How long before message becomes visible again
//
// RETURNS:
//   - ReceiptHandle for the consumer to use for ACK/NACK
//   - Error if backpressure or other issue
//
// FLOW:
//  1. Check backpressure (max in-flight per consumer)
//  2. Generate receipt handle
//  3. Create InFlightMessage
//  4. Add to visibility tracker
//  5. Update consumer state
//  6. Return receipt handle
func (am *AckManager) TrackDelivery(
	msg *Message,
	consumerID, groupID string,
	visibilityTimeout time.Duration,
) (string, error) {
	am.mu.RLock()
	if am.closed {
		am.mu.RUnlock()
		return "", ErrAckManagerClosed
	}
	am.mu.RUnlock()

	// Get consumer state
	state := am.GetOrCreateConsumerState(consumerID, groupID, msg.Topic, msg.Partition, msg.Offset)

	// Check backpressure
	if state.GetInFlightCount() >= am.config.MaxInFlightPerConsumer {
		return "", ErrBackpressure
	}

	// Generate receipt handle
	receiptHandle := GenerateReceiptHandle(msg.Topic, msg.Partition, msg.Offset, 1)

	// Create in-flight message
	inflight := &InFlightMessage{
		ReceiptHandle:      receiptHandle,
		MessageID:          MessageID(msg.Topic, msg.Partition, msg.Offset),
		Topic:              msg.Topic,
		Partition:          msg.Partition,
		Offset:             msg.Offset,
		ConsumerID:         consumerID,
		GroupID:            groupID,
		DeliveryCount:      1,
		MaxRetries:         am.config.MaxRetries,
		FirstDeliveryTime:  time.Now(),
		LastDeliveryTime:   time.Now(),
		VisibilityDeadline: time.Now().Add(visibilityTimeout),
		State:              StateInFlight,
		OriginalKey:        msg.Key,
		OriginalValue:      msg.Value,
		OriginalTimestamp:  msg.Timestamp,
	}

	// Add to visibility tracker
	if err := am.visibilityTracker.Track(inflight); err != nil {
		return "", fmt.Errorf("failed to track delivery: %w", err)
	}

	// Update consumer state
	state.MarkDelivered(msg.Offset)

	am.logger.Debug("tracked delivery",
		"receipt", receiptHandle,
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"consumer", consumerID)

	return receiptHandle, nil
}

// =============================================================================
// ACK/NACK/REJECT OPERATIONS
// =============================================================================

// Ack acknowledges successful processing of a message.
//
// FLOW:
//  1. Parse receipt handle to get message location
//  2. Remove from visibility tracker
//  3. Update consumer state (mark ACKed)
//  4. Return new committed offset
//
// SEMANTICS:
//   - Message is considered fully processed
//   - Will not be redelivered
//   - Committed offset may advance
func (am *AckManager) Ack(receiptHandle string) (*AckResult, error) {
	am.mu.RLock()
	if am.closed {
		am.mu.RUnlock()
		return nil, ErrAckManagerClosed
	}
	am.mu.RUnlock()

	// Remove from visibility tracker
	inflight, err := am.visibilityTracker.Untrack(receiptHandle)
	if err != nil {
		return nil, fmt.Errorf("ack failed: %w", err)
	}

	// Get consumer state and mark ACKed
	state := am.GetOrCreateConsumerState(
		inflight.ConsumerID,
		inflight.GroupID,
		inflight.Topic,
		inflight.Partition,
		inflight.Offset,
	)
	newOffset := state.MarkAcked(inflight.Offset)

	// Update stats
	am.mu.Lock()
	am.stats.TotalAcks++
	am.mu.Unlock()

	am.logger.Debug("message ACKed",
		"receipt", receiptHandle,
		"topic", inflight.Topic,
		"partition", inflight.Partition,
		"offset", inflight.Offset,
		"new_committed", newOffset)

	// Check if offset advanced (new committed > old committed)
	offsetAdvanced := newOffset >= inflight.Offset

	return &AckResult{
		Success:            true,
		Action:             "acked",
		Topic:              inflight.Topic,
		Partition:          inflight.Partition,
		Offset:             inflight.Offset,
		NewCommittedOffset: newOffset,
		OffsetAdvanced:     offsetAdvanced,
		RemainingInFlight:  state.InFlightCount,
	}, nil
}

// Nack indicates processing failed and message should be retried.
//
// PARAMETERS:
//   - receiptHandle: The receipt from delivery
//   - reason: Why the message failed (for logging/debugging)
//
// FLOW:
//  1. Remove from visibility tracker
//  2. Increment delivery count
//  3. If max retries exceeded, route to DLQ
//  4. Otherwise, schedule for retry with backoff
//
// SEMANTICS:
//   - Message will be redelivered after backoff delay
//   - Each NACK increments delivery count
//   - After MaxRetries, message goes to DLQ
func (am *AckManager) Nack(receiptHandle, reason string) (*AckResult, error) {
	am.mu.RLock()
	if am.closed {
		am.mu.RUnlock()
		return nil, ErrAckManagerClosed
	}
	am.mu.RUnlock()

	// Remove from visibility tracker
	inflight, err := am.visibilityTracker.Untrack(receiptHandle)
	if err != nil {
		return nil, fmt.Errorf("nack failed: %w", err)
	}

	// Update consumer state
	state := am.GetOrCreateConsumerState(
		inflight.ConsumerID,
		inflight.GroupID,
		inflight.Topic,
		inflight.Partition,
		inflight.Offset,
	)
	state.MarkNacked(inflight.Offset)

	// Update in-flight message
	inflight.DeliveryCount++
	inflight.LastError = reason

	// Check if max retries exceeded
	if inflight.DeliveryCount > inflight.MaxRetries {
		return am.routeToDLQ(inflight, DLQReasonMaxRetries)
	}

	// Schedule for retry with backoff
	backoff := am.config.CalculateBackoff(inflight.DeliveryCount)
	inflight.NextRetryTime = time.Now().Add(backoff)
	inflight.State = StateScheduledRetry

	// Queue for retry
	// NOTE: Non-blocking send - if queue is full, we route to DLQ with BACKPRESSURE reason.
	// This prevents unbounded memory growth when system is overloaded.
	// Operators should monitor DLQ for BACKPRESSURE messages and scale accordingly.
	select {
	case am.retryQueue <- inflight:
		am.mu.Lock()
		am.stats.TotalNacks++
		am.stats.CurrentQueued++
		am.mu.Unlock()
	default:
		// Queue full, route to DLQ with BACKPRESSURE reason (not max retries!)
		// This is a capacity issue, not a message processing failure.
		am.logger.Warn("retry queue full, routing to DLQ due to backpressure",
			"topic", inflight.Topic,
			"partition", inflight.Partition,
			"offset", inflight.Offset,
			"queue_capacity", cap(am.retryQueue))
		return am.routeToDLQ(inflight, DLQReasonBackpressure)
	}

	am.logger.Info("message NACKed, scheduled for retry",
		"receipt", receiptHandle,
		"topic", inflight.Topic,
		"partition", inflight.Partition,
		"offset", inflight.Offset,
		"delivery_count", inflight.DeliveryCount,
		"retry_after", backoff,
		"reason", reason)

	return &AckResult{
		Success:       true,
		Action:        "requeued",
		Topic:         inflight.Topic,
		Partition:     inflight.Partition,
		Offset:        inflight.Offset,
		DeliveryCount: inflight.DeliveryCount,
		NextVisibleAt: inflight.NextRetryTime,
	}, nil
}

// Reject sends a message directly to the dead letter queue.
//
// PARAMETERS:
//   - receiptHandle: The receipt from delivery
//   - reason: Why the message was rejected
//
// FLOW:
//  1. Remove from visibility tracker
//  2. Route immediately to DLQ (no retry)
//
// SEMANTICS:
//   - Message is considered "poison" (can never succeed)
//   - Immediately routed to DLQ
//   - No retry attempts
//
// USE CASES:
//   - Message format is invalid
//   - Business logic determines message is unprocessable
//   - Consumer explicitly marks as poison
func (am *AckManager) Reject(receiptHandle, reason string) (*AckResult, error) {
	am.mu.RLock()
	if am.closed {
		am.mu.RUnlock()
		return nil, ErrAckManagerClosed
	}
	am.mu.RUnlock()

	// Remove from visibility tracker
	inflight, err := am.visibilityTracker.Untrack(receiptHandle)
	if err != nil {
		return nil, fmt.Errorf("reject failed: %w", err)
	}

	// Update consumer state
	state := am.GetOrCreateConsumerState(
		inflight.ConsumerID,
		inflight.GroupID,
		inflight.Topic,
		inflight.Partition,
		inflight.Offset,
	)
	state.MarkNacked(inflight.Offset) // Treated as NACKed for offset calculation

	// Update in-flight message
	inflight.LastError = reason

	// Route to DLQ
	return am.routeToDLQ(inflight, DLQReasonRejected)
}

// ExtendVisibility extends the visibility timeout for a message.
//
// PARAMETERS:
//   - receiptHandle: The receipt from delivery
//   - extension: Additional time to add
//
// RETURNS:
//   - New visibility deadline
//   - Error if receipt not found or already expired
func (am *AckManager) ExtendVisibility(receiptHandle string, extension time.Duration) (time.Time, error) {
	am.mu.RLock()
	if am.closed {
		am.mu.RUnlock()
		return time.Time{}, ErrAckManagerClosed
	}
	am.mu.RUnlock()

	return am.visibilityTracker.ExtendVisibility(receiptHandle, extension)
}

// =============================================================================
// INTERNAL OPERATIONS
// =============================================================================

// routeToDLQ sends a message to the dead letter queue.
func (am *AckManager) routeToDLQ(inflight *InFlightMessage, reason DLQReason) (*AckResult, error) {
	inflight.State = StateRejected

	// Create DLQ message
	dlqMsg := &DLQMessage{
		OriginalTopic:     inflight.Topic,
		OriginalPartition: inflight.Partition,
		OriginalOffset:    inflight.Offset,
		OriginalTimestamp: inflight.OriginalTimestamp,
		OriginalKey:       inflight.OriginalKey,
		OriginalValue:     inflight.OriginalValue,
		DeliveryAttempts:  inflight.DeliveryCount,
		FirstDelivery:     inflight.FirstDeliveryTime,
		LastDelivery:      inflight.LastDeliveryTime,
		LastConsumer:      inflight.ConsumerID,
		LastGroup:         inflight.GroupID,
		Reason:            reason,
		LastError:         inflight.LastError,
		DLQTime:           time.Now(),
	}

	// Route via DLQ router
	if err := am.dlqRouter.Route(dlqMsg); err != nil {
		am.logger.Error("failed to route to DLQ",
			"topic", inflight.Topic,
			"partition", inflight.Partition,
			"offset", inflight.Offset,
			"error", err)
		return nil, fmt.Errorf("failed to route to DLQ: %w", err)
	}

	am.mu.Lock()
	am.stats.TotalRejects++
	am.stats.TotalDLQ++
	am.mu.Unlock()

	dlqTopic := am.dlqRouter.GetDLQTopic(inflight.Topic)

	am.logger.Info("message routed to DLQ",
		"topic", inflight.Topic,
		"partition", inflight.Partition,
		"offset", inflight.Offset,
		"dlq_topic", dlqTopic,
		"reason", reason,
		"delivery_attempts", inflight.DeliveryCount)

	return &AckResult{
		Success:       true,
		Action:        "dlq",
		Topic:         inflight.Topic,
		Partition:     inflight.Partition,
		Offset:        inflight.Offset,
		DeliveryCount: inflight.DeliveryCount,
		DLQTopic:      dlqTopic,
	}, nil
}

// onVisibilityExpired is called when a message's visibility timeout expires.
func (am *AckManager) onVisibilityExpired(inflight *InFlightMessage) {
	// Update consumer state
	am.mu.RLock()
	key := consumerKey(inflight.GroupID, inflight.ConsumerID, inflight.Topic, inflight.Partition)
	state, exists := am.consumerStates[key]
	am.mu.RUnlock()

	if exists {
		state.MarkNacked(inflight.Offset)
	}

	// Increment delivery count
	inflight.DeliveryCount++

	// Check if max retries exceeded
	if inflight.DeliveryCount > inflight.MaxRetries {
		_, _ = am.routeToDLQ(inflight, DLQReasonMaxRetries)
		return
	}

	// Schedule for immediate retry (already waited visibility timeout)
	inflight.NextRetryTime = time.Now()
	inflight.State = StateScheduledRetry

	select {
	case am.retryQueue <- inflight:
		am.mu.Lock()
		am.stats.TotalExpired++
		am.stats.CurrentQueued++
		am.mu.Unlock()
	default:
		am.logger.Warn("retry queue full on visibility expiry, routing to DLQ",
			"topic", inflight.Topic,
			"partition", inflight.Partition,
			"offset", inflight.Offset)
		_, _ = am.routeToDLQ(inflight, DLQReasonMaxRetries)
	}
}

// retryProcessor processes messages scheduled for retry.
func (am *AckManager) retryProcessor() {
	defer am.wg.Done()

	for {
		select {
		case <-am.ctx.Done():
			return
		case inflight := <-am.retryQueue:
			am.processRetry(inflight)
		}
	}
}

// processRetry handles a single retry message.
//
// GOROUTINE LEAK FIX:
// Uses time.NewTimer instead of time.After to avoid goroutine leaks when
// context is canceled while waiting for retry time.
func (am *AckManager) processRetry(inflight *InFlightMessage) {
	// Wait until retry time
	waitDuration := time.Until(inflight.NextRetryTime)
	if waitDuration > 0 {
		timer := time.NewTimer(waitDuration)
		select {
		case <-am.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}

	am.mu.Lock()
	am.stats.CurrentQueued--
	am.stats.TotalRetries++
	am.mu.Unlock()

	// Generate new receipt handle for this delivery
	newReceipt := GenerateReceiptHandle(
		inflight.Topic,
		inflight.Partition,
		inflight.Offset,
		inflight.DeliveryCount,
	)
	inflight.ReceiptHandle = newReceipt
	inflight.LastDeliveryTime = time.Now()
	inflight.VisibilityDeadline = time.Now().Add(
		time.Duration(am.config.VisibilityTimeoutMs) * time.Millisecond,
	)
	inflight.State = StateInFlight

	// Re-track in visibility tracker
	if err := am.visibilityTracker.Track(inflight); err != nil {
		am.logger.Error("failed to re-track retry message",
			"topic", inflight.Topic,
			"partition", inflight.Partition,
			"offset", inflight.Offset,
			"error", err)
		return
	}

	am.logger.Info("message redelivered for retry",
		"topic", inflight.Topic,
		"partition", inflight.Partition,
		"offset", inflight.Offset,
		"delivery_count", inflight.DeliveryCount,
		"new_receipt", newReceipt)
}

// =============================================================================
// QUERY OPERATIONS
// =============================================================================

// GetCommittedOffset returns the committed offset for a consumer/partition.
func (am *AckManager) GetCommittedOffset(consumerID, groupID, topic string, partition int) int64 {
	am.mu.RLock()
	defer am.mu.RUnlock()

	key := consumerKey(groupID, consumerID, topic, partition)
	state, exists := am.consumerStates[key]
	if !exists {
		return -1
	}
	return state.GetCommittedOffset()
}

// GetConsumerLag returns lag information for a consumer/partition.
//
// CONSUMER LAG COMPONENTS:
//   - Pending: Messages not yet delivered (in log, not fetched)
//   - InFlight: Messages delivered but not yet ACKed
//   - Total: Pending + InFlight
type ConsumerLag struct {
	CommittedOffset int64
	HighWaterMark   int64
	LogEndOffset    int64
	InFlightCount   int
	PendingCount    int64 // LogEnd - HighWaterMark
	TotalLag        int64 // LogEnd - Committed
}

func (am *AckManager) GetConsumerLag(consumerID, groupID, topic string, partition int) (*ConsumerLag, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	key := consumerKey(groupID, consumerID, topic, partition)
	state, exists := am.consumerStates[key]
	if !exists {
		return nil, ErrConsumerNotFound
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// Get log end offset from broker
	_, logEnd, err := am.broker.GetOffsetBounds(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get log end offset: %w", err)
	}

	return &ConsumerLag{
		CommittedOffset: state.CommittedOffset,
		HighWaterMark:   state.HighWaterMark,
		LogEndOffset:    logEnd,
		InFlightCount:   state.InFlightCount,
		PendingCount:    logEnd - state.HighWaterMark,
		TotalLag:        logEnd - state.CommittedOffset,
	}, nil
}

// Stats returns ACK manager statistics.
func (am *AckManager) Stats() AckManagerStats {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.stats
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Close shuts down the ACK manager.
func (am *AckManager) Close() error {
	am.mu.Lock()
	if am.closed {
		am.mu.Unlock()
		return nil
	}
	am.closed = true
	am.mu.Unlock()

	am.cancel()
	am.wg.Wait()

	if err := am.visibilityTracker.Close(); err != nil {
		am.logger.Error("error closing visibility tracker", "error", err)
	}

	am.logger.Info("ACK manager closed",
		"total_acks", am.stats.TotalAcks,
		"total_nacks", am.stats.TotalNacks,
		"total_rejects", am.stats.TotalRejects,
		"total_dlq", am.stats.TotalDLQ)

	return nil
}
