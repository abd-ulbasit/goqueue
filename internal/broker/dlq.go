// =============================================================================
// DEAD LETTER QUEUE (DLQ) ROUTER - HANDLING FAILED MESSAGES
// =============================================================================
//
// WHAT IS A DEAD LETTER QUEUE?
// A DLQ is a special topic that holds messages that couldn't be processed
// successfully after all retry attempts. It provides:
//
//   1. MESSAGE PRESERVATION
//      - Failed messages are not lost
//      - Original data is preserved for debugging
//      - Metadata explains why it failed
//
//   2. DECOUPLED HANDLING
//      - Main consumer continues processing good messages
//      - Operators can investigate DLQ separately
//      - Manual reprocessing or alerting possible
//
//   3. POISON MESSAGE ISOLATION
//      - Bad messages don't block the queue
//      - One bad message doesn't stop all processing
//      - System remains available
//
// WHY PER-TOPIC DLQ?
//
//   OPTION A: SINGLE GLOBAL DLQ
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ orders topic ────┐                                                      │
//   │                  ├────► global.dlq (mixed message types)                │
//   │ payments topic ──┘                                                      │
//   │                                                                         │
//   │ PRO: Simple admin (one place to look)                                   │
//   │ CON: Mixed schemas, hard to reprocess specific topics                   │
//   └─────────────────────────────────────────────────────────────────────────┘
//
//   OPTION B: PER-TOPIC DLQ (our choice)
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ orders topic ──────────► orders.dlq                                     │
//   │ payments topic ────────► payments.dlq                                   │
//   │                                                                         │
//   │ PRO: Clean separation, easy to reprocess                                │
//   │ PRO: Same schema as original topic                                      │
//   │ PRO: Independent retention policies                                     │
//   │ CON: More topics to manage                                              │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// DLQ MESSAGE FORMAT:
// We wrap the original message with metadata for debugging:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ DLQ MESSAGE                                                             │
//   │                                                                         │
//   │ {                                                                       │
//   │   "original_topic": "orders",                                           │
//   │   "original_partition": 2,                                              │
//   │   "original_offset": 12345,                                             │
//   │   "original_timestamp": "2025-01-15T10:30:00Z",                         │
//   │   "original_key": "order-123",                                          │
//   │   "original_value": "{...original json...}",                            │
//   │                                                                         │
//   │   "delivery_attempts": 4,                                               │
//   │   "first_delivery": "2025-01-15T10:30:05Z",                             │
//   │   "last_delivery": "2025-01-15T10:35:00Z",                              │
//   │   "last_consumer": "order-processor-abc123",                            │
//   │   "last_group": "order-processors",                                     │
//   │                                                                         │
//   │   "reason": "MAX_RETRIES_EXCEEDED",                                     │
//   │   "last_error": "database connection timeout",                          │
//   │   "dlq_time": "2025-01-15T10:35:00Z"                                    │
//   │ }                                                                       │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   ┌─────────────┬─────────────────────┬──────────────────────────────────────┐
//   │ System      │ DLQ Support         │ Notes                                │
//   ├─────────────┼─────────────────────┼──────────────────────────────────────┤
//   │ SQS         │ Per-queue DLQ       │ Configurable, automatic routing      │
//   │             │                     │ after maxReceiveCount                │
//   ├─────────────┼─────────────────────┼──────────────────────────────────────┤
//   │ Kafka       │ External (DIY)      │ No native DLQ, typically             │
//   │             │                     │ implemented by consumers             │
//   ├─────────────┼─────────────────────┼──────────────────────────────────────┤
//   │ RabbitMQ    │ Dead letter         │ Per-queue, configurable              │
//   │             │ exchange            │ routing key                          │
//   ├─────────────┼─────────────────────┼──────────────────────────────────────┤
//   │ goqueue     │ Per-topic DLQ       │ Auto-created, metadata preserved     │
//   └─────────────┴─────────────────────┴──────────────────────────────────────┘
//
// =============================================================================

package broker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
)

// =============================================================================
// DLQ ROUTER
// =============================================================================

// DLQRouter handles routing failed messages to dead letter queues.
//
// RESPONSIBILITIES:
//   - Create DLQ topics on-demand (auto-creation)
//   - Format messages with failure metadata
//   - Publish to appropriate DLQ topic
//   - Track DLQ statistics
//
// AUTO-CREATION:
// When a message needs to go to DLQ for topic "orders", we:
//  1. Check if "orders.dlq" exists
//  2. If not, create it with same partition count as original
//  3. Publish the DLQ message
type DLQRouter struct {
	// broker for topic operations
	broker *Broker

	// config holds DLQ configuration
	config ReliabilityConfig

	// createdDLQs tracks which DLQ topics have been created
	// Prevents redundant existence checks
	createdDLQs map[string]bool

	// mu protects createdDLQs
	mu sync.RWMutex

	// logger for DLQ operations
	logger *slog.Logger

	// stats for monitoring
	stats DLQStats
}

// DLQStats tracks DLQ router metrics.
type DLQStats struct {
	// TotalRouted is the total number of messages routed to DLQ
	TotalRouted int64

	// RoutesByReason counts messages by DLQ reason
	RoutesByReason map[DLQReason]int64

	// RoutesByTopic counts messages by original topic
	RoutesByTopic map[string]int64

	// CreateErrors counts DLQ topic creation failures
	CreateErrors int64

	// PublishErrors counts DLQ publish failures
	PublishErrors int64
}

// NewDLQRouter creates a new DLQ router.
func NewDLQRouter(broker *Broker, config ReliabilityConfig) *DLQRouter {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	return &DLQRouter{
		broker:      broker,
		config:      config,
		createdDLQs: make(map[string]bool),
		logger:      logger,
		stats: DLQStats{
			RoutesByReason: make(map[DLQReason]int64),
			RoutesByTopic:  make(map[string]int64),
		},
	}
}

// =============================================================================
// ROUTING OPERATIONS
// =============================================================================

// Route sends a message to the appropriate dead letter queue.
//
// FLOW:
//  1. Check if DLQ is enabled in config
//  2. Get or create DLQ topic
//  3. Serialize DLQ message with metadata
//  4. Publish to DLQ topic
//  5. Update statistics
//
// PARAMETERS:
//   - msg: DLQ message containing original + metadata
//
// RETURNS:
//   - Error if DLQ disabled, topic creation fails, or publish fails
func (r *DLQRouter) Route(msg *DLQMessage) error {
	// Check if DLQ is enabled
	if !r.config.DLQEnabled {
		r.logger.Debug("DLQ disabled, dropping message",
			"topic", msg.OriginalTopic,
			"offset", msg.OriginalOffset)
		return nil
	}

	// Get DLQ topic name
	dlqTopic := r.config.DLQTopicName(msg.OriginalTopic)

	// Ensure DLQ topic exists
	if err := r.ensureDLQTopic(dlqTopic, msg.OriginalTopic); err != nil {
		r.mu.Lock()
		r.stats.CreateErrors++
		r.mu.Unlock()
		return fmt.Errorf("failed to ensure DLQ topic: %w", err)
	}

	// Serialize DLQ message
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to serialize DLQ message: %w", err)
	}

	// Use original key for partition routing (keeps related messages together)
	key := msg.OriginalKey

	// Publish to DLQ
	partition, offset, err := r.broker.Publish(dlqTopic, key, value)
	if err != nil {
		r.mu.Lock()
		r.stats.PublishErrors++
		r.mu.Unlock()
		return fmt.Errorf("failed to publish to DLQ: %w", err)
	}

	// Update stats
	r.mu.Lock()
	r.stats.TotalRouted++
	r.stats.RoutesByReason[msg.Reason]++
	r.stats.RoutesByTopic[msg.OriginalTopic]++
	r.mu.Unlock()

	r.logger.Info("message routed to DLQ",
		"original_topic", msg.OriginalTopic,
		"original_partition", msg.OriginalPartition,
		"original_offset", msg.OriginalOffset,
		"dlq_topic", dlqTopic,
		"dlq_partition", partition,
		"dlq_offset", offset,
		"reason", msg.Reason,
		"delivery_attempts", msg.DeliveryAttempts)

	return nil
}

// ensureDLQTopic creates the DLQ topic if it doesn't exist.
//
// AUTO-CREATION STRATEGY:
//   - Same partition count as original topic
//   - Longer retention (14 days default vs 7 days)
//   - Created on first failure for that topic
func (r *DLQRouter) ensureDLQTopic(dlqTopic, originalTopic string) error {
	// Check cache first (fast path)
	r.mu.RLock()
	exists := r.createdDLQs[dlqTopic]
	r.mu.RUnlock()

	if exists {
		return nil
	}

	// Check if topic exists in broker
	if r.broker.TopicExists(dlqTopic) {
		r.mu.Lock()
		r.createdDLQs[dlqTopic] = true
		r.mu.Unlock()
		return nil
	}

	// Get original topic's partition count
	numPartitions := 1 // Default if original not found
	original, err := r.broker.GetTopic(originalTopic)
	if err == nil {
		numPartitions = original.NumPartitions()
	}

	// Create DLQ topic
	config := TopicConfig{
		Name:           dlqTopic,
		NumPartitions:  numPartitions,
		RetentionHours: r.config.DLQRetentionHours,
	}

	if err := r.broker.CreateTopic(config); err != nil {
		// Check if it was created by another goroutine
		if r.broker.TopicExists(dlqTopic) {
			r.mu.Lock()
			r.createdDLQs[dlqTopic] = true
			r.mu.Unlock()
			return nil
		}
		return err
	}

	r.mu.Lock()
	r.createdDLQs[dlqTopic] = true
	r.mu.Unlock()

	r.logger.Info("created DLQ topic",
		"topic", dlqTopic,
		"original_topic", originalTopic,
		"partitions", numPartitions,
		"retention_hours", r.config.DLQRetentionHours)

	return nil
}

// =============================================================================
// QUERY OPERATIONS
// =============================================================================

// GetDLQTopic returns the DLQ topic name for a given topic.
func (r *DLQRouter) GetDLQTopic(topic string) string {
	return r.config.DLQTopicName(topic)
}

// IsDLQTopic checks if a topic name is a DLQ topic.
func (r *DLQRouter) IsDLQTopic(topic string) bool {
	suffix := r.config.DLQSuffix
	if len(topic) <= len(suffix) {
		return false
	}
	return topic[len(topic)-len(suffix):] == suffix
}

// GetOriginalTopic returns the original topic name for a DLQ topic.
// Returns empty string if not a DLQ topic.
func (r *DLQRouter) GetOriginalTopic(dlqTopic string) string {
	if !r.IsDLQTopic(dlqTopic) {
		return ""
	}
	return dlqTopic[:len(dlqTopic)-len(r.config.DLQSuffix)]
}

// Stats returns DLQ router statistics.
func (r *DLQRouter) Stats() DLQStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Deep copy maps
	stats := DLQStats{
		TotalRouted:    r.stats.TotalRouted,
		CreateErrors:   r.stats.CreateErrors,
		PublishErrors:  r.stats.PublishErrors,
		RoutesByReason: make(map[DLQReason]int64),
		RoutesByTopic:  make(map[string]int64),
	}

	for k, v := range r.stats.RoutesByReason {
		stats.RoutesByReason[k] = v
	}
	for k, v := range r.stats.RoutesByTopic {
		stats.RoutesByTopic[k] = v
	}

	return stats
}

// =============================================================================
// DLQ MESSAGE OPERATIONS
// =============================================================================

// ParseDLQMessage parses a DLQ message from JSON.
//
// Useful for:
//   - Operators investigating failed messages
//   - Reprocessing tools
//   - DLQ consumer implementations
func ParseDLQMessage(data []byte) (*DLQMessage, error) {
	var msg DLQMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed to parse DLQ message: %w", err)
	}
	return &msg, nil
}

// =============================================================================
// REPROCESSING SUPPORT
// =============================================================================

// ReprocessRequest describes a request to reprocess DLQ messages.
type ReprocessRequest struct {
	// DLQTopic is the DLQ topic to read from
	DLQTopic string

	// TargetTopic is where to republish messages (usually the original topic)
	// If empty, uses the original_topic from DLQ message metadata
	TargetTopic string

	// FromOffset is the starting offset in the DLQ
	FromOffset int64

	// MaxMessages is the maximum number of messages to reprocess
	// 0 = no limit
	MaxMessages int

	// Filter is an optional function to filter which messages to reprocess
	// Return true to reprocess, false to skip
	Filter func(*DLQMessage) bool
}

// ReprocessResult contains the result of a reprocess operation.
type ReprocessResult struct {
	// Processed is the number of messages successfully reprocessed
	Processed int

	// Skipped is the number of messages filtered out
	Skipped int

	// Errors is the number of messages that failed to republish
	Errors int

	// LastOffset is the last DLQ offset processed
	LastOffset int64
}

// Reprocess republishes DLQ messages back to their original topic.
//
// USE CASES:
//   - Bug in consumer code was fixed, messages can now succeed
//   - Transient failure resolved, retry all failed messages
//   - Manual investigation complete, selected messages approved
//
// SAFETY CONSIDERATIONS:
//   - Messages will have new offsets in the target topic
//   - Deduplication is caller's responsibility
//   - Consider idempotency of message processing
//
// EXAMPLE:
//
//	result, err := router.Reprocess(ReprocessRequest{
//	    DLQTopic:    "orders.dlq",
//	    TargetTopic: "orders",
//	    FromOffset:  0,
//	    MaxMessages: 100,
//	    Filter: func(msg *DLQMessage) bool {
//	        return msg.Reason == DLQReasonMaxRetries // Skip rejected
//	    },
//	})
func (r *DLQRouter) Reprocess(req ReprocessRequest) (*ReprocessResult, error) {
	result := &ReprocessResult{}

	// Read messages from DLQ
	// TODD: why partition 0?
	messages, err := r.broker.Consume(req.DLQTopic, 0, req.FromOffset, req.MaxMessages)
	if err != nil {
		return nil, fmt.Errorf("failed to read from DLQ: %w", err)
	}

	for _, msg := range messages {
		result.LastOffset = msg.Offset

		// Parse DLQ message
		dlqMsg, err := ParseDLQMessage(msg.Value)
		if err != nil {
			r.logger.Error("failed to parse DLQ message",
				"dlq_topic", req.DLQTopic,
				"offset", msg.Offset,
				"error", err)
			result.Errors++
			continue
		}

		// Apply filter
		if req.Filter != nil && !req.Filter(dlqMsg) {
			result.Skipped++
			continue
		}

		// Determine target topic
		targetTopic := req.TargetTopic
		if targetTopic == "" {
			targetTopic = dlqMsg.OriginalTopic
		}

		// Republish original message
		_, _, err = r.broker.Publish(targetTopic, dlqMsg.OriginalKey, dlqMsg.OriginalValue)
		if err != nil {
			r.logger.Error("failed to republish DLQ message",
				"target_topic", targetTopic,
				"original_offset", dlqMsg.OriginalOffset,
				"error", err)
			result.Errors++
			continue
		}

		result.Processed++
	}

	r.logger.Info("DLQ reprocess complete",
		"dlq_topic", req.DLQTopic,
		"processed", result.Processed,
		"skipped", result.Skipped,
		"errors", result.Errors,
		"last_offset", result.LastOffset)

	return result, nil
}
