package broker

import (
	"errors"
	"fmt"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// MESSAGE TYPE (API representation)
// =============================================================================

// Message is the API representation of a message.
// This is what consumers receive (includes topic/partition context).
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Priority  storage.Priority // Priority level (0=Critical to 4=Background)
}

// =============================================================================
// BROKER METADATA
// =============================================================================

// Stats returns broker statistics.
type BrokerStats struct {
	NodeID     string
	Uptime     time.Duration
	TopicCount int
	TotalSize  int64
	TopicStats map[string]TopicStats
}

type TopicStats struct {
	Name          string
	Partitions    int
	TotalMessages int64
	TotalSize     int64
}

// =============================================================================
// PRIORITY STATISTICS (MILESTONE 6)
// =============================================================================
//
// Per-priority-per-partition metrics provide maximum granularity for:
//   - Understanding message distribution across priorities
//   - Monitoring queue health at the priority level
//   - Detecting priority imbalances or starvation
//   - Capacity planning based on priority patterns
//
// STRUCTURE:
//   BrokerPriorityStats
//     └── TopicPriorityStats (per topic)
//           └── PartitionPriorityStats (per partition)
//                 └── PriorityLevelStats (per priority level)
//
// COMPARISON WITH OTHER SYSTEMS:
//   - RabbitMQ: Queue-level priority stats only (no partition concept)
//   - Kafka: No native priority support
//   - SQS: Per-queue stats, no priority breakdown
//   - goqueue: Full per-priority-per-partition granularity
//
// =============================================================================

// BrokerPriorityStats provides aggregated priority statistics across all topics.
type BrokerPriorityStats struct {
	// TotalByPriority aggregates message counts across all topics/partitions
	TotalByPriority [5]int64

	// Topics maps topic name to its priority stats
	Topics map[string]*TopicPriorityStats
}

// TopicPriorityStats provides priority statistics for a single topic.
type TopicPriorityStats struct {
	Name string

	// TotalByPriority aggregates message counts across all partitions
	TotalByPriority [5]int64

	// Partitions maps partition ID to its priority stats
	Partitions map[int]*PartitionPriorityStats
}

// PartitionPriorityStats provides detailed priority metrics for a partition.
type PartitionPriorityStats struct {
	PartitionID int

	// Pending counts unconsumed messages at each priority level
	Pending [5]int64

	// Consumed counts messages marked as consumed at each priority
	Consumed [5]int64

	// Total is Pending + Consumed for each priority
	Total [5]int64

	// OldestPending tracks the timestamp of oldest pending message per priority
	// Zero time means no pending messages at that priority
	OldestPending [5]time.Time

	// AvgWaitTime tracks average wait time for consumed messages per priority
	// This helps identify if lower priorities are experiencing starvation
	AvgWaitTime [5]time.Duration
}

func (b *Broker) Stats() BrokerStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := BrokerStats{
		NodeID:     b.config.NodeID,
		Uptime:     time.Since(b.startedAt),
		TopicCount: len(b.topics),
		TopicStats: make(map[string]TopicStats),
	}

	for name, topic := range b.topics {
		ts := TopicStats{
			Name:          name,
			Partitions:    topic.NumPartitions(),
			TotalMessages: topic.TotalMessages(),
			TotalSize:     topic.TotalSize(),
		}
		stats.TopicStats[name] = ts
		stats.TotalSize += ts.TotalSize
	}

	return stats
}

// =============================================================================
// PriorityStats - Per-Priority-Per-Partition Statistics
// =============================================================================
//
// WHAT: Collects detailed priority metrics from all topics and partitions.
//
// WHY: The user requested "per priority per partition" metrics - the most
// granular option. This enables:
//   - Monitoring priority distribution across the cluster
//   - Detecting priority starvation before it impacts SLAs
//   - Understanding message flow patterns by priority
//   - Capacity planning based on priority usage
//
// HOW IT WORKS:
//  1. Iterates through all topics
//  2. For each topic, iterates through all partitions
//  3. Collects PriorityMetrics from each partition's priority index
//  4. Aggregates up to topic and broker levels
//
// PERFORMANCE NOTE:
// This method acquires read locks and iterates the entire broker state.
// For large deployments, consider caching or sampling strategies.
//
// =============================================================================
func (b *Broker) PriorityStats() BrokerPriorityStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := BrokerPriorityStats{
		Topics: make(map[string]*TopicPriorityStats),
	}

	for name, topic := range b.topics {
		topicStats := &TopicPriorityStats{
			Name:       name,
			Partitions: make(map[int]*PartitionPriorityStats),
		}

		// Collect stats from each partition
		numPartitions := topic.NumPartitions()
		for i := 0; i < numPartitions; i++ {
			partition, err := topic.Partition(i)
			if err != nil || partition == nil {
				continue
			}

			// Get priority metrics from the partition's priority index
			// Returns a slice with one snapshot per priority level
			metricsSlice := partition.PriorityMetrics()

			partitionStats := &PartitionPriorityStats{
				PartitionID: i,
			}

			// Process each priority level's metrics
			for _, m := range metricsSlice {
				p := int(m.Priority)
				if p < 0 || p >= 5 {
					continue
				}

				// Pending = unconsumed messages at this priority
				partitionStats.Pending[p] = int64(m.PendingMessages)

				// Total = all-time message count at this priority
				partitionStats.Total[p] = m.TotalMessages

				// Consumed: derived from Total minus Pending
				partitionStats.Consumed[p] = m.TotalMessages - int64(m.PendingMessages)

				// OldestPending timestamp conversion
				if m.OldestPendingTimestamp > 0 {
					partitionStats.OldestPending[p] = time.Unix(0, m.OldestPendingTimestamp)
				}

				// Aggregate to topic level
				topicStats.TotalByPriority[p] += m.TotalMessages
			}

			topicStats.Partitions[i] = partitionStats
		}

		// Aggregate to broker level
		for p := 0; p < 5; p++ {
			stats.TotalByPriority[p] += topicStats.TotalByPriority[p]
		}

		stats.Topics[name] = topicStats
	}

	return stats
}

// NodeID returns the broker's node identifier.
func (b *Broker) NodeID() string {
	return b.config.NodeID
}

// DataDir returns the data directory path.
func (b *Broker) DataDir() string {
	return b.config.DataDir
}

// GroupCoordinator returns the broker's consumer group coordinator.
// Used by the API layer for consumer group operations.
func (b *Broker) GroupCoordinator() *GroupCoordinator {
	return b.groupCoordinator
}

// CooperativeGroupCoordinator returns the broker's cooperative group coordinator.
// Used by the API layer for cooperative rebalancing operations (M12).
// Returns nil if cooperative rebalancing is not enabled.
func (b *Broker) CooperativeGroupCoordinator() *CooperativeGroupCoordinator {
	return b.cooperativeGroupCoordinator
}

// Uptime returns how long the broker has been running.
func (b *Broker) Uptime() time.Duration {
	return time.Since(b.startedAt)
}

// =============================================================================
// MILESTONE 4: RELIABILITY LAYER API
// =============================================================================
//
// These methods provide per-message acknowledgment (ACK/NACK/REJECT) on top
// of the Kafka-style offset-based consumption model.
//
// FLOW COMPARISON:
//
//   KAFKA (offset-only):
//   ┌────────┐  poll  ┌────────┐ process ┌────────┐ commit ┌────────┐
//   │Consumer│───────►│Receives│────────►│Process │───────►│Commit  │
//   │        │        │batch   │         │all     │        │offset  │
//   └────────┘        └────────┘         └────────┘        └────────┘
//
//   GOQUEUE (per-message ACK):
//   ┌────────┐  poll  ┌────────┐ process ┌────────┐ ack    ┌────────┐
//   │Consumer│───────►│Receives│────────►│Process │───────►│ACK each│
//   │        │        │+receipt│         │one msg │        │message │
//   └────────┘        └────────┘         └────────┘        └────────┘
//                                              │                │
//                                              │ fail           │ offset
//                                              ▼                │ advances
//                                         ┌────────┐            │
//                                         │NACK/   │────────────┘
//                                         │Reject  │
//                                         └────────┘
//
// =============================================================================

// AckManager returns the broker's ACK manager for per-message acknowledgment.
// Used by the API layer for ACK/NACK/REJECT operations.
func (b *Broker) AckManager() *AckManager {
	return b.ackManager
}

// Tracer returns the broker's message tracer for observability.
// Used by the API layer for trace query operations.
func (b *Broker) Tracer() *Tracer {
	return b.tracer
}

// ReliabilityConfig returns the current reliability configuration.
func (b *Broker) ReliabilityConfig() ReliabilityConfig {
	return b.reliabilityConfig
}

// ConsumeWithReceipts reads messages and tracks them for per-message ACK.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset
//   - maxMessages: Max messages to return
//   - consumerID, groupID: Consumer identification for tracking
//
// RETURNS:
//   - Messages with receipt handles attached
//   - Error if read fails
//
// IMPORTANT:
// Each returned message has a ReceiptHandle that MUST be used for ACK/NACK/REJECT.
// Messages not ACKed within VisibilityTimeout will be redelivered.
func (b *Broker) ConsumeWithReceipts(
	topic string,
	partition int,
	fromOffset int64,
	maxMessages int,
	consumerID, groupID string,
) ([]MessageWithReceipt, error) {
	// First, get the raw messages using existing Consume method
	messages, err := b.Consume(topic, partition, fromOffset, maxMessages)
	if err != nil {
		return nil, err
	}

	// Track each message for per-message ACK and generate receipt handles
	results := make([]MessageWithReceipt, 0, len(messages))
	visibilityTimeout := time.Duration(b.reliabilityConfig.VisibilityTimeoutMs) * time.Millisecond

	for _, msg := range messages {
		// Track delivery and get receipt handle
		receiptHandle, err := b.ackManager.TrackDelivery(&msg, consumerID, groupID, visibilityTimeout)
		if err != nil {
			// Backpressure or tracking error - stop here
			b.logger.Warn("failed to track delivery",
				"topic", topic,
				"partition", partition,
				"offset", msg.Offset,
				"error", err)
			break
		}

		results = append(results, MessageWithReceipt{
			Message:       msg,
			ReceiptHandle: receiptHandle,
		})
	}

	return results, nil
}

// MessageWithReceipt extends Message with a receipt handle for ACK/NACK/REJECT.
type MessageWithReceipt struct {
	Message
	ReceiptHandle string
}

// Ack acknowledges successful processing of a message.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//
// SEMANTICS:
//   - Message is considered fully processed
//   - Will not be redelivered
//   - Committed offset may advance (if contiguous)
func (b *Broker) Ack(receiptHandle string) (*AckResult, error) {
	ackStart := time.Now()
	result, err := b.ackManager.Ack(receiptHandle)
	if err != nil {
		return nil, err
	}

	// Record consume.acked span
	ctx := b.tracer.StartTrace(result.Topic, result.Partition, result.Offset)
	if !ctx.TraceID.IsZero() {
		span := NewSpan(ctx.TraceID, SpanEventConsumeAcked, result.Topic, result.Partition, result.Offset)
		span.WithAttribute("new_committed_offset", fmt.Sprintf("%d", result.NewCommittedOffset))
		span.WithAttribute("offset_advanced", fmt.Sprintf("%t", result.OffsetAdvanced))
		b.tracer.RecordSpan(span)
	}

	// METRICS: Record acknowledgment
	// consumerGroup is empty here - group context comes from ConsumerGroup
	InstrumentAck(result.Topic, "", ackStart)

	return result, nil
}

// Nack indicates processing failed and message should be retried.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//   - reason: Why the message failed (for logging/debugging)
//
// SEMANTICS:
//   - Message will be redelivered after exponential backoff
//   - Each NACK increments delivery count
//   - After MaxRetries, message goes to DLQ
func (b *Broker) Nack(receiptHandle, reason string) (*AckResult, error) {
	nackStart := time.Now()
	result, err := b.ackManager.Nack(receiptHandle, reason)
	if err != nil {
		return nil, err
	}

	// Record consume.nacked span
	ctx := b.tracer.StartTrace(result.Topic, result.Partition, result.Offset)
	if !ctx.TraceID.IsZero() {
		span := NewSpan(ctx.TraceID, SpanEventConsumeNacked, result.Topic, result.Partition, result.Offset)
		span.WithAttribute("reason", reason)
		span.WithAttribute("delivery_count", fmt.Sprintf("%d", result.DeliveryCount))
		if !result.NextVisibleAt.IsZero() {
			span.WithAttribute("next_visible_at", result.NextVisibleAt.Format(time.RFC3339))
		}
		b.tracer.RecordSpan(span)
	}

	// METRICS: Record negative acknowledgment
	InstrumentNack(result.Topic, "", nackStart)

	return result, nil
}

// Reject sends a message directly to the dead letter queue.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//   - reason: Why the message was rejected
//
// SEMANTICS:
//   - Message is considered "poison" (can never succeed)
//   - Immediately routed to DLQ (no retry)
//
// USE CASES:
//   - Message format is invalid
//   - Business logic determines message is unprocessable
func (b *Broker) Reject(receiptHandle, reason string) (*AckResult, error) {
	rejectStart := time.Now()
	result, err := b.ackManager.Reject(receiptHandle, reason)
	if err != nil {
		return nil, err
	}

	// Record consume.rejected span
	ctx := b.tracer.StartTrace(result.Topic, result.Partition, result.Offset)
	if !ctx.TraceID.IsZero() {
		span := NewSpan(ctx.TraceID, SpanEventConsumeRejected, result.Topic, result.Partition, result.Offset)
		span.WithAttribute("reason", reason)
		if result.DLQTopic != "" {
			span.WithAttribute("dlq_topic", result.DLQTopic)
		}
		b.tracer.RecordSpan(span)
	}

	// METRICS: Record message rejection (sent to DLQ)
	InstrumentReject(result.Topic, "", rejectStart)

	return result, nil
}

// ExtendVisibility extends the visibility timeout for a message.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//   - extension: Additional time to add
//
// USE CASE:
//   - Processing takes longer than expected
//   - Prevents timeout while still working on message
//
// EXAMPLE:
//
//	// Processing will take longer than 30s visibility timeout
//	if estimatedTime > 25*time.Second {
//	    broker.ExtendVisibility(receipt, 30*time.Second)
//	}
func (b *Broker) ExtendVisibility(receiptHandle string, extension time.Duration) (time.Time, error) {
	return b.ackManager.ExtendVisibility(receiptHandle, extension)
}

// GetConsumerLag returns lag information for a consumer.
func (b *Broker) GetConsumerLag(consumerID, groupID, topic string, partition int) (*ConsumerLag, error) {
	return b.ackManager.GetConsumerLag(consumerID, groupID, topic, partition)
}

// ReliabilityStats returns combined reliability layer statistics.
type ReliabilityStats struct {
	AckManager AckManagerStats
	Visibility VisibilityStats
	DLQ        DLQStats
}

func (b *Broker) ReliabilityStats() ReliabilityStats {
	return ReliabilityStats{
		AckManager: b.ackManager.Stats(),
		Visibility: b.ackManager.visibilityTracker.Stats(),
		DLQ:        b.ackManager.dlqRouter.Stats(),
	}
}

// =============================================================================
// MILESTONE 5: DELAYED MESSAGES API
// =============================================================================
//
// These methods provide delayed and scheduled message delivery capabilities.
// Messages can be published with a delay (relative) or deliverAt (absolute time).
//
// WHY DELAYED MESSAGES?
//
//   Many use cases require messages to be delivered at a future time:
//   - Scheduled tasks: "Send reminder email in 24 hours"
//   - Rate limiting: "Retry this API call in 30 seconds"
//   - Business logic: "Execute trade at market open"
//   - Debouncing: "Wait 5 seconds for more updates before processing"
//
// COMPARISON:
//
//   - Kafka: No native delay support - requires external schedulers or hacky
//     solutions like long-lived consumers that hold messages
//   - RabbitMQ: Plugin required (rabbitmq_delayed_message_exchange) with limits
//   - SQS: DelaySeconds (0-900s = 15 min max) and message timers (up to 15 min)
//   - Redis: ZADD with score as timestamp, poll for ready messages
//   - goqueue: Native support up to 7 days with millisecond precision
//
// FLOW:
//
//   PublishWithDelay(topic, key, value, delay=30s)
//   ┌────────────┐
//   │ Producer   │
//   └─────┬──────┘
//         │
//         ▼
//   ┌────────────────────────────────────────────────────────────────────┐
//   │ 1. Write to Log (immediate, ensures durability)                    │
//   │    → Returns: partition=2, offset=1234                             │
//   └─────┬──────────────────────────────────────────────────────────────┘
//         │
//         ▼
//   ┌────────────────────────────────────────────────────────────────────┐
//   │ 2. Register with Scheduler                                         │
//   │    → Timer wheel entry (in-memory, O(1))                           │
//   │    → Delay index entry (on-disk, crash-safe)                       │
//   └─────┬──────────────────────────────────────────────────────────────┘
//         │
//         │ ... time passes (30 seconds) ...
//         │
//         ▼
//   ┌────────────────────────────────────────────────────────────────────┐
//   │ 3. Timer Fires → handleDelayedMessageReady()                       │
//   │    → Message marked as visible in delay index                      │
//   │    → Normal consumers now see this message                         │
//   └────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

// PublishWithDelay publishes a message that becomes visible after the specified delay.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//   - delay: Duration before message becomes visible (max 7 days)
//
// RETURNS:
//   - Partition the message was written to
//   - Offset within that partition
//   - Error if publish fails or delay is invalid
//
// SEMANTICS:
//   - Message is written immediately to the log (durable from time of call)
//   - Message is hidden from consumers until delay expires
//   - If broker crashes and restarts, pending delays are recovered
//   - Delay resolution is ~10ms (timer wheel tick interval)
//
// EXAMPLE:
//
//	// Send reminder email in 24 hours
//	partition, offset, err := broker.PublishWithDelay(
//	    "email-reminders",
//	    []byte("user-123"),
//	    []byte(`{"type":"reminder","message":"Don't forget!"}`),
//	    24 * time.Hour,
//	)
func (b *Broker) PublishWithDelay(topic string, key, value []byte, delay time.Duration) (partition int, offset int64, err error) {
	// Calculate absolute delivery time from relative delay
	deliverAt := time.Now().Add(delay)
	return b.PublishAt(topic, key, value, deliverAt)
}

// PublishAt publishes a message that becomes visible at the specified time.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//   - deliverAt: Absolute time when message becomes visible
//
// RETURNS:
//   - Partition the message was written to
//   - Offset within that partition
//   - Error if publish fails or deliverAt is invalid
//
// SEMANTICS:
//   - If deliverAt is in the past, message is delivered immediately
//   - If deliverAt is more than MaxDelay in the future, error returned
//
// USE CASES:
//   - Scheduled jobs: "Run this at 9am Monday"
//   - Market operations: "Execute at market open"
//   - Time-zone aware: "Send at 10am user's local time"
//
// EXAMPLE:
//
//	// Execute trade at market open (9:30 AM ET)
//	marketOpen := time.Date(2024, 1, 15, 9, 30, 0, 0, nyLocation)
//	partition, offset, err := broker.PublishAt(
//	    "trades",
//	    []byte("AAPL"),
//	    []byte(`{"action":"buy","shares":100}`),
//	    marketOpen,
//	)
func (b *Broker) PublishAt(topic string, key, value []byte, deliverAt time.Time) (partition int, offset int64, err error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, 0, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	// Calculate delay duration for validation
	delay := time.Until(deliverAt)

	// If deliverAt is in the past or very near future, publish normally
	if delay <= 0 {
		return t.Publish(key, value)
	}

	// Check max delay limit
	if delay > b.scheduler.config.MaxDelay {
		return 0, 0, fmt.Errorf("delay %v exceeds maximum %v", delay, b.scheduler.config.MaxDelay)
	}

	// Step 1: Write message to log immediately (ensures durability)
	partition, offset, err = t.Publish(key, value)
	if err != nil {
		return 0, 0, err
	}

	// Step 2: Register with scheduler (timer + delay index)
	err = b.scheduler.ScheduleAt(topic, partition, offset, deliverAt)
	if err != nil {
		// Message is written but not scheduled - it will be visible immediately
		// This is acceptable as it's "at least once" delivery
		b.logger.Warn("failed to schedule delayed message, will be visible immediately",
			"topic", topic,
			"partition", partition,
			"offset", offset,
			"error", err)
		// Don't return error - message is published, just not delayed
	}

	b.logger.Debug("published delayed message",
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"deliver_at", deliverAt.Format(time.RFC3339),
		"delay", delay.String())

	return partition, offset, nil
}

// PublishWithDelayAndPriority publishes a delayed message with a specified priority.
//
// MILESTONE 5+6 INTEGRATION:
// This combines delayed delivery (M5) with priority queuing (M6). The message
// is written immediately with its priority, but remains hidden until the delay
// expires. When it becomes visible, it enters the priority-aware consumption.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//   - delay: Duration before message becomes visible (max 7 days)
//   - priority: Message priority level (Critical/High/Normal/Low/Background)
//
// RETURNS:
//   - Partition the message was written to
//   - Offset within that partition
//   - Error if publish fails, delay is invalid, or priority is invalid
//
// FLOW:
//
//	┌──────────┐                      ┌──────────────────┐
//	│ Producer │ PublishWithDelay.  ..│ Write with       │
//	│          │ ──────────────────►  │ Priority to Log  │
//	└──────────┘                      └────────┬─────────┘
//	                                           │
//	                                           ▼
//	                                  ┌──────────────────┐
//	                                  │ Register with    │
//	                                  │ Scheduler        │
//	                                  └────────┬─────────┘
//	                                           │
//	                                     delay expires
//	                                           │
//	                                           ▼
//	                                  ┌──────────────────┐
//	                                  │ Visible with     │
//	                                  │ Priority         │
//	                                  └──────────────────┘
//
// EXAMPLE:
//
//	// High-priority payment retry in 30 seconds
//	partition, offset, err := broker.PublishWithDelayAndPriority(
//	    "payments",
//	    []byte("order-123"),
//	    []byte(`{"action":"retry","amount":99.99}`),
//	    30 * time.Second,
//	    storage.PriorityHigh,
//	)
func (b *Broker) PublishWithDelayAndPriority(topic string, key, value []byte, delay time.Duration, priority storage.Priority) (partition int, offset int64, err error) {
	deliverAt := time.Now().Add(delay)
	return b.PublishAtWithPriority(topic, key, value, deliverAt, priority)
}

// PublishAtWithPriority publishes a message with priority that becomes visible at a specific time.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//   - deliverAt: Absolute time when message becomes visible
//   - priority: Message priority level
//
// RETURNS:
//   - Partition the message was written to
//   - Offset within that partition
//   - Error if publish fails, deliverAt is invalid, or priority is invalid
//
// SEMANTICS:
//   - If deliverAt is in the past, message is delivered immediately with priority
//   - Message priority is stored in the log at write time
//   - When delay expires, message enters priority-aware consumption
//
// USE CASES:
//   - Critical alerts scheduled for specific times
//   - High-priority batch jobs at off-peak hours
//   - Priority-based retry with specific retry times
func (b *Broker) PublishAtWithPriority(topic string, key, value []byte, deliverAt time.Time, priority storage.Priority) (partition int, offset int64, err error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, 0, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	// Validate priority
	if priority > storage.PriorityBackground {
		return 0, 0, fmt.Errorf("invalid priority: %d", priority)
	}

	// Calculate delay duration for validation
	delay := time.Until(deliverAt)

	// If deliverAt is in the past or very near future, publish with priority immediately
	// Uses broker method which handles cluster forwarding
	if delay <= 0 {
		return b.PublishWithPriority(topic, key, value, priority)
	}

	// Check max delay limit
	if delay > b.scheduler.config.MaxDelay {
		return 0, 0, fmt.Errorf("delay %v exceeds maximum %v", delay, b.scheduler.config.MaxDelay)
	}

	// =========================================================================
	// CLUSTER MODE: LEADERSHIP CHECK AND REQUEST FORWARDING
	// =========================================================================
	// For delayed messages, we need to determine the partition first, then check
	// leadership. The delay scheduling must happen on the leader node.
	partition = t.DeterminePartition(key)

	if b.clusterCoordinator != nil {
		isLeader := b.clusterCoordinator.IsLeaderFor(topic, partition)
		b.logger.Info("leadership check for delayed publish",
			"topic", topic,
			"partition", partition,
			"is_leader", isLeader,
			"deliver_at", deliverAt.Format(time.RFC3339))

		if !isLeader {
			// For delayed messages, we can't simply forward via HTTP because
			// the leader needs to register with its local scheduler.
			// TODO: Implement delayed message forwarding with scheduler registration
			// For now, we log and return an error (client should retry to leader)
			leaderAddr := b.clusterCoordinator.GetLeaderClientAddress(topic, partition)
			b.logger.Warn("delayed publish to non-leader not yet supported, client should publish to leader",
				"topic", topic,
				"partition", partition,
				"leader", leaderAddr)
			return 0, 0, fmt.Errorf("delayed publish must be sent to partition leader at %s", leaderAddr)
		}
	}

	// We ARE the leader - write message to log immediately WITH PRIORITY
	partition, offset, err = t.PublishWithPriority(key, value, priority)
	if err != nil {
		return 0, 0, err
	}

	// Step 2: Register with scheduler (timer + delay index)
	err = b.scheduler.ScheduleAt(topic, partition, offset, deliverAt)
	if err != nil {
		// Message is written but not scheduled - it will be visible immediately
		b.logger.Warn("failed to schedule delayed message with priority, will be visible immediately",
			"topic", topic,
			"partition", partition,
			"offset", offset,
			"priority", priority,
			"error", err)
	}

	b.logger.Debug("published delayed message with priority",
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"priority", priority,
		"deliver_at", deliverAt.Format(time.RFC3339),
		"delay", delay.String())

	return partition, offset, nil
}

// handleDelayedMessageReady is called when a delayed message timer expires.
// This makes the message visible to consumers.
//
// INTERNAL CALLBACK:
// Called by the scheduler when timer fires. The message is already in the log;
// this callback updates the delay index to mark it as delivered.
//
// FILTERING: Consumer methods (Consume, ConsumeByOffset) check IsDelayed()
// which uses the delay index to filter out pending delayed messages.
func (b *Broker) handleDelayedMessageReady(topic string, partition int, offset int64) error {
	b.logger.Debug("delayed message ready",
		"topic", topic,
		"partition", partition,
		"offset", offset)

	// The message is already in the log. The delay index tracks its state.
	// The scheduler has already marked this entry as DELIVERED in the delay index.
	// Consumer filtering is implemented in Consume() and ConsumeByOffset() methods
	// which call b.IsDelayed() to skip pending delayed messages.

	return nil
}

// CancelDelayed cancels a pending delayed message.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - offset: Message offset
//
// RETURNS:
//   - true if message was canceled
//   - false if message was already delivered, canceled, or not found
//
// SEMANTICS:
//   - Canceled messages are never delivered to consumers
//   - Cancellation is permanent (cannot be un-canceled)
//   - The message data still exists in the log but is marked canceled
//
// USE CASES:
//   - User cancels a scheduled email
//   - Order is modified before scheduled processing
//   - Duplicate prevention (cancel earlier version)
func (b *Broker) CancelDelayed(topic string, partition int, offset int64) (bool, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return false, ErrBrokerClosed
	}
	b.mu.RUnlock()

	err := b.scheduler.Cancel(topic, partition, offset)
	if err != nil {
		if errors.Is(err, ErrDelayedMessageNotFound) {
			return false, nil
		}
		if errors.Is(err, ErrDelayedMessageNotPending) {
			// Idempotency: already delivered/canceled/expired isn't an error.
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// IsDelayed checks if a message is currently delayed (not yet delivered).
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - offset: Message offset
//
// RETURNS:
//   - true if message is pending delivery
//   - false if delivered, canceled, expired, or not a delayed message
func (b *Broker) IsDelayed(topic string, partition int, offset int64) bool {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return false
	}
	b.mu.RUnlock()

	return b.scheduler.IsDelayed(topic, partition, offset)
}

// GetDelayedMessages returns pending delayed messages for a topic.
//
// PARAMETERS:
//   - topic: Topic name
//   - limit: Maximum messages to return (0 = default 100)
//   - skip: Number of messages to skip (for pagination)
//
// RETURNS:
//   - Slice of scheduled messages with their delivery times
func (b *Broker) GetDelayedMessages(topic string, limit, skip int) ([]*ScheduledMessage, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}
	b.mu.RUnlock()

	return b.scheduler.GetDelayedMessages(topic, limit, skip), nil
}

// GetDelayedMessage returns details about a specific delayed message.
func (b *Broker) GetDelayedMessage(topic string, partition int, offset int64) (*ScheduledMessage, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}
	b.mu.RUnlock()

	return b.scheduler.GetDelayedMessage(topic, partition, offset)
}

// Scheduler returns the broker's delay scheduler.
// Used by the API layer for delay-related operations.
func (b *Broker) Scheduler() *Scheduler {
	return b.scheduler
}

// DelayStats holds statistics for the delay/scheduling system.
type DelayStats struct {
	TotalScheduled uint64           `json:"total_scheduled"`
	TotalDelivered uint64           `json:"total_delivered"`
	TotalCanceled  uint64           `json:"total_canceled"`
	TotalPending   int64            `json:"total_pending"`
	ByTopic        map[string]int64 `json:"by_topic"`
	TimerWheel     TimerWheelStats  `json:"timer_wheel"`
}

// DelayStats returns statistics about the delay scheduling system.
func (b *Broker) DelayStats() DelayStats {
	if b.scheduler == nil {
		return DelayStats{}
	}

	stats := b.scheduler.Stats()
	return DelayStats{
		TotalScheduled: stats.TotalScheduled,
		TotalDelivered: stats.TotalDelivered,
		TotalCanceled:  stats.TotalCanceled,
		TotalPending:   stats.TotalPending,
		ByTopic:        stats.ByTopic,
		TimerWheel:     b.scheduler.timerWheel.Stats(),
	}
}

// =============================================================================
// SCHEMA REGISTRY INTERFACE (M8)
// =============================================================================

// SchemaRegistry returns the broker's schema registry.
// Used by the API layer for schema operations.
func (b *Broker) SchemaRegistry() *SchemaRegistry {
	return b.schemaRegistry
}

// SchemaStats returns statistics about the schema registry.
func (b *Broker) SchemaStats() SchemaRegistryStats {
	return b.schemaRegistry.Stats()
}

// =============================================================================
// TIME-BASED OFFSET LOOKUP (M14)
// =============================================================================
//
// GetOffsetByTimestamp finds the offset for a message at or after a given timestamp.
//
// This is used by the offset reset service to implement timestamp-based resets:
//   - "Reset to messages from 2 hours ago"
//   - "Reprocess from 9am this morning"
//
// DESIGN PATTERN:
// Instead of having the gRPC service layer handle topic/partition lookups,
// we encapsulate all orchestration in the broker. This follows the same
// pattern as PublishMessage():
//
//	gRPC Service              Broker                    Topic/Partition
//	─────────────────────────────────────────────────────────────────
//	Publish(topic, msg) ──► ProduceMessage()
//	                          └─► GetTopic()
//	                          └─► Partition()
//	                          └─► ProduceMessage()
//
//	GetOffsetByTimestamp() ──► GetOffsetByTimestamp()
//	(topic, partition,         └─► GetTopic()
//	 timestamp)                └─► Partition()
//	                           └─► GetOffsetByTimestamp()
//
// BENEFITS:
//   - Single point of call from gRPC layer
//   - Consistent error handling
//   - Easier to test
//   - Reduces coupling between layers
//
// RETURNS:
//   - The offset of the first message with timestamp >= given timestamp
//   - Error if topic/partition not found or timestamp lookup fails
func (b *Broker) GetOffsetByTimestamp(topic string, partition int, timestamp int64) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return 0, ErrBrokerClosed
	}

	// Get topic
	t, ok := b.topics[topic]
	if !ok {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	// Get partition
	p, err := t.Partition(partition)
	if err != nil {
		return 0, fmt.Errorf("partition not found: %w", err)
	}

	// Get offset by timestamp
	offset, err := p.GetOffsetByTimestamp(timestamp)
	if err != nil {
		return 0, fmt.Errorf("timestamp lookup failed: %w", err)
	}

	return offset, nil
}
