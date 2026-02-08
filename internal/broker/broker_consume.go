package broker

import (
	"fmt"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// CONSUMER INTERFACE
// =============================================================================

// Consume reads messages from a topic partition in priority order.
//
// DEFAULT BEHAVIOR: Messages are returned highest-priority-first.
//   - Priority 0 (Critical) before Priority 1 (High)
//   - Within same priority, FIFO order is maintained
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset (inclusive) - message at this offset can be returned
//   - maxMessages: Max messages to return (0 = no limit)
//
// OFFSET SEMANTICS: All consume methods use INCLUSIVE offsets.
//
//	fromOffset = 0 → returns message at offset 0 (if exists)
//	fromOffset = 5 → returns messages starting from offset 5
//
// RETURNS:
//   - Slice of messages (may be empty if no new messages)
//   - Error if read fails
//
// For offset-sequential consumption (Kafka-like FIFO), use ConsumeByOffset().
//
// FILTERING (read_committed isolation):
// This method filters out messages that should not be visible to consumers:
//  1. Control records (transaction markers) - handled in Partition.Consume
//  2. Delayed messages - messages scheduled for future delivery
//  3. Uncommitted transactions - messages from in-progress transactions
//
// This provides read_committed semantics: consumers only see messages from
// committed transactions. The uncommittedTracker maintains offsets belonging
// to active transactions and filters them during consume.
func (b *Broker) Consume(topic string, partition int, fromOffset int64, maxMessages int) ([]Message, error) {
	// =========================================================================
	// METRICS: Start timing for latency measurement
	// =========================================================================
	consumeStart := time.Now()

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	// =========================================================================
	// LOOP-BASED FETCH WITH FILTERING
	// =========================================================================
	//
	// PROBLEM: After filtering (control records, delayed, uncommitted, aborted),
	//          we might return fewer messages than requested.
	//
	// SOLUTION: Fetch in a loop with multiplier until we have enough messages.
	//           - fetchMultiplier = 3 (fetch 3x more than needed)
	//           - maxAttempts = 5 (avoid infinite loops on sparse partitions)
	//
	// EXAMPLE:
	//   Consumer requests 10 messages
	//   → Fetch 30 from storage
	//   → After filtering: 7 messages remain
	//   → Fetch another 9 messages (3x the shortfall)
	//   → After filtering: 10 total messages
	//   → Return 10 to consumer
	//
	// FILTERS APPLIED:
	//   1. Control records (COMMIT/ABORT markers)
	//   2. Delayed messages (deliverAt > now)
	//   3. Uncommitted transactions (read_committed isolation)
	//   4. Aborted transactions (permanently hidden)
	//
	// =========================================================================

	const maxAttempts = 5
	const fetchMultiplier = 2

	var allFiltered []*storage.Message
	currentOffset := fromOffset

	for attempt := 0; attempt < maxAttempts && len(allFiltered) < maxMessages; attempt++ {
		// Calculate how many more messages we need
		needed := maxMessages - len(allFiltered)
		fetchSize := needed * fetchMultiplier

		// Fetch from storage
		storageMessages, err := t.Consume(partition, currentOffset, fetchSize)
		if err != nil {
			return nil, err
		}

		// No more messages available
		if len(storageMessages) == 0 {
			break
		}

		// Apply filters to this batch
		for _, sm := range storageMessages {
			// Filter out control records (commit/abort markers)
			if sm.IsControlRecord() {
				continue
			}
			// Check if delayed
			if b.IsDelayed(topic, partition, sm.Offset) {
				continue
			}
			// Check uncommitted transaction
			if b.uncommittedTracker != nil && b.uncommittedTracker.IsUncommitted(topic, partition, sm.Offset) {
				continue
			}
			// Check aborted transaction
			if b.abortedTracker != nil && b.abortedTracker.IsAborted(topic, partition, sm.Offset) {
				continue
			}
			allFiltered = append(allFiltered, sm)

			// Stop if we have enough
			if len(allFiltered) >= maxMessages {
				break
			}
		}

		// Update offset for next iteration
		if len(storageMessages) > 0 {
			currentOffset = storageMessages[len(storageMessages)-1].Offset + 1
		}
	}

	// Limit to requested amount (in case we got more)
	filteredMessages := allFiltered
	if len(filteredMessages) > maxMessages {
		filteredMessages = filteredMessages[:maxMessages]
	}

	// Convert storage.Message to broker.Message for API
	messages := make([]Message, len(filteredMessages))
	for i, sm := range filteredMessages {
		messages[i] = Message{
			Topic:     topic,
			Partition: partition,
			Offset:    sm.Offset,
			Timestamp: time.Unix(0, sm.Timestamp),
			Key:       sm.Key,
			Value:     sm.Value,
			Priority:  sm.Priority,
		}

		// =====================================================================
		// TRACE CONTINUITY: Extract traceparent from stored message headers
		// =====================================================================
		//
		// WHY:
		// End-to-end tracing requires that the consume span links back to the
		// same TraceID as the publish span. The traceparent header was injected
		// during publish (M25) and stored on disk with the message.
		//
		// FLOW:
		//   Publish:  traceparent → message headers → disk
		//   Consume:  disk → message headers → extract traceparent → child span
		//
		// COMPARISON:
		//   - Kafka: Consumer interceptors extract headers for trace propagation
		//   - SQS: X-Ray SDK extracts trace from system attributes
		//   - goqueue: Native header extraction (no external SDK needed)
		//
		// =====================================================================
		var consumeTraceCtx TraceContext
		if traceparent, ok := sm.Headers["traceparent"]; ok && traceparent != "" {
			// Continue the trace from publish - extract the stored context
			parentCtx, err := ParseTraceparent(traceparent)
			if err == nil {
				// Create a child span context (same TraceID, new SpanID)
				consumeTraceCtx = parentCtx.NewChildContext()
			}
		}
		// Fall back to creating a new trace if no header found
		if consumeTraceCtx.TraceID.IsZero() {
			consumeTraceCtx = b.tracer.StartTrace(topic, partition, sm.Offset)
		}

		if !consumeTraceCtx.TraceID.IsZero() {
			span := NewSpan(consumeTraceCtx.TraceID, SpanEventConsumeFetched, topic, partition, sm.Offset)
			span.WithAttribute("priority", fmt.Sprintf("%d", sm.Priority))
			span.WithAttribute("trace_continuity", fmt.Sprintf("%v", sm.Headers["traceparent"] != ""))
			b.tracer.RecordSpan(span)
		}
	}

	// =========================================================================
	// METRICS: Record successful consume operation
	// =========================================================================
	// Calculate total bytes consumed for metrics
	totalBytes := 0
	for _, msg := range messages {
		totalBytes += len(msg.Value)
	}
	// consumerGroup is empty here since this is direct partition consumption
	// Consumer group consumption goes through ConsumerGroup.Consume() which has group context
	InstrumentConsume("", topic, len(messages), totalBytes, consumeStart)

	return messages, nil
}

// ConsumeByOffset reads messages sequentially by offset (FIFO, ignoring priority).
// This is Kafka-like behavior where you consume the log in strict offset order.
//
// Use this when:
//   - You need strict offset ordering (e.g., replication, replay)
//   - You want to consume ALL messages regardless of priority
//   - You're treating the partition as an append-only log
//
// FILTERING: Same as Consume() - delayed messages are filtered out.
func (b *Broker) ConsumeByOffset(topic string, partition int, fromOffset int64, maxMessages int) ([]Message, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	// Loop-based fetch with multiplier to handle filtering
	const maxAttempts = 5
	const fetchMultiplier = 2

	var allFiltered []*storage.Message
	currentOffset := fromOffset

	for attempt := 0; attempt < maxAttempts && len(allFiltered) < maxMessages; attempt++ {
		needed := maxMessages - len(allFiltered)
		fetchSize := needed * fetchMultiplier

		storageMessages, err := t.ConsumeByOffset(partition, currentOffset, fetchSize)
		if err != nil {
			return nil, err
		}

		if len(storageMessages) == 0 {
			break
		}

		// Apply filters to this batch
		for _, sm := range storageMessages {
			if sm.IsControlRecord() {
				continue
			}
			if b.IsDelayed(topic, partition, sm.Offset) {
				continue
			}
			if b.uncommittedTracker != nil && b.uncommittedTracker.IsUncommitted(topic, partition, sm.Offset) {
				continue
			}
			if b.abortedTracker != nil && b.abortedTracker.IsAborted(topic, partition, sm.Offset) {
				continue
			}
			allFiltered = append(allFiltered, sm)

			if len(allFiltered) >= maxMessages {
				break
			}
		}

		if len(storageMessages) > 0 {
			currentOffset = storageMessages[len(storageMessages)-1].Offset + 1
		}
	}

	// Limit to requested amount
	filteredMessages := allFiltered
	if len(filteredMessages) > maxMessages {
		filteredMessages = filteredMessages[:maxMessages]
	}

	// Convert storage.Message to broker.Message for API
	messages := make([]Message, len(filteredMessages))
	for i, sm := range filteredMessages {
		messages[i] = Message{
			Topic:     topic,
			Partition: partition,
			Offset:    sm.Offset,
			Timestamp: time.Unix(0, sm.Timestamp),
			Key:       sm.Key,
			Value:     sm.Value,
			Priority:  sm.Priority,
		}
	}

	return messages, nil
}

// =============================================================================
// PRIORITY-AWARE CONSUMER INTERFACE (M6)
// =============================================================================

// ConsumeByPriority reads messages from a partition respecting strict priority order.
// Higher priority messages are returned before lower priority ones.
//
// NOTE: This is now the same as default Consume(). Kept for explicit API clarity.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset (inclusive) - message at this offset can be returned
//   - maxMessages: Maximum messages to return
//
// This uses strict priority (Critical first, then High, etc.)
// For Weighted Fair Queuing, use ConsumeByPriorityWFQ.
//
// FILTERING: Same as Consume() - delayed messages are filtered out.
func (b *Broker) ConsumeByPriority(topic string, partition int, fromOffset int64, maxMessages int) ([]Message, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	p, err := t.Partition(partition)
	if err != nil {
		return nil, err
	}

	// Loop-based fetch with multiplier to handle filtering
	const maxAttempts = 5
	const fetchMultiplier = 2

	var allFiltered []*storage.Message
	currentOffset := fromOffset

	for attempt := 0; attempt < maxAttempts && len(allFiltered) < maxMessages; attempt++ {
		needed := maxMessages - len(allFiltered)
		fetchSize := needed * fetchMultiplier

		storageMessages, err := p.ConsumeByPriority(currentOffset, fetchSize)
		if err != nil {
			return nil, err
		}

		if len(storageMessages) == 0 {
			break
		}

		// Apply filters to this batch
		for _, sm := range storageMessages {
			if sm.IsControlRecord() {
				continue
			}
			if b.IsDelayed(topic, partition, sm.Offset) {
				continue
			}
			if b.uncommittedTracker != nil && b.uncommittedTracker.IsUncommitted(topic, partition, sm.Offset) {
				continue
			}
			if b.abortedTracker != nil && b.abortedTracker.IsAborted(topic, partition, sm.Offset) {
				continue
			}
			allFiltered = append(allFiltered, sm)

			if len(allFiltered) >= maxMessages {
				break
			}
		}

		if len(storageMessages) > 0 {
			currentOffset = storageMessages[len(storageMessages)-1].Offset + 1
		}
	}

	// Limit to requested amount
	filteredMessages := allFiltered
	if len(filteredMessages) > maxMessages {
		filteredMessages = filteredMessages[:maxMessages]
	}

	// Convert to API messages
	messages := make([]Message, len(filteredMessages))
	for i, sm := range filteredMessages {
		messages[i] = Message{
			Topic:     topic,
			Partition: partition,
			Offset:    sm.Offset,
			Timestamp: time.Unix(0, sm.Timestamp),
			Key:       sm.Key,
			Value:     sm.Value,
			Priority:  sm.Priority,
		}
	}

	return messages, nil
}

// ConsumeByPriorityWFQ reads messages using Weighted Fair Queuing.
// This provides fair distribution across priorities based on configurable weights.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset (inclusive) - messages below this are filtered
//   - maxMessages: Maximum messages to return
//   - scheduler: The WFQ scheduler to use (maintains fairness state)
//
// NOTE: The scheduler should be maintained across calls for proper WFQ behavior.
// Create one scheduler per consumer for best results.
//
// FILTERING: Same as Consume() - delayed messages are filtered out.
func (b *Broker) ConsumeByPriorityWFQ(topic string, partition int, fromOffset int64, maxMessages int, scheduler *PriorityScheduler) ([]Message, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	p, err := t.Partition(partition)
	if err != nil {
		return nil, err
	}

	// Loop-based fetch with multiplier to handle filtering
	const maxAttempts = 5
	const fetchMultiplier = 2

	var allFiltered []*storage.Message
	currentOffset := fromOffset

	for attempt := 0; attempt < maxAttempts && len(allFiltered) < maxMessages; attempt++ {
		needed := maxMessages - len(allFiltered)
		fetchSize := needed * fetchMultiplier

		storageMessages, err := p.ConsumeByPriorityWFQ(currentOffset, fetchSize, scheduler)
		if err != nil {
			return nil, err
		}

		if len(storageMessages) == 0 {
			break
		}

		// Apply filters to this batch
		for _, sm := range storageMessages {
			if sm.IsControlRecord() {
				continue
			}
			if b.IsDelayed(topic, partition, sm.Offset) {
				continue
			}
			if b.uncommittedTracker != nil && b.uncommittedTracker.IsUncommitted(topic, partition, sm.Offset) {
				continue
			}
			if b.abortedTracker != nil && b.abortedTracker.IsAborted(topic, partition, sm.Offset) {
				continue
			}
			allFiltered = append(allFiltered, sm)

			if len(allFiltered) >= maxMessages {
				break
			}
		}

		if len(storageMessages) > 0 {
			currentOffset = storageMessages[len(storageMessages)-1].Offset + 1
		}
	}

	// Limit to requested amount
	filteredMessages := allFiltered
	if len(filteredMessages) > maxMessages {
		filteredMessages = filteredMessages[:maxMessages]
	}

	// Convert to API messages
	messages := make([]Message, len(filteredMessages))
	for i, sm := range filteredMessages {
		messages[i] = Message{
			Topic:     topic,
			Partition: partition,
			Offset:    sm.Offset,
			Timestamp: time.Unix(0, sm.Timestamp),
			Key:       sm.Key,
			Value:     sm.Value,
			Priority:  sm.Priority,
		}
	}

	return messages, nil
}

// MarkConsumed marks a message as consumed in the priority index.
// Call this after processing to filter out the message from future priority queries.
func (b *Broker) MarkConsumed(topic string, partition int, offset int64) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	p, err := t.Partition(partition)
	if err != nil {
		return err
	}

	p.MarkConsumed(offset)
	return nil
}

// GetOffsetBounds returns the earliest and latest offsets for a partition.
// Useful for consumers to know the valid offset range.
func (b *Broker) GetOffsetBounds(topic string, partition int) (earliest, latest int64, err error) {
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

	p, err := t.Partition(partition)
	if err != nil {
		return 0, 0, err
	}

	return p.EarliestOffset(), p.LatestOffset(), nil
}

