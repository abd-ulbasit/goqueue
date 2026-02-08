package broker

import (
	"context"
	"fmt"
	"time"

	"goqueue/internal/storage"
)


// =============================================================================
// PRODUCER INTERFACE
// =============================================================================

// Publish writes a message to a topic.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//
// RETURNS:
//   - Partition the message was written to
//   - Offset within that partition
//   - Error if publish fails
//
// This is the main producer API. It:
//  1. Looks up the topic
//  2. Routes to appropriate partition (by key hash or round-robin)
//  3. Appends message to partition's log
//  4. Returns offset for producer acknowledgment
func (b *Broker) Publish(topic string, key, value []byte) (partition int, offset int64, err error) {
	return b.PublishWithTrace(topic, key, value, TraceContext{})
}

// PublishWithTrace writes a message with trace context propagation.
// If traceCtx is empty (zero TraceID), a new trace is started.
// This enables end-to-end tracing across services.
func (b *Broker) PublishWithTrace(topic string, key, value []byte, traceCtx TraceContext) (partition int, offset int64, err error) {
	// =========================================================================
	// METRICS: Start timing for latency measurement
	// =========================================================================
	publishStart := time.Now()

	// Start or continue trace
	var ctx TraceContext
	if traceCtx.TraceID.IsZero() {
		ctx = b.tracer.StartTrace(topic, 0, 0) // offset unknown yet
	} else {
		ctx = traceCtx
	}

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

	// =========================================================================
	// MILESTONE 8: SCHEMA VALIDATION
	// =========================================================================
	//
	// If schema validation is enabled for this topic (subject), validate the
	// message value against the registered schema before persisting.
	//
	// VALIDATION FLOW:
	//   1. Check if validation is enabled for topic
	//   2. Get latest schema for topic (subject = topic name)
	//   3. Validate message JSON against schema
	//   4. Reject with 400 error if invalid
	//
	// This ensures data quality at publish time - invalid messages never enter
	// the log, preventing downstream consumer failures.
	//
	// =========================================================================
	schemaStart := time.Now()
	if err := b.schemaRegistry.ValidateMessage(topic, value); err != nil {
		// Record validation failure span
		if !ctx.TraceID.IsZero() {
			span := NewSpan(ctx.TraceID, SpanEventValidationFailed, topic, 0, 0)
			span.WithError(err)
			span.WithAttribute("error_type", "schema_validation")
			b.tracer.RecordSpan(span)
		}
		// METRICS: Record schema validation failure
		InstrumentSchemaValidation(topic, false, schemaStart)
		InstrumentPublishError(topic, "validation")
		return 0, 0, fmt.Errorf("schema validation failed: %w", err)
	}
	// METRICS: Record schema validation success (if validation was performed)
	InstrumentSchemaValidation(topic, true, schemaStart)

	// Record publish.received span
	if !ctx.TraceID.IsZero() {
		span := NewSpan(ctx.TraceID, SpanEventPublishReceived, topic, 0, 0)
		span.WithAttribute("key_size", fmt.Sprintf("%d", len(key)))
		span.WithAttribute("value_size", fmt.Sprintf("%d", len(value)))
		b.tracer.RecordSpan(span)
	}

	// =========================================================================
	// CLUSTER MODE: LEADERSHIP CHECK AND REQUEST FORWARDING
	// =========================================================================
	//
	// WHY: In a partitioned cluster, only the partition LEADER should write
	// messages. If a producer's request lands on a non-leader, we forward it
	// to the actual leader transparently.
	//
	// FLOW:
	//   ┌──────────┐  publish   ┌─────────────┐            ┌────────┐
	//   │ Producer │──────────►│ Any Node    │──forward──►│ Leader │
	//   └──────────┘           │ (non-leader)│            │  Node  │
	//        ▲                 └─────────────┘            └───┬────┘
	//        │                                                │
	//        └────────────────────────────────────────────────┘
	//                          response
	//
	// COMPARISON:
	//   - Kafka: Client discovers leader, sends directly (smart client)
	//   - RabbitMQ: Any node can accept; internal routing
	//   - goqueue: Any node can accept; transparent forwarding
	//
	// WHY NOT SMART CLIENT?
	//   Simpler clients, easier load balancer setup, works with any HTTP client.
	//   Tradeoff: Extra hop for non-leader requests (acceptable for correctness).
	//
	// =========================================================================

	// Determine target partition BEFORE checking leadership
	// This must match Topic.Publish() logic exactly
	partition = t.DeterminePartition(key)

	// In cluster mode, check if we're the leader for this partition
	if b.clusterCoordinator != nil {
		isLeader := b.clusterCoordinator.IsLeaderFor(topic, partition)
		b.logger.Info("leadership check for publish",
			"topic", topic,
			"partition", partition,
			"is_leader", isLeader)

		if !isLeader {
			// We're NOT the leader - forward to actual leader
			leaderAddr := b.clusterCoordinator.GetLeaderClientAddress(topic, partition)
			if leaderAddr == "" {
				// Leader unknown - this shouldn't happen in normal operation
				b.logger.Error("leader address unknown for partition",
					"topic", topic,
					"partition", partition)
				InstrumentPublishError(topic, "no_leader")
				return 0, 0, fmt.Errorf("leader unknown for %s partition %d", topic, partition)
			}

			// Record forwarding span
			if !ctx.TraceID.IsZero() {
				span := NewSpan(ctx.TraceID, SpanEventPublishReceived, topic, partition, 0)
				span.WithAttribute("action", "forward_to_leader")
				span.WithAttribute("leader_addr", leaderAddr)
				b.tracer.RecordSpan(span)
			}

			// Forward to leader (includes ISR wait on leader side)
			forwardCtx, forwardCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer forwardCancel()

			offset, err = b.clusterCoordinator.ForwardPublish(forwardCtx, leaderAddr, topic, partition, key, value)
			if err != nil {
				b.logger.Error("failed to forward publish to leader",
					"topic", topic,
					"partition", partition,
					"leader", leaderAddr,
					"error", err)
				InstrumentPublishError(topic, "forward_failed")
				return 0, 0, fmt.Errorf("forward to leader failed: %w", err)
			}

			// Record success
			InstrumentPublish(topic, len(value), publishStart)
			return partition, offset, nil
		}
	}

	// We ARE the leader (or single-node mode) - write locally

	// =========================================================================
	// MILESTONE 27: DISK SPACE PRE-WRITE CHECK
	// =========================================================================
	//
	// WHY: If the disk fills completely, the broker becomes unrecoverable:
	//   - WAL writes fail → data corruption risk
	//   - OS can't create temp files → container crash loops
	//   - Recovery requires manual intervention (scale storage, delete data)
	//
	// HOW IT WORKS:
	//   - DiskMonitor runs a background goroutine polling unix.Statfs()
	//   - Sets an atomic.Bool when usage exceeds threshold (default 90%)
	//   - This check is a single atomic.Load (~1ns, zero contention)
	//   - Returns 503 Service Unavailable → client retries with backoff
	//
	// COMPARISON:
	//   - Kafka: No pre-write check; relies on operator monitoring
	//   - RabbitMQ: disk_free_limit → alarm → blocks ALL publishers
	//   - SQS: Managed, never exposed to users
	//   - goqueue: Per-publish atomic check, fast-fail with retryable error
	//
	// =========================================================================
	if b.diskMonitor != nil && b.diskMonitor.IsDiskFull() {
		if !ctx.TraceID.IsZero() {
			span := NewSpan(ctx.TraceID, SpanEventPublishReceived, topic, partition, 0)
			span.WithError(ErrDiskFull)
			span.WithAttribute("rejection_reason", "disk_full")
			b.tracer.RecordSpan(span)
		}
		InstrumentPublishError(topic, "disk_full")
		return 0, 0, ErrDiskFull
	}

	// =========================================================================
	// TRACE CONTINUITY: Inject traceparent header into message
	// =========================================================================
	//
	// WHY:
	// Without storing the trace context in message headers, the consume path
	// has no way to link consumed messages back to their original publish trace.
	// This breaks end-to-end observability.
	//
	// HOW IT WORKS:
	//   1. Publisher creates or propagates a trace context
	//   2. We inject the traceparent header into the message (stored on disk)
	//   3. Consumer reads message → extracts traceparent → continues trace
	//
	// COMPARISON:
	//   - Kafka: Producer injects headers (via interceptors), consumer extracts
	//   - SQS: X-Ray trace ID in system attributes
	//   - goqueue: traceparent header (W3C standard, same as Kafka best practice)
	//
	// =========================================================================
	if !ctx.TraceID.IsZero() {
		// Build message with traceparent header for trace continuity
		msg := storage.NewMessage(key, value)
		msg.Headers = map[string]string{
			"traceparent": ctx.Traceparent(),
		}
		msg.Flags |= storage.FlagHasHeaders

		// Determine partition (same logic as Topic.Publish)
		partition = t.DeterminePartition(key)

		// Write to partition with headers
		offset, err = t.PublishMessageToPartition(partition, msg)
	} else {
		// No trace context - standard publish (no extra header overhead)
		partition, offset, err = t.Publish(key, value)
	}

	if err != nil {
		// Record error span
		if !ctx.TraceID.IsZero() {
			span := NewSpan(ctx.TraceID, SpanEventPublishReceived, topic, partition, offset)
			span.WithError(err)
			b.tracer.RecordSpan(span)
		}
		// METRICS: Record publish error
		InstrumentPublishError(topic, "storage")
		return 0, 0, err
	}

	// Record publish.persisted span
	if !ctx.TraceID.IsZero() {
		span := NewSpan(ctx.TraceID, SpanEventPublishPersisted, topic, partition, offset)
		b.tracer.RecordSpan(span)
	}

	// =========================================================================
	// MILESTONE 11: SYNCHRONOUS REPLICATION (wait for ISR)
	// =========================================================================
	//
	// WHY: In cluster mode, waiting for ISR replication before ACK ensures
	// durability. If the leader crashes immediately after ACK, followers
	// have the message and can become leader without data loss.
	//
	// FLOW:
	//   ┌──────────┐  publish   ┌─────────┐  replicate  ┌──────────────┐
	//   │ Producer │──────────►│ Leader  │────────────►│ ISR Replicas │
	//   └──────────┘           │         │◄────────────│ (send ACK)   │
	//        ▲                 └────┬────┘             └──────────────┘
	//        │                      │
	//        │    ACK after ISR     │
	//        │◄─────────────────────┘
	//
	// COMPARISON:
	//   - Kafka acks=1:   ACK after leader writes (fast, less durable)
	//   - Kafka acks=all: ACK after all ISR replicate (slower, durable)
	//   - goqueue:        Always acks=all in cluster mode (safer default)
	//
	// TIMEOUT:
	//   - Default 10 seconds (configurable via ReplicationConfig)
	//   - If ISR replicas don't ACK in time, publish still succeeds
	//     (message is durable on leader) but logs warning
	//
	// SINGLE-NODE MODE:
	//   - No replication coordinator → skip wait
	//   - Message is durable on local disk
	//
	// =========================================================================
	if b.replicationCoordinator != nil {
		// Create timeout context for replication wait
		replicationCtx, replicationCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer replicationCancel()

		// Wait for followers in ISR to acknowledge
		if err := b.replicationCoordinator.WaitForReplication(replicationCtx, topic, partition, offset); err != nil {
			// Replication timeout/failure - message is on leader but ISR didn't ack
			// Log warning but don't fail the publish (message is durable on leader)
			b.logger.Warn("replication wait failed",
				"topic", topic,
				"partition", partition,
				"offset", offset,
				"error", err)

			// Record replication timeout span
			if !ctx.TraceID.IsZero() {
				span := NewSpan(ctx.TraceID, SpanEventReplicationTimeout, topic, partition, offset)
				span.WithError(err)
				b.tracer.RecordSpan(span)
			}

			// Note: We continue and return success because the message IS durable
			// on the leader. The ISR timeout just means followers are slow.
			// This matches Kafka's behavior with acks=all when ISR shrinks.
		} else if !ctx.TraceID.IsZero() {
			// Record successful replication span
			span := NewSpan(ctx.TraceID, SpanEventReplicationComplete, topic, partition, offset)
			b.tracer.RecordSpan(span)
		}
	}

	// =========================================================================
	// METRICS: Record successful publish
	// =========================================================================
	InstrumentPublish(topic, len(value), publishStart)

	return partition, offset, nil
}

// PublishBatch writes multiple messages to a topic.
// All messages are written to appropriate partitions based on their keys.
//
// PERFORMANCE:
//   - Uses batch append (single disk flush per partition)
//   - Groups messages by partition to reduce lock contention
//   - 10-100x faster than calling Publish() in a loop
//
// RETURNS:
//   - Slice of results (partition, offset) for each message
//   - Error if any publish fails (partial writes may have occurred)
func (b *Broker) PublishBatch(topic string, messages []struct {
	Key   []byte
	Value []byte
}) ([]struct {
	Partition int
	Offset    int64
}, error) {
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

	// Use optimized batch publish path
	return t.PublishBatch(messages)
}

// =============================================================================
// PRIORITY-AWARE PRODUCER INTERFACE (M6)
// =============================================================================

// PublishWithPriority writes a message with specified priority to a topic.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//   - priority: Message priority (Critical, High, Normal, Low, Background)
//
// Priority determines the order of delivery to consumers when using
// ConsumeByPriority or ConsumeByPriorityWFQ.
//
// EXAMPLE:
//
//	// High priority payment message
//	p, o, err := broker.PublishWithPriority("orders", orderID, data, storage.PriorityHigh)
//
//	// Background analytics event
//	p, o, err := broker.PublishWithPriority("events", nil, data, storage.PriorityBackground)
func (b *Broker) PublishWithPriority(topic string, key, value []byte, priority storage.Priority) (partition int, offset int64, err error) {
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

	// =========================================================================
	// CLUSTER MODE: LEADERSHIP CHECK AND REQUEST FORWARDING
	// =========================================================================
	// Determine target partition BEFORE checking leadership
	partition = t.DeterminePartition(key)

	// In cluster mode, check if we're the leader for this partition
	if b.clusterCoordinator != nil {
		isLeader := b.clusterCoordinator.IsLeaderFor(topic, partition)
		b.logger.Info("leadership check for publish with priority",
			"topic", topic,
			"partition", partition,
			"is_leader", isLeader)

		if !isLeader {
			// We're NOT the leader - forward to actual leader
			leaderAddr := b.clusterCoordinator.GetLeaderClientAddress(topic, partition)
			if leaderAddr == "" {
				b.logger.Error("leader address unknown for partition",
					"topic", topic,
					"partition", partition)
				return 0, 0, fmt.Errorf("leader unknown for %s partition %d", topic, partition)
			}

			b.logger.Info("forwarding publish to leader",
				"topic", topic,
				"partition", partition,
				"leader", leaderAddr)

			// Forward to leader
			forwardCtx, forwardCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer forwardCancel()

			offset, err = b.clusterCoordinator.ForwardPublish(forwardCtx, leaderAddr, topic, partition, key, value)
			if err != nil {
				b.logger.Error("failed to forward publish to leader",
					"topic", topic,
					"partition", partition,
					"leader", leaderAddr,
					"error", err)
				return 0, 0, fmt.Errorf("forward to leader failed: %w", err)
			}

			return partition, offset, nil
		}
	}

	// We ARE the leader (or single-node mode) - write locally with priority
	return t.PublishWithPriority(key, value, priority)
}

// PublishToPartitionWithPriority writes a message with priority to a specific partition.
//
// ============================================================================
// CLUSTER MODE: LEADERSHIP CHECK AND REQUEST FORWARDING
// ============================================================================
// In a cluster, only the partition leader can write. If this node is NOT the
// leader for the target partition, we forward the request to the actual leader.
//
// EXPLICIT PARTITION ROUTING:
//   - Caller specifies exact partition (no key-based routing)
//   - We still must check leadership for that specific partition
//   - Forward if not leader, write locally if we are
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Specific partition to write to
//   - key: Message key (for log storage, not routing)
//   - value: Message payload
//   - priority: Storage priority
//
// RETURNS:
//   - offset: Message offset within partition
//   - error: If write or forwarding fails
func (b *Broker) PublishToPartitionWithPriority(topic string, partition int, key, value []byte, priority storage.Priority) (offset int64, err error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	// Validate partition
	numPartitions := t.NumPartitions()
	if partition < 0 || partition >= numPartitions {
		return 0, fmt.Errorf("invalid partition %d (topic has %d partitions)", partition, numPartitions)
	}

	// =========================================================================
	// CLUSTER MODE: LEADERSHIP CHECK AND REQUEST FORWARDING
	// =========================================================================
	// In cluster mode, check if we're the leader for this specific partition
	if b.clusterCoordinator != nil {
		isLeader := b.clusterCoordinator.IsLeaderFor(topic, partition)
		b.logger.Info("leadership check for explicit partition publish",
			"topic", topic,
			"partition", partition,
			"is_leader", isLeader)

		if !isLeader {
			// We're NOT the leader - forward to actual leader
			leaderAddr := b.clusterCoordinator.GetLeaderClientAddress(topic, partition)
			if leaderAddr == "" {
				b.logger.Error("leader address unknown for partition",
					"topic", topic,
					"partition", partition)
				return 0, fmt.Errorf("leader unknown for %s partition %d", topic, partition)
			}

			b.logger.Info("forwarding explicit partition publish to leader",
				"topic", topic,
				"partition", partition,
				"leader", leaderAddr)

			// Forward to leader
			forwardCtx, forwardCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer forwardCancel()

			offset, err = b.clusterCoordinator.ForwardPublish(forwardCtx, leaderAddr, topic, partition, key, value)
			if err != nil {
				b.logger.Error("failed to forward explicit partition publish to leader",
					"topic", topic,
					"partition", partition,
					"leader", leaderAddr,
					"error", err)
				return 0, fmt.Errorf("forward to leader failed: %w", err)
			}

			return offset, nil
		}
	}

	// We ARE the leader (or single-node mode) - write locally with priority
	return t.PublishToPartitionWithPriority(partition, key, value, priority)
}

// PublishBatchWithPriority writes multiple messages with priorities to a topic.
//
// Each message can have its own priority, enabling mixed-priority batch writes.
//
// RETURNS:
//   - Slice of results (partition, offset) for each message
//   - Error if any publish fails (partial writes may have occurred)
func (b *Broker) PublishBatchWithPriority(topic string, messages []struct {
	Key      []byte
	Value    []byte
	Priority storage.Priority
}) ([]struct {
	Partition int
	Offset    int64
}, error) {
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

	results := make([]struct {
		Partition int
		Offset    int64
	}, len(messages))

	for i, msg := range messages {
		partition, offset, err := t.PublishWithPriority(msg.Key, msg.Value, msg.Priority)
		if err != nil {
			return results[:i], fmt.Errorf("failed at message %d: %w", i, err)
		}
		results[i] = struct {
			Partition int
			Offset    int64
		}{partition, offset}
	}

	return results, nil
}

// =============================================================================
// TRANSACTIONAL PRODUCER INTERFACE (M9)
// =============================================================================
//
// These methods implement the TransactionBroker interface required by the
// TransactionCoordinator for exactly-once semantics.
//
// KAFKA COMPARISON:
//   - Kafka: initTransactions(), beginTransaction(), send(), commitTransaction()
//   - goqueue: InitProducerID(), BeginTransaction(), PublishTransactional(), CommitTransaction()
//
// FLOW FOR TRANSACTIONAL PUBLISH:
//
//   ┌──────────────────────────────────────────────────────────────────────────┐
//   │  1. InitProducerID(txn.id)                                               │
//   │     └── Returns PID=123, Epoch=1                                         │
//   │                                                                          │
//   │  2. BeginTransaction(PID, Epoch)                                         │
//   │     └── Transaction state: Empty → Ongoing                               │
//   │                                                                          │
//   │  3. PublishTransactional(topic, key, value, PID, Epoch, seq)             │
//   │     └── Validates sequence number (deduplication)                        │
//   │     └── Writes message to partition log                                  │
//   │     └── Records partition in transaction                                 │
//   │                                                                          │
//   │  4. CommitTransaction(PID, Epoch)                                        │
//   │     └── Writes COMMIT control record to all partitions                   │
//   │     └── Transaction state: Ongoing → PrepareCommit → CompleteCommit      │
//   └──────────────────────────────────────────────────────────────────────────┘
//

// WriteControlRecord implements TransactionBroker interface.
// Writes a control record (COMMIT or ABORT marker) to a specific partition.
//
// Control records are special messages that mark transaction boundaries.
// They're used by consumers with read_committed isolation to filter out
// messages from aborted transactions.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - isCommit: true for COMMIT marker, false for ABORT marker
//   - producerID: Producer's unique identifier
//   - epoch: Producer's current epoch
//   - transactionalID: Transaction's string identifier
//
// WIRE FORMAT:
// The control record is written as a regular message with:
//   - FlagControlRecord set in Flags byte
//   - FlagTransactionCommit set if isCommit is true
//   - Value contains serialized ControlRecordPayload (PID, Epoch, TxnId)
//
// COMPARISON:
//   - Kafka: Uses ControlRecordType in special batch format
//   - goqueue: Uses flags byte in standard message format (simpler)
func (b *Broker) WriteControlRecord(topic string, partition int, isCommit bool, producerID int64, epoch int16, transactionalID string) error {
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

	// Create the control record message
	// Note: storage package uses uint64/uint16 for PID/Epoch, so we convert here
	var controlMsg *storage.Message
	if isCommit {
		controlMsg = storage.NewCommitControlRecord(0, uint64(producerID), uint16(epoch), transactionalID)
	} else {
		controlMsg = storage.NewAbortControlRecord(0, uint64(producerID), uint16(epoch), transactionalID)
	}

	// Write to the partition using PublishMessageToPartition to preserve Flags
	// The old approach (PublishToPartition(key, value)) lost the FlagControlRecord flag!
	offset, err := t.PublishMessageToPartition(partition, controlMsg)
	if err != nil {
		return fmt.Errorf("failed to write control record: %w", err)
	}

	// Log the control record for debugging
	recordType := "COMMIT"
	if !isCommit {
		recordType = "ABORT"
	}
	b.logger.Debug("wrote control record",
		"type", recordType,
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"producer_id", producerID,
		"epoch", epoch,
		"txn_id", transactionalID)

	return nil
}

// ClearUncommittedTransaction implements TransactionBroker interface.
// Clears tracked uncommitted offsets when a transaction commits or aborts.
//
// PARAMETERS:
//   - txnID: The transaction ID to clear
//
// RETURNS:
//   - List of offsets that were cleared (for abort filtering)
//
// WHY BOTH COMMIT AND ABORT CALL THIS:
//
//	COMMIT CASE:
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│  Transaction commits → offsets should become visible to consumers       │
//	│  1. Clear from uncommittedTracker (returns offsets, but we don't need   │
//	│     them since messages should be visible)                              │
//	│  2. COMMIT control record already written to log                        │
//	│  3. Consumers can now read these offsets                                │
//	└─────────────────────────────────────────────────────────────────────────┘
//
//	ABORT CASE:
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│  Transaction aborts → offsets remain invisible forever                  │
//	│  1. Clear from uncommittedTracker (returns offsets)                     │
//	│  2. MarkTransactionAborted() moves offsets to abortedTracker            │
//	│  3. Consumers filter aborted offsets via abortedTracker                 │
//	└─────────────────────────────────────────────────────────────────────────┘
func (b *Broker) ClearUncommittedTransaction(txnID string) []partitionOffset { //nolint:revive // internal package type
	if b.uncommittedTracker == nil {
		return nil
	}

	cleared := b.uncommittedTracker.ClearTransaction(txnID)
	b.logger.Debug("cleared uncommitted offsets for transaction",
		"txn_id", txnID,
		"offsets_cleared", len(cleared))
	return cleared
}

// MarkTransactionAborted implements TransactionBroker interface.
// Marks offsets from an aborted transaction as permanently invisible.
//
// PARAMETERS:
//   - offsets: List of offsets returned from ClearUncommittedTransaction
//
// WHEN CALLED:
//
//	Only when a transaction aborts, after ClearUncommittedTransaction.
//
// FLOW:
//
//	AbortTransaction → ClearUncommittedTransaction → MarkTransactionAborted
//
// The offsets will be filtered during consume operations forever.
func (b *Broker) MarkTransactionAborted(offsets []partitionOffset) {
	if b.abortedTracker == nil || len(offsets) == 0 {
		return
	}

	b.abortedTracker.MarkAborted(offsets)
	b.logger.Debug("marked offsets as aborted",
		"count", len(offsets))

	// =========================================================================
	// PERSIST ABORTED TRACKER TO DISK (M26 - Persistence)
	// =========================================================================
	//
	// Save aborted state after every abort so it survives restarts.
	// Aborts are infrequent in normal operation, so the write cost is acceptable.
	//
	// CRASH SAFETY:
	//   If we crash between MarkAborted and Save, the in-memory state is lost.
	//   On next restart, those offsets won't be filtered. To fully close this
	//   window, we'd need to write the WAL record first (write-ahead pattern).
	//   For now, this is acceptable since aborts are rare and the window is tiny.
	// =========================================================================
	abortedFilePath := AbortedTrackerFilePath(b.config.DataDir)
	if err := b.abortedTracker.Save(abortedFilePath); err != nil {
		b.logger.Error("failed to persist aborted tracker",
			"path", abortedFilePath,
			"error", err)
	}
}

// TrackUncommittedOffset registers a single message offset as part of an
// in-progress transaction. Delegates to UncommittedTracker.Track().
//
// ============================================================================
// RECOVERY USAGE
// ============================================================================
//
// Called during WAL recovery when the coordinator replays txn_publish records
// and discovers transactions that were in-progress at crash time. Without
// this, the UncommittedTracker would be empty after restart and in-progress
// transaction messages would be visible to read_committed consumers (BUG).
//
// NORMAL OPERATION: The broker calls uncommittedTracker.Track() directly
// in PublishTransactional(). This method exists solely for the coordinator
// to rebuild tracker state during recovery.
//
// COMPARISON:
//   - Kafka: Rebuilds from __transaction_state log + partition markers
//   - SQS: No transactions, not applicable
//   - RabbitMQ: Publisher confirms + journal replay, no read isolation
//   - goqueue: WAL txn_publish records → replay into UncommittedTracker
//
// ============================================================================
func (b *Broker) TrackUncommittedOffset(topic string, partition int, offset int64, txnID string, producerID int64, epoch int16) {
	if b.uncommittedTracker == nil {
		return
	}
	b.uncommittedTracker.Track(topic, partition, offset, txnID, producerID, epoch)
}

// PublishTransactional writes a message as part of an active transaction.
// This is the primary method for transactional producers to publish messages.
//
// SEQUENCE VALIDATION:
// The sequence number is validated against the expected sequence for this
// partition. If the sequence is lower than expected, it's a duplicate and
// will be silently ignored. If the sequence is higher than expected, an
// error is returned (missing sequence).
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Target partition (or -1 for automatic routing)
//   - key: Routing key (used for partition selection if partition=-1)
//   - value: Message payload
//   - producerID: Producer's unique identifier
//   - epoch: Producer's current epoch (for zombie fencing)
//   - sequence: Sequence number for this partition (for deduplication)
//
// RETURNS:
//   - partition: Actual partition the message was written to
//   - offset: Offset assigned to the message
//   - duplicate: true if this was a duplicate (already seen sequence)
//   - error: If validation fails or write fails
//
// FLOW:
//
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│  1. Validate producer epoch (zombie fencing)                            │
//	│     └── Reject if epoch < current epoch for this PID                    │
//	│                                                                         │
//	│  2. Check sequence number (deduplication)                               │
//	│     └── If seq < expected: duplicate, return (true, nil)                │
//	│     └── If seq > expected: error (missing sequence)                     │
//	│     └── If seq == expected: proceed                                     │
//	│                                                                         │
//	│  3. Determine partition                                                 │
//	│     └── If partition >= 0: use specified partition                      │
//	│     └── If partition == -1: use key-based routing                       │
//	│                                                                         │
//	│  4. Write message to partition log                                      │
//	│                                                                         │
//	│  5. Update sequence tracking (expected = seq + 1)                       │
//	│                                                                         │
//	│  6. Register partition with transaction coordinator                     │
//	│     └── So coordinator knows where to write control records             │
//	└─────────────────────────────────────────────────────────────────────────┘
func (b *Broker) PublishTransactional(
	topic string,
	partition int,
	key, value []byte,
	producerID int64,
	epoch int16,
	sequence int32,
) (actualPartition int, offset int64, duplicate bool, err error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0, false, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, 0, false, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	numPartitions := t.NumPartitions()
	b.mu.RUnlock()

	// Step 1: Determine actual partition (if not specified)
	actualPartition = partition
	if partition < 0 {
		// Use key-based routing
		if len(key) > 0 {
			actualPartition = int(murmur3Hash(key)) % numPartitions
		} else {
			// Round-robin (simplified - in production, use per-producer counter)
			actualPartition = int(time.Now().UnixNano()) % numPartitions
		}
	}

	if actualPartition < 0 || actualPartition >= numPartitions {
		return 0, 0, false, fmt.Errorf("invalid partition %d (topic has %d partitions)", partition, numPartitions)
	}

	// Step 2: Check sequence number with transaction coordinator
	// Build ProducerIDAndEpoch for the check
	pid := ProducerIDAndEpoch{
		ProducerID: producerID,
		Epoch:      epoch,
	}

	// CheckSequence returns (existingOffset, isDuplicate, error)
	// We pass 0 for offset since we don't have it yet - it will be assigned after write
	existingOffset, isDuplicate, err := b.transactionCoordinator.CheckSequence(pid, topic, actualPartition, sequence, 0)
	if err != nil {
		return 0, 0, false, fmt.Errorf("sequence check failed: %w", err)
	}

	if isDuplicate {
		// Duplicate message - return success without writing
		// This is idempotent behavior: same sequence returns success
		b.logger.Debug("duplicate message detected",
			"producer_id", producerID,
			"topic", topic,
			"partition", actualPartition,
			"sequence", sequence,
			"existing_offset", existingOffset)
		// METRICS: Record duplicate rejection for observability
		InstrumentDuplicateRejected()
		return actualPartition, existingOffset, true, nil
	}

	// Step 3: Write message to partition
	offset, err = t.PublishToPartition(actualPartition, key, value)
	if err != nil {
		return actualPartition, 0, false, fmt.Errorf("failed to publish: %w", err)
	}

	// =========================================================================
	// STEP 4: TRACK UNCOMMITTED OFFSET FOR read_committed ISOLATION
	// =========================================================================
	//
	// The message is now written to the log but the transaction is not committed.
	// Consumers with read_committed isolation should NOT see this message until
	// the transaction commits.
	//
	// We track the offset in uncommittedTracker so consume operations can filter
	// it out. When the transaction commits/aborts, we clear the tracking.
	//
	// NOTE: We lookup the transaction ID by producerID+epoch. If the producer
	// is in an active transaction, we track the offset. If not (non-transactional
	// publish), we don't track it.
	//
	// =========================================================================
	state := b.transactionCoordinator.GetProducerStateByProducerID(producerID, epoch)
	b.logger.Debug("looked up producer state by ID",
		"producer_id", producerID,
		"epoch", epoch,
		"state_found", state != nil)
	if state != nil {
		b.logger.Debug("producer state details",
			"state", state.State,
			"current_txn_id", state.CurrentTransactionID)
		if state.State == TransactionStateOngoing && state.CurrentTransactionID != "" {
			b.uncommittedTracker.Track(
				topic,
				actualPartition,
				offset,
				state.CurrentTransactionID,
				producerID,
				epoch,
			)
			b.logger.Debug("tracked uncommitted offset",
				"topic", topic,
				"partition", actualPartition,
				"offset", offset,
				"txn_id", state.CurrentTransactionID)

			// =========================================================================
			// WRITE WAL RECORD FOR TRANSACTIONAL PUBLISH (M26 - Recovery)
			// =========================================================================
			//
			// Record the offset in the transaction WAL so we can rebuild the
			// UncommittedTracker during recovery. Without this, restarted brokers
			// can't tell which offsets belong to in-progress transactions.
			// =========================================================================
			b.transactionCoordinator.RecordTxnPublish(
				state.TransactionalID,
				state.CurrentTransactionID,
				topic,
				actualPartition,
				offset,
				producerID,
				epoch,
			)
		}
	}

	b.logger.Debug("published transactional message",
		"producer_id", producerID,
		"epoch", epoch,
		"topic", topic,
		"partition", actualPartition,
		"offset", offset,
		"sequence", sequence)

	return actualPartition, offset, false, nil
}

// GetTransactionCoordinator returns the transaction coordinator for external access.
// Used by HTTP handlers to expose transaction APIs.
func (b *Broker) GetTransactionCoordinator() *TransactionCoordinator {
	return b.transactionCoordinator
}

// GetAckManager returns the acknowledgment manager for external access.
// Used by gRPC handlers to process ACK/NACK/REJECT operations.
//
// RETURNS:
//   - *AckManager if acknowledgment is enabled (M4)
//   - nil if not configured
func (b *Broker) GetAckManager() *AckManager {
	return b.ackManager
}

// GetGroupCoordinator returns the consumer group coordinator for external access.
// Used by gRPC handlers to manage consumer group membership and offset commits.
//
// RETURNS:
//   - *GroupCoordinator if consumer groups are enabled (M3)
//   - nil if not configured
func (b *Broker) GetGroupCoordinator() *GroupCoordinator {
	return b.groupCoordinator
}

// GetReplicationCoordinator returns the replication coordinator for external access.
// Used by HTTP handlers for forwarded publishes to wait for ISR replication.
//
// WHY EXPOSE THIS:
//
//	When a non-leader forwards a publish request to the leader, the leader's
//	API handler needs to wait for ISR replication before responding. This
//	matches the behavior of regular publishes (acks=all semantics).
//
// RETURNS:
//   - *replicationCoordinator if cluster mode is enabled (M11)
//   - nil if running in single-node mode
func (b *Broker) GetReplicationCoordinator() *replicationCoordinator { //nolint:revive // unexported-return: used only across internal packages
	return b.replicationCoordinator
}

