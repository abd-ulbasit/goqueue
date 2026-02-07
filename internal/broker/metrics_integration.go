// =============================================================================
// METRICS INTEGRATION FOR GOQUEUE BROKER
// =============================================================================
//
// WHAT THIS FILE DOES:
// Provides helper functions and middleware to instrument the broker with
// Prometheus metrics. This acts as a bridge between the metrics package
// and the broker/server components.
//
// WHY A SEPARATE INTEGRATION FILE?
// 1. Keeps metrics logic separate from business logic
// 2. Easier to enable/disable metrics without touching broker code
// 3. Single place to understand what's being measured
// 4. Follows separation of concerns principle
//
// INTEGRATION POINTS:
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                           GOQUEUE SERVER                                │
//   │                                                                         │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │  HTTP Server (api/server.go)                                     │  │
//   │   │    - /metrics endpoint                                           │  │
//   │   │    - Request instrumentation middleware                          │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                              │                                          │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │  Broker (broker/broker.go)                                       │  │
//   │   │    - Publish: InstrumentPublish()                                │  │
//   │   │    - Consume: InstrumentConsume()                                │  │
//   │   │    - Ack:     InstrumentAck()                                    │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                              │                                          │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │  Storage (storage/log.go)                                        │  │
//   │   │    - Write: InstrumentStorageWrite()                             │  │
//   │   │    - Read:  InstrumentStorageRead()                              │  │
//   │   │    - Fsync: InstrumentFsync()                                    │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                              │                                          │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │  Cluster (broker/cluster*.go)                                    │  │
//   │   │    - Node health updates                                         │  │
//   │   │    - Leader election events                                      │  │
//   │   │    - ISR changes                                                 │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// HOW TO USE:
//   1. Initialize metrics in main.go: metrics.Init(config)
//   2. Import this package in server.go
//   3. Call InstrumentXXX() functions at appropriate points
//   4. Metrics are automatically exposed at /metrics endpoint
//
// COMPARISON WITH OTHER SYSTEMS:
//   - Kafka: Uses Yammer metrics (Java), exposed via JMX
//   - RabbitMQ: Built-in metrics plugin, Prometheus exporter available
//   - SQS: CloudWatch metrics (AWS managed)
//   - goqueue: Prometheus client_golang (this file)
//
// =============================================================================

package broker

import (
	"time"

	"goqueue/internal/metrics"
)

// =============================================================================
// PUBLISH INSTRUMENTATION
// =============================================================================
//
// Call these functions from the broker's Publish methods to record metrics.
// Each function is designed to be called at a specific point in the flow.
//
// FLOW:
//   PublishWithTrace()
//       └── RecordPublishStart() ← Start timer
//           │
//           ├── [schema validation]
//           │   └── RecordSchemaValidation() ← If enabled
//           │
//           ├── [partition selection]
//           │
//           └── [write to log]
//               └── RecordPublishComplete() ← Record latency + counters
//
// =============================================================================

// InstrumentPublish records metrics for a successful publish operation.
// Call this after the message is durably persisted to storage.
//
// Parameters:
//   - topic:      The topic name (label for cardinality)
//   - sizeBytes:  Size of the message value in bytes
//   - startTime:  When the publish operation started (for latency)
//
// Metrics recorded:
//   - goqueue_broker_messages_published_total (counter)
//   - goqueue_broker_bytes_published_total (counter)
//   - goqueue_broker_publish_latency_seconds (histogram)
func InstrumentPublish(topic string, sizeBytes int, startTime time.Time) {
	m := metrics.Get()
	if m == nil {
		return // Metrics not initialized, skip silently
	}

	// Record the successful publish
	// Convert time.Duration to float64 seconds for Prometheus histogram
	m.Broker.RecordPublish(topic, sizeBytes, time.Since(startTime).Seconds())
}

// InstrumentPublishError records metrics for a failed publish operation.
// Call this when publish fails for any reason.
//
// Parameters:
//   - topic:     The topic name
//   - errorType: Category of error (validation, storage, timeout, etc.)
//
// Metrics recorded:
//   - goqueue_broker_messages_failed_total (counter)
func InstrumentPublishError(topic, errorType string) {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Broker.RecordPublishError(topic, errorType)
}

// InstrumentSchemaValidation records schema validation metrics.
// Call this after schema validation attempt (success or failure).
//
// Parameters:
//   - topic:     The topic/subject name
//   - success:   Whether validation passed
//   - startTime: When validation started (for latency)
//
// Metrics recorded:
//   - goqueue_broker_schema_validations_total (counter)
//   - goqueue_broker_schema_validation_latency_seconds (histogram)
func InstrumentSchemaValidation(topic string, success bool, startTime time.Time) {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Broker.RecordSchemaValidation(topic, success, time.Since(startTime).Seconds())
}

// =============================================================================
// CONSUME INSTRUMENTATION
// =============================================================================
//
// Call these functions from the broker's Consume methods.
//
// FLOW:
//   Consume()
//       └── RecordConsumeStart() ← Start timer
//           │
//           ├── [fetch from storage]
//           │
//           ├── [filter: control records, delayed, uncommitted]
//           │
//           └── RecordConsumeComplete() ← Record latency + counters
//
// =============================================================================

// InstrumentConsume records metrics for a successful consume operation.
// Call this after messages are fetched and filtered.
//
// Parameters:
//   - consumerGroup: The consumer group ID
//   - topic:         The topic name
//   - messageCount:  Number of messages returned
//   - sizeBytes:     Total size of messages in bytes
//   - startTime:     When the consume operation started
//
// Metrics recorded:
//   - goqueue_broker_messages_consumed_total (counter)
//   - goqueue_broker_bytes_consumed_total (counter)
//   - goqueue_broker_fetch_latency_seconds (histogram)
func InstrumentConsume(consumerGroup, topic string, messageCount, sizeBytes int, startTime time.Time) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordConsume signature: (topic, consumerGroup, count, bytes, latency)
	// Convert time.Duration to float64 seconds
	m.Broker.RecordConsume(topic, consumerGroup, messageCount, sizeBytes, time.Since(startTime).Seconds())
}

// =============================================================================
// ACK/NACK INSTRUMENTATION
// =============================================================================
//
// Record acknowledgment events for reliability metrics.
// These are critical for understanding message processing success rates.
//
// =============================================================================

// InstrumentAck records a message acknowledgment.
// Call this when consumer successfully acks a message.
//
// Parameters:
//   - topic:         The topic name
//   - consumerGroup: The consumer group ID
//   - startTime:     When the ack operation started
//
// Metrics recorded:
//   - goqueue_broker_messages_acked_total (counter)
//   - goqueue_broker_ack_latency_seconds (histogram)
func InstrumentAck(topic, consumerGroup string, startTime time.Time) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordAck signature: (topic, consumerGroup, ackType, latency)
	m.Broker.RecordAck(topic, consumerGroup, "ack", time.Since(startTime).Seconds())
}

// InstrumentNack records a message negative acknowledgment.
// Call this when consumer nacks a message (will be redelivered).
//
// Parameters:
//   - topic:         The topic name
//   - consumerGroup: The consumer group ID
//   - startTime:     When the nack operation started
//
// Metrics recorded:
//   - goqueue_broker_messages_nacked_total (counter)
//   - goqueue_broker_ack_latency_seconds (histogram)
func InstrumentNack(topic, consumerGroup string, startTime time.Time) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordAck handles nack via ackType parameter
	m.Broker.RecordAck(topic, consumerGroup, "nack", time.Since(startTime).Seconds())
}

// InstrumentReject records a message rejection.
// Call this when consumer rejects a message (goes to DLQ).
//
// Parameters:
//   - topic:         The topic name
//   - consumerGroup: The consumer group ID
//   - startTime:     When the reject operation started
//
// Metrics recorded:
//   - goqueue_broker_messages_rejected_total (counter)
//   - goqueue_broker_ack_latency_seconds (histogram)
func InstrumentReject(topic, consumerGroup string, startTime time.Time) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordAck handles reject via ackType parameter
	m.Broker.RecordAck(topic, consumerGroup, "reject", time.Since(startTime).Seconds())
}

// =============================================================================
// DELAYED MESSAGE INSTRUMENTATION
// =============================================================================

// InstrumentDelayedMessage records a delayed message being scheduled.
// Call this when a message is published with a delay.
//
// Parameters:
//   - topic: The topic name
//
// Metrics recorded:
//   - goqueue_broker_delayed_messages_scheduled_total (counter)
//   - goqueue_broker_delayed_messages_pending (gauge, incremented)
func InstrumentDelayedMessage(topic string) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordDelayedMessage signature: (topic, scheduled bool)
	// scheduled=true increments pending and scheduled counters
	m.Broker.RecordDelayedMessage(topic, true)
}

// InstrumentDelayedMessageDelivered records a delayed message becoming ready.
// Call this when a delayed message expires and becomes visible.
//
// Parameters:
//   - topic: The topic name
//
// Metrics recorded:
//   - goqueue_broker_delayed_messages_fired_total (counter)
//   - goqueue_broker_delayed_messages_pending (gauge, decremented)
func InstrumentDelayedMessageDelivered(topic string) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordDelayedMessage signature: (topic, scheduled bool)
	// scheduled=false increments fired counter and decrements pending gauge
	m.Broker.RecordDelayedMessage(topic, false)
}

// =============================================================================
// PRIORITY MESSAGE INSTRUMENTATION
// =============================================================================

// InstrumentPriorityMessage records a priority message being published.
// Call this when a message is published with non-default priority.
//
// Parameters:
//   - topic:    The topic name
//   - priority: Priority name ("critical", "high", "normal", "low", "background")
//
// Metrics recorded:
//   - goqueue_broker_priority_messages_published_total (counter)
func InstrumentPriorityMessage(topic, priority string) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordPriorityPublish signature: (topic, priority string)
	m.Broker.RecordPriorityPublish(topic, priority)
}

// =============================================================================
// TRANSACTION INSTRUMENTATION
// =============================================================================
//
// TRANSACTION LIFECYCLE IN GOQUEUE:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                      TRANSACTION FLOW                                   │
//   │                                                                         │
//   │   BeginTransaction()                                                    │
//   │        │                                                                │
//   │        ├──► InstrumentTransactionStarted()  (record start time)         │
//   │        │                                                                │
//   │        ▼                                                                │
//   │   PublishTransactional() (1..N times)                                   │
//   │        │                                                                │
//   │        ├──► InstrumentPublish() per message                             │
//   │        │                                                                │
//   │        ▼                                                                │
//   │   ┌─────────────────┐                                                   │
//   │   │  Commit?        │                                                   │
//   │   └────────┬────────┘                                                   │
//   │            │                                                            │
//   │     ┌──Yes─┴──No──┐                                                     │
//   │     ▼             ▼                                                     │
//   │   Commit        Abort                                                   │
//   │     │             │                                                     │
//   │     ├──► InstrumentTransactionCommitted(latency)                        │
//   │     │             │                                                     │
//   │     │             └──► InstrumentTransactionAborted()                   │
//   │     ▼                                                                   │
//   │   Done                                                                  │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// METRICS TRACKED:
//   - goqueue_broker_transactions_started_total (counter)
//   - goqueue_broker_transactions_committed_total (counter)
//   - goqueue_broker_transactions_aborted_total (counter)
//   - goqueue_broker_transaction_latency_seconds (histogram)
//
// =============================================================================

// InstrumentTransactionStarted records a new transaction being started.
// Call this when BeginTransaction() is called.
//
// Returns the start time to pass to InstrumentTransactionCommitted.
//
// Metrics recorded:
//   - goqueue_broker_transactions_started_total (counter)
func InstrumentTransactionStarted() time.Time {
	m := metrics.Get()
	if m == nil {
		return time.Now()
	}

	m.Broker.RecordTransactionStarted()
	return time.Now()
}

// InstrumentTransactionCommitted records a successful transaction commit.
// Call this when CommitTransaction() succeeds.
//
// Parameters:
//   - startTime: The time returned by InstrumentTransactionStarted
//
// Metrics recorded:
//   - goqueue_broker_transactions_committed_total (counter)
//   - goqueue_broker_transaction_latency_seconds (histogram)
func InstrumentTransactionCommitted(startTime time.Time) {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Broker.RecordTransactionCommitted(time.Since(startTime).Seconds())
}

// InstrumentTransactionAborted records a transaction abort.
// Call this when AbortTransaction() is called.
//
// Metrics recorded:
//   - goqueue_broker_transactions_aborted_total (counter)
func InstrumentTransactionAborted() {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Broker.RecordTransactionAborted()
}

// InstrumentDuplicateRejected records a duplicate message rejection.
// Call this when idempotent producer sequence checking rejects a duplicate.
//
// Metrics recorded:
//   - goqueue_broker_messages_failed_total{error_type="duplicate"} (counter)
func InstrumentDuplicateRejected() {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Broker.RecordDuplicateRejected()
}

// =============================================================================
// CONNECTION INSTRUMENTATION
// =============================================================================

// InstrumentConnectionOpened records a new client connection.
// Call this when a client connects to the broker.
//
// Metrics recorded:
//   - goqueue_broker_active_connections (gauge, incremented)
func InstrumentConnectionOpened() {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Broker.ActiveConnections.Inc()
}

// InstrumentConnectionClosed records a client disconnection.
// Call this when a client disconnects from the broker.
//
// Metrics recorded:
//   - goqueue_broker_active_connections (gauge, decremented)
func InstrumentConnectionClosed() {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Broker.ActiveConnections.Dec()
}

// =============================================================================
// STORAGE INSTRUMENTATION
// =============================================================================
//
// These functions bridge broker operations to storage metrics.
// Storage-level instrumentation is also available directly in storage package.
//
// =============================================================================

// InstrumentStorageWrite records a storage write operation.
// Call this after writing message(s) to the log.
//
// Parameters:
//   - topic:     The topic name
//   - partition: The partition number
//   - bytes:     Number of bytes written
//
// Metrics recorded:
//   - goqueue_storage_bytes_written_total (counter)
func InstrumentStorageWrite(topic string, partition, bytes int) {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Storage.RecordWrite(topic, partition, bytes)
}

// InstrumentStorageRead records a storage read operation.
// Call this after reading message(s) from the log.
//
// Parameters:
//   - topic:     The topic name
//   - partition: The partition number
//   - bytes:     Number of bytes read
//
// Metrics recorded:
//   - goqueue_storage_bytes_read_total (counter)
func InstrumentStorageRead(topic string, partition, bytes int) {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Storage.RecordRead(topic, partition, bytes)
}

// InstrumentFsync records an fsync operation.
// Call this after fsyncing data to disk.
//
// Parameters:
//   - topic:   The topic name
//   - latency: Time taken for the fsync
//   - err:     Error if fsync failed (nil for success)
//
// Metrics recorded:
//   - goqueue_storage_fsync_total (counter)
//   - goqueue_storage_fsync_latency_seconds (histogram)
//   - goqueue_storage_fsync_errors_total (counter, if err != nil)
func InstrumentFsync(topic string, latency time.Duration, err error) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordFsync signature: (topic, latency float64, err)
	m.Storage.RecordFsync(topic, latency.Seconds(), err)
}

// InstrumentLogEndOffset sets the current log end offset (LEO).
// Call this after appending messages to update the LEO metric.
//
// Parameters:
//   - topic:     The topic name
//   - partition: The partition number
//   - offset:    The new LEO
//
// Metrics recorded:
//   - goqueue_storage_log_end_offset (gauge)
func InstrumentLogEndOffset(topic string, partition int, offset int64) {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Storage.SetLogEndOffset(topic, partition, offset)
}

// =============================================================================
// CONSUMER GROUP INSTRUMENTATION
// =============================================================================

// InstrumentConsumerGroupMembers sets the current member count for a group.
// Call this when members join or leave a consumer group.
//
// Parameters:
//   - group:       The consumer group ID
//   - memberCount: Current number of members
//
// Metrics recorded:
//   - goqueue_consumer_group_members (gauge)
func InstrumentConsumerGroupMembers(group string, memberCount int) {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Consumer.SetGroupMembers(group, memberCount)
}

// InstrumentConsumerGroupGeneration sets the current generation for a group.
// Call this when a rebalance completes and generation increments.
//
// Parameters:
//   - group:      The consumer group ID
//   - generation: Current generation number
//
// Metrics recorded:
//   - goqueue_consumer_group_generation (gauge)
func InstrumentConsumerGroupGeneration(group string, generation int64) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// SetGroupGeneration expects int, convert from int64
	m.Consumer.SetGroupGeneration(group, int(generation))
}

// InstrumentRebalance records a consumer group rebalance event.
// Call this when a rebalance is triggered.
//
// Parameters:
//   - group:         The consumer group ID
//   - trigger:       What triggered the rebalance (member_join, member_leave, etc.)
//   - rebalanceType: Type of rebalance (eager, cooperative)
//   - latency:       Time taken to complete the rebalance
//   - revoked:       Number of partitions revoked
//   - assigned:      Number of partitions assigned
//
// Metrics recorded:
//   - goqueue_consumer_rebalances_total (counter)
//   - goqueue_consumer_rebalance_latency_seconds (histogram)
//   - goqueue_consumer_partitions_revoked_total (counter)
//   - goqueue_consumer_partitions_assigned_total (counter)
func InstrumentRebalance(group, trigger, rebalanceType string, latency time.Duration, revoked, assigned int) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordRebalance signature: (group, trigger, rebalanceType, latency, revoked, assigned)
	m.Consumer.RecordRebalance(group, trigger, rebalanceType, latency.Seconds(), revoked, assigned)
}

// InstrumentOffsetCommit records an offset commit.
// Call this when a consumer commits offsets.
//
// Parameters:
//   - group:     The consumer group ID
//   - topic:     The topic name
//   - startTime: When the commit operation started
//   - err:       Error if commit failed (nil for success)
//
// Metrics recorded:
//   - goqueue_consumer_offset_commits_total (counter)
//   - goqueue_consumer_offset_commit_latency_seconds (histogram)
//   - goqueue_consumer_offset_commit_errors_total (counter, if err != nil)
func InstrumentOffsetCommit(group, topic string, startTime time.Time, err error) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordOffsetCommit signature: (topic, group, latency, err)
	m.Consumer.RecordOffsetCommit(topic, group, time.Since(startTime).Seconds(), err)
}

// InstrumentCommittedOffset sets the current committed offset for a group.
// Call this after an offset commit to update the lag calculation metric.
//
// Parameters:
//   - topic:     The topic name
//   - partition: The partition number
//   - group:     The consumer group ID
//   - offset:    The committed offset
//
// Metrics recorded:
//   - goqueue_consumer_committed_offset (gauge)
func InstrumentCommittedOffset(topic string, partition int, group string, offset int64) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// SetCommittedOffset signature: (topic, partition int, consumerGroup, offset)
	m.Consumer.SetCommittedOffset(topic, partition, group, offset)
}

// InstrumentPoll records a consumer poll operation.
// Call this when a consumer fetches messages.
//
// Parameters:
//   - group:     The consumer group ID
//   - startTime: When the poll operation started
//   - messages:  Number of messages returned in the poll
//
// Metrics recorded:
//   - goqueue_consumer_polls_total (counter)
//   - goqueue_consumer_poll_latency_seconds (histogram)
//   - goqueue_consumer_poll_batch_size (histogram)
//   - goqueue_consumer_poll_empty_total (counter, if messages == 0)
func InstrumentPoll(group string, startTime time.Time, messages int) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordPoll signature: (group, latency float64, messageCount)
	m.Consumer.RecordPoll(group, time.Since(startTime).Seconds(), messages)
}

// =============================================================================
// CLUSTER INSTRUMENTATION
// =============================================================================

// InstrumentNodeCounts sets the total and healthy node counts.
// Call this when cluster membership changes.
//
// Parameters:
//   - total:   Total number of nodes in cluster
//   - healthy: Number of healthy nodes
//
// Metrics recorded:
//   - goqueue_cluster_nodes_total (gauge)
//   - goqueue_cluster_nodes_healthy (gauge)
func InstrumentNodeCounts(total, healthy int) {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Cluster.SetNodeCounts(total, healthy)
}

// InstrumentNodeState sets the state of a specific node.
// Call this when a node's state changes.
//
// Parameters:
//   - nodeID: The node identifier
//   - state:  The state name ("offline", "online", "syncing")
//
// Metrics recorded:
//   - goqueue_cluster_node_state (gauge)
func InstrumentNodeState(nodeID, state string) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// SetNodeState expects (nodeID, state string)
	m.Cluster.SetNodeState(nodeID, state)
}

// InstrumentLeaderElection records a leader election event.
// Call this when a partition leadership changes.
//
// Parameters:
//   - topic:         The topic name
//   - partition:     The partition number
//   - electionType:  Type of election ("clean", "unclean")
//   - startTime:     When the election started
//
// Metrics recorded:
//   - goqueue_cluster_leader_elections_total (counter)
//   - goqueue_cluster_leader_election_latency_seconds (histogram)
//   - goqueue_cluster_unclean_leader_elections_total (counter, if unclean)
func InstrumentLeaderElection(topic string, partition int, electionType string, startTime time.Time) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// RecordLeaderElection signature: (topic, partition int, electionType, latency)
	m.Cluster.RecordLeaderElection(topic, partition, electionType, time.Since(startTime).Seconds())
}

// InstrumentISRChange records an ISR shrink or expand event.
// Call this when a replica joins or leaves the ISR.
//
// Parameters:
//   - topic:     The topic name
//   - partition: The partition number
//   - shrink:    True if ISR shrank, false if expanded
//
// Metrics recorded:
//   - goqueue_cluster_isr_shrinks_total (counter, if shrink)
func InstrumentISRChange(topic string, partition int, shrink bool) {
	m := metrics.Get()
	if m == nil {
		return
	}

	if shrink {
		m.Cluster.RecordISRShrink(topic, partition)
	}
	// Note: ISR expansions are typically not alerted on, but could be added
}

// InstrumentReplicationLag sets the replication lag for a follower.
// Call this periodically to update replication health metrics.
//
// Parameters:
//   - topic:     The topic name
//   - partition: The partition number
//   - nodeID:    The follower node ID
//   - lag:       Number of messages behind leader
//
// Metrics recorded:
//   - goqueue_cluster_replication_lag (gauge)
func InstrumentReplicationLag(topic string, partition int, nodeID string, lag int64) {
	m := metrics.Get()
	if m == nil {
		return
	}

	// SetReplicationLag signature: (topic, partition int, nodeID, lag)
	m.Cluster.SetReplicationLag(topic, partition, nodeID, lag)
}

// InstrumentClusterPartitions sets partition counts for the cluster.
// Call this when partition metadata changes.
//
// Parameters:
//   - total:           Total partitions across all topics
//   - underReplicated: Partitions with ISR < replication factor
//   - offline:         Partitions with no leader
//
// Metrics recorded:
//   - goqueue_cluster_partition_count (gauge)
//   - goqueue_cluster_under_replicated_partitions (gauge)
//   - goqueue_cluster_offline_partitions (gauge)
func InstrumentClusterPartitions(total, underReplicated, offline int) {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Cluster.PartitionCount.Set(float64(total))
	m.Cluster.UnderReplicatedPartitions.Set(float64(underReplicated))
	m.Cluster.OfflinePartitions.Set(float64(offline))
}

// =============================================================================
// TOPIC METRICS
// =============================================================================

// InstrumentTopicCount sets the current number of topics.
// Call this when topics are created or deleted.
//
// Parameters:
//   - count: Current number of topics
//
// Metrics recorded:
//   - goqueue_broker_topic_count (gauge)
func InstrumentTopicCount(count int) {
	m := metrics.Get()
	if m == nil {
		return
	}

	m.Broker.TopicCount.Set(float64(count))
}
