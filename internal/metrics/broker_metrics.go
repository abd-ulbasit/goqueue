// =============================================================================
// BROKER METRICS - MESSAGE FLOW INSTRUMENTATION
// =============================================================================
//
// WHAT ARE BROKER METRICS?
// Broker metrics track the flow of messages through goqueue:
//   - How many messages are being published?
//   - How fast are they being consumed?
//   - What's the latency for operations?
//   - Are there errors?
//
// WHY THESE METRICS MATTER:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                    BROKER METRICS USAGE                                 │
//   │                                                                         │
//   │   CAPACITY PLANNING                                                     │
//   │   ─────────────────                                                     │
//   │   Q: "Can our cluster handle Black Friday traffic?"                     │
//   │   A: Look at messages_published_total rate, compare to max throughput   │
//   │                                                                         │
//   │   DEBUGGING                                                             │
//   │   ─────────                                                             │
//   │   Q: "Why are orders taking longer to process?"                         │
//   │   A: Check publish_latency_seconds p99, look for spikes                 │
//   │                                                                         │
//   │   ALERTING                                                              │
//   │   ────────                                                              │
//   │   Rule: "Alert if error rate > 1% for 5 minutes"                        │
//   │   PromQL: rate(messages_failed_total[5m]) /                             │
//   │           rate(messages_published_total[5m]) > 0.01                     │
//   │                                                                         │
//   │   SLA MONITORING                                                        │
//   │   ──────────────                                                        │
//   │   Rule: "p99 publish latency must be < 10ms"                            │
//   │   PromQL: histogram_quantile(0.99,                                      │
//   │             rate(publish_latency_seconds_bucket[5m])) > 0.01            │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON WITH KAFKA METRICS:
//
//   ┌────────────────────────────┬──────────────────────────────────────────┐
//   │ Kafka Metric               │ goqueue Equivalent                       │
//   ├────────────────────────────┼──────────────────────────────────────────┤
//   │ MessagesInPerSec           │ messages_published_total (rate)          │
//   │ BytesInPerSec              │ bytes_published_total (rate)             │
//   │ MessagesOutPerSec          │ messages_consumed_total (rate)           │
//   │ RequestsPerSec             │ active_connections (gauge)               │
//   │ ProduceRequestLatencyMs    │ publish_latency_seconds (histogram)      │
//   │ FetchRequestLatencyMs      │ fetch_latency_seconds (histogram)        │
//   │ FailedProduceRequestsPerSec│ messages_failed_total (rate)             │
//   └────────────────────────────┴──────────────────────────────────────────┘
//
// =============================================================================

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// BrokerMetrics contains all broker-level metrics.
//
// METRIC NAMING:
// All metrics follow the pattern: goqueue_broker_{name}_{unit}
//   - goqueue: namespace (identifies our application)
//   - broker: subsystem (identifies component)
//   - name: descriptive metric name
//   - unit: seconds, bytes, total (for counters)
type BrokerMetrics struct {
	// =========================================================================
	// MESSAGE COUNTERS
	// =========================================================================
	//
	// COUNTERS vs GAUGES FOR MESSAGES:
	// We use counters (not gauges) because:
	//   - Counters survive restarts (rate calculation still works)
	//   - rate() in PromQL gives per-second rate
	//   - Monotonic increase makes anomaly detection easier
	//
	// TO GET RATE:
	//   rate(goqueue_broker_messages_published_total[5m])
	//
	// TO GET TOTAL OVER TIME:
	//   increase(goqueue_broker_messages_published_total[1h])
	//

	// MessagesPublished counts total messages published per topic.
	// Labels: topic
	//
	// PROMQL EXAMPLES:
	//   # Messages per second by topic
	//   rate(goqueue_broker_messages_published_total[5m])
	//
	//   # Top 5 busiest topics
	//   topk(5, rate(goqueue_broker_messages_published_total[5m]))
	MessagesPublished *prometheus.CounterVec

	// MessagesConsumed counts total messages consumed per topic and group.
	// Labels: topic, consumer_group
	//
	// PROMQL EXAMPLES:
	//   # Consumption rate by consumer group
	//   rate(goqueue_broker_messages_consumed_total[5m])
	//
	//   # Compare publish vs consume rate (should be similar)
	//   rate(goqueue_broker_messages_published_total[5m])
	//   - rate(goqueue_broker_messages_consumed_total[5m])
	MessagesConsumed *prometheus.CounterVec

	// MessagesFailed counts messages that failed to publish.
	// Labels: topic, error_type (validation, storage, timeout, unknown)
	//
	// ALERTING:
	//   # Alert if error rate > 1%
	//   rate(goqueue_broker_messages_failed_total[5m]) /
	//   rate(goqueue_broker_messages_published_total[5m]) > 0.01
	MessagesFailed *prometheus.CounterVec

	// MessagesAcked counts messages that were acknowledged.
	// Labels: topic, consumer_group
	MessagesAcked *prometheus.CounterVec

	// MessagesNacked counts messages that were negatively acknowledged.
	// Labels: topic, consumer_group
	MessagesNacked *prometheus.CounterVec

	// MessagesRejected counts messages sent to DLQ.
	// Labels: topic, consumer_group
	MessagesRejected *prometheus.CounterVec

	// =========================================================================
	// BYTE COUNTERS
	// =========================================================================
	//
	// WHY TRACK BYTES?
	//   - Network capacity planning
	//   - Storage growth prediction
	//   - Cost estimation (cloud charges by bytes)
	//   - Detect anomalies (sudden spike in message size)
	//

	// BytesPublished counts total bytes published per topic.
	// Labels: topic
	//
	// PROMQL:
	//   # Throughput in MB/s
	//   rate(goqueue_broker_bytes_published_total[5m]) / 1024 / 1024
	BytesPublished *prometheus.CounterVec

	// BytesConsumed counts total bytes consumed per topic and group.
	// Labels: topic, consumer_group
	BytesConsumed *prometheus.CounterVec

	// =========================================================================
	// LATENCY HISTOGRAMS
	// =========================================================================
	//
	// WHY HISTOGRAMS (not Summaries)?
	//
	// HISTOGRAM:
	//   + Aggregatable across instances (can calculate cluster-wide p99)
	//   + Fixed bucket boundaries (consistent across restarts)
	//   + Can calculate ANY percentile after the fact
	//   - Less accurate (depends on bucket boundaries)
	//   - Higher cardinality (one time series per bucket)
	//
	// SUMMARY:
	//   + More accurate percentiles (calculated on client)
	//   - NOT aggregatable (can't combine p99 from multiple instances)
	//   - Fixed quantiles (must decide at instrumentation time)
	//   - More expensive to calculate
	//
	// PROMETHEUS BEST PRACTICE: Use histograms, calculate percentiles with
	// histogram_quantile() in PromQL.
	//

	// PublishLatency measures time to persist a message (end-to-end).
	// Labels: topic
	//
	// INCLUDES:
	//   - Partition assignment
	//   - Message serialization
	//   - Disk write (or buffer)
	//   - Index update
	//
	// EXCLUDES:
	//   - Network latency (measured at HTTP layer)
	//   - Queue time (if producer batching)
	//
	// PROMQL:
	//   # p99 latency by topic
	//   histogram_quantile(0.99,
	//     rate(goqueue_broker_publish_latency_seconds_bucket[5m])
	//   )
	//
	//   # p50 (median) latency
	//   histogram_quantile(0.5,
	//     rate(goqueue_broker_publish_latency_seconds_bucket[5m])
	//   )
	PublishLatency *prometheus.HistogramVec

	// FetchLatency measures time to fetch messages for a consumer.
	// Labels: topic, consumer_group
	//
	// INCLUDES:
	//   - Index lookup
	//   - Message deserialization
	//   - Filter application (visibility, read_committed)
	//
	// NOTE: Long-poll wait time is NOT included. If no messages are
	// available and consumer waits, that's a separate metric.
	FetchLatency *prometheus.HistogramVec

	// AckLatency measures time to process an ACK/NACK/REJECT.
	// Labels: topic, ack_type (ack, nack, reject)
	AckLatency *prometheus.HistogramVec

	// =========================================================================
	// CONNECTION METRICS
	// =========================================================================

	// ActiveConnections is the current number of active client connections.
	// This is a gauge because it can go up and down.
	//
	// ALERTING:
	//   # Alert if connections spike (possible connection leak)
	//   goqueue_broker_active_connections > 10000
	//
	//   # Alert if connections drop to zero (broker might be unhealthy)
	//   goqueue_broker_active_connections == 0
	ActiveConnections prometheus.Gauge

	// ActiveConsumers is the current number of connected consumers.
	// Labels: topic, consumer_group
	ActiveConsumers *prometheus.GaugeVec

	// =========================================================================
	// TOPIC METRICS
	// =========================================================================

	// TopicPartitions is the number of partitions per topic.
	// Labels: topic
	TopicPartitions *prometheus.GaugeVec

	// TopicCount is the total number of topics.
	TopicCount prometheus.Gauge

	// =========================================================================
	// DELAY & PRIORITY METRICS (M5/M6)
	// =========================================================================

	// DelayedMessagesScheduled counts messages scheduled for delayed delivery.
	// Labels: topic
	DelayedMessagesScheduled *prometheus.CounterVec

	// DelayedMessagesFired counts delayed messages that became visible.
	// Labels: topic
	DelayedMessagesFired *prometheus.CounterVec

	// DelayedMessagesPending is the current number of pending delayed messages.
	// Labels: topic
	DelayedMessagesPending *prometheus.GaugeVec

	// PriorityMessagesPublished counts messages published by priority level.
	// Labels: topic, priority (critical, high, normal, low, background)
	PriorityMessagesPublished *prometheus.CounterVec

	// =========================================================================
	// TRANSACTION METRICS (M9)
	// =========================================================================

	// TransactionsStarted counts initiated transactions.
	TransactionsStarted prometheus.Counter

	// TransactionsCommitted counts successfully committed transactions.
	TransactionsCommitted prometheus.Counter

	// TransactionsAborted counts aborted transactions.
	TransactionsAborted prometheus.Counter

	// TransactionLatency measures transaction commit time.
	TransactionLatency prometheus.Histogram

	// =========================================================================
	// SCHEMA VALIDATION METRICS (M8)
	// =========================================================================

	// SchemaValidationsTotal counts schema validation attempts.
	// Labels: topic, result (success, failure)
	SchemaValidationsTotal *prometheus.CounterVec

	// SchemaValidationLatency measures schema validation time.
	// Labels: topic
	SchemaValidationLatency *prometheus.HistogramVec

	// =========================================================================
	// INTERNAL REFERENCE
	// =========================================================================

	registry *Registry
}

// newBrokerMetrics creates and registers all broker metrics.
func newBrokerMetrics(r *Registry) *BrokerMetrics {
	m := &BrokerMetrics{registry: r}

	// =========================================================================
	// MESSAGE COUNTERS
	// =========================================================================

	m.MessagesPublished = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "messages_published_total",
			Help:      "Total number of messages published to the broker",
		},
		[]string{"topic"},
	)

	m.MessagesConsumed = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "messages_consumed_total",
			Help:      "Total number of messages consumed from the broker",
		},
		[]string{"topic", "consumer_group"},
	)

	m.MessagesFailed = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "messages_failed_total",
			Help:      "Total number of messages that failed to publish",
		},
		[]string{"topic", "error_type"},
	)

	m.MessagesAcked = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "messages_acked_total",
			Help:      "Total number of messages acknowledged by consumers",
		},
		[]string{"topic", "consumer_group"},
	)

	m.MessagesNacked = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "messages_nacked_total",
			Help:      "Total number of messages negatively acknowledged (will retry)",
		},
		[]string{"topic", "consumer_group"},
	)

	m.MessagesRejected = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "messages_rejected_total",
			Help:      "Total number of messages rejected and sent to DLQ",
		},
		[]string{"topic", "consumer_group"},
	)

	// =========================================================================
	// BYTE COUNTERS
	// =========================================================================

	m.BytesPublished = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "bytes_published_total",
			Help:      "Total bytes published to the broker",
		},
		[]string{"topic"},
	)

	m.BytesConsumed = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "bytes_consumed_total",
			Help:      "Total bytes consumed from the broker",
		},
		[]string{"topic", "consumer_group"},
	)

	// =========================================================================
	// LATENCY HISTOGRAMS
	// =========================================================================

	m.PublishLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "broker",
			Name:      "publish_latency_seconds",
			Help:      "Time to publish a message (includes partition assignment, write, index update)",
			Buckets:   r.config.HistogramBuckets,
		},
		[]string{"topic"},
	)

	m.FetchLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "broker",
			Name:      "fetch_latency_seconds",
			Help:      "Time to fetch messages for a consumer (excludes long-poll wait)",
			Buckets:   r.config.HistogramBuckets,
		},
		[]string{"topic", "consumer_group"},
	)

	m.AckLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "broker",
			Name:      "ack_latency_seconds",
			Help:      "Time to process message acknowledgments",
			Buckets:   r.config.HistogramBuckets,
		},
		[]string{"topic", "ack_type"},
	)

	// =========================================================================
	// CONNECTION METRICS
	// =========================================================================

	m.ActiveConnections = r.newGauge(
		prometheus.GaugeOpts{
			Subsystem: "broker",
			Name:      "active_connections",
			Help:      "Current number of active client connections",
		},
	)

	m.ActiveConsumers = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "broker",
			Name:      "active_consumers",
			Help:      "Current number of active consumers per topic and group",
		},
		[]string{"topic", "consumer_group"},
	)

	// =========================================================================
	// TOPIC METRICS
	// =========================================================================

	m.TopicPartitions = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "broker",
			Name:      "topic_partitions",
			Help:      "Number of partitions per topic",
		},
		[]string{"topic"},
	)

	m.TopicCount = r.newGauge(
		prometheus.GaugeOpts{
			Subsystem: "broker",
			Name:      "topic_count",
			Help:      "Total number of topics",
		},
	)

	// =========================================================================
	// DELAY & PRIORITY METRICS
	// =========================================================================

	m.DelayedMessagesScheduled = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "delayed_messages_scheduled_total",
			Help:      "Total messages scheduled for delayed delivery",
		},
		[]string{"topic"},
	)

	m.DelayedMessagesFired = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "delayed_messages_fired_total",
			Help:      "Total delayed messages that became visible",
		},
		[]string{"topic"},
	)

	m.DelayedMessagesPending = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "broker",
			Name:      "delayed_messages_pending",
			Help:      "Current number of pending delayed messages",
		},
		[]string{"topic"},
	)

	m.PriorityMessagesPublished = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "priority_messages_published_total",
			Help:      "Messages published by priority level",
		},
		[]string{"topic", "priority"},
	)

	// =========================================================================
	// TRANSACTION METRICS
	// =========================================================================

	m.TransactionsStarted = r.newCounter(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "transactions_started_total",
			Help:      "Total transactions initiated",
		},
	)

	m.TransactionsCommitted = r.newCounter(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "transactions_committed_total",
			Help:      "Total transactions successfully committed",
		},
	)

	m.TransactionsAborted = r.newCounter(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "transactions_aborted_total",
			Help:      "Total transactions aborted",
		},
	)

	m.TransactionLatency = r.newHistogram(
		prometheus.HistogramOpts{
			Subsystem: "broker",
			Name:      "transaction_latency_seconds",
			Help:      "Time to commit a transaction",
			Buckets:   r.config.HistogramBuckets,
		},
	)

	// =========================================================================
	// SCHEMA VALIDATION METRICS
	// =========================================================================

	m.SchemaValidationsTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "broker",
			Name:      "schema_validations_total",
			Help:      "Total schema validation attempts",
		},
		[]string{"topic", "result"},
	)

	m.SchemaValidationLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "broker",
			Name:      "schema_validation_latency_seconds",
			Help:      "Time to validate message schema",
			// Schema validation should be very fast
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05},
		},
		[]string{"topic"},
	)

	return m
}

// =============================================================================
// CONVENIENCE METHODS
// =============================================================================
//
// These methods provide type-safe, documented ways to record metrics.
// They also handle the "metrics disabled" case gracefully.
//

// RecordPublish records a successful message publish.
//
// USAGE:
//
//	start := time.Now()
//	// ... publish message ...
//	metrics.Get().Broker.RecordPublish("orders", len(message), time.Since(start))
func (m *BrokerMetrics) RecordPublish(topic string, bytes int, latency float64) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.MessagesPublished.WithLabelValues(topic).Inc()
	m.BytesPublished.WithLabelValues(topic).Add(float64(bytes))
	m.PublishLatency.WithLabelValues(topic).Observe(latency)
}

// RecordPublishError records a failed message publish.
//
// ERROR TYPES:
//   - "validation": Schema validation failed
//   - "storage": Disk write failed
//   - "timeout": Operation timed out
//   - "unknown": Other errors
func (m *BrokerMetrics) RecordPublishError(topic, errorType string) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.MessagesFailed.WithLabelValues(topic, errorType).Inc()
}

// RecordConsume records a successful message consumption.
func (m *BrokerMetrics) RecordConsume(topic, consumerGroup string, count, bytes int, latency float64) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.MessagesConsumed.WithLabelValues(topic, consumerGroup).Add(float64(count))
	m.BytesConsumed.WithLabelValues(topic, consumerGroup).Add(float64(bytes))
	m.FetchLatency.WithLabelValues(topic, consumerGroup).Observe(latency)
}

// RecordAck records a message acknowledgment.
//
// ACK TYPES:
//   - "ack": Successful processing
//   - "nack": Transient failure (will retry)
//   - "reject": Permanent failure (to DLQ)
func (m *BrokerMetrics) RecordAck(topic, consumerGroup, ackType string, latency float64) {
	if m == nil || !m.registry.enabled {
		return
	}
	switch ackType {
	case "ack":
		m.MessagesAcked.WithLabelValues(topic, consumerGroup).Inc()
	case "nack":
		m.MessagesNacked.WithLabelValues(topic, consumerGroup).Inc()
	case "reject":
		m.MessagesRejected.WithLabelValues(topic, consumerGroup).Inc()
	}
	m.AckLatency.WithLabelValues(topic, ackType).Observe(latency)
}

// RecordDelayedMessage records a delayed message being scheduled.
func (m *BrokerMetrics) RecordDelayedMessage(topic string, scheduled bool) {
	if m == nil || !m.registry.enabled {
		return
	}
	if scheduled {
		m.DelayedMessagesScheduled.WithLabelValues(topic).Inc()
		m.DelayedMessagesPending.WithLabelValues(topic).Inc()
	} else {
		// Message fired
		m.DelayedMessagesFired.WithLabelValues(topic).Inc()
		m.DelayedMessagesPending.WithLabelValues(topic).Dec()
	}
}

// RecordPriorityPublish records a message published with priority.
//
// PRIORITY NAMES: "critical", "high", "normal", "low", "background"
func (m *BrokerMetrics) RecordPriorityPublish(topic, priority string) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.PriorityMessagesPublished.WithLabelValues(topic, priority).Inc()
}

// RecordSchemaValidation records a schema validation attempt.
func (m *BrokerMetrics) RecordSchemaValidation(topic string, success bool, latency float64) {
	if m == nil || !m.registry.enabled {
		return
	}
	result := "success"
	if !success {
		result = "failure"
	}
	m.SchemaValidationsTotal.WithLabelValues(topic, result).Inc()
	m.SchemaValidationLatency.WithLabelValues(topic).Observe(latency)
}

// SetTopicPartitions sets the partition count for a topic.
func (m *BrokerMetrics) SetTopicPartitions(topic string, count int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.TopicPartitions.WithLabelValues(topic).Set(float64(count))
}

// SetTopicCount sets the total topic count.
func (m *BrokerMetrics) SetTopicCount(count int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.TopicCount.Set(float64(count))
}

// SetActiveConnections sets the current connection count.
func (m *BrokerMetrics) SetActiveConnections(count int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.ActiveConnections.Set(float64(count))
}

// IncActiveConnections increments the connection count.
func (m *BrokerMetrics) IncActiveConnections() {
	if m == nil || !m.registry.enabled {
		return
	}
	m.ActiveConnections.Inc()
}

// DecActiveConnections decrements the connection count.
func (m *BrokerMetrics) DecActiveConnections() {
	if m == nil || !m.registry.enabled {
		return
	}
	m.ActiveConnections.Dec()
}

// =============================================================================
// TRANSACTION CONVENIENCE METHODS
// =============================================================================
//
// TRANSACTION LIFECYCLE:
//   BeginTransaction() → RecordTransactionStarted()
//   CommitTransaction() → RecordTransactionCommitted(latency)
//   AbortTransaction() → RecordTransactionAborted()
//
// METRICS TRACKED:
//   - goqueue_broker_transactions_started_total (counter)
//   - goqueue_broker_transactions_committed_total (counter)
//   - goqueue_broker_transactions_aborted_total (counter)
//   - goqueue_broker_transaction_latency_seconds (histogram)
//
// PROMQL EXAMPLES:
//   # Transaction success rate
//   rate(goqueue_broker_transactions_committed_total[5m]) /
//   rate(goqueue_broker_transactions_started_total[5m])
//
//   # p99 transaction commit latency
//   histogram_quantile(0.99, rate(goqueue_broker_transaction_latency_seconds_bucket[5m]))
//
// =============================================================================

// RecordTransactionStarted records a new transaction being started.
func (m *BrokerMetrics) RecordTransactionStarted() {
	if m == nil || !m.registry.enabled {
		return
	}
	m.TransactionsStarted.Inc()
}

// RecordTransactionCommitted records a successful transaction commit.
// latency is the time from BeginTransaction to CommitTransaction in seconds.
func (m *BrokerMetrics) RecordTransactionCommitted(latency float64) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.TransactionsCommitted.Inc()
	m.TransactionLatency.Observe(latency)
}

// RecordTransactionAborted records a transaction abort.
func (m *BrokerMetrics) RecordTransactionAborted() {
	if m == nil || !m.registry.enabled {
		return
	}
	m.TransactionsAborted.Inc()
}

// RecordDuplicateRejected records an idempotent producer duplicate rejection.
func (m *BrokerMetrics) RecordDuplicateRejected() {
	if m == nil || !m.registry.enabled {
		return
	}
	// Use the failed counter with a specific error type
	m.MessagesFailed.WithLabelValues("_all", "duplicate").Inc()
}
