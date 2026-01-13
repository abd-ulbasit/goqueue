// =============================================================================
// CONSUMER METRICS - CONSUMER GROUP AND LAG INSTRUMENTATION
// =============================================================================
//
// WHAT ARE CONSUMER METRICS?
// Consumer metrics track the consumption side of goqueue:
//   - How many consumers are active?
//   - What's the consumer lag (how far behind are consumers)?
//   - How often are rebalances happening?
//   - What's the offset commit rate?
//
// WHY CONSUMER LAG IS THE MOST IMPORTANT METRIC:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                    CONSUMER LAG EXPLAINED                               │
//   │                                                                         │
//   │   WHAT IS LAG?                                                          │
//   │   Lag = Log End Offset (LEO) - Committed Offset                         │
//   │                                                                         │
//   │   ┌────────────────────────────────────────────────────────────────┐    │
//   │   │  Partition Log:                                                │    │
//   │   │                                                                │    │
//   │   │  [0][1][2][3][4][5][6][7][8][9][10][11][12]                    │    │
//   │   │                    ↑              ↑                            │    │
//   │   │              committed         LEO (next write)                │    │
//   │   │              offset = 5        offset = 12                     │    │
//   │   │                                                                │    │
//   │   │              LAG = 12 - 5 = 7 messages                         │    │
//   │   └────────────────────────────────────────────────────────────────┘    │
//   │                                                                         │
//   │   WHY LAG MATTERS:                                                      │
//   │   - LAG = 0: Consumer is caught up (good!)                              │
//   │   - LAG growing: Consumer can't keep up (bad!)                          │
//   │   - LAG stable > 0: Consumer is behind but keeping pace (okay)          │
//   │                                                                         │
//   │   ALERTING ON LAG:                                                      │
//   │   - Absolute: Alert if lag > 10,000 messages                            │
//   │   - Rate: Alert if lag increasing for 5+ minutes                        │
//   │   - Both are useful for different scenarios                             │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// KAFKA-STYLE LAG CALCULATION:
// We DON'T calculate lag in the metrics code. Instead, we expose:
//   - goqueue_storage_log_end_offset (from storage_metrics.go)
//   - goqueue_consumer_committed_offset (from this file)
//
// Grafana/PromQL calculates lag:
//   lag = goqueue_storage_log_end_offset - goqueue_consumer_committed_offset
//
// WHY THIS APPROACH?
//   - More flexible (can calculate lag different ways)
//   - Lower overhead (no background calculation)
//   - Standard pattern (Kafka does the same)
//
// COMPARISON WITH KAFKA CONSUMER METRICS:
//
//   ┌────────────────────────────┬──────────────────────────────────────────┐
//   │ Kafka Metric               │ goqueue Equivalent                       │
//   ├────────────────────────────┼──────────────────────────────────────────┤
//   │ records-lag (per partition)│ Calculate: LEO - committed_offset        │
//   │ records-lag-max            │ max(lag) by consumer_group               │
//   │ consumer-group-members     │ group_members                            │
//   │ commit-rate                │ offset_commits_total (rate)              │
//   │ rebalance-rate             │ rebalances_total (rate)                  │
//   │ join-rate                  │ group_joins_total (rate)                 │
//   │ assigned-partitions        │ assigned_partitions                      │
//   └────────────────────────────┴──────────────────────────────────────────┘
//
// =============================================================================

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// ConsumerMetrics contains all consumer-level metrics.
type ConsumerMetrics struct {
	// =========================================================================
	// OFFSET TRACKING
	// =========================================================================
	//
	// KAFKA-STYLE LAG:
	// We expose raw offsets and let Prometheus/Grafana calculate lag.
	// This is more flexible than calculating lag ourselves.
	//
	// LAG CALCULATION IN PROMQL:
	//   goqueue_storage_log_end_offset{topic="orders",partition="0"}
	//   - goqueue_consumer_committed_offset{topic="orders",partition="0",consumer_group="order-service"}
	//

	// CommittedOffset is the last committed offset per topic/partition/group.
	// Labels: topic, partition (if enabled), consumer_group
	//
	// CRITICAL for lag calculation!
	//
	// PROMQL:
	//   # Lag by consumer group
	//   goqueue_storage_log_end_offset - ignoring(consumer_group)
	//   goqueue_consumer_committed_offset
	CommittedOffset *prometheus.GaugeVec

	// OffsetCommitsTotal counts offset commit operations.
	// Labels: topic, consumer_group
	//
	// PROMQL:
	//   # Commits per second (should match consumption rate)
	//   rate(goqueue_consumer_offset_commits_total[5m])
	OffsetCommitsTotal *prometheus.CounterVec

	// OffsetCommitLatency measures time to commit offsets.
	// Labels: consumer_group
	OffsetCommitLatency *prometheus.HistogramVec

	// OffsetCommitErrors counts failed offset commits.
	// Labels: topic, consumer_group, error_type
	//
	// ALERTING:
	//   # Alert if commit errors > 0 for 5 minutes
	//   increase(goqueue_consumer_offset_commit_errors_total[5m]) > 0
	OffsetCommitErrors *prometheus.CounterVec

	// =========================================================================
	// CONSUMER GROUP MEMBERSHIP
	// =========================================================================
	//
	// WHAT IS A CONSUMER GROUP?
	// A consumer group is a set of consumers that:
	//   - Share work (each partition assigned to one consumer)
	//   - Maintain shared offset (all see same "committed" position)
	//   - Coordinate via rebalancing
	//
	// HEALTHY GROUP INDICATORS:
	//   - Stable member count
	//   - Low rebalance frequency
	//   - Even partition distribution
	//

	// GroupMembers is the current number of members in each group.
	// Labels: consumer_group
	//
	// PROMQL:
	//   # Groups with unstable membership (high variance)
	//   stddev_over_time(goqueue_consumer_group_members[1h]) > 1
	GroupMembers *prometheus.GaugeVec

	// GroupGenerations is the current generation ID.
	// Labels: consumer_group
	//
	// Generation increments on each rebalance.
	// Useful for detecting rebalance storms.
	GroupGenerations *prometheus.GaugeVec

	// GroupState tracks the current state of each consumer group.
	// Labels: consumer_group, state
	//
	// States: Empty, Stable, PreparingRebalance, CompletingRebalance, Dead
	//
	// We use a gauge with state as label - value is 1 for current state.
	GroupState *prometheus.GaugeVec

	// AssignedPartitions is partitions assigned per consumer group.
	// Labels: consumer_group
	//
	// Should roughly equal total partitions / group members.
	AssignedPartitions *prometheus.GaugeVec

	// =========================================================================
	// REBALANCE METRICS
	// =========================================================================
	//
	// WHAT IS A REBALANCE?
	// When consumers join/leave or partitions change, the group must
	// redistribute partitions. This is a "rebalance".
	//
	// REBALANCE TYPES:
	//   - EAGER (stop-the-world): All consumers stop, redistribute, resume
	//   - COOPERATIVE (incremental): Only affected partitions stop (M12)
	//
	// WHY REBALANCES MATTER:
	//   - During rebalance, affected partitions aren't being processed
	//   - Too many rebalances = processing delays
	//   - Long rebalances = processing gaps
	//
	// REBALANCE TRIGGERS:
	//   - Consumer join (new consumer added)
	//   - Consumer leave (consumer crashed or departed)
	//   - Partition added (topic scaled)
	//   - Heartbeat timeout (consumer presumed dead)
	//

	// RebalancesTotal counts total rebalance events.
	// Labels: consumer_group, trigger (join, leave, partition_added, timeout)
	//
	// ALERTING:
	//   # Alert if more than 10 rebalances in 5 minutes (rebalance storm)
	//   increase(goqueue_consumer_rebalances_total[5m]) > 10
	RebalancesTotal *prometheus.CounterVec

	// RebalanceLatency measures time to complete rebalance.
	// Labels: consumer_group, rebalance_type (eager, cooperative)
	//
	// PROMQL:
	//   # p99 rebalance time
	//   histogram_quantile(0.99,
	//     rate(goqueue_consumer_rebalance_latency_seconds_bucket[1h])
	//   )
	//
	// TARGET: < 1 second for cooperative rebalances
	RebalanceLatency *prometheus.HistogramVec

	// PartitionsRevoked counts partitions revoked during rebalance.
	// Labels: consumer_group
	//
	// For cooperative rebalance, should be minimal.
	PartitionsRevoked *prometheus.CounterVec

	// PartitionsAssigned counts partitions assigned during rebalance.
	// Labels: consumer_group
	PartitionsAssigned *prometheus.CounterVec

	// =========================================================================
	// GROUP JOIN/LEAVE METRICS
	// =========================================================================

	// GroupJoinsTotal counts consumer join events.
	// Labels: consumer_group
	GroupJoinsTotal *prometheus.CounterVec

	// GroupLeavesTotal counts consumer leave events.
	// Labels: consumer_group, reason (graceful, timeout, error)
	GroupLeavesTotal *prometheus.CounterVec

	// HeartbeatsSent counts heartbeat messages sent.
	// Labels: consumer_group
	//
	// Heartbeat frequency is configurable (default: 3s).
	// Use this to verify consumers are healthy.
	HeartbeatsSent *prometheus.CounterVec

	// HeartbeatLatency measures heartbeat round-trip time.
	// Labels: consumer_group
	HeartbeatLatency *prometheus.HistogramVec

	// HeartbeatTimeouts counts missed heartbeats.
	// Labels: consumer_group
	//
	// ALERTING:
	//   # Alert if heartbeat timeouts increasing
	//   increase(goqueue_consumer_heartbeat_timeouts_total[5m]) > 0
	HeartbeatTimeouts *prometheus.CounterVec

	// =========================================================================
	// SESSION TIMEOUT METRICS
	// =========================================================================

	// SessionTimeoutsTotal counts session timeout events.
	// Labels: consumer_group
	//
	// Session timeout = consumer presumed dead, triggers rebalance.
	// Should be very rare in healthy clusters.
	SessionTimeoutsTotal *prometheus.CounterVec

	// =========================================================================
	// POLL METRICS
	// =========================================================================
	//
	// POLL-BASED CONSUMPTION:
	// Consumers call poll() to fetch messages. These metrics track that.
	//

	// PollsTotal counts poll operations.
	// Labels: consumer_group
	PollsTotal *prometheus.CounterVec

	// PollLatency measures time spent in poll.
	// Labels: consumer_group
	//
	// Includes:
	//   - Message fetch time
	//   - Long-poll wait (if no messages)
	PollLatency *prometheus.HistogramVec

	// PollBatchSize measures messages returned per poll.
	// Labels: consumer_group
	//
	// PROMQL:
	//   # Average messages per poll
	//   rate(goqueue_consumer_poll_batch_size_sum[5m]) /
	//   rate(goqueue_consumer_poll_batch_size_count[5m])
	PollBatchSize *prometheus.HistogramVec

	// PollEmpty counts polls that returned no messages.
	// Labels: consumer_group
	//
	// High empty poll rate may indicate:
	//   - Consumer is caught up (good)
	//   - No messages being produced (okay)
	//   - Partition assignment issues (bad)
	PollEmpty *prometheus.CounterVec

	// =========================================================================
	// INTERNAL REFERENCE
	// =========================================================================

	registry *Registry
}

// newConsumerMetrics creates and registers all consumer metrics.
func newConsumerMetrics(r *Registry) *ConsumerMetrics {
	m := &ConsumerMetrics{registry: r}

	// Determine label names based on config
	topicGroupLabels := []string{"topic", "consumer_group"}
	topicPartitionGroupLabels := topicGroupLabels
	if r.config.IncludePartitionLabel {
		topicPartitionGroupLabels = []string{"topic", "partition", "consumer_group"}
	}

	// =========================================================================
	// OFFSET TRACKING
	// =========================================================================

	m.CommittedOffset = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "consumer",
			Name:      "committed_offset",
			Help:      "Last committed offset per topic/partition/group (for lag calculation)",
		},
		topicPartitionGroupLabels,
	)

	m.OffsetCommitsTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "offset_commits_total",
			Help:      "Total offset commit operations",
		},
		topicGroupLabels,
	)

	m.OffsetCommitLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "consumer",
			Name:      "offset_commit_latency_seconds",
			Help:      "Time to commit offsets",
			Buckets:   r.config.HistogramBuckets,
		},
		[]string{"consumer_group"},
	)

	m.OffsetCommitErrors = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "offset_commit_errors_total",
			Help:      "Total failed offset commits",
		},
		[]string{"topic", "consumer_group", "error_type"},
	)

	// =========================================================================
	// CONSUMER GROUP MEMBERSHIP
	// =========================================================================

	m.GroupMembers = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "consumer",
			Name:      "group_members",
			Help:      "Current number of members in consumer group",
		},
		[]string{"consumer_group"},
	)

	m.GroupGenerations = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "consumer",
			Name:      "group_generation",
			Help:      "Current generation ID of consumer group",
		},
		[]string{"consumer_group"},
	)

	m.GroupState = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "consumer",
			Name:      "group_state",
			Help:      "Current state of consumer group (1 = current state)",
		},
		[]string{"consumer_group", "state"},
	)

	m.AssignedPartitions = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "consumer",
			Name:      "assigned_partitions",
			Help:      "Total partitions assigned to consumer group",
		},
		[]string{"consumer_group"},
	)

	// =========================================================================
	// REBALANCE METRICS
	// =========================================================================

	m.RebalancesTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "rebalances_total",
			Help:      "Total rebalance events",
		},
		[]string{"consumer_group", "trigger"},
	)

	m.RebalanceLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "consumer",
			Name:      "rebalance_latency_seconds",
			Help:      "Time to complete rebalance",
			// Rebalances can take a while
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		},
		[]string{"consumer_group", "rebalance_type"},
	)

	m.PartitionsRevoked = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "partitions_revoked_total",
			Help:      "Total partitions revoked during rebalances",
		},
		[]string{"consumer_group"},
	)

	m.PartitionsAssigned = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "partitions_assigned_total",
			Help:      "Total partitions assigned during rebalances",
		},
		[]string{"consumer_group"},
	)

	// =========================================================================
	// GROUP JOIN/LEAVE METRICS
	// =========================================================================

	m.GroupJoinsTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "group_joins_total",
			Help:      "Total consumer join events",
		},
		[]string{"consumer_group"},
	)

	m.GroupLeavesTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "group_leaves_total",
			Help:      "Total consumer leave events",
		},
		[]string{"consumer_group", "reason"},
	)

	m.HeartbeatsSent = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "heartbeats_sent_total",
			Help:      "Total heartbeat messages sent",
		},
		[]string{"consumer_group"},
	)

	m.HeartbeatLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "consumer",
			Name:      "heartbeat_latency_seconds",
			Help:      "Heartbeat round-trip time",
			// Heartbeats should be fast
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
		},
		[]string{"consumer_group"},
	)

	m.HeartbeatTimeouts = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "heartbeat_timeouts_total",
			Help:      "Total missed heartbeats",
		},
		[]string{"consumer_group"},
	)

	// =========================================================================
	// SESSION TIMEOUT METRICS
	// =========================================================================

	m.SessionTimeoutsTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "session_timeouts_total",
			Help:      "Total session timeout events (consumer presumed dead)",
		},
		[]string{"consumer_group"},
	)

	// =========================================================================
	// POLL METRICS
	// =========================================================================

	m.PollsTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "polls_total",
			Help:      "Total poll operations",
		},
		[]string{"consumer_group"},
	)

	m.PollLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "consumer",
			Name:      "poll_latency_seconds",
			Help:      "Time spent in poll operation",
			Buckets:   r.config.HistogramBuckets,
		},
		[]string{"consumer_group"},
	)

	m.PollBatchSize = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "consumer",
			Name:      "poll_batch_size",
			Help:      "Number of messages returned per poll",
			// Batch sizes - message counts, not latencies
			Buckets: []float64{0, 1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
		[]string{"consumer_group"},
	)

	m.PollEmpty = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "consumer",
			Name:      "polls_empty_total",
			Help:      "Total polls that returned no messages",
		},
		[]string{"consumer_group"},
	)

	return m
}

// =============================================================================
// CONVENIENCE METHODS
// =============================================================================

// SetCommittedOffset sets the committed offset for a partition.
//
// USAGE:
//
//	metrics.Get().Consumer.SetCommittedOffset("orders", 0, "order-service", 12345)
func (m *ConsumerMetrics) SetCommittedOffset(topic string, partition int, consumerGroup string, offset int64) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.CommittedOffset.WithLabelValues(topic, partitionStr(partition), consumerGroup).Set(float64(offset))
	} else {
		m.CommittedOffset.WithLabelValues(topic, consumerGroup).Set(float64(offset))
	}
}

// RecordOffsetCommit records an offset commit operation.
func (m *ConsumerMetrics) RecordOffsetCommit(topic, consumerGroup string, latency float64, err error) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.OffsetCommitsTotal.WithLabelValues(topic, consumerGroup).Inc()
	m.OffsetCommitLatency.WithLabelValues(consumerGroup).Observe(latency)
	if err != nil {
		m.OffsetCommitErrors.WithLabelValues(topic, consumerGroup, "unknown").Inc()
	}
}

// SetGroupMembers sets the member count for a consumer group.
func (m *ConsumerMetrics) SetGroupMembers(consumerGroup string, count int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.GroupMembers.WithLabelValues(consumerGroup).Set(float64(count))
}

// SetGroupGeneration sets the generation ID for a consumer group.
func (m *ConsumerMetrics) SetGroupGeneration(consumerGroup string, generation int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.GroupGenerations.WithLabelValues(consumerGroup).Set(float64(generation))
}

// SetGroupState sets the state for a consumer group.
//
// States: "Empty", "Stable", "PreparingRebalance", "CompletingRebalance", "Dead"
func (m *ConsumerMetrics) SetGroupState(consumerGroup, state string) {
	if m == nil || !m.registry.enabled {
		return
	}
	// Reset all states to 0
	states := []string{"Empty", "Stable", "PreparingRebalance", "CompletingRebalance", "Dead"}
	for _, s := range states {
		m.GroupState.WithLabelValues(consumerGroup, s).Set(0)
	}
	// Set current state to 1
	m.GroupState.WithLabelValues(consumerGroup, state).Set(1)
}

// SetAssignedPartitions sets the assigned partition count for a group.
func (m *ConsumerMetrics) SetAssignedPartitions(consumerGroup string, count int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.AssignedPartitions.WithLabelValues(consumerGroup).Set(float64(count))
}

// RecordRebalance records a rebalance event.
//
// TRIGGERS: "join", "leave", "partition_added", "timeout"
// TYPES: "eager", "cooperative"
func (m *ConsumerMetrics) RecordRebalance(consumerGroup, trigger, rebalanceType string, latency float64, revoked, assigned int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.RebalancesTotal.WithLabelValues(consumerGroup, trigger).Inc()
	m.RebalanceLatency.WithLabelValues(consumerGroup, rebalanceType).Observe(latency)
	m.PartitionsRevoked.WithLabelValues(consumerGroup).Add(float64(revoked))
	m.PartitionsAssigned.WithLabelValues(consumerGroup).Add(float64(assigned))
}

// RecordGroupJoin records a consumer joining a group.
func (m *ConsumerMetrics) RecordGroupJoin(consumerGroup string) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.GroupJoinsTotal.WithLabelValues(consumerGroup).Inc()
}

// RecordGroupLeave records a consumer leaving a group.
//
// REASONS: "graceful", "timeout", "error"
func (m *ConsumerMetrics) RecordGroupLeave(consumerGroup, reason string) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.GroupLeavesTotal.WithLabelValues(consumerGroup, reason).Inc()
}

// RecordHeartbeat records a heartbeat.
func (m *ConsumerMetrics) RecordHeartbeat(consumerGroup string, latency float64, timedOut bool) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.HeartbeatsSent.WithLabelValues(consumerGroup).Inc()
	m.HeartbeatLatency.WithLabelValues(consumerGroup).Observe(latency)
	if timedOut {
		m.HeartbeatTimeouts.WithLabelValues(consumerGroup).Inc()
	}
}

// RecordSessionTimeout records a session timeout.
func (m *ConsumerMetrics) RecordSessionTimeout(consumerGroup string) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.SessionTimeoutsTotal.WithLabelValues(consumerGroup).Inc()
}

// RecordPoll records a poll operation.
func (m *ConsumerMetrics) RecordPoll(consumerGroup string, latency float64, messageCount int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.PollsTotal.WithLabelValues(consumerGroup).Inc()
	m.PollLatency.WithLabelValues(consumerGroup).Observe(latency)
	m.PollBatchSize.WithLabelValues(consumerGroup).Observe(float64(messageCount))
	if messageCount == 0 {
		m.PollEmpty.WithLabelValues(consumerGroup).Inc()
	}
}
