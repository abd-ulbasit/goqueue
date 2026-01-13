// =============================================================================
// CLUSTER METRICS - DISTRIBUTED SYSTEM HEALTH INSTRUMENTATION
// =============================================================================
//
// WHAT ARE CLUSTER METRICS?
// Cluster metrics track the distributed aspects of goqueue:
//   - How many nodes are healthy?
//   - Is there a leader for each partition?
//   - How is replication performing?
//   - Are ISRs (In-Sync Replicas) healthy?
//
// WHY CLUSTER METRICS ARE CRITICAL:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                    CLUSTER HEALTH INDICATORS                            │
//   │                                                                         │
//   │   THE "BIG THREE" CLUSTER ALERTS:                                       │
//   │                                                                         │
//   │   1. UNDER-REPLICATED PARTITIONS                                        │
//   │      ─────────────────────────────                                      │
//   │      Definition: Partitions where ISR size < replication factor         │
//   │      Why: Reduced durability - some replicas are behind                 │
//   │      Action: Check lagging broker, network issues                       │
//   │                                                                         │
//   │   2. OFFLINE PARTITIONS                                                 │
//   │      ───────────────────                                                │
//   │      Definition: Partitions with no leader                              │
//   │      Why: NO reads or writes possible to that partition                 │
//   │      Action: IMMEDIATE - check if broker crashed                        │
//   │                                                                         │
//   │   3. LEADER IMBALANCE                                                   │
//   │      ─────────────────                                                  │
//   │      Definition: One broker has many more leaders than others           │
//   │      Why: Hot spots, uneven load                                        │
//   │      Action: Run leader rebalance                                       │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// ISR (IN-SYNC REPLICAS) EXPLAINED:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                    IN-SYNC REPLICAS (ISR)                               │
//   │                                                                         │
//   │   WHAT IS ISR?                                                          │
//   │   Set of replicas that are "caught up" with the leader.                 │
//   │                                                                         │
//   │   ┌─────────────────────────────────────────────────────────────────┐   │
//   │   │  Leader (LEO=1000)                                              │   │
//   │   │     │                                                           │   │
//   │   │     ├── Follower 1 (LEO=999)  ✓ IN ISR (close enough)           │   │
//   │   │     ├── Follower 2 (LEO=998)  ✓ IN ISR                          │   │
//   │   │     └── Follower 3 (LEO=500)  ✗ OUT OF ISR (too far behind)     │   │
//   │   │                                                                 │   │
//   │   │  ISR = {Leader, Follower1, Follower2}                           │   │
//   │   │  ISR size = 3 (of 4 total replicas)                             │   │
//   │   └─────────────────────────────────────────────────────────────────┘   │
//   │                                                                         │
//   │   WHY ISR MATTERS:                                                      │
//   │   - Write is ACKed when all ISR replicas have it                        │
//   │   - If ISR shrinks to 1 (just leader), durability is reduced            │
//   │   - ISR expansion = follower caught up = good sign                      │
//   │   - ISR shrink = follower fell behind = investigate                     │
//   │                                                                         │
//   │   KAFKA ISR CRITERIA (and goqueue):                                     │
//   │   - Follower must be within replica.lag.time.max.ms (10s default)       │
//   │   - Follower must be within replica.lag.max.messages (1000 default)     │
//   │   - Both conditions must be met to stay in ISR                          │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON WITH KAFKA CLUSTER METRICS:
//
//   ┌────────────────────────────────────┬────────────────────────────────────┐
//   │ Kafka Metric                       │ goqueue Equivalent                 │
//   ├────────────────────────────────────┼────────────────────────────────────┤
//   │ UnderReplicatedPartitions          │ under_replicated_partitions        │
//   │ OfflinePartitionsCount             │ offline_partitions                 │
//   │ ActiveControllerCount              │ is_controller (per node)           │
//   │ LeaderCount                        │ leaders_per_node                   │
//   │ PartitionCount                     │ partition_count                    │
//   │ IsrShrinkRate                      │ isr_shrinks_total                  │
//   │ IsrExpandRate                      │ isr_expands_total                  │
//   │ LeaderElectionRateAndTimeMs        │ leader_elections_total, latency    │
//   │ UncleanLeaderElectionsPerSec       │ unclean_leader_elections_total     │
//   └────────────────────────────────────┴────────────────────────────────────┘
//
// =============================================================================

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// ClusterMetrics contains all cluster-level metrics.
type ClusterMetrics struct {
	// =========================================================================
	// NODE METRICS
	// =========================================================================
	//
	// WHAT IS A NODE?
	// A node is a single goqueue broker instance. In a cluster, multiple
	// nodes work together to provide high availability.
	//

	// NodesTotal is the total number of nodes in the cluster.
	//
	// PROMQL:
	//   # Should match expected cluster size
	//   goqueue_cluster_nodes_total == 3  # for 3-node cluster
	NodesTotal prometheus.Gauge

	// NodesHealthy is the number of healthy (responding) nodes.
	//
	// ALERTING:
	//   # Alert if any node is unhealthy
	//   goqueue_cluster_nodes_healthy < goqueue_cluster_nodes_total
	NodesHealthy prometheus.Gauge

	// NodeState tracks each node's state.
	// Labels: node_id, state (healthy, suspect, dead)
	//
	// State transitions:
	//   healthy → suspect (missed heartbeats)
	//   suspect → dead (confirmed down)
	//   dead → healthy (came back)
	NodeState *prometheus.GaugeVec

	// IsController indicates if this node is the controller.
	// Value: 1 if controller, 0 otherwise.
	//
	// WHAT IS THE CONTROLLER?
	// One node is elected as "controller" and handles:
	//   - Partition leader election
	//   - Cluster membership changes
	//   - Topic creation/deletion
	//
	// There must be EXACTLY 1 controller in a healthy cluster.
	IsController prometheus.Gauge

	// =========================================================================
	// PARTITION METRICS
	// =========================================================================

	// PartitionCount is the total number of partitions across all topics.
	PartitionCount prometheus.Gauge

	// LeadersPerNode tracks how many partitions each node is leader for.
	// Labels: node_id
	//
	// IDEAL: Even distribution across nodes
	// PROBLEM: One node with many more leaders = hot spot
	//
	// PROMQL:
	//   # Leader imbalance (variance)
	//   stddev(goqueue_cluster_leaders_per_node)
	LeadersPerNode *prometheus.GaugeVec

	// FollowersPerNode tracks how many partitions each node is follower for.
	// Labels: node_id
	FollowersPerNode *prometheus.GaugeVec

	// =========================================================================
	// ISR METRICS
	// =========================================================================
	//
	// ISR = In-Sync Replicas
	// The set of replicas that are "caught up" with the leader.
	//

	// ISRSize tracks the ISR size per partition.
	// Labels: topic, partition (if enabled)
	//
	// PROMQL:
	//   # Partitions with shrunk ISR
	//   goqueue_cluster_isr_size < on(topic,partition) goqueue_topic_replication_factor
	ISRSize *prometheus.GaugeVec

	// UnderReplicatedPartitions counts partitions where ISR < replication factor.
	//
	// THIS IS A KEY HEALTH INDICATOR!
	//
	// ALERTING:
	//   # Alert if any under-replicated partitions
	//   goqueue_cluster_under_replicated_partitions > 0
	UnderReplicatedPartitions prometheus.Gauge

	// OfflinePartitions counts partitions with no leader.
	//
	// THIS IS CRITICAL! No leader = no reads/writes.
	//
	// ALERTING:
	//   # CRITICAL: Any offline partition is an outage
	//   goqueue_cluster_offline_partitions > 0
	OfflinePartitions prometheus.Gauge

	// ISRShrinksTotal counts ISR shrink events.
	// Labels: topic, partition (if enabled)
	//
	// ISR shrink = a follower fell behind = investigate
	ISRShrinksTotal *prometheus.CounterVec

	// ISRExpandsTotal counts ISR expansion events.
	// Labels: topic, partition (if enabled)
	//
	// ISR expand = a follower caught up = recovery
	ISRExpandsTotal *prometheus.CounterVec

	// =========================================================================
	// LEADER ELECTION METRICS
	// =========================================================================
	//
	// WHAT IS LEADER ELECTION?
	// When a partition leader fails, a new leader must be elected.
	// - CLEAN election: New leader chosen from ISR (no data loss)
	// - UNCLEAN election: Leader chosen from non-ISR (potential data loss)
	//

	// LeaderElectionsTotal counts leader election events.
	// Labels: topic, partition (if enabled), election_type (clean, unclean)
	LeaderElectionsTotal *prometheus.CounterVec

	// LeaderElectionLatency measures time to elect a new leader.
	// Labels: topic
	//
	// TARGET: < 5 seconds (our goal from PROGRESS.md)
	LeaderElectionLatency *prometheus.HistogramVec

	// UncleanLeaderElectionsTotal counts unclean leader elections.
	//
	// UNCLEAN = potential data loss!
	// Should be 0 in production (unclean elections are disabled by default).
	//
	// ALERTING:
	//   # CRITICAL: Unclean election may have caused data loss
	//   increase(goqueue_cluster_unclean_leader_elections_total[1h]) > 0
	UncleanLeaderElectionsTotal prometheus.Counter

	// PreferredLeaderImbalance counts partitions not on preferred leader.
	//
	// WHAT IS PREFERRED LEADER?
	// The first replica in the replica list is the "preferred" leader.
	// If leaders migrate due to failures, they may not return to preferred.
	// This metric tracks how many are "out of place".
	PreferredLeaderImbalance prometheus.Gauge

	// =========================================================================
	// REPLICATION METRICS
	// =========================================================================
	//
	// HOW REPLICATION WORKS:
	// 1. Producer writes to leader
	// 2. Followers fetch from leader (pull-based, like Kafka)
	// 3. Leader tracks follower progress
	// 4. High watermark advances when ISR catches up
	//

	// ReplicationLag tracks how far each follower is behind.
	// Labels: topic, partition (if enabled), node_id
	//
	// PROMQL:
	//   # Max lag across all followers
	//   max(goqueue_cluster_replication_lag)
	ReplicationLag *prometheus.GaugeVec

	// FetchRequestsTotal counts fetch requests from followers.
	// Labels: node_id (the follower making the request)
	FetchRequestsTotal *prometheus.CounterVec

	// FetchLatency measures fetch request latency.
	// Labels: node_id
	FetchLatency *prometheus.HistogramVec

	// BytesReplicatedTotal counts bytes replicated to followers.
	// Labels: topic
	BytesReplicatedTotal *prometheus.CounterVec

	// HighWatermark tracks the high watermark per partition.
	// Labels: topic, partition (if enabled)
	//
	// WHAT IS HIGH WATERMARK (HW)?
	// The offset up to which all ISR replicas have the data.
	// Consumers can only read up to HW (not LEO).
	//
	//   ┌────────────────────────────────────────────────────────────┐
	//   │  [0][1][2][3][4][5][6][7][8][9][10][11][12]                │
	//   │                 ↑              ↑                           │
	//   │                HW             LEO                          │
	//   │           (safe to read)  (latest write)                   │
	//   │                                                            │
	//   │  Messages 5-11 are written but not yet replicated to all   │
	//   │  ISR members. Consumers only see up to offset 4.           │
	//   └────────────────────────────────────────────────────────────┘
	HighWatermark *prometheus.GaugeVec

	// =========================================================================
	// CONTROLLER METRICS
	// =========================================================================

	// ControllerElectionsTotal counts controller election events.
	ControllerElectionsTotal prometheus.Counter

	// ControllerAge tracks how long the current controller has been active.
	// Exposed as a gauge that resets on controller change.
	//
	// PROMQL:
	//   # How long has controller been stable
	//   goqueue_cluster_controller_age_seconds
	//
	// Frequent controller changes = instability
	ControllerAgeSeconds prometheus.Gauge

	// =========================================================================
	// NETWORK METRICS
	// =========================================================================

	// InterBrokerNetworkIn counts bytes received from other brokers.
	InterBrokerNetworkIn prometheus.Counter

	// InterBrokerNetworkOut counts bytes sent to other brokers.
	InterBrokerNetworkOut prometheus.Counter

	// InterBrokerLatency measures latency for inter-broker communication.
	// Labels: target_node_id
	InterBrokerLatency *prometheus.HistogramVec

	// =========================================================================
	// FAILURE DETECTION METRICS
	// =========================================================================

	// FailureDetections counts node failure detections.
	// Labels: node_id
	FailureDetections *prometheus.CounterVec

	// HeartbeatsReceived counts heartbeats received from each node.
	// Labels: node_id
	HeartbeatsReceived *prometheus.CounterVec

	// HeartbeatsMissed counts missed heartbeats per node.
	// Labels: node_id
	HeartbeatsMissed *prometheus.CounterVec

	// =========================================================================
	// INTERNAL REFERENCE
	// =========================================================================

	registry *Registry
}

// newClusterMetrics creates and registers all cluster metrics.
func newClusterMetrics(r *Registry) *ClusterMetrics {
	m := &ClusterMetrics{registry: r}

	// Determine label names based on config
	topicLabels := []string{"topic"}
	topicPartitionLabels := topicLabels
	if r.config.IncludePartitionLabel {
		topicPartitionLabels = []string{"topic", "partition"}
	}

	// =========================================================================
	// NODE METRICS
	// =========================================================================

	m.NodesTotal = r.newGauge(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "nodes_total",
			Help:      "Total number of nodes in the cluster",
		},
	)

	m.NodesHealthy = r.newGauge(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "nodes_healthy",
			Help:      "Number of healthy (responding) nodes",
		},
	)

	m.NodeState = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "node_state",
			Help:      "State of each node (1 = current state)",
		},
		[]string{"node_id", "state"},
	)

	m.IsController = r.newGauge(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "is_controller",
			Help:      "Whether this node is the cluster controller (1 = yes)",
		},
	)

	// =========================================================================
	// PARTITION METRICS
	// =========================================================================

	m.PartitionCount = r.newGauge(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "partition_count",
			Help:      "Total number of partitions across all topics",
		},
	)

	m.LeadersPerNode = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "leaders_per_node",
			Help:      "Number of partition leaders per node",
		},
		[]string{"node_id"},
	)

	m.FollowersPerNode = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "followers_per_node",
			Help:      "Number of partition followers per node",
		},
		[]string{"node_id"},
	)

	// =========================================================================
	// ISR METRICS
	// =========================================================================

	m.ISRSize = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "isr_size",
			Help:      "Size of the in-sync replica set per partition",
		},
		topicPartitionLabels,
	)

	m.UnderReplicatedPartitions = r.newGauge(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "under_replicated_partitions",
			Help:      "Number of partitions where ISR size < replication factor",
		},
	)

	m.OfflinePartitions = r.newGauge(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "offline_partitions",
			Help:      "Number of partitions with no leader (CRITICAL)",
		},
	)

	m.ISRShrinksTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "isr_shrinks_total",
			Help:      "Total ISR shrink events (follower fell behind)",
		},
		topicPartitionLabels,
	)

	m.ISRExpandsTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "isr_expands_total",
			Help:      "Total ISR expand events (follower caught up)",
		},
		topicPartitionLabels,
	)

	// =========================================================================
	// LEADER ELECTION METRICS
	// =========================================================================

	electionLabels := []string{"topic", "election_type"}
	if r.config.IncludePartitionLabel {
		electionLabels = []string{"topic", "partition", "election_type"}
	}

	m.LeaderElectionsTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "leader_elections_total",
			Help:      "Total leader election events",
		},
		electionLabels,
	)

	m.LeaderElectionLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "cluster",
			Name:      "leader_election_latency_seconds",
			Help:      "Time to elect a new leader",
			// Election should be fast
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		},
		topicLabels,
	)

	m.UncleanLeaderElectionsTotal = r.newCounter(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "unclean_leader_elections_total",
			Help:      "Total unclean leader elections (potential data loss)",
		},
	)

	m.PreferredLeaderImbalance = r.newGauge(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "preferred_leader_imbalance",
			Help:      "Number of partitions not on preferred leader",
		},
	)

	// =========================================================================
	// REPLICATION METRICS
	// =========================================================================

	replicationLabels := []string{"topic", "node_id"}
	if r.config.IncludePartitionLabel {
		replicationLabels = []string{"topic", "partition", "node_id"}
	}

	m.ReplicationLag = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "replication_lag",
			Help:      "Number of messages a follower is behind the leader",
		},
		replicationLabels,
	)

	m.FetchRequestsTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "fetch_requests_total",
			Help:      "Total fetch requests from followers",
		},
		[]string{"node_id"},
	)

	m.FetchLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "cluster",
			Name:      "fetch_latency_seconds",
			Help:      "Fetch request latency for replication",
			Buckets:   r.config.HistogramBuckets,
		},
		[]string{"node_id"},
	)

	m.BytesReplicatedTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "bytes_replicated_total",
			Help:      "Total bytes replicated to followers",
		},
		topicLabels,
	)

	m.HighWatermark = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "high_watermark",
			Help:      "High watermark per partition (safe to read up to this offset)",
		},
		topicPartitionLabels,
	)

	// =========================================================================
	// CONTROLLER METRICS
	// =========================================================================

	m.ControllerElectionsTotal = r.newCounter(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "controller_elections_total",
			Help:      "Total controller election events",
		},
	)

	m.ControllerAgeSeconds = r.newGauge(
		prometheus.GaugeOpts{
			Subsystem: "cluster",
			Name:      "controller_age_seconds",
			Help:      "How long the current controller has been active",
		},
	)

	// =========================================================================
	// NETWORK METRICS
	// =========================================================================

	m.InterBrokerNetworkIn = r.newCounter(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "inter_broker_network_in_bytes_total",
			Help:      "Total bytes received from other brokers",
		},
	)

	m.InterBrokerNetworkOut = r.newCounter(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "inter_broker_network_out_bytes_total",
			Help:      "Total bytes sent to other brokers",
		},
	)

	m.InterBrokerLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "cluster",
			Name:      "inter_broker_latency_seconds",
			Help:      "Latency for inter-broker communication",
			Buckets:   r.config.HistogramBuckets,
		},
		[]string{"target_node_id"},
	)

	// =========================================================================
	// FAILURE DETECTION METRICS
	// =========================================================================

	m.FailureDetections = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "failure_detections_total",
			Help:      "Total node failure detections",
		},
		[]string{"node_id"},
	)

	m.HeartbeatsReceived = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "heartbeats_received_total",
			Help:      "Total heartbeats received from nodes",
		},
		[]string{"node_id"},
	)

	m.HeartbeatsMissed = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "cluster",
			Name:      "heartbeats_missed_total",
			Help:      "Total missed heartbeats per node",
		},
		[]string{"node_id"},
	)

	return m
}

// =============================================================================
// CONVENIENCE METHODS
// =============================================================================

// SetNodeCounts sets the total and healthy node counts.
func (m *ClusterMetrics) SetNodeCounts(total, healthy int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.NodesTotal.Set(float64(total))
	m.NodesHealthy.Set(float64(healthy))
}

// SetNodeState sets the state for a node.
//
// States: "healthy", "suspect", "dead"
func (m *ClusterMetrics) SetNodeState(nodeID, state string) {
	if m == nil || !m.registry.enabled {
		return
	}
	// Reset all states to 0
	states := []string{"healthy", "suspect", "dead"}
	for _, s := range states {
		m.NodeState.WithLabelValues(nodeID, s).Set(0)
	}
	// Set current state to 1
	m.NodeState.WithLabelValues(nodeID, state).Set(1)
}

// SetIsController sets whether this node is the controller.
func (m *ClusterMetrics) SetIsController(isController bool) {
	if m == nil || !m.registry.enabled {
		return
	}
	if isController {
		m.IsController.Set(1)
	} else {
		m.IsController.Set(0)
	}
}

// SetLeadersPerNode sets the leader count for a node.
func (m *ClusterMetrics) SetLeadersPerNode(nodeID string, count int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.LeadersPerNode.WithLabelValues(nodeID).Set(float64(count))
}

// SetISRSize sets the ISR size for a partition.
func (m *ClusterMetrics) SetISRSize(topic string, partition int, size int) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.ISRSize.WithLabelValues(topic, partitionStr(partition)).Set(float64(size))
	} else {
		m.ISRSize.WithLabelValues(topic).Set(float64(size))
	}
}

// SetUnderReplicatedPartitions sets the under-replicated partition count.
func (m *ClusterMetrics) SetUnderReplicatedPartitions(count int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.UnderReplicatedPartitions.Set(float64(count))
}

// SetOfflinePartitions sets the offline partition count.
func (m *ClusterMetrics) SetOfflinePartitions(count int) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.OfflinePartitions.Set(float64(count))
}

// RecordISRShrink records an ISR shrink event.
func (m *ClusterMetrics) RecordISRShrink(topic string, partition int) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.ISRShrinksTotal.WithLabelValues(topic, partitionStr(partition)).Inc()
	} else {
		m.ISRShrinksTotal.WithLabelValues(topic).Inc()
	}
}

// RecordISRExpand records an ISR expansion event.
func (m *ClusterMetrics) RecordISRExpand(topic string, partition int) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.ISRExpandsTotal.WithLabelValues(topic, partitionStr(partition)).Inc()
	} else {
		m.ISRExpandsTotal.WithLabelValues(topic).Inc()
	}
}

// RecordLeaderElection records a leader election event.
//
// electionType: "clean" or "unclean"
func (m *ClusterMetrics) RecordLeaderElection(topic string, partition int, electionType string, latency float64) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.LeaderElectionsTotal.WithLabelValues(topic, partitionStr(partition), electionType).Inc()
	} else {
		m.LeaderElectionsTotal.WithLabelValues(topic, electionType).Inc()
	}
	m.LeaderElectionLatency.WithLabelValues(topic).Observe(latency)

	if electionType == "unclean" {
		m.UncleanLeaderElectionsTotal.Inc()
	}
}

// SetReplicationLag sets the replication lag for a follower.
func (m *ClusterMetrics) SetReplicationLag(topic string, partition int, nodeID string, lag int64) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.ReplicationLag.WithLabelValues(topic, partitionStr(partition), nodeID).Set(float64(lag))
	} else {
		m.ReplicationLag.WithLabelValues(topic, nodeID).Set(float64(lag))
	}
}

// RecordFetchRequest records a replication fetch request.
func (m *ClusterMetrics) RecordFetchRequest(nodeID string, latency float64) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.FetchRequestsTotal.WithLabelValues(nodeID).Inc()
	m.FetchLatency.WithLabelValues(nodeID).Observe(latency)
}

// SetHighWatermark sets the high watermark for a partition.
func (m *ClusterMetrics) SetHighWatermark(topic string, partition int, hw int64) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.HighWatermark.WithLabelValues(topic, partitionStr(partition)).Set(float64(hw))
	} else {
		m.HighWatermark.WithLabelValues(topic).Set(float64(hw))
	}
}

// RecordFailureDetection records a node failure detection.
func (m *ClusterMetrics) RecordFailureDetection(nodeID string) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.FailureDetections.WithLabelValues(nodeID).Inc()
}

// RecordHeartbeat records a heartbeat received from a node.
func (m *ClusterMetrics) RecordHeartbeat(nodeID string, missed bool) {
	if m == nil || !m.registry.enabled {
		return
	}
	if missed {
		m.HeartbeatsMissed.WithLabelValues(nodeID).Inc()
	} else {
		m.HeartbeatsReceived.WithLabelValues(nodeID).Inc()
	}
}

// SetControllerAge sets the controller age in seconds.
func (m *ClusterMetrics) SetControllerAge(seconds float64) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.ControllerAgeSeconds.Set(seconds)
}

// RecordControllerElection records a controller election.
func (m *ClusterMetrics) RecordControllerElection() {
	if m == nil || !m.registry.enabled {
		return
	}
	m.ControllerElectionsTotal.Inc()
	m.ControllerAgeSeconds.Set(0) // Reset age
}
