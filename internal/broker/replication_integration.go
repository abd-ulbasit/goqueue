// =============================================================================
// REPLICATION INTEGRATION - BROKER REPLICATION SUPPORT (M11)
// =============================================================================
//
// WHAT: Integrates M11 replication components with the broker.
//
// NEW COMPONENTS (M11):
//   - ReplicaManager: Manages local replicas (leader/follower state)
//   - ISRManager: Tracks in-sync replicas
//   - FollowerFetcher: Background loop fetching from leaders
//   - SnapshotManager: Fast follower catch-up
//   - PartitionLeaderElector: Leader election for partitions
//   - ReplicationServer: HTTP endpoints for replication
//   - ReplicationClient: HTTP client for follower fetching
//
// =============================================================================

package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"time"

	"goqueue/internal/cluster"
	"goqueue/internal/storage"
)

// =============================================================================
// REPLICATION COORDINATOR
// =============================================================================

// replicationCoordinator manages replication for the broker.
type replicationCoordinator struct {
	// broker reference
	broker *Broker

	// replicaManager manages local replica state
	replicaManager *cluster.ReplicaManager

	// snapshotManager handles snapshots
	snapshotManager *cluster.SnapshotManager

	// partitionElector handles partition leader election
	partitionElector *cluster.PartitionLeaderElector

	// replicationServer serves HTTP endpoints
	replicationServer *cluster.ReplicationServer

	// config for replication
	config cluster.ReplicationConfig

	// logger for operations
	logger *slog.Logger

	// ctx and cancel for lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

// newReplicationCoordinator creates the replication coordinator.
func newReplicationCoordinator(broker *Broker, logger *slog.Logger) (*replicationCoordinator, error) {
	if broker.clusterCoordinator == nil {
		return nil, fmt.Errorf("cluster coordinator required for replication")
	}

	cc := broker.clusterCoordinator.coordinator

	// Load replication config
	config := cluster.DefaultReplicationConfig()

	// Get snapshot directory
	snapshotDir := filepath.Join(broker.config.DataDir, "snapshots")

	// Create snapshot manager
	snapshotManager := cluster.NewSnapshotManager(snapshotDir, logger)

	// Get partition elector from coordinator (M11 - centralized election).
	// The coordinator owns the partition elector so that failover is triggered
	// automatically when nodes die (via membership events).
	partitionElector := cc.PartitionElector()

	// Create replica manager
	replicaManager := cluster.NewReplicaManager(
		cc.Node().ID(),
		config,
		cluster.NewClusterClient(cc.Node(), cc.Membership(), logger),
		broker.config.DataDir,
		logger,
	)

	// Create replication server
	replicationServer := cluster.NewReplicationServer(
		replicaManager,
		snapshotManager,
		partitionElector,
		cc.MetadataStore(),
		config,
		logger,
	)

	ctx, cancel := context.WithCancel(context.Background())

	rc := &replicationCoordinator{
		broker:            broker,
		replicaManager:    replicaManager,
		snapshotManager:   snapshotManager,
		partitionElector:  partitionElector,
		replicationServer: replicationServer,
		config:            config,
		logger:            logger.With("component", "replication-coordinator"),
		ctx:               ctx,
		cancel:            cancel,
	}

	// Wire up storage callbacks for replication server.
	// These allow the cluster package to access broker storage without circular dependencies.
	replicationServer.SetLogReader(rc.readMessagesFromStorage)
	replicationServer.SetLogDirFn(rc.getLogDirectory)

	// Register for replica events
	replicaManager.AddListener(rc.handleReplicaEvent)

	// Register for metadata changes (M11/M12 - role transitions on failover).
	// When assignments change (e.g., after leader election), we need to
	// transition replicas between leader and follower roles.
	cc.MetadataStore().AddListener(rc.handleMetadataChange)

	return rc, nil
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Start begins replication operations.
func (rc *replicationCoordinator) Start(ctx context.Context) error {
	rc.logger.Info("starting replication coordinator",
		"fetch_interval_ms", rc.config.FetchIntervalMs,
		"isr_lag_time_ms", rc.config.ISRLagTimeMaxMs,
		"isr_lag_max_messages", rc.config.ISRLagMaxMessages,
		"min_insync_replicas", rc.config.MinInSyncReplicas,
		"unclean_election", rc.config.UncleanLeaderElection)

	// Initialize replicas for partitions assigned to this node
	if err := rc.initializeReplicas(); err != nil {
		return fmt.Errorf("initialize replicas: %w", err)
	}

	// Start ISR check background task
	go rc.runISRChecker()

	return nil
}

// Stop gracefully shuts down replication.
func (rc *replicationCoordinator) Stop(ctx context.Context) error {
	rc.logger.Info("stopping replication coordinator")

	// Cancel background tasks
	rc.cancel()

	// Stop replica manager (stops all fetchers)
	if rc.replicaManager != nil {
		if err := rc.replicaManager.Stop(); err != nil {
			rc.logger.Warn("error stopping replica manager", "error", err)
		}
	}

	return nil
}

// RegisterRoutes registers HTTP endpoints for replication.
func (rc *replicationCoordinator) RegisterRoutes(mux *http.ServeMux) {
	rc.replicationServer.RegisterRoutes(mux)
}

// =============================================================================
// REPLICA INITIALIZATION
// =============================================================================

// initializeReplicas creates local replicas for all partitions assigned to this node.
func (rc *replicationCoordinator) initializeReplicas() error {
	cc := rc.broker.clusterCoordinator.coordinator
	nodeID := cc.Node().ID()
	metadataStore := cc.MetadataStore()

	// Get all assignments for this node
	assignments := metadataStore.GetAssignmentsForNode(nodeID)

	for _, assignment := range assignments {
		isLeader := assignment.Leader == nodeID

		rc.logger.Info("initializing replica",
			"topic", assignment.Topic,
			"partition", assignment.Partition,
			"role", map[bool]string{true: "leader", false: "follower"}[isLeader])

		// Get current log end offset from storage
		logEndOffset, err := rc.getLogEndOffset(assignment.Topic, assignment.Partition)
		if err != nil {
			rc.logger.Warn("failed to get log end offset, using 0",
				"topic", assignment.Topic,
				"partition", assignment.Partition,
				"error", err)
			logEndOffset = 0
		}

		// Set role
		if isLeader {
			if err := rc.replicaManager.BecomeLeader(
				assignment.Topic,
				assignment.Partition,
				assignment.Version,
				assignment.Replicas,
				logEndOffset,
			); err != nil {
				return fmt.Errorf("become leader for %s-%d: %w", assignment.Topic, assignment.Partition, err)
			}
		} else {
			// Get leader address
			leaderAddr := rc.getNodeAddress(assignment.Leader)

			if err := rc.replicaManager.BecomeFollower(
				assignment.Topic,
				assignment.Partition,
				assignment.Leader,
				leaderAddr,
				assignment.Version,
				logEndOffset,
			); err != nil {
				return fmt.Errorf("become follower for %s-%d: %w", assignment.Topic, assignment.Partition, err)
			}
		}
	}

	return nil
}

// getLogEndOffset gets the current end offset for a partition's log.
func (rc *replicationCoordinator) getLogEndOffset(topicName string, partitionID int) (int64, error) {
	rc.broker.mu.RLock()
	topic, exists := rc.broker.topics[topicName]
	rc.broker.mu.RUnlock()

	if !exists {
		return 0, nil // Topic doesn't exist yet
	}

	partition := topic.partitions[partitionID]
	if partition == nil {
		return 0, nil // Partition doesn't exist yet
	}

	return partition.NextOffset(), nil
}

// getNodeAddress gets the cluster address for a node.
func (rc *replicationCoordinator) getNodeAddress(nodeID cluster.NodeID) string {
	cc := rc.broker.clusterCoordinator.coordinator
	nodeInfo := cc.Membership().GetNode(nodeID)
	if nodeInfo == nil {
		return ""
	}
	return nodeInfo.ClusterAddress.String()
}

// =============================================================================
// ISR MANAGEMENT
// =============================================================================

// runISRChecker periodically checks and updates ISR.
func (rc *replicationCoordinator) runISRChecker() {
	checkIntervalMs := 5000 // 5 seconds default
	ticker := time.NewTicker(time.Duration(checkIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			rc.checkISRs()
		}
	}
}

// checkISRs checks all leader replicas for ISR changes.
func (rc *replicationCoordinator) checkISRs() {
	replicas := rc.replicaManager.GetAllReplicas()

	for _, replica := range replicas {
		if replica.Role != cluster.ReplicaRoleLeader {
			continue
		}

		// Get ISR from replica manager
		isr := rc.replicaManager.GetISR(replica.Topic, replica.Partition)

		// Check against metadata store
		cc := rc.broker.clusterCoordinator.coordinator
		assignment := cc.MetadataStore().GetAssignment(replica.Topic, replica.Partition)
		if assignment == nil {
			continue
		}

		// Compare ISR lists
		if !isrEqual(isr, assignment.ISR) {
			rc.logger.Debug("ISR changed",
				"topic", replica.Topic,
				"partition", replica.Partition,
				"local_isr", isr,
				"metadata_isr", assignment.ISR)

			// Update metadata store with new ISR
			if err := cc.MetadataStore().UpdateISR(replica.Topic, replica.Partition, isr); err != nil {
				rc.logger.Error("failed to update ISR in metadata",
					"topic", replica.Topic,
					"partition", replica.Partition,
					"error", err)
			}
		}
	}
}

// isrEqual checks if two ISR lists are equal.
func isrEqual(a, b []cluster.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	aMap := make(map[cluster.NodeID]bool)
	for _, id := range a {
		aMap[id] = true
	}
	for _, id := range b {
		if !aMap[id] {
			return false
		}
	}
	return true
}

// =============================================================================
// LEADER OPERATIONS
// =============================================================================

// IsLeaderFor returns true if this node is the leader for the partition.
func (rc *replicationCoordinator) IsLeaderFor(topic string, partition int) bool {
	return rc.replicaManager.IsLeader(topic, partition)
}

// WaitForReplication waits for followers to replicate a message.
// Used when producer ACK mode is AckAll.
func (rc *replicationCoordinator) WaitForReplication(ctx context.Context, topic string, partition int, offset int64) error {
	return rc.replicaManager.WaitForAcks(ctx, topic, partition, offset)
}

// GetHighWatermark returns the high watermark for a partition.
func (rc *replicationCoordinator) GetHighWatermark(topic string, partition int) int64 {
	return rc.replicaManager.GetHighWatermark(topic, partition)
}

// RecordWrite records a write operation to update LEO.
func (rc *replicationCoordinator) RecordWrite(topic string, partition int, newLEO int64) error {
	return rc.replicaManager.UpdateLogEndOffset(topic, partition, newLEO)
}

// =============================================================================
// EVENT HANDLING
// =============================================================================

// handleReplicaEvent handles replica state change events.
func (rc *replicationCoordinator) handleReplicaEvent(event cluster.ReplicaEvent) {
	rc.logger.Info("replica event",
		"type", event.Type,
		"topic", event.Topic,
		"partition", event.Partition,
		"details", event.Details)

	switch event.Type {
	case cluster.ReplicaEventBecameLeader:
		// Notify any listeners that we're now leader
		rc.logger.Info("became leader for partition",
			"topic", event.Topic,
			"partition", event.Partition)

	case cluster.ReplicaEventBecameFollower:
		// We're now a follower
		rc.logger.Info("became follower for partition",
			"topic", event.Topic,
			"partition", event.Partition)

	case cluster.ReplicaEventISRChanged:
		// ISR membership changed
		rc.logger.Info("ISR changed",
			"topic", event.Topic,
			"partition", event.Partition)

	case cluster.ReplicaEventHighWatermarkAdvanced:
		// HW moved forward - pending acks may be completed
		rc.logger.Debug("high watermark advanced",
			"topic", event.Topic,
			"partition", event.Partition)
	}
}

// =============================================================================
// METADATA CHANGE HANDLING (M11/M12 - AUTOMATIC FAILOVER)
// =============================================================================
//
// WHEN METADATA CHANGES:
//   - Leader election completed → assignments changed
//   - Topic created/deleted → new assignments
//   - ISR updated → inform replicas
//
// WHAT WE DO:
//   - Compare new assignments with current replica states
//   - If we're now leader → transition to leader mode
//   - If we're now follower → transition to follower mode
//   - If partition removed → stop replica
//
// =============================================================================

// handleMetadataChange is called when cluster metadata changes.
// This is triggered after leader elections, topic changes, etc.
func (rc *replicationCoordinator) handleMetadataChange(meta *cluster.ClusterMeta) {
	rc.logger.Debug("metadata changed, checking for role transitions",
		"version", meta.Version)

	cc := rc.broker.clusterCoordinator.coordinator
	nodeID := cc.Node().ID()

	// Check all assignments for this node
	for _, assignment := range meta.Assignments {
		// Check if this node is in the replica list
		isReplica := false
		for _, replica := range assignment.Replicas {
			if replica == nodeID {
				isReplica = true
				break
			}
		}

		if !isReplica {
			continue // Not our partition
		}

		// Get current replica state
		currentState := rc.replicaManager.GetReplicaState(assignment.Topic, assignment.Partition)
		shouldBeLeader := assignment.Leader == nodeID

		// Transition if needed
		if shouldBeLeader && (currentState == nil || currentState.Role != cluster.ReplicaRoleLeader) {
			// We should be leader but we're not
			rc.logger.Info("transitioning to leader after failover",
				"topic", assignment.Topic,
				"partition", assignment.Partition,
				"epoch", assignment.Version)

			logEndOffset, err := rc.getLogEndOffset(assignment.Topic, assignment.Partition)
			if err != nil {
				rc.logger.Warn("failed to get log end offset for transition",
					"topic", assignment.Topic,
					"partition", assignment.Partition,
					"error", err)
				logEndOffset = 0
			}

			if err := rc.replicaManager.BecomeLeader(
				assignment.Topic,
				assignment.Partition,
				assignment.Version,
				assignment.Replicas,
				logEndOffset,
			); err != nil {
				rc.logger.Error("failed to become leader",
					"topic", assignment.Topic,
					"partition", assignment.Partition,
					"error", err)
			}

		} else if !shouldBeLeader && (currentState == nil || currentState.Role != cluster.ReplicaRoleFollower) {
			// We should be follower but we're not
			leaderAddr := rc.getNodeAddress(assignment.Leader)
			if leaderAddr == "" {
				rc.logger.Warn("cannot transition to follower - leader address unknown",
					"topic", assignment.Topic,
					"partition", assignment.Partition,
					"leader", assignment.Leader)
				continue
			}

			rc.logger.Info("transitioning to follower after failover",
				"topic", assignment.Topic,
				"partition", assignment.Partition,
				"leader", assignment.Leader,
				"epoch", assignment.Version)

			logEndOffset, err := rc.getLogEndOffset(assignment.Topic, assignment.Partition)
			if err != nil {
				rc.logger.Warn("failed to get log end offset for transition",
					"topic", assignment.Topic,
					"partition", assignment.Partition,
					"error", err)
				logEndOffset = 0
			}

			if err := rc.replicaManager.BecomeFollower(
				assignment.Topic,
				assignment.Partition,
				assignment.Leader,
				leaderAddr,
				assignment.Version,
				logEndOffset,
			); err != nil {
				rc.logger.Error("failed to become follower",
					"topic", assignment.Topic,
					"partition", assignment.Partition,
					"error", err)
			}
		}
	}
}

// =============================================================================
// BROKER INTEGRATION HELPERS
// =============================================================================

// ApplyFetchedMessages applies messages fetched by followers to local storage.
// This is called by the follower fetcher when it receives messages.
func (rc *replicationCoordinator) ApplyFetchedMessages(topic string, partition int, messages []cluster.ReplicatedMessage, leaderHW int64, leaderLEO int64) error {
	rc.broker.mu.RLock()
	topicObj, exists := rc.broker.topics[topic]
	rc.broker.mu.RUnlock()

	if !exists {
		return fmt.Errorf("topic %s not found", topic)
	}

	// Defensive bounds check: callers pass partition IDs over the network.
	// Without this, a bad partition index would panic and crash the broker.
	if partition < 0 || partition >= len(topicObj.partitions) {
		return fmt.Errorf("partition %d not found in topic %s", partition, topic)
	}

	partitionObj := topicObj.partitions[partition]
	if partitionObj == nil {
		return fmt.Errorf("partition %d not found in topic %s", partition, topic)
	}

	// Append messages to local log
	for _, msg := range messages {
		storageMsg := rc.toStorageMessage(&msg)
		_, err := partitionObj.AppendAtOffset(storageMsg, msg.Offset)
		if err != nil {
			return fmt.Errorf("append at offset %d: %w", msg.Offset, err)
		}
	}

	// Update replica manager state
	return rc.replicaManager.ApplyFetchedMessages(topic, partition, messages, leaderHW, leaderLEO)
}

// toStorageMessage converts a replicated message to storage format.
func (rc *replicationCoordinator) toStorageMessage(msg *cluster.ReplicatedMessage) *storage.Message {
	// ReplicatedMessage.Timestamp is milliseconds, storage.Message.Timestamp is nanoseconds
	return &storage.Message{
		Key:       msg.Key,
		Value:     msg.Value,
		Timestamp: msg.Timestamp * 1e6, // Convert ms to ns
	}
}

// =============================================================================
// STATISTICS
// =============================================================================

// Stats returns replication statistics.
func (rc *replicationCoordinator) Stats() ReplicationStats {
	stats := ReplicationStats{
		Replicas: make(map[string]ReplicaStats),
	}

	replicas := rc.replicaManager.GetAllReplicas()
	for _, r := range replicas {
		key := fmt.Sprintf("%s-%d", r.Topic, r.Partition)
		stats.Replicas[key] = ReplicaStats{
			Topic:         r.Topic,
			Partition:     r.Partition,
			Role:          string(r.Role),
			LogEndOffset:  r.LogEndOffset,
			HighWatermark: r.HighWatermark,
			Epoch:         r.LeaderEpoch,
		}

		if r.Role == cluster.ReplicaRoleLeader {
			stats.LeaderCount++
		} else {
			stats.FollowerCount++
		}
	}

	if rc.partitionElector != nil {
		electStats := rc.partitionElector.GetStats()
		stats.PendingElections = electStats.PendingElections
		stats.LeaderlessPartitions = electStats.LeaderlessCount
	}

	return stats
}

// ReplicationStats contains replication statistics.
type ReplicationStats struct {
	Replicas             map[string]ReplicaStats `json:"replicas"`
	LeaderCount          int                     `json:"leader_count"`
	FollowerCount        int                     `json:"follower_count"`
	PendingElections     int                     `json:"pending_elections"`
	LeaderlessPartitions int                     `json:"leaderless_partitions"`
}

// ReplicaStats contains statistics for a single replica.
type ReplicaStats struct {
	Topic         string `json:"topic"`
	Partition     int    `json:"partition"`
	Role          string `json:"role"`
	LogEndOffset  int64  `json:"log_end_offset"`
	HighWatermark int64  `json:"high_watermark"`
	Epoch         int64  `json:"epoch"`
}

// =============================================================================
// STORAGE CALLBACKS
// =============================================================================
//
// These methods are passed to ReplicationServer as callbacks.
// They allow the cluster package to read from broker storage without
// creating a circular dependency.

// readMessagesFromStorage reads messages from a partition's log.
//
// HOW IT WORKS:
//  1. Look up the topic and partition in broker's storage
//  2. Use storage.Log.ReadFrom() to get messages
//  3. Convert storage.Message to cluster.ReplicatedMessage
//  4. Return messages and the current log end offset (LEO)
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset (inclusive)
//   - maxBytes: Maximum bytes to read (approximate)
//
// RETURNS:
//   - messages: Slice of ReplicatedMessage for replication
//   - logEndOffset: Current end of log (for ISR tracking)
//   - error: If partition not found or read fails
func (rc *replicationCoordinator) readMessagesFromStorage(
	topicName string,
	partitionID int,
	fromOffset int64,
	maxBytes int64,
) ([]cluster.ReplicatedMessage, int64, error) {
	// Look up the partition.
	rc.broker.mu.RLock()
	topic, exists := rc.broker.topics[topicName]
	rc.broker.mu.RUnlock()

	if !exists {
		return nil, 0, fmt.Errorf("topic %s not found", topicName)
	}

	if partitionID >= len(topic.partitions) || topic.partitions[partitionID] == nil {
		return nil, 0, fmt.Errorf("partition %d not found in topic %s", partitionID, topicName)
	}

	partition := topic.partitions[partitionID]

	// Get current log end offset.
	logEndOffset := partition.NextOffset()

	// Validate offset range.
	earliestOffset := partition.EarliestOffset()
	if fromOffset < earliestOffset {
		return nil, logEndOffset, fmt.Errorf("offset out of range: %d < earliest %d", fromOffset, earliestOffset)
	}

	if fromOffset >= logEndOffset {
		// No new messages to read.
		return []cluster.ReplicatedMessage{}, logEndOffset, nil
	}

	// Calculate max messages based on approximate message size.
	// Assume average message is ~1KB for estimation.
	maxMessages := int(maxBytes / 1024)
	if maxMessages < 1 {
		maxMessages = 1
	}
	if maxMessages > 10000 {
		maxMessages = 10000 // Cap to prevent excessive memory use
	}

	// Read messages from storage.
	storageMessages, err := partition.ConsumeByOffset(fromOffset, maxMessages)
	if err != nil {
		return nil, logEndOffset, fmt.Errorf("read from storage: %w", err)
	}

	// Convert to ReplicatedMessage format.
	replicatedMessages := make([]cluster.ReplicatedMessage, len(storageMessages))
	for i, msg := range storageMessages {
		replicatedMessages[i] = cluster.ReplicatedMessage{
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp, // Already in nanoseconds
			Key:       msg.Key,
			Value:     msg.Value,
			Priority:  uint8(msg.Priority),
			Headers:   msg.Headers,
			// IsControlRecord is set based on message flags.
			IsControlRecord: msg.Flags&storage.FlagControlRecord != 0,
		}
	}

	rc.logger.Debug("read messages for replication",
		"topic", topicName,
		"partition", partitionID,
		"from_offset", fromOffset,
		"messages", len(replicatedMessages),
		"leo", logEndOffset)

	return replicatedMessages, logEndOffset, nil
}

// getLogDirectory returns the directory path for a partition's log files.
//
// USED FOR:
//   - Snapshot creation: Need to know where segment files are stored
//   - Recovery: Finding log directory for a partition
//
// PATH STRUCTURE:
//
//	{DataDir}/logs/{topic}/{partition}/
//
// Example:
//
//	/data/goqueue/logs/orders/0/
func (rc *replicationCoordinator) getLogDirectory(topic string, partition int) string {
	return filepath.Join(rc.broker.config.DataDir, "logs", topic, fmt.Sprintf("%d", partition))
}
