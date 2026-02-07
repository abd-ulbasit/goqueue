// =============================================================================
// REPLICA MANAGER - LOCAL REPLICA STATE MANAGEMENT
// =============================================================================
//
// WHAT: Manages all local replicas (partitions) on this node.
//
// RESPONSIBILITIES:
//   - Track leader/follower state for each partition
//   - Coordinate replication for leader partitions
//   - Fetch from leader for follower partitions
//   - Update high watermark as ISR acks come in
//   - Handle leader failover transitions
//
// ARCHITECTURE:
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                         REPLICA MANAGER                                 │
//   │                                                                         │
//   │  ┌───────────────────────────────────────────────────────────────────┐  │
//   │  │                    ReplicaState Map                               │  │
//   │  │  "orders-0" → {role: leader,  LEO: 1000, HW: 950}                 │  │
//   │  │  "orders-1" → {role: follower, LEO: 800,  HW: 780}                │  │
//   │  │  "users-0"  → {role: leader,  LEO: 500,  HW: 500}                 │  │
//   │  └───────────────────────────────────────────────────────────────────┘  │
//   │                                                                         │
//   │  ┌───────────────────────────────────────────────────────────────────┐  │
//   │  │                For each LEADER partition                          │  │
//   │  │  - ISR Manager (track follower progress)                          │  │
//   │  │  - Pending Acks (wait for ISR on AckAll)                          │  │
//   │  │  - Snapshot Manager (create snapshots for slow followers)         │  │
//   │  └───────────────────────────────────────────────────────────────────┘  │
//   │                                                                         │
//   │  ┌───────────────────────────────────────────────────────────────────┐  │
//   │  │                For each FOLLOWER partition                        │  │
//   │  │  - Follower Fetcher (background loop fetching from leader)        │  │
//   │  │  - Snapshot Loader (load snapshot if far behind)                  │  │
//   │  └───────────────────────────────────────────────────────────────────┘  │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// THREAD SAFETY:
//   - ReplicaManager is thread-safe
//   - State transitions are serialized with mutex
//   - Background fetchers run concurrently
//
// =============================================================================

package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// =============================================================================
// REPLICA MANAGER
// =============================================================================

// ReplicaManager manages all local replicas on this node.
type ReplicaManager struct {
	// nodeID is this node's identifier.
	nodeID NodeID

	// config is replication configuration.
	config ReplicationConfig

	// replicas maps "topic-partition" to replica state.
	replicas map[string]*LocalReplica

	// mu protects replicas map.
	mu sync.RWMutex

	// fetchers holds active follower fetchers.
	fetchers map[string]*FollowerFetcher

	// fetchersMu protects fetchers map.
	fetchersMu sync.Mutex

	// snapshotManager handles snapshot operations.
	snapshotManager *SnapshotManager

	// dataDir is the root data directory.
	dataDir string

	// clusterClient for communicating with other nodes.
	clusterClient *ClusterClient

	// logger for operations.
	logger *slog.Logger

	// ctx is the manager's context.
	ctx context.Context

	// cancel cancels background operations.
	cancel context.CancelFunc

	// wg waits for background goroutines.
	wg sync.WaitGroup

	// eventCh receives replica events.
	eventCh chan ReplicaEvent

	// listeners receive replica events.
	listeners []func(ReplicaEvent)
}

// LocalReplica represents a local partition replica with its state and components.
type LocalReplica struct {
	// State is the current replica state.
	State *ReplicaState

	// For leaders: ISR management.
	isrManager *ISRManager

	// For leaders: pending acks waiting for ISR.
	pendingAcks map[int64]*PendingAck

	// pendingAcksMu protects pendingAcks.
	pendingAcksMu sync.Mutex

	// mu protects state updates.
	mu sync.RWMutex
}

// ReplicaEvent represents a replica state change.
type ReplicaEvent struct {
	// Type is the event type.
	Type ReplicaEventType

	// Topic is the affected topic.
	Topic string

	// Partition is the affected partition.
	Partition int

	// OldRole is the previous role (for transitions).
	OldRole ReplicaRole

	// NewRole is the new role (for transitions).
	NewRole ReplicaRole

	// Details provides additional context.
	Details string
}

// ReplicaEventType is the type of replica event.
type ReplicaEventType string

const (
	// ReplicaEventBecameLeader indicates this node became leader.
	ReplicaEventBecameLeader ReplicaEventType = "became_leader"

	// ReplicaEventBecameFollower indicates this node became follower.
	ReplicaEventBecameFollower ReplicaEventType = "became_follower"

	// ReplicaEventISRChanged indicates ISR membership changed.
	ReplicaEventISRChanged ReplicaEventType = "isr_changed"

	// ReplicaEventHighWatermarkAdvanced indicates HW moved forward.
	ReplicaEventHighWatermarkAdvanced ReplicaEventType = "hw_advanced"

	// ReplicaEventReplicaAdded indicates a new replica was added.
	ReplicaEventReplicaAdded ReplicaEventType = "replica_added"

	// ReplicaEventReplicaRemoved indicates a replica was removed.
	ReplicaEventReplicaRemoved ReplicaEventType = "replica_removed"
)

// =============================================================================
// CONSTRUCTOR
// =============================================================================

// NewReplicaManager creates a new replica manager.
func NewReplicaManager(nodeID NodeID, config ReplicationConfig, client *ClusterClient, dataDir string, logger *slog.Logger) *ReplicaManager {
	ctx, cancel := context.WithCancel(context.Background())

	rm := &ReplicaManager{
		nodeID:        nodeID,
		config:        config,
		dataDir:       dataDir,
		replicas:      make(map[string]*LocalReplica),
		fetchers:      make(map[string]*FollowerFetcher),
		clusterClient: client,
		logger:        logger.With("component", "replica-manager"),
		ctx:           ctx,
		cancel:        cancel,
		eventCh:       make(chan ReplicaEvent, 100),
		listeners:     make([]func(ReplicaEvent), 0),
	}

	// Create snapshot manager if enabled.
	if config.SnapshotEnabled {
		rm.snapshotManager = NewSnapshotManager(dataDir, logger)
	}

	// Start event dispatcher.
	rm.wg.Add(1)
	go rm.dispatchEvents()

	return rm
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Stop shuts down the replica manager.
func (rm *ReplicaManager) Stop() error {
	rm.logger.Info("stopping replica manager")

	// Signal shutdown.
	rm.cancel()

	// Stop all fetchers.
	rm.fetchersMu.Lock()
	for key, fetcher := range rm.fetchers {
		rm.logger.Debug("stopping fetcher", "partition", key)
		fetcher.Stop()
	}
	rm.fetchersMu.Unlock()

	// Close event channel.
	close(rm.eventCh)

	// Wait for goroutines.
	rm.wg.Wait()

	rm.logger.Info("replica manager stopped")
	return nil
}

// dispatchEvents sends events to listeners.
func (rm *ReplicaManager) dispatchEvents() {
	defer rm.wg.Done()

	for event := range rm.eventCh {
		for _, listener := range rm.listeners {
			// Call listener in goroutine to avoid blocking.
			go listener(event)
		}
	}
}

// AddListener registers a callback for replica events.
func (rm *ReplicaManager) AddListener(listener func(ReplicaEvent)) {
	rm.listeners = append(rm.listeners, listener)
}

// =============================================================================
// REPLICA STATE MANAGEMENT
// =============================================================================

// partitionKey creates a map key from topic and partition.
func partitionKey(topic string, partition int) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

// GetReplicaState returns the state for a partition.
func (rm *ReplicaManager) GetReplicaState(topic string, partition int) *ReplicaState {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	key := partitionKey(topic, partition)
	replica, ok := rm.replicas[key]
	if !ok {
		return nil
	}

	replica.mu.RLock()
	defer replica.mu.RUnlock()

	// Return a copy.
	stateCopy := *replica.State
	return &stateCopy
}

// GetAllReplicas returns all local replica states.
func (rm *ReplicaManager) GetAllReplicas() []*ReplicaState {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	states := make([]*ReplicaState, 0, len(rm.replicas))
	for _, replica := range rm.replicas {
		replica.mu.RLock()
		stateCopy := *replica.State
		replica.mu.RUnlock()
		states = append(states, &stateCopy)
	}
	return states
}

// IsLeader returns true if this node is leader for the partition.
func (rm *ReplicaManager) IsLeader(topic string, partition int) bool {
	state := rm.GetReplicaState(topic, partition)
	if state == nil {
		return false
	}
	return state.Role == ReplicaRoleLeader
}

// GetLocalReplica returns the local replica for a partition.
// Used internally for operations that need direct access to the LocalReplica.
// Returns nil if the partition is not stored locally.
func (rm *ReplicaManager) GetLocalReplica(topic string, partition int) *LocalReplica {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	key := partitionKey(topic, partition)
	return rm.replicas[key]
}

// GetHighWatermark returns the HW for a partition.
func (rm *ReplicaManager) GetHighWatermark(topic string, partition int) int64 {
	state := rm.GetReplicaState(topic, partition)
	if state == nil {
		return -1
	}
	return state.HighWatermark
}

// GetLogEndOffset returns the LEO for a partition.
func (rm *ReplicaManager) GetLogEndOffset(topic string, partition int) int64 {
	state := rm.GetReplicaState(topic, partition)
	if state == nil {
		return -1
	}
	return state.LogEndOffset
}

// =============================================================================
// LEADER INITIALIZATION
// =============================================================================

// BecomeLeader transitions a partition to leader state.
// Called when this node is elected leader.
func (rm *ReplicaManager) BecomeLeader(topic string, partition int, epoch int64, replicas []NodeID, initialLEO int64) error {
	key := partitionKey(topic, partition)
	rm.logger.Info("becoming leader",
		"topic", topic,
		"partition", partition,
		"epoch", epoch,
		"replicas", replicas)

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Stop any existing follower fetcher.
	rm.stopFetcherLocked(key)

	// Get or create replica.
	replica, exists := rm.replicas[key]
	oldRole := ReplicaRoleOffline
	if exists {
		replica.mu.Lock()
		oldRole = replica.State.Role
	} else {
		replica = &LocalReplica{
			State: &ReplicaState{
				Topic:     topic,
				Partition: partition,
			},
			pendingAcks: make(map[int64]*PendingAck),
		}
		rm.replicas[key] = replica
		replica.mu.Lock()
	}

	// Update state.
	replica.State.Role = ReplicaRoleLeader
	replica.State.LeaderID = rm.nodeID
	replica.State.LeaderEpoch = epoch
	replica.State.LogEndOffset = initialLEO
	replica.State.HighWatermark = initialLEO // Initially HW = LEO for new leader
	replica.State.UpdatedAt = time.Now()

	// Create ISR manager for leader.
	isrConfig := ISRConfig{
		LagTimeMaxMs:      rm.config.ISRLagTimeMaxMs,
		LagMaxMessages:    rm.config.ISRLagMaxMessages,
		MinInSyncReplicas: rm.config.MinInSyncReplicas,
	}
	replica.isrManager = NewISRManager(topic, partition, rm.nodeID, replicas, isrConfig, rm.logger)

	replica.mu.Unlock()

	// Emit event.
	rm.emitEvent(ReplicaEvent{
		Type:      ReplicaEventBecameLeader,
		Topic:     topic,
		Partition: partition,
		OldRole:   oldRole,
		NewRole:   ReplicaRoleLeader,
		Details:   fmt.Sprintf("epoch=%d", epoch),
	})

	return nil
}

// =============================================================================
// FOLLOWER INITIALIZATION
// =============================================================================

// BecomeFollower transitions a partition to follower state.
// Called when this node should follow another leader.
func (rm *ReplicaManager) BecomeFollower(topic string, partition int, leaderID NodeID, leaderAddr string, epoch, initialLEO int64) error {
	key := partitionKey(topic, partition)
	rm.logger.Info("becoming follower",
		"topic", topic,
		"partition", partition,
		"leader", leaderID,
		"epoch", epoch)

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Get or create replica.
	replica, exists := rm.replicas[key]
	oldRole := ReplicaRoleOffline
	if exists {
		replica.mu.Lock()
		oldRole = replica.State.Role

		// Clear leader-specific state.
		replica.isrManager = nil
		replica.pendingAcks = nil
	} else {
		replica = &LocalReplica{
			State: &ReplicaState{
				Topic:     topic,
				Partition: partition,
			},
		}
		rm.replicas[key] = replica
		replica.mu.Lock()
	}

	// Update state.
	replica.State.Role = ReplicaRoleFollower
	replica.State.LeaderID = leaderID
	replica.State.LeaderEpoch = epoch
	replica.State.LogEndOffset = initialLEO
	// HW will be updated when we fetch from leader.
	replica.State.UpdatedAt = time.Now()

	replica.mu.Unlock()

	// Start follower fetcher.
	rm.startFetcherLocked(topic, partition, leaderID, leaderAddr, epoch)

	// Emit event.
	rm.emitEvent(ReplicaEvent{
		Type:      ReplicaEventBecameFollower,
		Topic:     topic,
		Partition: partition,
		OldRole:   oldRole,
		NewRole:   ReplicaRoleFollower,
		Details:   fmt.Sprintf("leader=%s epoch=%d", leaderID, epoch),
	})

	return nil
}

// =============================================================================
// FETCHER MANAGEMENT
// =============================================================================

// startFetcherLocked starts a follower fetcher. Must be called with rm.mu held.
func (rm *ReplicaManager) startFetcherLocked(topic string, partition int, leaderID NodeID, leaderAddr string, epoch int64) {
	key := partitionKey(topic, partition)

	// Stop existing fetcher if any.
	rm.stopFetcherLocked(key)

	// Create and start new fetcher.
	fetcherConfig := FollowerFetcherConfig{
		Topic:           topic,
		Partition:       partition,
		LeaderID:        leaderID,
		LeaderAddr:      leaderAddr,
		LeaderEpoch:     epoch,
		ReplicaID:       rm.nodeID,
		FetchIntervalMs: rm.config.FetchIntervalMs,
		FetchMaxBytes:   rm.config.FetchMaxBytes,
		SnapshotDir:     rm.GetSnapshotDir(), // Enable snapshot-based catch-up
	}

	fetcher := NewFollowerFetcher(fetcherConfig, rm, rm.clusterClient, rm.logger)

	// IMPORTANT:
	//   Seed the fetch offset from the local replica state while we already have
	//   access to it (and while we're holding rm.mu in the caller).
	//
	// WHY:
	//   Having FollowerFetcher.Start() call back into ReplicaManager to fetch this
	//   value can deadlock if Start() is invoked while rm.mu is held.
	if replica, ok := rm.replicas[key]; ok {
		replica.mu.RLock()
		initialOffset := replica.State.LogEndOffset
		replica.mu.RUnlock()
		fetcher.SetFetchOffset(initialOffset)
	}

	rm.fetchersMu.Lock()
	rm.fetchers[key] = fetcher
	rm.fetchersMu.Unlock()

	fetcher.Start()
}

// stopFetcherLocked stops a follower fetcher. Must be called with rm.mu held.
func (rm *ReplicaManager) stopFetcherLocked(key string) {
	rm.fetchersMu.Lock()
	defer rm.fetchersMu.Unlock()

	if fetcher, ok := rm.fetchers[key]; ok {
		fetcher.Stop()
		delete(rm.fetchers, key)
	}
}

// =============================================================================
// LEADER OPERATIONS
// =============================================================================

// UpdateLogEndOffset updates LEO after a write (leader only).
func (rm *ReplicaManager) UpdateLogEndOffset(topic string, partition int, newLEO int64) error {
	key := partitionKey(topic, partition)

	rm.mu.RLock()
	replica, ok := rm.replicas[key]
	rm.mu.RUnlock()

	if !ok {
		return fmt.Errorf("replica %s not found", key)
	}

	replica.mu.Lock()
	defer replica.mu.Unlock()

	if replica.State.Role != ReplicaRoleLeader {
		return fmt.Errorf("not leader for %s", key)
	}

	replica.State.LogEndOffset = newLEO
	replica.State.UpdatedAt = time.Now()

	return nil
}

// RecordFollowerFetch records that a follower fetched data.
// This updates the leader's view of follower progress.
func (rm *ReplicaManager) RecordFollowerFetch(topic string, partition int, followerID NodeID, fetchOffset int64) error {
	key := partitionKey(topic, partition)

	rm.mu.RLock()
	replica, ok := rm.replicas[key]
	rm.mu.RUnlock()

	if !ok {
		return fmt.Errorf("replica %s not found", key)
	}

	replica.mu.Lock()
	defer replica.mu.Unlock()

	if replica.State.Role != ReplicaRoleLeader {
		return fmt.Errorf("not leader for %s", key)
	}

	if replica.isrManager == nil {
		return fmt.Errorf("ISR manager not initialized for %s", key)
	}

	// Update follower progress in ISR manager.
	replica.isrManager.RecordFetch(followerID, fetchOffset, replica.State.LogEndOffset)

	// Check if HW can advance.
	oldHW := replica.State.HighWatermark
	newHW := replica.isrManager.CalculateHighWatermark(replica.State.LogEndOffset)

	if newHW > oldHW {
		replica.State.HighWatermark = newHW
		replica.State.UpdatedAt = time.Now()

		// Complete pending acks that are now satisfied.
		rm.completePendingAcks(replica, newHW)

		// Emit event.
		rm.emitEvent(ReplicaEvent{
			Type:      ReplicaEventHighWatermarkAdvanced,
			Topic:     topic,
			Partition: partition,
			Details:   fmt.Sprintf("HW: %d → %d", oldHW, newHW),
		})
	}

	return nil
}

// WaitForAcks waits for ISR acknowledgment up to the given offset.
// Used for AckAll producer writes.
func (rm *ReplicaManager) WaitForAcks(ctx context.Context, topic string, partition int, offset int64) error {
	key := partitionKey(topic, partition)

	rm.mu.RLock()
	replica, ok := rm.replicas[key]
	rm.mu.RUnlock()

	if !ok {
		return fmt.Errorf("replica %s not found", key)
	}

	replica.mu.Lock()

	if replica.State.Role != ReplicaRoleLeader {
		replica.mu.Unlock()
		return fmt.Errorf("not leader for %s", key)
	}

	// Check if already committed.
	if offset <= replica.State.HighWatermark {
		replica.mu.Unlock()
		return nil
	}

	// Check ISR size.
	if replica.isrManager != nil && !replica.isrManager.HasMinISR() {
		replica.mu.Unlock()
		return fmt.Errorf("not enough in-sync replicas (have %d, need %d)",
			replica.isrManager.ISRSize(), rm.config.MinInSyncReplicas)
	}

	// Create pending ack.
	waitCh := make(chan struct{})
	pendingAck := &PendingAck{
		Offset:       offset,
		RequiredAcks: replica.isrManager.ISRSize() - 1, // -1 because leader already has it
		ReceivedAcks: make(map[NodeID]bool),
		WaitCh:       waitCh,
		CreatedAt:    time.Now(),
		TimeoutAt:    time.Now().Add(time.Duration(rm.config.AckTimeoutMs) * time.Millisecond),
	}

	// If only leader is in ISR, no waiting needed.
	if pendingAck.RequiredAcks <= 0 {
		replica.mu.Unlock()
		return nil
	}

	replica.pendingAcksMu.Lock()
	replica.pendingAcks[offset] = pendingAck
	replica.pendingAcksMu.Unlock()

	replica.mu.Unlock()

	// Wait for acks or timeout.
	// GOROUTINE LEAK FIX: Use NewTimer instead of time.After
	timer := time.NewTimer(time.Duration(rm.config.AckTimeoutMs) * time.Millisecond)
	select {
	case <-waitCh:
		timer.Stop()
		return nil
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		// Clean up pending ack.
		replica.pendingAcksMu.Lock()
		delete(replica.pendingAcks, offset)
		replica.pendingAcksMu.Unlock()
		return fmt.Errorf("ack timeout waiting for offset %d", offset)
	}
}

// completePendingAcks completes pending acks up to the new HW.
func (rm *ReplicaManager) completePendingAcks(replica *LocalReplica, newHW int64) {
	replica.pendingAcksMu.Lock()
	defer replica.pendingAcksMu.Unlock()

	for offset, pending := range replica.pendingAcks {
		if offset <= newHW {
			close(pending.WaitCh)
			delete(replica.pendingAcks, offset)
		}
	}
}

// =============================================================================
// FOLLOWER OPERATIONS
// =============================================================================

// ApplyFetchedMessages applies messages fetched from leader.
// Called by FollowerFetcher after successful fetch.
func (rm *ReplicaManager) ApplyFetchedMessages(topic string, partition int, messages []ReplicatedMessage, leaderHW, leaderLEO int64) error {
	key := partitionKey(topic, partition)

	rm.mu.RLock()
	replica, ok := rm.replicas[key]
	rm.mu.RUnlock()

	if !ok {
		return fmt.Errorf("replica %s not found", key)
	}

	replica.mu.Lock()
	defer replica.mu.Unlock()

	if replica.State.Role != ReplicaRoleFollower {
		return fmt.Errorf("not follower for %s", key)
	}

	// Update local LEO based on messages.
	if len(messages) > 0 {
		lastMsg := messages[len(messages)-1]
		replica.State.LogEndOffset = lastMsg.Offset + 1
	}

	// Update HW (bounded by our LEO).
	if leaderHW > replica.State.HighWatermark {
		newHW := leaderHW
		if newHW > replica.State.LogEndOffset {
			newHW = replica.State.LogEndOffset
		}
		replica.State.HighWatermark = newHW
	}

	replica.State.UpdatedAt = time.Now()

	return nil
}

// =============================================================================
// EVENT EMISSION
// =============================================================================

func (rm *ReplicaManager) emitEvent(event ReplicaEvent) {
	select {
	case rm.eventCh <- event:
	default:
		// Channel full, log and drop.
		rm.logger.Warn("replica event channel full, dropping event",
			"type", event.Type,
			"topic", event.Topic,
			"partition", event.Partition)
	}
}

// =============================================================================
// ISR ACCESS (FOR LEADER)
// =============================================================================

// GetISR returns the current ISR for a partition (leader only).
func (rm *ReplicaManager) GetISR(topic string, partition int) []NodeID {
	key := partitionKey(topic, partition)

	rm.mu.RLock()
	replica, ok := rm.replicas[key]
	rm.mu.RUnlock()

	if !ok {
		return nil
	}

	replica.mu.RLock()
	defer replica.mu.RUnlock()

	if replica.isrManager == nil {
		return nil
	}

	return replica.isrManager.GetISR()
}

// GetFollowerProgress returns progress info for a follower (leader only).
func (rm *ReplicaManager) GetFollowerProgress(topic string, partition int, followerID NodeID) *FollowerProgress {
	key := partitionKey(topic, partition)

	rm.mu.RLock()
	replica, ok := rm.replicas[key]
	rm.mu.RUnlock()

	if !ok {
		return nil
	}

	replica.mu.RLock()
	defer replica.mu.RUnlock()

	if replica.isrManager == nil {
		return nil
	}

	return replica.isrManager.GetFollowerProgress(followerID)
}

// =============================================================================
// SNAPSHOT AND LOG DIRECTORY HELPERS
// =============================================================================

// GetSnapshotManager returns the snapshot manager.
// Used by FollowerFetcher for snapshot-based catch-up.
func (rm *ReplicaManager) GetSnapshotManager() *SnapshotManager {
	return rm.snapshotManager
}

// GetLogDir returns the log directory for a partition.
//
// PATH STRUCTURE:
//
//	{dataDir}/logs/{topic}/{partition}/
//
// Example:
//
//	/data/goqueue/logs/orders/0/
func (rm *ReplicaManager) GetLogDir(topic string, partition int) string {
	return fmt.Sprintf("%s/logs/%s/%d", rm.dataDir, topic, partition)
}

// GetSnapshotDir returns the snapshot directory.
// Used by FollowerFetcher for downloading snapshots.
func (rm *ReplicaManager) GetSnapshotDir() string {
	return fmt.Sprintf("%s/snapshots", rm.dataDir)
}
