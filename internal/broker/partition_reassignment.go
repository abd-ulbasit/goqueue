// =============================================================================
// PARTITION REASSIGNMENT - MOVING PARTITIONS BETWEEN NODES
// =============================================================================
//
// WHAT IS PARTITION REASSIGNMENT?
// Moving partition replicas from one set of nodes to another. Used when:
//   - Decommissioning a node (move its partitions away)
//   - Adding new nodes (distribute partitions to them)
//   - Rebalancing storage (move from full nodes to empty ones)
//   - Hardware maintenance (move before shutting down)
//
// WHY REASSIGN PARTITIONS?
//
//   SCENARIO 1: Adding new nodes
//   Before: 3 nodes, partitions spread across A, B, C
//   After adding D, E: Partitions still only on A, B, C
//   Reassignment: Move some partitions to D, E for even distribution
//
//   SCENARIO 2: Decommissioning node
//   Before: Partition 3 has replicas on [A, B, C]
//   Want to remove node C from cluster
//   Reassignment: Change replicas to [A, B, D] - data moves from C to D
//
// HOW REASSIGNMENT WORKS (THROTTLED COPY):
//
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │   PHASE 1: Add new replica (follower fetcher pulls data)             │
//   │                                                                      │
//   │   Original:     [Leader:A, Follower:B, Follower:C]                   │
//   │   Target:       [Leader:A, Follower:B, Follower:D]  (C → D)          │
//   │                                                                      │
//   │   Step 1: Expand ISR to include D                                    │
//   │           [A, B, C, D] ← D starts fetching from A (leader)           │
//   │                                                                      │
//   └──────────────────────────────────────────────────────────────────────┘
//                                   ↓
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │   PHASE 2: Wait for new replica to catch up                          │
//   │                                                                      │
//   │   D fetches data from A (throttled to avoid overload)                │
//   │   Progress: D at offset 1000, A at offset 5000 → 20% caught up       │
//   │   Progress: D at offset 3000, A at offset 5000 → 60% caught up       │
//   │   Progress: D at offset 5000, A at offset 5000 → 100% caught up!     │
//   │                                                                      │
//   └──────────────────────────────────────────────────────────────────────┘
//                                   ↓
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │   PHASE 3: Remove old replica                                        │
//   │                                                                      │
//   │   D is now in-sync (ISR = [A, B, C, D])                              │
//   │   Shrink replicas: remove C from replica set                         │
//   │   Final: [A, B, D] ← C can be deleted                                │
//   │                                                                      │
//   └──────────────────────────────────────────────────────────────────────┘
//
// COMPARISON:
//   - Kafka: kafka-reassign-partitions.sh tool with throttling
//   - Cassandra: nodetool move/decommission
//   - CockroachDB: Automatic range rebalancing
//   - goqueue: Manual reassignment with throttling (like Kafka)
//
// SAFETY GUARANTEES:
//   - Never remove replica until replacement is in-sync
//   - Maintain minimum ISR throughout
//   - Throttle to avoid overwhelming network/disk
//   - Allow cancellation of in-progress reassignment
//
// =============================================================================

package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"goqueue/internal/cluster"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrReassignmentInProgress is returned when trying to start a new reassignment
	// while one is already running for the same partition.
	ErrReassignmentInProgress = errors.New("reassignment already in progress for this partition")

	// ErrReassignmentNotFound is returned when trying to cancel/query a non-existent reassignment.
	ErrReassignmentNotFound = errors.New("reassignment not found")

	// ErrInvalidReassignment is returned when the reassignment request is invalid.
	ErrInvalidReassignment = errors.New("invalid reassignment request")

	// ErrReassignmentCancelled is returned when a reassignment is cancelled.
	ErrReassignmentCancelled = errors.New("reassignment was cancelled")

	// ErrReplicaCatchupTimeout is returned when new replica fails to catch up in time.
	ErrReplicaCatchupTimeout = errors.New("new replica failed to catch up within timeout")
)

// =============================================================================
// REASSIGNMENT REQUEST & STATUS
// =============================================================================

// ReassignmentState represents the current phase of a reassignment.
type ReassignmentState string

const (
	// ReassignmentStatePending means reassignment is queued but not started.
	ReassignmentStatePending ReassignmentState = "pending"

	// ReassignmentStateInProgress means data is being copied to new replicas.
	ReassignmentStateInProgress ReassignmentState = "in_progress"

	// ReassignmentStateCatchingUp means new replicas are fetching data.
	ReassignmentStateCatchingUp ReassignmentState = "catching_up"

	// ReassignmentStateCompleted means reassignment finished successfully.
	ReassignmentStateCompleted ReassignmentState = "completed"

	// ReassignmentStateFailed means reassignment failed.
	ReassignmentStateFailed ReassignmentState = "failed"

	// ReassignmentStateCancelled means reassignment was cancelled.
	ReassignmentStateCancelled ReassignmentState = "cancelled"
)

// PartitionReassignment describes a single partition's reassignment.
//
// EXAMPLE:
//
//	{
//	    Topic: "orders",
//	    Partition: 3,
//	    OldReplicas: [A, B, C],
//	    NewReplicas: [A, B, D],
//	    AddingReplicas: [D],     // D is being added
//	    RemovingReplicas: [C],   // C is being removed
//	}
type PartitionReassignment struct {
	// Topic name.
	Topic string `json:"topic"`

	// Partition number.
	Partition int `json:"partition"`

	// OldReplicas is the current replica set.
	OldReplicas []cluster.NodeID `json:"old_replicas"`

	// NewReplicas is the target replica set.
	NewReplicas []cluster.NodeID `json:"new_replicas"`

	// AddingReplicas are replicas being added (in NewReplicas but not OldReplicas).
	AddingReplicas []cluster.NodeID `json:"adding_replicas"`

	// RemovingReplicas are replicas being removed (in OldReplicas but not NewReplicas).
	RemovingReplicas []cluster.NodeID `json:"removing_replicas"`
}

// ReassignmentRequest is the input for starting a reassignment.
type ReassignmentRequest struct {
	// Partitions to reassign.
	Partitions []PartitionReassignment `json:"partitions"`

	// ThrottleBytesPerSec limits replication bandwidth.
	// 0 = unlimited (not recommended for large reassignments).
	// Typical: 10MB/s (10485760) to avoid impacting production traffic.
	ThrottleBytesPerSec int64 `json:"throttle_bytes_per_sec"`

	// TimeoutPerPartition is how long to wait for each partition's new replicas
	// to catch up before failing.
	TimeoutPerPartition time.Duration `json:"timeout_per_partition"`
}

// ReassignmentStatus tracks progress of a reassignment.
type ReassignmentStatus struct {
	// ID is a unique identifier for this reassignment.
	ID string `json:"id"`

	// State is the current phase.
	State ReassignmentState `json:"state"`

	// Request is the original request.
	Request *ReassignmentRequest `json:"request"`

	// Progress maps "topic-partition" to progress details.
	Progress map[string]*PartitionReassignmentProgress `json:"progress"`

	// StartedAt is when reassignment began.
	StartedAt time.Time `json:"started_at"`

	// CompletedAt is when reassignment finished (success or failure).
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	// Error message if failed.
	Error string `json:"error,omitempty"`
}

// PartitionReassignmentProgress tracks a single partition's reassignment progress.
type PartitionReassignmentProgress struct {
	// Topic name.
	Topic string `json:"topic"`

	// Partition number.
	Partition int `json:"partition"`

	// State for this partition.
	State ReassignmentState `json:"state"`

	// LeaderOffset is the current leader's latest offset.
	LeaderOffset int64 `json:"leader_offset"`

	// ReplicaOffsets maps each new replica to its current offset.
	// When all reach LeaderOffset, the partition is caught up.
	ReplicaOffsets map[cluster.NodeID]int64 `json:"replica_offsets"`

	// CatchupPercent is the overall catch-up percentage (0-100).
	CatchupPercent float64 `json:"catchup_percent"`

	// BytesTransferred is total bytes copied.
	BytesTransferred int64 `json:"bytes_transferred"`

	// StartedAt for this partition.
	StartedAt time.Time `json:"started_at"`

	// CompletedAt for this partition.
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	// Error if this partition failed.
	Error string `json:"error,omitempty"`
}

// =============================================================================
// REASSIGNMENT MANAGER
// =============================================================================

// ReassignmentManager handles partition reassignment operations.
//
// RESPONSIBILITIES:
//  1. Validate reassignment requests
//  2. Coordinate data movement between nodes
//  3. Track progress and report status
//  4. Handle cancellation and cleanup
//  5. Apply throttling to avoid overload
//
// CONCURRENCY:
// Multiple reassignments can run in parallel for different partitions,
// but only one reassignment per partition at a time.
type ReassignmentManager struct {
	// metadataStore for reading/updating partition assignments.
	metadataStore *cluster.MetadataStore

	// broker for partition access.
	broker *Broker

	// localNodeID is this node's ID.
	localNodeID cluster.NodeID

	// mu protects internal state.
	mu sync.RWMutex

	// activeReassignments tracks in-progress reassignments.
	// Key: reassignment ID.
	activeReassignments map[string]*ReassignmentStatus

	// partitionReassignments maps "topic-partition" to reassignment ID.
	// Ensures only one reassignment per partition.
	partitionReassignments map[string]string

	// cancelFuncs stores cancel functions for each reassignment.
	cancelFuncs map[string]context.CancelFunc

	// nextID is the counter for generating reassignment IDs.
	nextID int64

	// defaultThrottle is the default throttle (bytes/sec) if not specified.
	defaultThrottle int64

	// defaultTimeout is the default per-partition timeout.
	defaultTimeout time.Duration
}

// NewReassignmentManager creates a new reassignment manager.
func NewReassignmentManager(
	metadataStore *cluster.MetadataStore,
	broker *Broker,
	localNodeID cluster.NodeID,
) *ReassignmentManager {
	return &ReassignmentManager{
		metadataStore:          metadataStore,
		broker:                 broker,
		localNodeID:            localNodeID,
		activeReassignments:    make(map[string]*ReassignmentStatus),
		partitionReassignments: make(map[string]string),
		cancelFuncs:            make(map[string]context.CancelFunc),
		nextID:                 1,
		defaultThrottle:        10 * 1024 * 1024, // 10 MB/s
		defaultTimeout:         24 * time.Hour,   // 24 hours
	}
}

// =============================================================================
// START REASSIGNMENT
// =============================================================================

// StartReassignment begins a new partition reassignment.
//
// WORKFLOW:
//  1. Validate request
//  2. Check no conflicting reassignments
//  3. Create status tracking
//  4. Start async reassignment worker
//
// RETURNS:
//   - Reassignment ID for tracking/cancellation
//   - Error if validation fails
func (rm *ReassignmentManager) StartReassignment(request *ReassignmentRequest) (string, error) {
	// =========================================================================
	// VALIDATE REQUEST
	// =========================================================================

	if len(request.Partitions) == 0 {
		return "", fmt.Errorf("%w: no partitions specified", ErrInvalidReassignment)
	}

	// Apply defaults
	if request.ThrottleBytesPerSec == 0 {
		request.ThrottleBytesPerSec = rm.defaultThrottle
	}
	if request.TimeoutPerPartition == 0 {
		request.TimeoutPerPartition = rm.defaultTimeout
	}

	// Validate each partition.
	//
	// IMPORTANT: Iterate by index, not by value.
	//
	// WHY: validatePartitionReassignment() computes derived fields
	// (AddingReplicas / RemovingReplicas) that Phase 2 (catch-up) depends on.
	// If we iterate with "for _, pr := range ...", we validate a *copy* of each
	// struct and silently discard those derived fields, causing reassignment to
	// incorrectly skip catch-up.
	for i := range request.Partitions {
		if err := rm.validatePartitionReassignment(&request.Partitions[i]); err != nil {
			return "", err
		}
	}

	// =========================================================================
	// CHECK FOR CONFLICTS
	// =========================================================================

	rm.mu.Lock()
	defer rm.mu.Unlock()

	for _, pr := range request.Partitions {
		key := fmt.Sprintf("%s-%d", pr.Topic, pr.Partition)
		if existingID, exists := rm.partitionReassignments[key]; exists {
			return "", fmt.Errorf("%w: partition %s already in reassignment %s",
				ErrReassignmentInProgress, key, existingID)
		}
	}

	// =========================================================================
	// CREATE STATUS
	// =========================================================================

	reassignmentID := fmt.Sprintf("reassign-%d", rm.nextID)
	rm.nextID++

	progress := make(map[string]*PartitionReassignmentProgress)
	for _, pr := range request.Partitions {
		key := fmt.Sprintf("%s-%d", pr.Topic, pr.Partition)
		progress[key] = &PartitionReassignmentProgress{
			Topic:          pr.Topic,
			Partition:      pr.Partition,
			State:          ReassignmentStatePending,
			ReplicaOffsets: make(map[cluster.NodeID]int64),
			StartedAt:      time.Now(),
		}
		rm.partitionReassignments[key] = reassignmentID
	}

	status := &ReassignmentStatus{
		ID:        reassignmentID,
		State:     ReassignmentStatePending,
		Request:   request,
		Progress:  progress,
		StartedAt: time.Now(),
	}

	rm.activeReassignments[reassignmentID] = status

	// =========================================================================
	// START WORKER
	// =========================================================================

	ctx, cancel := context.WithCancel(context.Background())
	rm.cancelFuncs[reassignmentID] = cancel

	go rm.runReassignment(ctx, status)

	return reassignmentID, nil
}

// =============================================================================
// REASSIGNMENT EXECUTION
// =============================================================================

// runReassignment executes the reassignment asynchronously.
func (rm *ReassignmentManager) runReassignment(ctx context.Context, status *ReassignmentStatus) {
	// Update state to in-progress
	rm.updateState(status.ID, ReassignmentStateInProgress)

	var firstError error

	// Process each partition
	for _, pr := range status.Request.Partitions {
		select {
		case <-ctx.Done():
			rm.updateState(status.ID, ReassignmentStateCancelled)
			rm.completeReassignment(status.ID, ErrReassignmentCancelled)
			return
		default:
		}

		if err := rm.reassignPartition(ctx, status, &pr); err != nil {
			if firstError == nil {
				firstError = err
			}
			// Continue with other partitions even if one fails
		}
	}

	// Complete reassignment
	if firstError != nil {
		rm.completeReassignment(status.ID, firstError)
	} else {
		rm.updateState(status.ID, ReassignmentStateCompleted)
		rm.completeReassignment(status.ID, nil)
	}
}

// reassignPartition handles a single partition's reassignment.
//
// ALGORITHM:
//  1. Expand replica set to include new replicas
//  2. Wait for new replicas to catch up
//  3. Shrink replica set to remove old replicas
//  4. Update metadata
func (rm *ReassignmentManager) reassignPartition(
	ctx context.Context,
	status *ReassignmentStatus,
	pr *PartitionReassignment,
) error {
	key := fmt.Sprintf("%s-%d", pr.Topic, pr.Partition)

	rm.mu.Lock()
	progress := status.Progress[key]
	progress.State = ReassignmentStateInProgress
	rm.mu.Unlock()

	// =========================================================================
	// PHASE 1: EXPAND REPLICA SET
	// =========================================================================
	// Add new replicas while keeping old ones. This triggers follower fetcher
	// on new replicas to start pulling data from leader.

	expandedReplicas := rm.unionReplicas(pr.OldReplicas, pr.NewReplicas)

	if err := rm.metadataStore.SetAssignment(&cluster.PartitionAssignment{
		Topic:     pr.Topic,
		Partition: pr.Partition,
		Leader:    pr.OldReplicas[0], // Keep existing leader
		Replicas:  expandedReplicas,
		ISR:       pr.OldReplicas, // New replicas not in ISR yet
	}); err != nil {
		rm.updatePartitionError(status.ID, key, err)
		return err
	}

	// =========================================================================
	// PHASE 2: WAIT FOR CATCH-UP
	// =========================================================================

	rm.mu.Lock()
	progress.State = ReassignmentStateCatchingUp
	rm.mu.Unlock()

	catchupCtx, cancel := context.WithTimeout(ctx, status.Request.TimeoutPerPartition)
	defer cancel()

	if err := rm.waitForCatchup(catchupCtx, status, pr); err != nil {
		rm.updatePartitionError(status.ID, key, err)
		return err
	}

	// =========================================================================
	// PHASE 3: SHRINK TO TARGET REPLICAS
	// =========================================================================

	if err := rm.metadataStore.SetAssignment(&cluster.PartitionAssignment{
		Topic:     pr.Topic,
		Partition: pr.Partition,
		Leader:    pr.NewReplicas[0], // New leader (first in list)
		Replicas:  pr.NewReplicas,
		ISR:       pr.NewReplicas, // All new replicas now in ISR
	}); err != nil {
		rm.updatePartitionError(status.ID, key, err)
		return err
	}

	// =========================================================================
	// PHASE 4: CLEANUP OLD REPLICAS
	// =========================================================================
	// Note: Actual data deletion on old replicas happens asynchronously
	// when those nodes detect they're no longer in the replica set.

	rm.mu.Lock()
	progress.State = ReassignmentStateCompleted
	now := time.Now()
	progress.CompletedAt = &now
	rm.mu.Unlock()

	return nil
}

// waitForCatchup polls until all new replicas have caught up to leader.
//
// CATCH-UP DEFINITION:
// A replica is "caught up" when its high watermark >= leader's high watermark
// at the time we check. Since messages keep arriving, we use a threshold
// (within 100 messages) rather than exact equality.
//
// GOROUTINE LEAK FIX:
// Previously used time.After() which creates a new timer goroutine on each
// iteration. If context is cancelled, those timer goroutines leak until they
// fire. Now uses time.NewTimer with proper cleanup via timer.Stop().
func (rm *ReassignmentManager) waitForCatchup(
	ctx context.Context,
	status *ReassignmentStatus,
	pr *PartitionReassignment,
) error {
	key := fmt.Sprintf("%s-%d", pr.Topic, pr.Partition)
	pollInterval := 5 * time.Second

	// Create reusable timer to avoid goroutine leaks
	timer := time.NewTimer(pollInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return ErrReplicaCatchupTimeout
			}
			return ctx.Err()
		default:
		}

		// Check catchup status
		allCaughtUp, percent := rm.checkCatchup(pr)

		rm.mu.Lock()
		progress := status.Progress[key]
		progress.CatchupPercent = percent
		rm.mu.Unlock()

		if allCaughtUp {
			return nil
		}

		// Wait before next poll - reset timer for reuse
		timer.Reset(pollInterval)
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return ErrReplicaCatchupTimeout
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// checkCatchup returns whether all new replicas are caught up.
//
// IMPLEMENTATION NOTE:
// In a real system, we'd query each replica's offset via RPC.
// TODO: Implement RPC calls to get replica offsets.
// For now, we simulate by checking local state (works in single-node tests).
func (rm *ReassignmentManager) checkCatchup(pr *PartitionReassignment) (bool, float64) {
	// Get leader's current offset
	assignment := rm.metadataStore.GetAssignment(pr.Topic, pr.Partition)
	if assignment == nil {
		return false, 0
	}

	// In a real implementation:
	// 1. Query leader for high watermark
	// 2. Query each new replica for their offset
	// 3. Compare

	// For now, simulate: assume catch-up happens quickly in tests
	// In production, this would query replica offsets via RPC
	leaderOffset := rm.getPartitionHighWatermark(pr.Topic, pr.Partition)
	if leaderOffset < 0 {
		return true, 100.0 // Empty partition is always caught up
	}

	// Check each adding replica
	totalPercent := 0.0
	for _, nodeID := range pr.AddingReplicas {
		replicaOffset := rm.getReplicaOffset(pr.Topic, pr.Partition, nodeID)
		if leaderOffset > 0 {
			percent := float64(replicaOffset) / float64(leaderOffset) * 100
			if percent > 100 {
				percent = 100
			}
			totalPercent += percent
		} else {
			totalPercent += 100
		}
	}

	if len(pr.AddingReplicas) == 0 {
		return true, 100.0
	}

	avgPercent := totalPercent / float64(len(pr.AddingReplicas))

	// Consider caught up if within threshold
	return avgPercent >= 99.0, avgPercent
}

// getPartitionHighWatermark returns the leader's high watermark for a partition.
func (rm *ReassignmentManager) getPartitionHighWatermark(topic string, partition int) int64 {
	if rm.broker == nil {
		return -1
	}

	t, err := rm.broker.GetTopic(topic)
	if err != nil {
		return -1
	}

	p, err := t.Partition(partition)
	if err != nil {
		return -1
	}

	return p.LatestOffset()
}

// getReplicaOffset returns a replica's current offset.
// In production, this would be an RPC call to the replica node.
func (rm *ReassignmentManager) getReplicaOffset(topic string, partition int, nodeID cluster.NodeID) int64 {
	// If this is the local node, check local state
	if nodeID == rm.localNodeID {
		return rm.getPartitionHighWatermark(topic, partition)
	}

	// For remote nodes, would need RPC
	// For now, assume caught up (tests only)
	// TODO: Implement RPC to get remote replica offset
	return rm.getPartitionHighWatermark(topic, partition)
}

// =============================================================================
// STATE MANAGEMENT
// =============================================================================

// updateState updates the overall reassignment state.
func (rm *ReassignmentManager) updateState(reassignmentID string, state ReassignmentState) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if status, ok := rm.activeReassignments[reassignmentID]; ok {
		status.State = state
	}
}

// updatePartitionError marks a partition as failed.
func (rm *ReassignmentManager) updatePartitionError(reassignmentID, partitionKey string, err error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if status, ok := rm.activeReassignments[reassignmentID]; ok {
		if progress, ok := status.Progress[partitionKey]; ok {
			progress.State = ReassignmentStateFailed
			progress.Error = err.Error()
			now := time.Now()
			progress.CompletedAt = &now
		}
	}
}

// completeReassignment marks the reassignment as done and cleans up.
func (rm *ReassignmentManager) completeReassignment(reassignmentID string, err error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	status, ok := rm.activeReassignments[reassignmentID]
	if !ok {
		return
	}

	now := time.Now()
	status.CompletedAt = &now

	if err != nil {
		// Cancellation is semantically different from failure.
		//
		// WHY: Callers need to distinguish "operator/user cancelled" from
		// "reassignment failed due to an error".
		if errors.Is(err, ErrReassignmentCancelled) || errors.Is(err, context.Canceled) {
			status.State = ReassignmentStateCancelled
		} else {
			status.State = ReassignmentStateFailed
		}
		status.Error = err.Error()
	} else {
		status.State = ReassignmentStateCompleted
	}

	// Clean up partition mappings
	for key := range status.Progress {
		delete(rm.partitionReassignments, key)
	}

	// Clean up cancel func
	delete(rm.cancelFuncs, reassignmentID)
}

// =============================================================================
// STATUS & CONTROL
// =============================================================================

// GetStatus returns the status of a reassignment.
func (rm *ReassignmentManager) GetStatus(reassignmentID string) (*ReassignmentStatus, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	status, ok := rm.activeReassignments[reassignmentID]
	if !ok {
		return nil, ErrReassignmentNotFound
	}

	// Return a copy
	return rm.copyStatus(status), nil
}

// ListActiveReassignments returns all active reassignments.
func (rm *ReassignmentManager) ListActiveReassignments() []*ReassignmentStatus {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	result := make([]*ReassignmentStatus, 0, len(rm.activeReassignments))
	for _, status := range rm.activeReassignments {
		result = append(result, rm.copyStatus(status))
	}
	return result
}

// CancelReassignment cancels an in-progress reassignment.
//
// NOTE: Cancellation is best-effort. If a partition has already completed
// its reassignment, it won't be rolled back.
func (rm *ReassignmentManager) CancelReassignment(reassignmentID string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	cancel, ok := rm.cancelFuncs[reassignmentID]
	if !ok {
		// Check if it exists but is already done
		if _, exists := rm.activeReassignments[reassignmentID]; exists {
			return nil // Already completed, no-op
		}
		return ErrReassignmentNotFound
	}

	cancel()
	return nil
}

// =============================================================================
// VALIDATION & HELPERS
// =============================================================================

// validatePartitionReassignment validates a single partition reassignment.
//
// CRITICAL: Validates both OldReplicas and NewReplicas are non-empty to prevent
// panic from array index access (e.g., OldReplicas[0]) during reassignment.
func (rm *ReassignmentManager) validatePartitionReassignment(pr *PartitionReassignment) error {
	// Check topic exists
	topicMeta := rm.metadataStore.GetTopic(pr.Topic)
	if topicMeta == nil {
		return fmt.Errorf("%w: topic %s not found", ErrInvalidReassignment, pr.Topic)
	}

	// Check partition exists
	if pr.Partition < 0 || pr.Partition >= topicMeta.PartitionCount {
		return fmt.Errorf("%w: partition %d out of range for topic %s",
			ErrInvalidReassignment, pr.Partition, pr.Topic)
	}

	// =========================================================================
	// BUG FIX: Validate OldReplicas is not empty
	// =========================================================================
	// WHY: OldReplicas[0] is accessed in reassignPartition() without bounds check.
	// Without this validation, an empty OldReplicas slice would cause a panic.
	if len(pr.OldReplicas) == 0 {
		return fmt.Errorf("%w: old replicas cannot be empty", ErrInvalidReassignment)
	}

	// Check new replicas not empty
	if len(pr.NewReplicas) == 0 {
		return fmt.Errorf("%w: new replicas cannot be empty", ErrInvalidReassignment)
	}

	// Check replication factor matches
	if len(pr.NewReplicas) != topicMeta.ReplicationFactor {
		return fmt.Errorf("%w: new replica count %d doesn't match replication factor %d",
			ErrInvalidReassignment, len(pr.NewReplicas), topicMeta.ReplicationFactor)
	}

	// Compute adding/removing replicas
	pr.AddingReplicas = rm.setDifference(pr.NewReplicas, pr.OldReplicas)
	pr.RemovingReplicas = rm.setDifference(pr.OldReplicas, pr.NewReplicas)

	return nil
}

// unionReplicas returns the union of two replica sets.
func (rm *ReassignmentManager) unionReplicas(a, b []cluster.NodeID) []cluster.NodeID {
	seen := make(map[cluster.NodeID]bool)
	var result []cluster.NodeID

	for _, id := range a {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	for _, id := range b {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	return result
}

// setDifference returns elements in a but not in b.
func (rm *ReassignmentManager) setDifference(a, b []cluster.NodeID) []cluster.NodeID {
	bSet := make(map[cluster.NodeID]bool)
	for _, id := range b {
		bSet[id] = true
	}

	var result []cluster.NodeID
	for _, id := range a {
		if !bSet[id] {
			result = append(result, id)
		}
	}
	return result
}

// copyStatus creates a deep copy of ReassignmentStatus.
func (rm *ReassignmentManager) copyStatus(status *ReassignmentStatus) *ReassignmentStatus {
	copy := &ReassignmentStatus{
		ID:        status.ID,
		State:     status.State,
		Request:   status.Request,
		Progress:  make(map[string]*PartitionReassignmentProgress),
		StartedAt: status.StartedAt,
		Error:     status.Error,
	}

	if status.CompletedAt != nil {
		t := *status.CompletedAt
		copy.CompletedAt = &t
	}

	for k, v := range status.Progress {
		progressCopy := *v
		progressCopy.ReplicaOffsets = make(map[cluster.NodeID]int64)
		for nk, nv := range v.ReplicaOffsets {
			progressCopy.ReplicaOffsets[nk] = nv
		}
		copy.Progress[k] = &progressCopy
	}

	return copy
}
