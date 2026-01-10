// =============================================================================
// PARTITION SCALER - ONLINE PARTITION EXPANSION
// =============================================================================
//
// WHAT IS PARTITION SCALING?
// Adding more partitions to an existing topic without downtime. This is a
// critical operation for growing systems that need more parallelism.
//
// WHY SCALE PARTITIONS?
//   1. INCREASED PARALLELISM
//      - More partitions = more consumers can work in parallel
//      - Original: 4 partitions, max 4 parallel consumers
//      - After scaling: 8 partitions, max 8 parallel consumers
//
//   2. HANDLING GROWTH
//      - As message volume grows, spread load across more partitions
//      - Each partition handles less traffic
//
//   3. AVOIDING HOTSPOTS
//      - If some keys are "hot", more partitions distribute better
//
// KAFKA-STYLE PARTITION SCALING:
// Kafka allows ONLY adding partitions, never reducing. This is because:
//   - Messages already routed by hash(key) % oldPartitionCount
//   - Reducing partitions would leave orphaned messages
//   - Adding is safe: new messages may route to new partitions
//
// WHAT HAPPENS TO EXISTING DATA?
// NOTHING! Existing messages stay where they are. Only NEW messages may
// go to new partitions (depending on their key hash).
//
// ┌─────────────────────────────────────────────────────────────────────────┐
// │                     BEFORE: Topic "orders" (3 partitions)               │
// │                                                                         │
// │   Partition 0: [msg1, msg4, msg7...]  ← key "user-123" hashes here      │
// │   Partition 1: [msg2, msg5, msg8...]  ← key "user-456" hashes here      │
// │   Partition 2: [msg3, msg6, msg9...]  ← key "user-789" hashes here      │
// │                                                                         │
// └─────────────────────────────────────────────────────────────────────────┘
//
//                           ADD 3 MORE PARTITIONS
//                                   ↓
//
// ┌─────────────────────────────────────────────────────────────────────────┐
// │                     AFTER: Topic "orders" (6 partitions)                │
// │                                                                         │
// │   Partition 0: [msg1, msg4, msg7...]  ← OLD data stays                  │
// │   Partition 1: [msg2, msg5, msg8...]  ← OLD data stays                  │
// │   Partition 2: [msg3, msg6, msg9...]  ← OLD data stays                  │
// │   Partition 3: []                     ← NEW, empty                      │
// │   Partition 4: []                     ← NEW, empty                      │
// │   Partition 5: []                     ← NEW, empty                      │
// │                                                                         │
// │   AFTER SCALING:                                                        │
// │   - "user-123" now hashes to partition 3 (hash % 6 != hash % 3)         │
// │   - New messages for "user-123" go to partition 3                       │
// │   - Old messages for "user-123" remain in partition 0                   │
// │                                                                         │
// └─────────────────────────────────────────────────────────────────────────┘
//
// IMPACT ON CONSUMERS:
//   - Consumer groups are notified of new partitions
//   - Cooperative rebalance assigns new partitions to consumers
//   - Consumers may see "out of order" messages for same key across partitions
//
// COMPARISON:
//   - Kafka: Supports adding partitions only (no reduction)
//   - RabbitMQ: Queues are not partitioned (different model)
//   - Pulsar: Supports partition expansion with similar semantics
//   - goqueue: Follows Kafka model - add only, no data movement
//
// WORKFLOW:
//   1. Admin calls AddPartitions(topic, newCount)
//   2. Controller validates request
//   3. Controller assigns leaders/replicas for new partitions
//   4. Cluster creates new partition directories
//   5. Metadata updated across cluster
//   6. Consumer groups notified → trigger rebalance
//
// =============================================================================

package broker

import (
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
	// ErrCannotReducePartitions is returned when trying to reduce partition count.
	// Kafka-style semantics: you can only ADD partitions, never remove.
	ErrCannotReducePartitions = errors.New("cannot reduce partition count (only increasing is allowed)")

	// ErrNoChange is returned when the new partition count equals current.
	ErrNoChange = errors.New("partition count unchanged")

	// ErrInsufficientNodes is returned when there aren't enough nodes for replication.
	ErrInsufficientNodes = errors.New("insufficient nodes for replication factor")

	// ErrScalingInProgress is returned when another scaling operation is running.
	ErrScalingInProgress = errors.New("partition scaling already in progress")

	// ErrInternalTopicScaling is returned when trying to scale an internal topic.
	ErrInternalTopicScaling = errors.New("cannot scale internal topics")
)

// =============================================================================
// PARTITION SCALING REQUEST & RESULT
// =============================================================================

// PartitionScaleRequest contains the request to add partitions.
//
// FIELDS:
//   - TopicName: Which topic to scale
//   - NewPartitionCount: Target number of partitions (must be > current)
//   - ReplicaAssignments: Optional explicit replica assignments for new partitions
//     If nil, controller auto-assigns using round-robin
type PartitionScaleRequest struct {
	// TopicName is the topic to scale.
	TopicName string `json:"topic_name"`

	// NewPartitionCount is the target partition count.
	// Must be greater than current count.
	NewPartitionCount int `json:"new_partition_count"`

	// ReplicaAssignments optionally specifies replica placement for new partitions.
	// Key: partition number (starting from current count)
	// Value: list of node IDs (first is leader)
	// If nil, controller auto-assigns.
	//
	// EXAMPLE:
	//   Current: 3 partitions (0, 1, 2)
	//   New: 6 partitions
	//   ReplicaAssignments: {
	//     3: [node-a, node-b, node-c],  // partition 3
	//     4: [node-b, node-c, node-a],  // partition 4
	//     5: [node-c, node-a, node-b],  // partition 5
	//   }
	ReplicaAssignments map[int][]cluster.NodeID `json:"replica_assignments,omitempty"`
}

// PartitionScaleResult contains the result of a scaling operation.
type PartitionScaleResult struct {
	// Success indicates if the scaling completed.
	Success bool `json:"success"`

	// TopicName is the scaled topic.
	TopicName string `json:"topic_name"`

	// OldPartitionCount is the count before scaling.
	OldPartitionCount int `json:"old_partition_count"`

	// NewPartitionCount is the count after scaling.
	NewPartitionCount int `json:"new_partition_count"`

	// PartitionsAdded lists the new partition IDs.
	PartitionsAdded []int `json:"partitions_added"`

	// Assignments maps new partition IDs to their assignments.
	Assignments map[int]*cluster.PartitionAssignment `json:"assignments"`

	// Error message if scaling failed.
	Error string `json:"error,omitempty"`

	// Duration is how long the scaling took.
	Duration time.Duration `json:"duration"`

	// StartedAt is when scaling started.
	StartedAt time.Time `json:"started_at"`

	// CompletedAt is when scaling finished.
	CompletedAt time.Time `json:"completed_at"`
}

// =============================================================================
// PARTITION SCALER
// =============================================================================

// PartitionScaler handles online partition expansion.
//
// RESPONSIBILITIES:
//  1. Validate scaling requests
//  2. Assign replicas for new partitions
//  3. Coordinate with cluster to create partitions
//  4. Update metadata atomically
//  5. Notify consumer groups of changes
//
// CONCURRENCY:
// Only one scaling operation per topic at a time. The scaler tracks
// in-progress operations to prevent conflicts.
type PartitionScaler struct {
	// metadataStore holds cluster-wide metadata.
	metadataStore *cluster.MetadataStore

	// broker reference for creating actual partitions.
	broker *Broker

	// mu protects internal state.
	mu sync.RWMutex

	// scalingInProgress tracks topics currently being scaled.
	// Key: topic name, Value: timestamp when scaling started.
	scalingInProgress map[string]time.Time

	// listeners are notified when partitions are added.
	// Consumer groups subscribe to this to trigger rebalance.
	listeners []PartitionChangeListener

	// nodeIDs is the list of available nodes for replica assignment.
	nodeIDs []cluster.NodeID

	// localNodeID is this node's ID.
	localNodeID cluster.NodeID
}

// PartitionChangeListener is called when partition count changes.
// Consumer groups use this to trigger rebalance.
//
// PARAMETERS:
//   - topicName: Which topic changed
//   - oldCount: Previous partition count
//   - newCount: New partition count
//   - newPartitions: List of new partition IDs
type PartitionChangeListener func(topicName string, oldCount, newCount int, newPartitions []int)

// NewPartitionScaler creates a new partition scaler.
//
// PARAMETERS:
//   - metadataStore: Cluster metadata store
//   - broker: Broker instance for creating partitions
//   - localNodeID: This node's ID
//   - nodeIDs: All available nodes for replica assignment
func NewPartitionScaler(
	metadataStore *cluster.MetadataStore,
	broker *Broker,
	localNodeID cluster.NodeID,
	nodeIDs []cluster.NodeID,
) *PartitionScaler {
	return &PartitionScaler{
		metadataStore:     metadataStore,
		broker:            broker,
		localNodeID:       localNodeID,
		nodeIDs:           nodeIDs,
		scalingInProgress: make(map[string]time.Time),
		listeners:         make([]PartitionChangeListener, 0),
	}
}

// =============================================================================
// SCALING OPERATIONS
// =============================================================================

// AddPartitions expands a topic's partition count.
//
// WORKFLOW:
//  1. Validate request (new count > old count)
//  2. Check no scaling in progress for this topic
//  3. Assign replicas for new partitions
//  4. Create partition directories
//  5. Update metadata
//  6. Notify listeners (consumer groups)
//
// ERRORS:
//   - ErrCannotReducePartitions: If new < old
//   - ErrNoChange: If new == old
//   - ErrScalingInProgress: If another scaling operation is running
//   - ErrTopicNotFound: If topic doesn't exist
//
// EXAMPLE:
//
//	request := &PartitionScaleRequest{
//	    TopicName:         "orders",
//	    NewPartitionCount: 12,  // Current: 6, add 6 more
//	}
//	result, err := scaler.AddPartitions(request)
func (ps *PartitionScaler) AddPartitions(request *PartitionScaleRequest) (*PartitionScaleResult, error) {
	startTime := time.Now()

	result := &PartitionScaleResult{
		TopicName: request.TopicName,
		StartedAt: startTime,
	}

	// =========================================================================
	// STEP 1: VALIDATE REQUEST
	// =========================================================================

	// Check for internal topics
	if isInternalTopic(request.TopicName) {
		result.Error = ErrInternalTopicScaling.Error()
		return result, ErrInternalTopicScaling
	}

	// Get current topic metadata
	topicMeta := ps.metadataStore.GetTopic(request.TopicName)
	if topicMeta == nil {
		result.Error = fmt.Sprintf("topic %s not found", request.TopicName)
		return result, fmt.Errorf("topic %s not found", request.TopicName)
	}

	currentCount := topicMeta.PartitionCount
	result.OldPartitionCount = currentCount

	// Validate partition count
	if request.NewPartitionCount < currentCount {
		result.Error = ErrCannotReducePartitions.Error()
		return result, ErrCannotReducePartitions
	}

	if request.NewPartitionCount == currentCount {
		result.Error = ErrNoChange.Error()
		return result, ErrNoChange
	}

	// =========================================================================
	// STEP 2: CHECK FOR IN-PROGRESS OPERATIONS
	// =========================================================================

	ps.mu.Lock()
	if _, inProgress := ps.scalingInProgress[request.TopicName]; inProgress {
		ps.mu.Unlock()
		result.Error = ErrScalingInProgress.Error()
		return result, ErrScalingInProgress
	}
	ps.scalingInProgress[request.TopicName] = startTime
	ps.mu.Unlock()

	// Ensure we clean up the in-progress marker
	defer func() {
		ps.mu.Lock()
		delete(ps.scalingInProgress, request.TopicName)
		ps.mu.Unlock()
	}()

	// =========================================================================
	// STEP 3: ASSIGN REPLICAS FOR NEW PARTITIONS
	// =========================================================================

	replicationFactor := topicMeta.ReplicationFactor
	if len(ps.nodeIDs) < replicationFactor {
		result.Error = ErrInsufficientNodes.Error()
		return result, ErrInsufficientNodes
	}

	partitionsToAdd := request.NewPartitionCount - currentCount
	newPartitionIDs := make([]int, 0, partitionsToAdd)
	assignments := make(map[int]*cluster.PartitionAssignment)

	for i := 0; i < partitionsToAdd; i++ {
		partitionID := currentCount + i
		newPartitionIDs = append(newPartitionIDs, partitionID)

		// Use provided assignment or auto-assign
		var replicas []cluster.NodeID
		if request.ReplicaAssignments != nil {
			if provided, ok := request.ReplicaAssignments[partitionID]; ok {
				replicas = provided
			}
		}

		if replicas == nil {
			// Auto-assign using round-robin
			replicas = ps.autoAssignReplicas(partitionID, replicationFactor)
		}

		// Leader is first replica
		leader := replicas[0]

		assignments[partitionID] = &cluster.PartitionAssignment{
			Topic:     request.TopicName,
			Partition: partitionID,
			Leader:    leader,
			Replicas:  replicas,
			ISR:       replicas, // Initially all replicas are in-sync
			Version:   1,
		}
	}

	// =========================================================================
	// STEP 4: CREATE PARTITION DIRECTORIES (LOCAL BROKER)
	// =========================================================================

	// On this node, create partitions we're responsible for
	for partitionID, assignment := range assignments {
		isReplica := false
		for _, nodeID := range assignment.Replicas {
			if nodeID == ps.localNodeID {
				isReplica = true
				break
			}
		}

		if isReplica {
			// Create the partition using broker's Topic
			if err := ps.createLocalPartition(request.TopicName, partitionID); err != nil {
				result.Error = fmt.Sprintf("failed to create partition %d: %v", partitionID, err)
				return result, err
			}
		}
	}

	// =========================================================================
	// STEP 5: UPDATE METADATA STORE
	// =========================================================================

	// Update topic metadata
	if err := ps.metadataStore.UpdatePartitionCount(request.TopicName, request.NewPartitionCount); err != nil {
		result.Error = fmt.Sprintf("failed to update topic metadata: %v", err)
		return result, err
	}

	// Add partition assignments
	for _, assignment := range assignments {
		if err := ps.metadataStore.SetAssignment(assignment); err != nil {
			result.Error = fmt.Sprintf("failed to set partition assignment: %v", err)
			return result, err
		}
	}

	// =========================================================================
	// STEP 6: NOTIFY LISTENERS (CONSUMER GROUPS)
	// =========================================================================

	ps.notifyListeners(request.TopicName, currentCount, request.NewPartitionCount, newPartitionIDs)

	// =========================================================================
	// RETURN RESULT
	// =========================================================================

	result.Success = true
	result.NewPartitionCount = request.NewPartitionCount
	result.PartitionsAdded = newPartitionIDs
	result.Assignments = assignments
	result.CompletedAt = time.Now()
	result.Duration = result.CompletedAt.Sub(startTime)

	return result, nil
}

// =============================================================================
// REPLICA ASSIGNMENT
// =============================================================================

// autoAssignReplicas creates replica assignments using round-robin.
//
// ALGORITHM:
// For partition P with replication factor R:
//
//	Leader = nodes[(P % numNodes)]
//	Replica1 = nodes[((P + 1) % numNodes)]
//	Replica2 = nodes[((P + 2) % numNodes)]
//	... and so on
//
// This ensures:
//   - Even distribution of leaders across nodes
//   - Even distribution of replicas across nodes
//   - Partition N's replicas don't all land on same nodes
//
// EXAMPLE (3 nodes, replication=3):
//
//	Partition 0: [A, B, C]
//	Partition 1: [B, C, A]
//	Partition 2: [C, A, B]
//	Partition 3: [A, B, C]  // wraps around
func (ps *PartitionScaler) autoAssignReplicas(partitionID int, replicationFactor int) []cluster.NodeID {
	numNodes := len(ps.nodeIDs)
	replicas := make([]cluster.NodeID, replicationFactor)

	for i := 0; i < replicationFactor; i++ {
		nodeIndex := (partitionID + i) % numNodes
		replicas[i] = ps.nodeIDs[nodeIndex]
	}

	return replicas
}

// =============================================================================
// LOCAL PARTITION CREATION
// =============================================================================

// createLocalPartition creates a partition directory on this node.
//
// This doesn't create the full Partition object - that happens when the
// broker loads the partition on next access or on demand.
func (ps *PartitionScaler) createLocalPartition(topicName string, partitionID int) error {
	if ps.broker == nil {
		// In test mode without broker
		return nil
	}

	// Get the topic
	topic, err := ps.broker.GetTopic(topicName)
	if err != nil {
		return fmt.Errorf("topic %s not found in broker: %w", topicName, err)
	}

	// Add partition to topic
	return topic.AddPartition(partitionID)
}

// =============================================================================
// LISTENER MANAGEMENT
// =============================================================================

// AddListener registers a callback for partition changes.
// Consumer groups use this to trigger rebalance when partitions are added.
func (ps *PartitionScaler) AddListener(listener PartitionChangeListener) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.listeners = append(ps.listeners, listener)
}

// notifyListeners calls all registered listeners.
func (ps *PartitionScaler) notifyListeners(topicName string, oldCount, newCount int, newPartitions []int) {
	ps.mu.RLock()
	listeners := make([]PartitionChangeListener, len(ps.listeners))
	copy(listeners, ps.listeners)
	ps.mu.RUnlock()

	// Notify asynchronously to avoid blocking scaling
	for _, listener := range listeners {
		go listener(topicName, oldCount, newCount, newPartitions)
	}
}

// =============================================================================
// STATUS & INSPECTION
// =============================================================================

// IsScalingInProgress checks if a topic is currently being scaled.
func (ps *PartitionScaler) IsScalingInProgress(topicName string) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	_, inProgress := ps.scalingInProgress[topicName]
	return inProgress
}

// GetScalingTopics returns all topics currently being scaled.
func (ps *PartitionScaler) GetScalingTopics() map[string]time.Time {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	result := make(map[string]time.Time, len(ps.scalingInProgress))
	for k, v := range ps.scalingInProgress {
		result[k] = v
	}
	return result
}

// UpdateNodeList updates the available nodes for replica assignment.
// Called when cluster membership changes.
func (ps *PartitionScaler) UpdateNodeList(nodeIDs []cluster.NodeID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.nodeIDs = make([]cluster.NodeID, len(nodeIDs))
	copy(ps.nodeIDs, nodeIDs)
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// isInternalTopic checks if a topic is an internal system topic.
// Internal topics like __consumer_offsets have fixed partition counts.
func isInternalTopic(topicName string) bool {
	return len(topicName) > 0 && topicName[0] == '_' && topicName[1] == '_'
}
