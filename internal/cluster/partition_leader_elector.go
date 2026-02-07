// =============================================================================
// PARTITION LEADER ELECTOR - LEADER ELECTION FOR PARTITIONS
// =============================================================================
//
// WHAT: Handles leader election for individual partitions.
//
// DISTINCTION FROM CONTROLLER ELECTION (M10):
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                                                                         │
//   │   CONTROLLER (M10 - Done)         PARTITION LEADER (M11 - This file)    │
//   │   ─────────────────────           ──────────────────────────────────    │
//   │   • ONE per cluster               • ONE per partition                   │
//   │   • Manages metadata              • Handles reads/writes                │
//   │   • Decides WHO leads partitions  • Actually processes messages         │
//   │   • Elected via lease             • Assigned by controller              │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// ELECTION FLOW:
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                                                                         │
//   │   1. Leader dies (detected by failure detector)                         │
//   │                    │                                                    │
//   │                    ▼                                                    │
//   │   2. Follower notices (fetch error or controller notification)          │
//   │                    │                                                    │
//   │                    ▼                                                    │
//   │   3. Controller picks new leader from ISR                               │
//   │      (or from all replicas if unclean election enabled)                 │
//   │                    │                                                    │
//   │                    ▼                                                    │
//   │   4. Controller updates metadata + epoch                                │
//   │                    │                                                    │
//   │                    ▼                                                    │
//   │   5. New leader notified → starts accepting writes                      │
//   │                    │                                                    │
//   │                    ▼                                                    │
//   │   6. Followers notified → start fetching from new leader                │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// UNCLEAN ELECTION:
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                                                                         │
//   │   CLEAN ELECTION (Default):                                             │
//   │   • Only ISR members can become leader                                  │
//   │   • If no ISR members available → partition unavailable                 │
//   │   • NO DATA LOSS (ISR was caught up)                                    │
//   │                                                                         │
//   │   UNCLEAN ELECTION (Optional):                                          │
//   │   • Any replica can become leader                                       │
//   │   • If all ISR dead, use non-ISR replica                                │
//   │   • POSSIBLE DATA LOSS (non-ISR was behind)                             │
//   │   • Trade-off: Availability over Consistency                            │
//   │                                                                         │
//   │   Configuration:                                                        │
//   │     unclean_leader_election: false  # default, prefer consistency       │
//   │     unclean_leader_election: true   # availability-critical systems     │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package cluster

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// =============================================================================
// PARTITION LEADER ELECTOR
// =============================================================================

// PartitionLeaderElector handles leader election for partitions.
// This runs on the CONTROLLER node only.
type PartitionLeaderElector struct {
	// metadataStore contains partition assignments.
	metadataStore *MetadataStore

	// membership provides node health info.
	membership *Membership

	// replicationConfig controls election behavior.
	config ReplicationConfig

	// mu protects state.
	mu sync.Mutex

	// logger for operations.
	logger *slog.Logger

	// pendingElections tracks ongoing elections.
	pendingElections map[string]time.Time
}

// NewPartitionLeaderElector creates a new partition leader elector.
func NewPartitionLeaderElector(metadataStore *MetadataStore, membership *Membership, config ReplicationConfig, logger *slog.Logger) *PartitionLeaderElector {
	return &PartitionLeaderElector{
		metadataStore:    metadataStore,
		membership:       membership,
		config:           config,
		logger:           logger.With("component", "partition-elector"),
		pendingElections: make(map[string]time.Time),
	}
}

// =============================================================================
// LEADER ASSIGNMENT (TOPIC CREATION)
// =============================================================================

// AssignLeadersForTopic assigns initial leaders for a new topic.
// Uses offset-start round-robin for even distribution.
func (pe *PartitionLeaderElector) AssignLeadersForTopic(topic string, partitions, replicationFactor int) ([]*PartitionAssignment, error) {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	// Get alive nodes.
	aliveNodes := pe.membership.AliveNodes()
	if len(aliveNodes) == 0 {
		return nil, fmt.Errorf("no alive nodes for leader assignment")
	}

	if replicationFactor > len(aliveNodes) {
		pe.logger.Warn("replication factor exceeds alive nodes, reducing",
			"requested", replicationFactor,
			"available", len(aliveNodes))
		replicationFactor = len(aliveNodes)
	}

	// Sort nodes for deterministic assignment.
	sortedNodes := make([]NodeID, len(aliveNodes))
	for i, n := range aliveNodes {
		sortedNodes[i] = n.ID
	}
	sort.Slice(sortedNodes, func(i, j int) bool {
		return string(sortedNodes[i]) < string(sortedNodes[j])
	})

	assignments := make([]*PartitionAssignment, partitions)

	for p := 0; p < partitions; p++ {
		// OFFSET-START ROUND-ROBIN:
		// Partition p starts assignment at node (p % len(nodes)).
		// This distributes leaders more evenly than simple round-robin.
		//
		// Example: 3 nodes, 6 partitions
		//   P0: starts at node 0 → leader=0, replicas=[0,1]
		//   P1: starts at node 1 → leader=1, replicas=[1,2]
		//   P2: starts at node 2 → leader=2, replicas=[2,0]
		//   P3: starts at node 0 → leader=0, replicas=[0,1]
		//   ...
		//
		// Result: Each node leads 2 partitions (even distribution).

		startIdx := p % len(sortedNodes)

		// Build replica list.
		replicas := make([]NodeID, replicationFactor)
		for r := 0; r < replicationFactor; r++ {
			nodeIdx := (startIdx + r) % len(sortedNodes)
			replicas[r] = sortedNodes[nodeIdx]
		}

		// Leader is first replica.
		leader := replicas[0]

		assignments[p] = &PartitionAssignment{
			Topic:     topic,
			Partition: p,
			Leader:    leader,
			Replicas:  replicas,
			ISR:       replicas, // Initially all replicas are in-sync.
			Version:   1,
		}

		pe.logger.Info("assigned partition leader",
			"topic", topic,
			"partition", p,
			"leader", leader,
			"replicas", replicas)
	}

	return assignments, nil
}

// =============================================================================
// LEADER ELECTION (FAILOVER)
// =============================================================================

// ElectLeader elects a new leader for a partition.
// Called when current leader fails or is unavailable.
func (pe *PartitionLeaderElector) ElectLeader(req *LeaderElectionRequest) (*LeaderElectionResponse, error) {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	key := partitionKey(req.Topic, req.Partition)
	pe.logger.Info("starting leader election",
		"topic", req.Topic,
		"partition", req.Partition,
		"reason", req.Reason,
		"allow_unclean", req.AllowUnclean)

	// Get current assignment.
	assignment := pe.metadataStore.GetAssignment(req.Topic, req.Partition)
	if assignment == nil {
		return &LeaderElectionResponse{
			Topic:        req.Topic,
			Partition:    req.Partition,
			ErrorCode:    LeaderElectionNoISR,
			ErrorMessage: "partition assignment not found",
		}, nil
	}

	// Try to elect from ISR first.
	newLeader, err := pe.electFromISR(assignment)
	if err == nil {
		return pe.completeElection(assignment, newLeader)
	}

	// ISR election failed - check if unclean is allowed.
	allowUnclean := req.AllowUnclean || pe.config.UncleanLeaderElection
	if !allowUnclean {
		pe.logger.Warn("no ISR available, unclean election disabled",
			"topic", req.Topic,
			"partition", req.Partition)
		return &LeaderElectionResponse{
			Topic:        req.Topic,
			Partition:    req.Partition,
			ErrorCode:    LeaderElectionUncleanDisabled,
			ErrorMessage: "no in-sync replica available and unclean election disabled",
		}, nil
	}

	// Try unclean election from all replicas.
	pe.logger.Warn("attempting unclean leader election",
		"topic", req.Topic,
		"partition", req.Partition)

	newLeader, err = pe.electFromReplicas(assignment)
	if err != nil {
		return &LeaderElectionResponse{
			Topic:        req.Topic,
			Partition:    req.Partition,
			ErrorCode:    LeaderElectionNoISR,
			ErrorMessage: fmt.Sprintf("no available replica: %v", err),
		}, nil
	}

	// Mark pending election.
	pe.pendingElections[key] = time.Now()

	return pe.completeElection(assignment, newLeader)
}

// electFromISR tries to elect a leader from ISR members.
func (pe *PartitionLeaderElector) electFromISR(assignment *PartitionAssignment) (NodeID, error) {
	for _, nodeID := range assignment.ISR {
		// Skip current leader.
		if nodeID == assignment.Leader {
			continue
		}

		// Check if node is alive.
		if pe.isNodeAlive(nodeID) {
			return nodeID, nil
		}
	}

	return "", fmt.Errorf("no alive ISR member found")
}

// electFromReplicas tries to elect a leader from all replicas (unclean).
func (pe *PartitionLeaderElector) electFromReplicas(assignment *PartitionAssignment) (NodeID, error) {
	for _, nodeID := range assignment.Replicas {
		// Skip current leader.
		if nodeID == assignment.Leader {
			continue
		}

		// Check if node is alive.
		if pe.isNodeAlive(nodeID) {
			return nodeID, nil
		}
	}

	return "", fmt.Errorf("no alive replica found")
}

// isNodeAlive checks if a node is alive via membership.
func (pe *PartitionLeaderElector) isNodeAlive(nodeID NodeID) bool {
	node := pe.membership.GetNode(nodeID)
	if node == nil {
		return false
	}
	return node.Status == NodeStatusAlive
}

// completeElection updates metadata and returns success response.
func (pe *PartitionLeaderElector) completeElection(assignment *PartitionAssignment, newLeader NodeID) (*LeaderElectionResponse, error) {
	// Bump epoch.
	newEpoch := assignment.Version + 1

	// Update leader in metadata store.
	if err := pe.metadataStore.UpdateLeader(assignment.Topic, assignment.Partition, newLeader); err != nil {
		return nil, fmt.Errorf("failed to update leader: %w", err)
	}

	// Update ISR to only include the new leader initially.
	// Other replicas must catch up to rejoin ISR.
	newISR := []NodeID{newLeader}
	if err := pe.metadataStore.UpdateISR(assignment.Topic, assignment.Partition, newISR); err != nil {
		return nil, fmt.Errorf("failed to update ISR: %w", err)
	}

	pe.logger.Info("leader election complete",
		"topic", assignment.Topic,
		"partition", assignment.Partition,
		"new_leader", newLeader,
		"new_epoch", newEpoch)

	return &LeaderElectionResponse{
		Topic:     assignment.Topic,
		Partition: assignment.Partition,
		NewLeader: newLeader,
		NewEpoch:  newEpoch,
		ErrorCode: LeaderElectionSuccess,
	}, nil
}

// =============================================================================
// PREFERRED LEADER ELECTION
// =============================================================================

// ElectPreferredLeader attempts to move leadership to the preferred (first) replica.
// Used for rebalancing after node recovery.
func (pe *PartitionLeaderElector) ElectPreferredLeader(topic string, partition int) (*LeaderElectionResponse, error) {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	assignment := pe.metadataStore.GetAssignment(topic, partition)
	if assignment == nil {
		return nil, fmt.Errorf("partition assignment not found")
	}

	// Preferred leader is the first replica.
	if len(assignment.Replicas) == 0 {
		return nil, fmt.Errorf("no replicas configured")
	}
	preferredLeader := assignment.Replicas[0]

	// Already the leader?
	if assignment.Leader == preferredLeader {
		pe.logger.Debug("already preferred leader",
			"topic", topic,
			"partition", partition,
			"leader", preferredLeader)
		return &LeaderElectionResponse{
			Topic:     topic,
			Partition: partition,
			NewLeader: preferredLeader,
			NewEpoch:  assignment.Version,
			ErrorCode: LeaderElectionSuccess,
		}, nil
	}

	// Check if preferred leader is in ISR.
	inISR := false
	for _, nodeID := range assignment.ISR {
		if nodeID == preferredLeader {
			inISR = true
			break
		}
	}

	if !inISR {
		pe.logger.Warn("preferred leader not in ISR",
			"topic", topic,
			"partition", partition,
			"preferred", preferredLeader)
		return &LeaderElectionResponse{
			Topic:        topic,
			Partition:    partition,
			ErrorCode:    LeaderElectionNoISR,
			ErrorMessage: "preferred leader not in ISR",
		}, nil
	}

	// Check if preferred leader is alive.
	if !pe.isNodeAlive(preferredLeader) {
		return &LeaderElectionResponse{
			Topic:        topic,
			Partition:    partition,
			ErrorCode:    LeaderElectionNoISR,
			ErrorMessage: "preferred leader not alive",
		}, nil
	}

	// Elect preferred leader.
	return pe.completeElection(assignment, preferredLeader)
}

// =============================================================================
// BATCH OPERATIONS
// =============================================================================

// ElectLeadersForNode handles leader election for all partitions led by a failed node.
func (pe *PartitionLeaderElector) ElectLeadersForNode(failedNodeID NodeID) ([]*LeaderElectionResponse, error) {
	pe.logger.Info("electing leaders for failed node", "node", failedNodeID)

	// Get all assignments where failed node was leader.
	allAssignments := pe.metadataStore.Meta().Assignments
	var results []*LeaderElectionResponse

	for _, assignment := range allAssignments {
		if assignment.Leader == failedNodeID {
			req := &LeaderElectionRequest{
				Topic:        assignment.Topic,
				Partition:    assignment.Partition,
				Reason:       fmt.Sprintf("node %s failed", failedNodeID),
				AllowUnclean: pe.config.UncleanLeaderElection,
			}

			resp, err := pe.ElectLeader(req)
			if err != nil {
				pe.logger.Error("failed to elect leader",
					"topic", assignment.Topic,
					"partition", assignment.Partition,
					"error", err)
				continue
			}
			results = append(results, resp)
		}
	}

	return results, nil
}

// =============================================================================
// STATISTICS
// =============================================================================

// ElectionStats contains election statistics.
type ElectionStats struct {
	PendingElections int
	TotalPartitions  int
	LeaderlessCount  int
}

// GetStats returns current election statistics.
func (pe *PartitionLeaderElector) GetStats() ElectionStats {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	allAssignments := pe.metadataStore.Meta().Assignments
	leaderless := 0

	for _, assignment := range allAssignments {
		if assignment.Leader.IsEmpty() || !pe.isNodeAlive(assignment.Leader) {
			leaderless++
		}
	}

	return ElectionStats{
		PendingElections: len(pe.pendingElections),
		TotalPartitions:  len(allAssignments),
		LeaderlessCount:  leaderless,
	}
}
