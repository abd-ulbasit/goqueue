// =============================================================================
// CONSUMER GROUP - COORDINATED MESSAGE CONSUMPTION
// =============================================================================
//
// WHAT IS A CONSUMER GROUP?
// A consumer group is a set of consumers that cooperatively consume messages
// from a topic. Each partition is assigned to exactly ONE consumer in the group,
// enabling parallel processing while maintaining ordering within each partition.
//
// WHY CONSUMER GROUPS?
//
//   1. PARALLEL PROCESSING
//      - Multiple consumers share the workload
//      - Each consumer processes different partitions
//      - N consumers can process N partitions in parallel
//
//   2. FAULT TOLERANCE
//      - If a consumer dies, its partitions are reassigned
//      - Processing continues without message loss
//      - Automatic recovery from failures
//
//   3. LOAD BALANCING
//      - Partitions distributed evenly across consumers
//      - Adding consumers spreads the load
//      - Removing consumers redistributes partitions
//
// CONSUMER GROUP LIFECYCLE:
//
//   ┌──────────────────────────────────────────────────────────────────────────┐
//   │                    CONSUMER GROUP LIFECYCLE                              │
//   │                                                                          │
//   │   Consumer A joins          Consumer B joins          Consumer A leaves  │
//   │         │                         │                         │            │
//   │         ▼                         ▼                         ▼            │
//   │   ┌──────────────┐         ┌──────────────┐         ┌──────────────┐     │
//   │   │  Rebalance   │         │  Rebalance   │         │  Rebalance   │     │
//   │   │  Gen 1       │         │  Gen 2       │         │  Gen 3       │     │
//   │   └──────────────┘         └──────────────┘         └──────────────┘     │
//   │         │                         │                         │            │
//   │         ▼                         ▼                         ▼            │
//   │   A: [P0,P1,P2]            A: [P0,P1]                B: [P0,P1,P2]        │
//   │                            B: [P2]                                        │
//   │                                                                          │
//   └──────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON - How other systems implement consumer groups:
//
//   - Kafka: Consumer groups with partition assignment, offset commits
//            Uses "group coordinator" (a broker) to manage membership
//
//   - RabbitMQ: No native consumer groups. Uses competing consumers pattern
//               where multiple consumers read from same queue
//
//   - SQS: Implicit consumer groups via visibility timeout
//          Messages become invisible when one consumer receives them
//
//   - goqueue: Kafka-style consumer groups with heartbeat-based membership
//
// GENERATION ID (EPOCH):
// Each rebalance increments the generation ID. This prevents "zombie" consumers
// from making stale commits after being evicted.
//
//   Scenario without generation ID:
//   1. Consumer A has partition 0, processing message at offset 100
//   2. Consumer A becomes slow, misses heartbeat, gets evicted
//   3. Rebalance: Consumer B gets partition 0, starts at offset 50
//   4. Consumer B processes 50-100, commits offset 100
//   5. Consumer A wakes up, commits offset 100 (STALE! Should be rejected)
//
//   With generation ID:
//   - Consumer A has generation 1
//   - After rebalance, generation becomes 2
//   - Consumer A's commit with generation 1 is rejected (stale generation)
//
// =============================================================================

package broker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrGroupNotFound means the consumer group doesn't exist
	ErrGroupNotFound = errors.New("consumer group not found")

	// ErrMemberNotFound means the member is not in the group
	ErrMemberNotFound = errors.New("member not found in group")

	// ErrStaleGeneration means the operation used an outdated generation ID
	ErrStaleGeneration = errors.New("stale generation ID")

	// ErrRebalanceInProgress means a rebalance is currently happening
	ErrRebalanceInProgress = errors.New("rebalance in progress")

	// ErrNotAssigned means the member doesn't own the partition
	ErrNotAssigned = errors.New("partition not assigned to this member")

	// ErrSessionTimeout means the consumer's session has expired
	ErrSessionTimeout = errors.New("session timeout")
)

// =============================================================================
// CONSUMER GROUP CONFIGURATION
// =============================================================================

// ConsumerGroupConfig holds configuration for a consumer group.
type ConsumerGroupConfig struct {
	// SessionTimeoutMs is how long before declaring a consumer dead
	// if no heartbeat received. Default: 30000 (30 seconds)
	//
	// COMPARISON:
	//   - Kafka default: 45000ms (changed from 10000ms in newer versions)
	//   - SQS: VisibilityTimeout serves similar purpose (default 30s)
	//
	// TRADEOFF:
	//   - Too short: False positives, unnecessary rebalances
	//   - Too long: Slow failure detection, partitions idle longer
	SessionTimeoutMs int

	// HeartbeatIntervalMs is how often consumers should send heartbeats.
	// Should be <= 1/3 of SessionTimeoutMs. Default: 3000 (3 seconds)
	HeartbeatIntervalMs int

	// RebalanceTimeoutMs is how long to wait for consumers to rejoin
	// during a rebalance. Default: 60000 (60 seconds)
	RebalanceTimeoutMs int

	// AutoCommit enables automatic offset commits. Default: true
	AutoCommit bool

	// AutoCommitIntervalMs is how often to auto-commit offsets.
	// Only used if AutoCommit is true. Default: 5000 (5 seconds)
	AutoCommitIntervalMs int
}

// DefaultConsumerGroupConfig returns sensible defaults matching Kafka conventions.
func DefaultConsumerGroupConfig() ConsumerGroupConfig {
	return ConsumerGroupConfig{
		SessionTimeoutMs:     30000,
		HeartbeatIntervalMs:  3000,
		RebalanceTimeoutMs:   60000,
		AutoCommit:           true,
		AutoCommitIntervalMs: 5000,
	}
}

// =============================================================================
// MEMBER - A CONSUMER WITHIN A GROUP
// =============================================================================

// Member represents a consumer that is part of a consumer group.
type Member struct {
	// ID is the unique identifier for this member (server-generated with client prefix)
	// Format: "{clientID}-{uuid}" e.g., "order-processor-abc123def456"
	ID string

	// ClientID is the client-provided identifier (optional, for debugging)
	ClientID string

	// JoinedAt is when this member joined the group
	JoinedAt time.Time

	// LastHeartbeat is the last time we received a heartbeat from this member
	LastHeartbeat time.Time

	// AssignedPartitions is the list of partition IDs assigned to this member
	// This is set during rebalance based on the assignment strategy
	AssignedPartitions []int

	// Generation is the generation ID when this member joined
	// Used to detect stale operations from zombie consumers
	Generation int
}

// IsExpired checks if the member's session has timed out.
func (m *Member) IsExpired(sessionTimeoutMs int) bool {
	return time.Since(m.LastHeartbeat) > time.Duration(sessionTimeoutMs)*time.Millisecond
}

// =============================================================================
// CONSUMER GROUP STATE
// =============================================================================

// GroupState represents the current state of a consumer group.
type GroupState int

const (
	// GroupStateEmpty means no members in the group
	GroupStateEmpty GroupState = iota

	// GroupStateStable means the group has active members and stable assignments
	GroupStateStable

	// GroupStateRebalancing means the group is redistributing partitions
	GroupStateRebalancing

	// GroupStateDead means the group has been deleted
	GroupStateDead
)

// String returns the string representation of the group state.
func (s GroupState) String() string {
	switch s {
	case GroupStateEmpty:
		return "Empty"
	case GroupStateStable:
		return "Stable"
	case GroupStateRebalancing:
		return "Rebalancing"
	case GroupStateDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// =============================================================================
// CONSUMER GROUP
// =============================================================================

// ConsumerGroup manages a set of consumers that cooperatively consume from topics.
type ConsumerGroup struct {
	// ID is the unique identifier for this consumer group
	ID string

	// Topics is the list of topics this group is subscribed to
	Topics []string

	// Config holds the group configuration
	Config ConsumerGroupConfig

	// State is the current state of the group
	State GroupState

	// Generation is incremented on each rebalance
	// Used to detect stale operations from zombie consumers
	//
	// FLOW:
	//   Gen 0: Initial (no members)
	//   Gen 1: First member joins → rebalance → assignment
	//   Gen 2: Member joins/leaves → rebalance → new assignment
	//   ...
	Generation int

	// Members maps member ID to Member
	Members map[string]*Member

	// mu protects all fields
	mu sync.RWMutex

	// createdAt is when the group was created
	createdAt time.Time
}

// NewConsumerGroup creates a new consumer group.
func NewConsumerGroup(id string, topics []string, config ConsumerGroupConfig) *ConsumerGroup {
	return &ConsumerGroup{
		ID:         id,
		Topics:     topics,
		Config:     config,
		State:      GroupStateEmpty,
		Generation: 0,
		Members:    make(map[string]*Member),
		createdAt:  time.Now(),
	}
}

// =============================================================================
// MEMBERSHIP OPERATIONS
// =============================================================================

// JoinResult contains the result of a successful join operation.
type JoinResult struct {
	MemberID   string
	Generation int
	LeaderID   string // First member is the leader (for client-side assignment in future)
	Members    []string
	Partitions []int // Partitions assigned to this member
}

// Join adds a new member to the consumer group and triggers a rebalance.
//
// FLOW:
//  1. Generate unique member ID (clientID + UUID suffix)
//  2. Add member to group
//  3. Trigger rebalance (stop-the-world)
//  4. Assign partitions using range strategy
//  5. Return assignment to the joining member
//
// COMPARISON:
//   - Kafka: JoinGroup request triggers "JoinGroup phase" where all members
//     rejoin. Leader performs assignment. SyncGroup distributes assignments.
//   - goqueue (M3): Simplified - broker does assignment directly
func (g *ConsumerGroup) Join(clientID string, partitionCount int) (*JoinResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.State == GroupStateDead {
		return nil, fmt.Errorf("consumer group %s is dead", g.ID)
	}

	// Generate unique member ID
	// Format: {clientID}-{timestamp_hex} for simplicity
	// In production, use UUID, but timestamp is more debuggable
	memberID := fmt.Sprintf("%s-%x", clientID, time.Now().UnixNano())

	// Create new member
	member := &Member{
		ID:            memberID,
		ClientID:      clientID,
		JoinedAt:      time.Now(),
		LastHeartbeat: time.Now(),
	}

	// Add to group
	g.Members[memberID] = member

	// Trigger rebalance
	g.rebalance(partitionCount)

	// Update member's generation
	member.Generation = g.Generation

	// Build result
	memberIDs := make([]string, 0, len(g.Members))
	for id := range g.Members {
		memberIDs = append(memberIDs, id)
	}

	// Find leader (first member by join time for determinism)
	var leaderID string
	var earliestJoin time.Time
	for _, m := range g.Members {
		if leaderID == "" || m.JoinedAt.Before(earliestJoin) {
			leaderID = m.ID
			earliestJoin = m.JoinedAt
		}
	}

	return &JoinResult{
		MemberID:   memberID,
		Generation: g.Generation,
		LeaderID:   leaderID,
		Members:    memberIDs,
		Partitions: member.AssignedPartitions,
	}, nil
}

// Heartbeat updates the member's last heartbeat time.
//
// RETURNS:
//   - nil if heartbeat accepted
//   - ErrMemberNotFound if member not in group
//   - ErrStaleGeneration if generation doesn't match (zombie consumer)
//
// WHY HEARTBEATS?
// Without heartbeats, we can't detect if a consumer has crashed. The broker
// needs periodic signals to know the consumer is still alive and processing.
//
// COMPARISON:
//   - Kafka: Heartbeats sent by background thread, separate from fetch
//   - SQS: No explicit heartbeat; visibility timeout serves similar purpose
//   - RabbitMQ: TCP keepalive + prefetch acknowledgments
func (g *ConsumerGroup) Heartbeat(memberID string, generation int) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	member, exists := g.Members[memberID]
	if !exists {
		return ErrMemberNotFound
	}

	// Check generation to detect zombie consumers
	if generation != g.Generation {
		return ErrStaleGeneration
	}

	member.LastHeartbeat = time.Now()
	return nil
}

// Leave removes a member from the group and triggers a rebalance.
//
// GRACEFUL vs UNGRACEFUL LEAVE:
//   - Graceful: Consumer calls Leave() before shutdown
//     → Immediate rebalance, minimal disruption
//   - Ungraceful: Consumer crashes, no Leave() called
//     → Wait for session timeout, then rebalance
//
// COMPARISON:
//   - Kafka: LeaveGroup request for graceful shutdown
//   - SQS: No explicit leave; visibility timeout handles it
func (g *ConsumerGroup) Leave(memberID string, partitionCount int) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.Members[memberID]; !exists {
		return ErrMemberNotFound
	}

	delete(g.Members, memberID)

	// Trigger rebalance if members remain
	if len(g.Members) > 0 {
		g.rebalance(partitionCount)
	} else {
		g.State = GroupStateEmpty
	}

	return nil
}

// =============================================================================
// REBALANCING
// =============================================================================

// rebalance redistributes partitions among members using the range strategy.
// Must be called with g.mu held.
//
// RANGE ASSIGNMENT STRATEGY:
// Partitions are divided into contiguous ranges and assigned to consumers.
//
// Example with 6 partitions and 2 consumers:
//   - Consumer A: partitions 0, 1, 2
//   - Consumer B: partitions 3, 4, 5
//
// Example with 7 partitions and 3 consumers:
//   - Consumer A: partitions 0, 1, 2 (gets extra because 7 % 3 = 1)
//   - Consumer B: partitions 3, 4
//   - Consumer C: partitions 5, 6
//
// COMPARISON:
//   - Range (this): Contiguous ranges, simple, may be uneven across topics
//   - RoundRobin: p0→A, p1→B, p2→A, p3→B... More even, but scattered
//   - Sticky: Like RoundRobin but minimizes moves during rebalance (M12)
//
// WHY STOP-THE-WORLD (EAGER)?
// In M3, we use eager rebalancing where ALL partitions are revoked and
// reassigned. This is simpler but causes a brief processing pause.
// Cooperative/incremental rebalancing (M12) will minimize this disruption.
func (g *ConsumerGroup) rebalance(partitionCount int) {
	// Increment generation
	g.Generation++
	g.State = GroupStateRebalancing

	// Get sorted list of member IDs for deterministic assignment
	memberIDs := make([]string, 0, len(g.Members))
	for id := range g.Members {
		memberIDs = append(memberIDs, id)
	}

	// Sort for determinism (in real implementation, sort by join time or member ID)
	// Using simple string sort for now
	for i := 0; i < len(memberIDs)-1; i++ {
		for j := i + 1; j < len(memberIDs); j++ {
			if memberIDs[i] > memberIDs[j] {
				memberIDs[i], memberIDs[j] = memberIDs[j], memberIDs[i]
			}
		}
	}

	// Clear existing assignments
	for _, member := range g.Members {
		member.AssignedPartitions = nil
	}

	if len(memberIDs) == 0 {
		g.State = GroupStateEmpty
		return
	}

	// Range assignment
	//
	// ALGORITHM:
	//   partitionsPerConsumer = partitionCount / memberCount
	//   extraPartitions = partitionCount % memberCount
	//
	//   First 'extraPartitions' consumers get (partitionsPerConsumer + 1) partitions
	//   Remaining consumers get 'partitionsPerConsumer' partitions
	//
	// EXAMPLE (7 partitions, 3 consumers):
	//   partitionsPerConsumer = 7 / 3 = 2
	//   extraPartitions = 7 % 3 = 1
	//
	//   Consumer 0: partitions 0, 1, 2 (2+1 because index < extraPartitions)
	//   Consumer 1: partitions 3, 4     (2 partitions)
	//   Consumer 2: partitions 5, 6     (2 partitions)
	memberCount := len(memberIDs)
	partitionsPerConsumer := partitionCount / memberCount
	extraPartitions := partitionCount % memberCount

	currentPartition := 0
	for i, memberID := range memberIDs {
		member := g.Members[memberID]

		// Calculate how many partitions this consumer gets
		count := partitionsPerConsumer
		if i < extraPartitions {
			count++
		}

		// Assign contiguous range
		partitions := make([]int, count)
		for j := 0; j < count; j++ {
			partitions[j] = currentPartition
			currentPartition++
		}
		member.AssignedPartitions = partitions
	}

	g.State = GroupStateStable
}

// =============================================================================
// MEMBERSHIP QUERIES
// =============================================================================

// GetMember returns the member with the given ID.
func (g *ConsumerGroup) GetMember(memberID string) (*Member, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	member, exists := g.Members[memberID]
	if !exists {
		return nil, ErrMemberNotFound
	}
	return member, nil
}

// GetAssignment returns the partitions assigned to a member and the current generation.
func (g *ConsumerGroup) GetAssignment(memberID string) ([]int, int, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	member, exists := g.Members[memberID]
	if !exists {
		return nil, 0, ErrMemberNotFound
	}
	return member.AssignedPartitions, g.Generation, nil
}

// IsPartitionAssigned checks if a partition is assigned to a specific member.
func (g *ConsumerGroup) IsPartitionAssigned(memberID string, partition int) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	member, exists := g.Members[memberID]
	if !exists {
		return false
	}

	for _, p := range member.AssignedPartitions {
		if p == partition {
			return true
		}
	}
	return false
}

// MemberCount returns the number of members in the group.
func (g *ConsumerGroup) MemberCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.Members)
}

// GetGeneration returns the current generation ID.
func (g *ConsumerGroup) GetGeneration() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.Generation
}

// GetState returns the current group state.
func (g *ConsumerGroup) GetState() GroupState {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.State
}

// =============================================================================
// SESSION MANAGEMENT
// =============================================================================

// CheckSessions identifies members whose sessions have timed out.
// Returns list of expired member IDs.
func (g *ConsumerGroup) CheckSessions() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	expired := make([]string, 0)
	for id, member := range g.Members {
		if member.IsExpired(g.Config.SessionTimeoutMs) {
			expired = append(expired, id)
		}
	}
	return expired
}

// EvictExpiredMembers removes timed-out members and triggers rebalance.
// Returns the number of members evicted.
func (g *ConsumerGroup) EvictExpiredMembers(partitionCount int) int {
	g.mu.Lock()
	defer g.mu.Unlock()

	expired := make([]string, 0)
	for id, member := range g.Members {
		if member.IsExpired(g.Config.SessionTimeoutMs) {
			expired = append(expired, id)
		}
	}

	if len(expired) == 0 {
		return 0
	}

	// Remove expired members
	for _, id := range expired {
		delete(g.Members, id)
	}

	// Trigger rebalance if members remain
	if len(g.Members) > 0 {
		g.rebalance(partitionCount)
	} else {
		g.State = GroupStateEmpty
	}

	return len(expired)
}

// =============================================================================
// GROUP INFO
// =============================================================================

// GroupInfo contains a snapshot of consumer group state for reporting.
type GroupInfo struct {
	ID         string
	State      string
	Generation int
	Topics     []string
	Members    []MemberInfo
	CreatedAt  time.Time
}

// MemberInfo contains member information for reporting.
type MemberInfo struct {
	ID                 string
	ClientID           string
	AssignedPartitions []int
	LastHeartbeat      time.Time
	JoinedAt           time.Time
}

// GetInfo returns a snapshot of the group state.
func (g *ConsumerGroup) GetInfo() GroupInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()

	members := make([]MemberInfo, 0, len(g.Members))
	for _, m := range g.Members {
		members = append(members, MemberInfo{
			ID:                 m.ID,
			ClientID:           m.ClientID,
			AssignedPartitions: m.AssignedPartitions,
			LastHeartbeat:      m.LastHeartbeat,
			JoinedAt:           m.JoinedAt,
		})
	}

	return GroupInfo{
		ID:         g.ID,
		State:      g.State.String(),
		Generation: g.Generation,
		Topics:     g.Topics,
		Members:    members,
		CreatedAt:  g.createdAt,
	}
}
