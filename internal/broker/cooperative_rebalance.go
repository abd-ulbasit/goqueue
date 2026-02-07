// =============================================================================
// COOPERATIVE REBALANCING - ZERO-DOWNTIME CONSUMER GROUP REBALANCING
// =============================================================================
//
// WHAT IS COOPERATIVE REBALANCING?
// Cooperative rebalancing (a.k.a. incremental rebalancing) is a protocol that
// minimizes disruption when consumers join or leave a group. Unlike "eager"
// (stop-the-world) rebalancing where ALL consumers stop, cooperative only
// affects the partitions that actually need to move.
//
// THE PROBLEM WITH EAGER REBALANCING (What we had before):
//
//   ┌────────────────────────────────────────────────────────────────────────────┐
//   │                   EAGER REBALANCE (OLD WAY)                                │
//   │                                                                            │
//   │   BEFORE: A=[P0,P1], B=[P2,P3]                                             │
//   │                                                                            │
//   │   C joins → ALL consumers STOP → Recalculate → ALL restart                 │
//   │                                                                            │
//   │   Time ─────────────────────────────────────────────────────►              │
//   │   A: █████████░░░░░░░░░░░░░░░░░░░████████████████████                      │
//   │   B: █████████░░░░░░░░░░░░░░░░░░░████████████████████                      │
//   │   C:          ░░░░░░░░░░░░░░░░░░░████████████████████                      │
//   │              ▲                   ▲                                         │
//   │         ALL STOP           ALL RESTART                                     │
//   │                                                                            │
//   │   AFTER: A=[P0], B=[P1,P2], C=[P3]                                         │
//   │                                                                            │
//   │   PROBLEM: A kept P0 but still had to stop! Unnecessary disruption.        │
//   └────────────────────────────────────────────────────────────────────────────┘
//
// THE SOLUTION: COOPERATIVE REBALANCING (Kafka KIP-429)
//
//   ┌────────────────────────────────────────────────────────────────────────────┐
//   │                   COOPERATIVE REBALANCE (NEW WAY)                          │
//   │                                                                            │
//   │   BEFORE: A=[P0,P1], B=[P2,P3]                                             │
//   │                                                                            │
//   │   C joins → Only B revokes P3 → C gets P3 → Done!                          │
//   │                                                                            │
//   │   Time ─────────────────────────────────────────────────────►              │
//   │   A: █████████████████████████████████████████████  (no change!)           │
//   │   B: █████████████████░░██████████████████████████  (brief pause P3)       │
//   │   C:                  ░░██████████████████████████  (starts P3)            │
//   │                       ▲▲                                                   │
//   │                  Revoke  Assign                                            │
//   │                  Phase   Phase                                             │
//   │                                                                            │
//   │   AFTER: A=[P0,P1], B=[P2], C=[P3]                                         │
//   │                                                                            │
//   │   Only P3 experienced any disruption!                                      │
//   └────────────────────────────────────────────────────────────────────────────┘
//
// HOW IT WORKS - TWO-PHASE PROTOCOL:
//
//   PHASE 1: REVOCATION
//   ┌─────────────────────────────────────────────────────────────────────────────┐
//   │                                                                             │
//   │   1. Coordinator calculates new assignment                                  │
//   │   2. For each consumer, compare old vs new partitions                       │
//   │   3. If consumer LOSES partitions → send revocation request                 │
//   │   4. Consumer stops processing revoked partitions                           │
//   │   5. Consumer commits offsets for revoked partitions                        │
//   │   6. Consumer acknowledges revocation complete                              │
//   │   7. Wait for ALL revocations to complete (or timeout)                      │
//   │                                                                             │
//   └─────────────────────────────────────────────────────────────────────────────┘
//
//   PHASE 2: ASSIGNMENT
//   ┌─────────────────────────────────────────────────────────────────────────────┐
//   │                                                                             │
//   │   1. All revocations complete (partitions are "free")                       │
//   │   2. Coordinator assigns freed partitions to new owners                     │
//   │   3. Consumers receive new partitions via heartbeat response                │
//   │   4. Consumers start processing newly assigned partitions                   │
//   │   5. Rebalance complete!                                                    │
//   │                                                                             │
//   └─────────────────────────────────────────────────────────────────────────────┘
//
// STICKY ASSIGNMENT:
//
//   The key to minimizing disruption is STICKY assignment - keeping partitions
//   with their current owners whenever possible:
//
//   ┌─────────────────────────────────────────────────────────────────────────────┐
//   │                      RANGE vs STICKY ASSIGNMENT                             │
//   │                                                                             │
//   │   Initial:   A=[0,1,2,3], B=[4,5,6,7]                                       │
//   │   C joins... need to redistribute 8 partitions among 3 consumers            │
//   │                                                                             │
//   │   RANGE (BAD):                        STICKY (GOOD):                        │
//   │   A=[0,1,2]   lost 3                  A=[0,1,2]   lost 3                    │
//   │   B=[3,4,5]   lost 6,7, gained 3      B=[4,5,6]   lost 7                    │
//   │   C=[6,7]     gained 6,7              C=[3,7]     gained 3,7                │
//   │                                                                             │
//   │   Movements: 4 partitions             Movements: 2 partitions               │
//   │   (3→B, 6→C, 7→C, 3→B's old)          (3→C, 7→C)                            │
//   │                                                                             │
//   └─────────────────────────────────────────────────────────────────────────────┘
//
// GENERATION EPOCHS:
//
//   Each rebalance increments the generation twice (once per phase):
//
//   Gen 1: Initial state
//   Gen 2: Revocation phase (consumers know to revoke)
//   Gen 3: Assignment phase (new assignment is active)
//
//   This prevents stale operations from zombie consumers.
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   ┌────────────────────────────────────────────────────────────────────────────┐
//   │   System         Protocol          Who Assigns?      Disruption            │
//   │   ──────────────────────────────────────────────────────────────────────── │
//   │   Kafka (old)    Eager             Leader consumer   All consumers stop    │
//   │   Kafka (new)    Cooperative       Leader consumer   Only affected stop    │
//   │   RabbitMQ       N/A               Broker            No groups             │
//   │   SQS            N/A               N/A               Visibility timeout    │
//   │   Pulsar         Key_Shared        Broker            Similar to sticky     │
//   │   goqueue        Cooperative       Broker            Only affected stop    │
//   └────────────────────────────────────────────────────────────────────────────┘
//
// TIMEOUT HANDLING:
//
//   If a consumer doesn't acknowledge revocation within the timeout:
//   1. Mark partition as force-revoked
//   2. Proceed with assignment anyway
//   3. Mark consumer as suspect (may be evicted on next timeout)
//
//   This prevents hung consumers from blocking rebalances forever.
//
// =============================================================================

package broker

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// =============================================================================
// REBALANCE PROTOCOL TYPES
// =============================================================================

// RebalanceProtocol represents the rebalancing strategy.
type RebalanceProtocol int

const (
	// RebalanceProtocolEager is the traditional stop-the-world rebalancing.
	// All consumers stop, revoke all partitions, then receive new assignments.
	// This is the OLD protocol, kept for backward compatibility.
	RebalanceProtocolEager RebalanceProtocol = iota

	// RebalanceProtocolCooperative is the new incremental rebalancing.
	// Only affected partitions are revoked, minimizing disruption.
	// This is the DEFAULT for new consumer groups.
	RebalanceProtocolCooperative
)

// String returns the string representation of the protocol.
func (p RebalanceProtocol) String() string {
	switch p {
	case RebalanceProtocolEager:
		return "eager"
	case RebalanceProtocolCooperative:
		return "cooperative"
	default:
		return "unknown"
	}
}

// ParseRebalanceProtocol parses a string to RebalanceProtocol.
func ParseRebalanceProtocol(s string) (RebalanceProtocol, error) {
	switch s {
	case "eager":
		return RebalanceProtocolEager, nil
	case "cooperative":
		return RebalanceProtocolCooperative, nil
	default:
		return RebalanceProtocolEager, fmt.Errorf("unknown rebalance protocol: %s", s)
	}
}

// =============================================================================
// ASSIGNMENT STRATEGY TYPES
// =============================================================================

// AssignmentStrategy represents how partitions are assigned to consumers.
type AssignmentStrategy int

const (
	// AssignmentStrategyRange assigns contiguous partition ranges to consumers.
	// Simple but may cause uneven distribution across multiple topics.
	AssignmentStrategyRange AssignmentStrategy = iota

	// AssignmentStrategyRoundRobin distributes partitions in round-robin fashion.
	// More even distribution but partitions are scattered.
	AssignmentStrategyRoundRobin

	// AssignmentStrategySticky minimizes partition movement during rebalances.
	// Best for cooperative rebalancing - this is the DEFAULT.
	AssignmentStrategySticky
)

// String returns the string representation of the strategy.
func (s AssignmentStrategy) String() string {
	switch s {
	case AssignmentStrategyRange:
		return "range"
	case AssignmentStrategyRoundRobin:
		return "roundrobin"
	case AssignmentStrategySticky:
		return "sticky"
	default:
		return "unknown"
	}
}

// ParseAssignmentStrategy parses a string to AssignmentStrategy.
func ParseAssignmentStrategy(s string) (AssignmentStrategy, error) {
	switch s {
	case "range":
		return AssignmentStrategyRange, nil
	case "roundrobin":
		return AssignmentStrategyRoundRobin, nil
	case "sticky":
		return AssignmentStrategySticky, nil
	default:
		return AssignmentStrategySticky, fmt.Errorf("unknown assignment strategy: %s", s)
	}
}

// =============================================================================
// REBALANCE STATE
// =============================================================================

// RebalanceState tracks the current state of a rebalance operation.
type RebalanceState int

const (
	// RebalanceStateNone means no rebalance is in progress.
	RebalanceStateNone RebalanceState = iota

	// RebalanceStatePendingRevoke means waiting for consumers to revoke partitions.
	RebalanceStatePendingRevoke

	// RebalanceStatePendingAssign means revocations complete, sending new assignments.
	RebalanceStatePendingAssign

	// RebalanceStateComplete means rebalance finished successfully.
	RebalanceStateComplete
)

// String returns the string representation of the state.
func (s RebalanceState) String() string {
	switch s {
	case RebalanceStateNone:
		return "none"
	case RebalanceStatePendingRevoke:
		return "pending_revoke"
	case RebalanceStatePendingAssign:
		return "pending_assign"
	case RebalanceStateComplete:
		return "complete"
	default:
		return "unknown"
	}
}

// =============================================================================
// TOPIC PARTITION
// =============================================================================

// TopicPartition identifies a specific partition within a topic.
type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

// String returns a human-readable representation.
func (tp TopicPartition) String() string {
	return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition)
}

// TopicPartitionList is a sortable list of TopicPartitions.
type TopicPartitionList []TopicPartition

func (l TopicPartitionList) Len() int      { return len(l) }
func (l TopicPartitionList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l TopicPartitionList) Less(i, j int) bool {
	if l[i].Topic != l[j].Topic {
		return l[i].Topic < l[j].Topic
	}
	return l[i].Partition < l[j].Partition
}

// =============================================================================
// MEMBER ASSIGNMENT
// =============================================================================

// MemberAssignment represents the partitions assigned to a consumer.
type MemberAssignment struct {
	MemberID   string           `json:"member_id"`
	Partitions []TopicPartition `json:"partitions"`
	Generation int              `json:"generation"`
	Timestamp  time.Time        `json:"timestamp"`
}

// TopicPartitionSet returns partitions as a set for fast lookup.
func (ma *MemberAssignment) TopicPartitionSet() map[TopicPartition]struct{} {
	set := make(map[TopicPartition]struct{}, len(ma.Partitions))
	for _, tp := range ma.Partitions {
		set[tp] = struct{}{}
	}
	return set
}

// HasPartition checks if this member has the given partition.
func (ma *MemberAssignment) HasPartition(tp TopicPartition) bool {
	for _, p := range ma.Partitions {
		if p.Topic == tp.Topic && p.Partition == tp.Partition {
			return true
		}
	}
	return false
}

// =============================================================================
// REVOCATION REQUEST
// =============================================================================

// RevocationRequest represents partitions a consumer must give up.
type RevocationRequest struct {
	MemberID   string           `json:"member_id"`
	Partitions []TopicPartition `json:"partitions"`
	Generation int              `json:"generation"`
	Deadline   time.Time        `json:"deadline"`
}

// RevocationResponse represents a consumer's acknowledgment of revocation.
type RevocationResponse struct {
	MemberID          string           `json:"member_id"`
	RevokedPartitions []TopicPartition `json:"revoked_partitions"`
	Generation        int              `json:"generation"`
	Success           bool             `json:"success"`
	Error             string           `json:"error,omitempty"`
}

// =============================================================================
// PENDING REVOCATION TRACKER
// =============================================================================

// PendingRevocation tracks revocations that haven't been acknowledged.
type PendingRevocation struct {
	MemberID    string
	Partitions  map[TopicPartition]struct{}
	RequestedAt time.Time
	Deadline    time.Time
}

// =============================================================================
// REBALANCE CONTEXT
// =============================================================================

// RebalanceContext holds all state for an in-progress rebalance.
type RebalanceContext struct {
	// Generation when this rebalance started
	StartGeneration int

	// Generation for the assignment phase (StartGeneration + 1)
	AssignGeneration int

	// State of the rebalance
	State RebalanceState

	// Reason for the rebalance (for logging/metrics)
	Reason string

	// When the rebalance started
	StartedAt time.Time

	// When the rebalance completed (or zero if ongoing)
	CompletedAt time.Time

	// Pending revocations - members that need to revoke partitions
	PendingRevocations map[string]*PendingRevocation

	// Completed revocations - members that have acknowledged
	CompletedRevocations map[string][]TopicPartition

	// Target assignment - where we want to end up
	TargetAssignment map[string]*MemberAssignment

	// Previous assignment - where we started from
	PreviousAssignment map[string]*MemberAssignment

	// Partitions being transferred
	PartitionsInTransit map[TopicPartition]struct{}

	// mu protects all fields
	mu sync.RWMutex
}

// NewRebalanceContext creates a new rebalance context.
func NewRebalanceContext(generation int, reason string) *RebalanceContext {
	return &RebalanceContext{
		StartGeneration:      generation,
		AssignGeneration:     generation + 1,
		State:                RebalanceStatePendingRevoke,
		Reason:               reason,
		StartedAt:            time.Now(),
		PendingRevocations:   make(map[string]*PendingRevocation),
		CompletedRevocations: make(map[string][]TopicPartition),
		TargetAssignment:     make(map[string]*MemberAssignment),
		PreviousAssignment:   make(map[string]*MemberAssignment),
		PartitionsInTransit:  make(map[TopicPartition]struct{}),
	}
}

// AllRevocationsComplete checks if all pending revocations are acknowledged.
func (rc *RebalanceContext) AllRevocationsComplete() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return len(rc.PendingRevocations) == 0
}

// GetPendingRevocationCount returns the number of pending revocations.
func (rc *RebalanceContext) GetPendingRevocationCount() int {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return len(rc.PendingRevocations)
}

// AddPendingRevocation adds a revocation request for a member.
func (rc *RebalanceContext) AddPendingRevocation(memberID string, partitions []TopicPartition, deadline time.Time) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	partitionSet := make(map[TopicPartition]struct{}, len(partitions))
	for _, tp := range partitions {
		partitionSet[tp] = struct{}{}
		rc.PartitionsInTransit[tp] = struct{}{}
	}

	rc.PendingRevocations[memberID] = &PendingRevocation{
		MemberID:    memberID,
		Partitions:  partitionSet,
		RequestedAt: time.Now(),
		Deadline:    deadline,
	}
}

// CompleteRevocation marks a member's revocation as complete.
func (rc *RebalanceContext) CompleteRevocation(memberID string, partitions []TopicPartition) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	delete(rc.PendingRevocations, memberID)
	rc.CompletedRevocations[memberID] = partitions
}

// GetExpiredRevocations returns members whose revocation deadline has passed.
func (rc *RebalanceContext) GetExpiredRevocations() []string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	now := time.Now()
	var expired []string
	for memberID, pending := range rc.PendingRevocations {
		if now.After(pending.Deadline) {
			expired = append(expired, memberID)
		}
	}
	return expired
}

// ForceCompleteRevocation marks revocation complete without consumer ack.
func (rc *RebalanceContext) ForceCompleteRevocation(memberID string) []TopicPartition {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	pending, exists := rc.PendingRevocations[memberID]
	if !exists {
		return nil
	}

	partitions := make([]TopicPartition, 0, len(pending.Partitions))
	for tp := range pending.Partitions {
		partitions = append(partitions, tp)
	}

	delete(rc.PendingRevocations, memberID)
	rc.CompletedRevocations[memberID] = partitions

	return partitions
}

// =============================================================================
// REBALANCE METRICS
// =============================================================================

// RebalanceMetrics tracks statistics about rebalancing operations.
type RebalanceMetrics struct {
	// TotalRebalances is the total number of rebalances triggered
	TotalRebalances int64 `json:"total_rebalances"`

	// SuccessfulRebalances is rebalances that completed successfully
	SuccessfulRebalances int64 `json:"successful_rebalances"`

	// FailedRebalances is rebalances that failed or timed out
	FailedRebalances int64 `json:"failed_rebalances"`

	// TotalPartitionsRevoked is total partitions revoked across all rebalances
	TotalPartitionsRevoked int64 `json:"total_partitions_revoked"`

	// TotalPartitionsAssigned is total partitions assigned across all rebalances
	TotalPartitionsAssigned int64 `json:"total_partitions_assigned"`

	// TotalRevocationTimeouts is revocations that timed out
	TotalRevocationTimeouts int64 `json:"total_revocation_timeouts"`

	// LastRebalanceDurationMs is how long the last rebalance took
	LastRebalanceDurationMs int64 `json:"last_rebalance_duration_ms"`

	// AverageRebalanceDurationMs is the average rebalance duration
	AverageRebalanceDurationMs float64 `json:"average_rebalance_duration_ms"`

	// LastRebalanceAt is when the last rebalance occurred
	LastRebalanceAt time.Time `json:"last_rebalance_at"`

	// RebalancesByReason tracks rebalance counts by trigger reason
	RebalancesByReason map[string]int64 `json:"rebalances_by_reason"`

	// mu protects all fields
	mu sync.RWMutex
}

// NewRebalanceMetrics creates new metrics.
func NewRebalanceMetrics() *RebalanceMetrics {
	return &RebalanceMetrics{
		RebalancesByReason: make(map[string]int64),
	}
}

// RecordRebalanceStart records the start of a rebalance.
func (m *RebalanceMetrics) RecordRebalanceStart(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalRebalances++
	m.RebalancesByReason[reason]++
}

// RecordRebalanceComplete records completion of a rebalance.
func (m *RebalanceMetrics) RecordRebalanceComplete(durationMs int64, partitionsRevoked, partitionsAssigned int, success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastRebalanceDurationMs = durationMs
	m.LastRebalanceAt = time.Now()
	m.TotalPartitionsRevoked += int64(partitionsRevoked)
	m.TotalPartitionsAssigned += int64(partitionsAssigned)

	if success {
		m.SuccessfulRebalances++
	} else {
		m.FailedRebalances++
	}

	// Update running average
	total := m.SuccessfulRebalances + m.FailedRebalances
	if total > 0 {
		// Exponential moving average
		alpha := 0.3 // Weight for new value
		m.AverageRebalanceDurationMs = alpha*float64(durationMs) + (1-alpha)*m.AverageRebalanceDurationMs
	}
}

// RecordRevocationTimeout records a revocation timeout.
func (m *RebalanceMetrics) RecordRevocationTimeout() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TotalRevocationTimeouts++
}

// GetSnapshot returns a copy of the current metrics.
func (m *RebalanceMetrics) GetSnapshot() *RebalanceMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	reasonsCopy := make(map[string]int64, len(m.RebalancesByReason))
	for k, v := range m.RebalancesByReason {
		reasonsCopy[k] = v
	}

	return &RebalanceMetrics{
		TotalRebalances:            m.TotalRebalances,
		SuccessfulRebalances:       m.SuccessfulRebalances,
		FailedRebalances:           m.FailedRebalances,
		TotalPartitionsRevoked:     m.TotalPartitionsRevoked,
		TotalPartitionsAssigned:    m.TotalPartitionsAssigned,
		TotalRevocationTimeouts:    m.TotalRevocationTimeouts,
		LastRebalanceDurationMs:    m.LastRebalanceDurationMs,
		AverageRebalanceDurationMs: m.AverageRebalanceDurationMs,
		LastRebalanceAt:            m.LastRebalanceAt,
		RebalancesByReason:         reasonsCopy,
	}
}

// =============================================================================
// REBALANCE CALLBACK (for consumer notification)
// =============================================================================

// RebalanceCallback is invoked when partition assignment changes.
// This allows consumers to prepare for partition changes (flush buffers,
// save state, etc.)
type RebalanceCallback interface {
	// OnPartitionsRevoked is called BEFORE partitions are revoked.
	// The consumer should:
	//   1. Stop processing messages for these partitions
	//   2. Commit any pending offsets
	//   3. Flush any buffers or save local state
	//
	// The deadline is when the broker expects acknowledgment. If not
	// acknowledged by then, the partition will be force-revoked.
	OnPartitionsRevoked(partitions []TopicPartition, deadline time.Time) error

	// OnPartitionsAssigned is called AFTER new partitions are assigned.
	// The consumer should:
	//   1. Initialize any state needed for these partitions
	//   2. Seek to the appropriate offset if needed
	//   3. Begin processing messages
	OnPartitionsAssigned(partitions []TopicPartition) error
}

// =============================================================================
// HEARTBEAT RESPONSE (for communicating rebalance state)
// =============================================================================

// HeartbeatResponse contains the broker's response to a heartbeat.
// This is how we communicate rebalance requests to consumers.
type HeartbeatResponse struct {
	// RebalanceRequired indicates a rebalance is needed
	RebalanceRequired bool `json:"rebalance_required"`

	// PartitionsToRevoke is the list of partitions to give up
	// Only set during revocation phase of cooperative rebalance
	PartitionsToRevoke []TopicPartition `json:"partitions_to_revoke,omitempty"`

	// PartitionsAssigned is the new full assignment for this consumer
	// Set after revocation phase completes
	PartitionsAssigned []TopicPartition `json:"partitions_assigned,omitempty"`

	// Generation is the current generation (for stale detection)
	Generation int `json:"generation"`

	// Protocol is the rebalance protocol in use
	Protocol RebalanceProtocol `json:"protocol"`

	// State is the current rebalance state
	State RebalanceState `json:"state"`

	// RevocationDeadline is when revocations must be acknowledged
	RevocationDeadline time.Time `json:"revocation_deadline,omitempty"`

	// Error message if any
	Error string `json:"error,omitempty"`
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// DiffPartitions returns partitions in 'a' but not in 'b'.
// Used to calculate which partitions to revoke or assign.
func DiffPartitions(a, b []TopicPartition) []TopicPartition {
	bSet := make(map[TopicPartition]struct{}, len(b))
	for _, tp := range b {
		bSet[tp] = struct{}{}
	}

	var diff []TopicPartition
	for _, tp := range a {
		if _, exists := bSet[tp]; !exists {
			diff = append(diff, tp)
		}
	}
	return diff
}

// IntersectPartitions returns partitions in both 'a' and 'b'.
func IntersectPartitions(a, b []TopicPartition) []TopicPartition {
	bSet := make(map[TopicPartition]struct{}, len(b))
	for _, tp := range b {
		bSet[tp] = struct{}{}
	}

	var intersection []TopicPartition
	for _, tp := range a {
		if _, exists := bSet[tp]; exists {
			intersection = append(intersection, tp)
		}
	}
	return intersection
}

// UnionPartitions returns the union of partitions in 'a' and 'b'.
func UnionPartitions(a, b []TopicPartition) []TopicPartition {
	set := make(map[TopicPartition]struct{}, len(a)+len(b))
	for _, tp := range a {
		set[tp] = struct{}{}
	}
	for _, tp := range b {
		set[tp] = struct{}{}
	}

	union := make([]TopicPartition, 0, len(set))
	for tp := range set {
		union = append(union, tp)
	}

	// Sort for determinism
	sort.Sort(TopicPartitionList(union))
	return union
}

// PartitionsToInts converts TopicPartitions to partition ints (for single topic).
// Used for backward compatibility with old API.
func PartitionsToInts(partitions []TopicPartition, topic string) []int {
	var result []int
	for _, tp := range partitions {
		if tp.Topic == topic {
			result = append(result, tp.Partition)
		}
	}
	sort.Ints(result)
	return result
}

// IntsToPartitions converts partition ints to TopicPartitions.
func IntsToPartitions(partitions []int, topic string) []TopicPartition {
	result := make([]TopicPartition, len(partitions))
	for i, p := range partitions {
		result[i] = TopicPartition{Topic: topic, Partition: p}
	}
	return result
}
