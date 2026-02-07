// =============================================================================
// COOPERATIVE GROUP - INTEGRATION OF COOPERATIVE REBALANCING WITH CONSUMER GROUPS
// =============================================================================
//
// This file provides the integration layer between the cooperative rebalancer
// and the existing consumer group infrastructure. It adds:
//
//   1. Extended ConsumerGroup configuration for cooperative mode
//   2. CooperativeGroupCoordinator that wraps GroupCoordinator
//   3. New heartbeat response types with rebalance information
//   4. Revocation acknowledgment handling
//
// =============================================================================

package broker

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// =============================================================================
// EXTENDED CONSUMER GROUP CONFIG
// =============================================================================

// CooperativeGroupConfig extends ConsumerGroupConfig with cooperative settings.
type CooperativeGroupConfig struct {
	ConsumerGroupConfig

	// Protocol is the rebalancing protocol to use.
	// Default: RebalanceProtocolCooperative
	Protocol RebalanceProtocol

	// AssignmentStrategy is how partitions are assigned.
	// Default: AssignmentStrategySticky
	AssignmentStrategy AssignmentStrategy

	// RevocationTimeoutMs is how long to wait for revocation acks.
	// Default: 60000 (60 seconds)
	RevocationTimeoutMs int
}

// DefaultCooperativeGroupConfig returns sensible defaults.
func DefaultCooperativeGroupConfig() CooperativeGroupConfig {
	return CooperativeGroupConfig{
		ConsumerGroupConfig: DefaultConsumerGroupConfig(),
		Protocol:            RebalanceProtocolCooperative,
		AssignmentStrategy:  AssignmentStrategySticky,
		RevocationTimeoutMs: 60000,
	}
}

// =============================================================================
// EXTENDED MEMBER WITH MULTI-TOPIC SUPPORT
// =============================================================================

// CooperativeMember extends Member with cooperative rebalancing state.
type CooperativeMember struct {
	Member

	// AssignedTopicPartitions is the full assignment (multi-topic support)
	AssignedTopicPartitions []TopicPartition

	// PendingRevocations are partitions awaiting revocation acknowledgment
	PendingRevocations []TopicPartition

	// RevocationDeadline is when revocations must be acknowledged
	RevocationDeadline time.Time

	// LastKnownGeneration is the generation this member has acknowledged
	LastKnownGeneration int
}

// =============================================================================
// COOPERATIVE GROUP
// =============================================================================

// CooperativeGroup wraps ConsumerGroup with cooperative rebalancing support.
type CooperativeGroup struct {
	*ConsumerGroup

	// config holds cooperative-specific configuration
	cooperativeConfig CooperativeGroupConfig

	// rebalancer handles the cooperative rebalancing protocol
	rebalancer *CooperativeRebalancer

	// members maps member ID to CooperativeMember (extended member info)
	cooperativeMembers map[string]*CooperativeMember

	// currentAssignment is the current partition assignment (TopicPartition-based)
	currentAssignment map[string][]TopicPartition

	// topicPartitions maps topic to partition count (for multi-topic support)
	topicPartitions map[string]int

	mu sync.RWMutex
}

// NewCooperativeGroup creates a new cooperative consumer group.
func NewCooperativeGroup(
	id string,
	topics []string,
	config CooperativeGroupConfig,
	rebalancer *CooperativeRebalancer,
) *CooperativeGroup {
	base := NewConsumerGroup(id, topics, config.ConsumerGroupConfig)

	cg := &CooperativeGroup{
		ConsumerGroup:      base,
		cooperativeConfig:  config,
		rebalancer:         rebalancer,
		cooperativeMembers: make(map[string]*CooperativeMember),
		currentAssignment:  make(map[string][]TopicPartition),
		topicPartitions:    make(map[string]int),
	}

	return cg
}

// SetTopicPartitions sets the partition counts for subscribed topics.
func (cg *CooperativeGroup) SetTopicPartitions(topicPartitions map[string]int) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.topicPartitions = topicPartitions
}

// =============================================================================
// JOIN WITH COOPERATIVE REBALANCING
// =============================================================================

// CooperativeJoinResult extends JoinResult with cooperative fields.
type CooperativeJoinResult struct {
	JoinResult

	// TopicPartitions is the full assignment (for multi-topic)
	TopicPartitions []TopicPartition

	// RebalanceRequired indicates if consumer should wait for rebalance
	RebalanceRequired bool

	// Protocol is the rebalancing protocol in use
	Protocol RebalanceProtocol
}

// JoinCooperative adds a member to the group with cooperative rebalancing.
func (cg *CooperativeGroup) JoinCooperative(clientID string) (*CooperativeJoinResult, error) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if cg.ConsumerGroup.State == GroupStateDead {
		return nil, fmt.Errorf("consumer group %s is dead", cg.ConsumerGroup.ID)
	}

	// Generate unique member ID
	memberID := fmt.Sprintf("%s-%x", clientID, time.Now().UnixNano())

	// Create base member
	member := &Member{
		ID:            memberID,
		ClientID:      clientID,
		JoinedAt:      time.Now(),
		LastHeartbeat: time.Now(),
	}

	// Create cooperative member
	coopMember := &CooperativeMember{
		Member:                  *member,
		AssignedTopicPartitions: []TopicPartition{},
		PendingRevocations:      []TopicPartition{},
		LastKnownGeneration:     cg.ConsumerGroup.Generation,
	}

	// Add to both maps
	cg.ConsumerGroup.Members[memberID] = member
	cg.cooperativeMembers[memberID] = coopMember

	// Get current member list
	members := make([]string, 0, len(cg.ConsumerGroup.Members))
	for id := range cg.ConsumerGroup.Members {
		members = append(members, id)
	}

	// Trigger cooperative rebalance
	if cg.rebalancer != nil {
		ctx, err := cg.rebalancer.TriggerRebalance(
			cg.ConsumerGroup.ID,
			"member_joined",
			cg.ConsumerGroup.Generation,
			members,
			cg.topicPartitions,
			cg.currentAssignment,
		)
		if err != nil {
			// Remove member on failure
			delete(cg.ConsumerGroup.Members, memberID)
			delete(cg.cooperativeMembers, memberID)
			return nil, fmt.Errorf("failed to trigger rebalance: %w", err)
		}

		// Update generation
		cg.ConsumerGroup.Generation = ctx.AssignGeneration
		cg.ConsumerGroup.State = GroupStateRebalancing
	}

	// Build result
	result := &CooperativeJoinResult{
		JoinResult: JoinResult{
			MemberID:   memberID,
			Generation: cg.ConsumerGroup.Generation,
			Members:    members,
		},
		RebalanceRequired: true,
		Protocol:          cg.cooperativeConfig.Protocol,
	}

	// Set leader
	var earliestJoin time.Time
	for _, m := range cg.ConsumerGroup.Members {
		if result.LeaderID == "" || m.JoinedAt.Before(earliestJoin) {
			result.LeaderID = m.ID
			earliestJoin = m.JoinedAt
		}
	}

	return result, nil
}

// =============================================================================
// LEAVE WITH COOPERATIVE REBALANCING
// =============================================================================

// LeaveCooperative removes a member with cooperative rebalancing.
func (cg *CooperativeGroup) LeaveCooperative(memberID string) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if _, exists := cg.ConsumerGroup.Members[memberID]; !exists {
		return ErrMemberNotFound
	}

	// Remove from both maps
	delete(cg.ConsumerGroup.Members, memberID)
	delete(cg.cooperativeMembers, memberID)

	// Remove from current assignment
	delete(cg.currentAssignment, memberID)

	if len(cg.ConsumerGroup.Members) == 0 {
		cg.ConsumerGroup.State = GroupStateEmpty
		return nil
	}

	// Get current member list
	members := make([]string, 0, len(cg.ConsumerGroup.Members))
	for id := range cg.ConsumerGroup.Members {
		members = append(members, id)
	}

	// Trigger cooperative rebalance
	if cg.rebalancer != nil {
		ctx, err := cg.rebalancer.TriggerRebalance(
			cg.ConsumerGroup.ID,
			"member_left",
			cg.ConsumerGroup.Generation,
			members,
			cg.topicPartitions,
			cg.currentAssignment,
		)
		if err != nil {
			return fmt.Errorf("failed to trigger rebalance: %w", err)
		}

		cg.ConsumerGroup.Generation = ctx.AssignGeneration
		cg.ConsumerGroup.State = GroupStateRebalancing
	}

	return nil
}

// =============================================================================
// HEARTBEAT WITH REBALANCE RESPONSE
// =============================================================================

// HeartbeatCooperative processes a heartbeat and returns rebalance info.
func (cg *CooperativeGroup) HeartbeatCooperative(memberID string, generation int) (*HeartbeatResponse, error) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	member, exists := cg.ConsumerGroup.Members[memberID]
	if !exists {
		return nil, ErrMemberNotFound
	}

	// Update heartbeat time
	member.LastHeartbeat = time.Now()

	// Get rebalance response from rebalancer
	if cg.rebalancer != nil {
		response := cg.rebalancer.GetHeartbeatResponse(
			cg.ConsumerGroup.ID,
			memberID,
			generation,
		)

		// Update member's known generation
		if coopMember, exists := cg.cooperativeMembers[memberID]; exists {
			coopMember.LastKnownGeneration = generation

			// If we got new assignment, update our tracking
			if len(response.PartitionsAssigned) > 0 && !response.RebalanceRequired {
				cg.currentAssignment[memberID] = response.PartitionsAssigned
				coopMember.AssignedTopicPartitions = response.PartitionsAssigned

				// Also update the base member's partition ints (for backward compat)
				if len(cg.Topics) > 0 {
					member.AssignedPartitions = PartitionsToInts(response.PartitionsAssigned, cg.Topics[0])
				}
			}
		}

		return response, nil
	}

	// No rebalancer - return simple response
	return &HeartbeatResponse{
		RebalanceRequired: false,
		Generation:        cg.ConsumerGroup.Generation,
		State:             RebalanceStateNone,
	}, nil
}

// =============================================================================
// REVOCATION ACKNOWLEDGMENT
// =============================================================================

// AcknowledgeRevocation records that a member has revoked partitions.
func (cg *CooperativeGroup) AcknowledgeRevocation(
	memberID string,
	generation int,
	revokedPartitions []TopicPartition,
) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if _, exists := cg.ConsumerGroup.Members[memberID]; !exists {
		return ErrMemberNotFound
	}

	// Forward to rebalancer
	if cg.rebalancer != nil {
		if err := cg.rebalancer.AcknowledgeRevocation(
			cg.ConsumerGroup.ID,
			memberID,
			generation,
			revokedPartitions,
		); err != nil {
			return err
		}
	}

	// Update cooperative member state
	if coopMember, exists := cg.cooperativeMembers[memberID]; exists {
		// Remove revoked partitions from pending
		coopMember.PendingRevocations = nil

		// Remove from current assignment
		newAssignment := make([]TopicPartition, 0)
		revokedSet := make(map[TopicPartition]struct{})
		for _, tp := range revokedPartitions {
			revokedSet[tp] = struct{}{}
		}
		for _, tp := range coopMember.AssignedTopicPartitions {
			if _, revoked := revokedSet[tp]; !revoked {
				newAssignment = append(newAssignment, tp)
			}
		}
		coopMember.AssignedTopicPartitions = newAssignment
		cg.currentAssignment[memberID] = newAssignment
	}

	return nil
}

// =============================================================================
// ASSIGNMENT QUERIES
// =============================================================================

// GetCurrentAssignment returns the current assignment for all members.
func (cg *CooperativeGroup) GetCurrentAssignment() map[string][]TopicPartition {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	result := make(map[string][]TopicPartition, len(cg.currentAssignment))
	for memberID, partitions := range cg.currentAssignment {
		result[memberID] = append([]TopicPartition{}, partitions...)
	}
	return result
}

// GetMemberAssignment returns the current assignment for a specific member.
func (cg *CooperativeGroup) GetMemberAssignment(memberID string) ([]TopicPartition, bool) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	partitions, exists := cg.currentAssignment[memberID]
	if !exists {
		return nil, false
	}
	// Return a copy to prevent aliasing and potential data races
	result := make([]TopicPartition, len(partitions))
	copy(result, partitions)
	return result, true
}

// IsPartitionAssignedCoop checks if a partition is assigned to a member (TopicPartition version).
func (cg *CooperativeGroup) IsPartitionAssignedCoop(memberID string, tp TopicPartition) bool {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	partitions, exists := cg.currentAssignment[memberID]
	if !exists {
		return false
	}

	for _, p := range partitions {
		if p.Topic == tp.Topic && p.Partition == tp.Partition {
			return true
		}
	}
	return false
}

// =============================================================================
// COOPERATIVE GROUP INFO
// =============================================================================

// CooperativeGroupInfo extends GroupInfo with cooperative state.
type CooperativeGroupInfo struct {
	GroupInfo

	// Protocol is the rebalancing protocol
	Protocol string `json:"protocol"`

	// AssignmentStrategy is the partition assignment strategy
	AssignmentStrategy string `json:"assignment_strategy"`

	// RebalanceState is the current rebalance state
	RebalanceState string `json:"rebalance_state"`

	// MemberAssignments shows current assignments per member
	MemberAssignments map[string][]TopicPartition `json:"member_assignments"`

	// RebalanceMetrics contains rebalance statistics
	RebalanceMetrics *RebalanceMetrics `json:"rebalance_metrics,omitempty"`
}

// GetCooperativeInfo returns extended group information.
func (cg *CooperativeGroup) GetCooperativeInfo() CooperativeGroupInfo {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	baseInfo := cg.ConsumerGroup.GetInfo()

	info := CooperativeGroupInfo{
		GroupInfo:          baseInfo,
		Protocol:           cg.cooperativeConfig.Protocol.String(),
		AssignmentStrategy: cg.cooperativeConfig.AssignmentStrategy.String(),
		MemberAssignments:  make(map[string][]TopicPartition),
	}

	// Copy assignments
	for memberID, partitions := range cg.currentAssignment {
		info.MemberAssignments[memberID] = append([]TopicPartition{}, partitions...)
	}

	// Get rebalance state
	if cg.rebalancer != nil {
		ctx := cg.rebalancer.GetRebalanceContext(cg.ConsumerGroup.ID)
		if ctx != nil {
			info.RebalanceState = ctx.State.String()
		} else {
			info.RebalanceState = RebalanceStateNone.String()
		}

		metrics := cg.rebalancer.GetMetrics()
		info.RebalanceMetrics = metrics
	}

	return info
}

// =============================================================================
// COOPERATIVE GROUP COORDINATOR
// =============================================================================

// CooperativeGroupCoordinator extends GroupCoordinator with cooperative support.
type CooperativeGroupCoordinator struct {
	*GroupCoordinator

	// rebalancer handles cooperative rebalancing
	rebalancer *CooperativeRebalancer

	// cooperativeGroups maps group ID to CooperativeGroup
	cooperativeGroups map[string]*CooperativeGroup

	// defaultConfig is the default config for new cooperative groups
	defaultConfig CooperativeGroupConfig

	mu sync.RWMutex
}

// NewCooperativeGroupCoordinator creates a new cooperative group coordinator.
func NewCooperativeGroupCoordinator(
	baseCoordinator *GroupCoordinator,
	defaultConfig CooperativeGroupConfig,
) *CooperativeGroupCoordinator {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	rebalancerConfig := CooperativeRebalancerConfig{
		Protocol:            defaultConfig.Protocol,
		Strategy:            defaultConfig.AssignmentStrategy,
		RevocationTimeoutMs: defaultConfig.RevocationTimeoutMs,
		RebalanceDelayMs:    500,
		CheckIntervalMs:     500,
	}

	rebalancer := NewCooperativeRebalancer(rebalancerConfig, logger)
	rebalancer.Start()

	return &CooperativeGroupCoordinator{
		GroupCoordinator:  baseCoordinator,
		rebalancer:        rebalancer,
		cooperativeGroups: make(map[string]*CooperativeGroup),
		defaultConfig:     defaultConfig,
	}
}

// GetOrCreateCooperativeGroup returns or creates a cooperative group.
func (cgc *CooperativeGroupCoordinator) GetOrCreateCooperativeGroup(
	groupID string,
	topics []string,
) (*CooperativeGroup, error) {
	cgc.mu.Lock()
	defer cgc.mu.Unlock()

	if existing, exists := cgc.cooperativeGroups[groupID]; exists {
		return existing, nil
	}

	// Create base group first
	baseGroup, err := cgc.GroupCoordinator.GetOrCreateGroup(groupID, topics)
	if err != nil {
		return nil, err
	}

	// Wrap with cooperative group
	coopGroup := &CooperativeGroup{
		ConsumerGroup:      baseGroup,
		cooperativeConfig:  cgc.defaultConfig,
		rebalancer:         cgc.rebalancer,
		cooperativeMembers: make(map[string]*CooperativeMember),
		currentAssignment:  make(map[string][]TopicPartition),
		topicPartitions:    make(map[string]int),
	}

	// Set partition counts from coordinator
	for _, topic := range topics {
		if count, exists := cgc.GroupCoordinator.GetPartitionCount(topic); exists {
			coopGroup.topicPartitions[topic] = count
		}
	}

	cgc.cooperativeGroups[groupID] = coopGroup

	return coopGroup, nil
}

// GetCooperativeGroup returns a cooperative group by ID.
func (cgc *CooperativeGroupCoordinator) GetCooperativeGroup(groupID string) (*CooperativeGroup, bool) {
	cgc.mu.RLock()
	defer cgc.mu.RUnlock()
	group, exists := cgc.cooperativeGroups[groupID]
	return group, exists
}

// JoinCooperativeGroup adds a consumer to a cooperative group.
func (cgc *CooperativeGroupCoordinator) JoinCooperativeGroup(
	groupID string,
	clientID string,
	topics []string,
) (*CooperativeJoinResult, error) {
	// Get or create the group
	group, err := cgc.GetOrCreateCooperativeGroup(groupID, topics)
	if err != nil {
		return nil, err
	}

	// Update topic partitions
	cgc.mu.Lock()
	for _, topic := range topics {
		if count, exists := cgc.GroupCoordinator.GetPartitionCount(topic); exists {
			group.topicPartitions[topic] = count
		}
	}
	cgc.mu.Unlock()

	// Join the group
	return group.JoinCooperative(clientID)
}

// LeaveCooperativeGroup removes a consumer from a cooperative group.
func (cgc *CooperativeGroupCoordinator) LeaveCooperativeGroup(
	groupID string,
	memberID string,
) error {
	group, exists := cgc.GetCooperativeGroup(groupID)
	if !exists {
		return ErrGroupNotFound
	}
	return group.LeaveCooperative(memberID)
}

// HeartbeatCooperative processes a heartbeat with cooperative response.
func (cgc *CooperativeGroupCoordinator) HeartbeatCooperative(
	groupID string,
	memberID string,
	generation int,
) (*HeartbeatResponse, error) {
	group, exists := cgc.GetCooperativeGroup(groupID)
	if !exists {
		// Fall back to base coordinator heartbeat
		if err := cgc.GroupCoordinator.Heartbeat(groupID, memberID, generation); err != nil {
			return nil, err
		}
		return &HeartbeatResponse{
			RebalanceRequired: false,
			Generation:        generation,
			State:             RebalanceStateNone,
		}, nil
	}

	return group.HeartbeatCooperative(memberID, generation)
}

// AcknowledgeRevocation records a revocation acknowledgment.
func (cgc *CooperativeGroupCoordinator) AcknowledgeRevocation(
	groupID string,
	memberID string,
	generation int,
	revokedPartitions []TopicPartition,
) error {
	group, exists := cgc.GetCooperativeGroup(groupID)
	if !exists {
		return ErrGroupNotFound
	}
	return group.AcknowledgeRevocation(memberID, generation, revokedPartitions)
}

// GetRebalanceMetrics returns rebalance metrics for all groups.
func (cgc *CooperativeGroupCoordinator) GetRebalanceMetrics() *RebalanceMetrics {
	return cgc.rebalancer.GetMetrics()
}

// Close shuts down the coordinator.
func (cgc *CooperativeGroupCoordinator) Close() error {
	cgc.rebalancer.Stop()
	return cgc.GroupCoordinator.Close()
}

// =============================================================================
// COMPLETE REBALANCE
// =============================================================================

// CompleteRebalance marks a group's rebalance as complete and updates state.
func (cgc *CooperativeGroupCoordinator) CompleteRebalance(groupID string) {
	group, exists := cgc.GetCooperativeGroup(groupID)
	if !exists {
		return
	}

	// Get final assignment from rebalancer
	ctx := cgc.rebalancer.GetRebalanceContext(groupID)
	if ctx != nil && ctx.State == RebalanceStatePendingAssign {
		group.mu.Lock()
		// Update current assignment from target
		for memberID, assignment := range ctx.TargetAssignment {
			group.currentAssignment[memberID] = assignment.Partitions

			// Update cooperative member
			if coopMember, exists := group.cooperativeMembers[memberID]; exists {
				coopMember.AssignedTopicPartitions = assignment.Partitions
			}

			// Update base member partition ints (backward compat)
			if member, exists := group.ConsumerGroup.Members[memberID]; exists {
				if len(group.Topics) > 0 {
					member.AssignedPartitions = PartitionsToInts(assignment.Partitions, group.Topics[0])
				}
			}
		}
		group.ConsumerGroup.State = GroupStateStable
		group.mu.Unlock()
	}

	cgc.rebalancer.CompleteRebalance(groupID)
}
