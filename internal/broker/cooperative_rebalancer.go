// =============================================================================
// COOPERATIVE REBALANCER - ORCHESTRATES INCREMENTAL REBALANCING
// =============================================================================
//
// WHAT IS THIS?
// The CooperativeRebalancer orchestrates the two-phase cooperative rebalancing
// protocol. It coordinates between the GroupCoordinator (manages groups) and
// individual consumers (via heartbeat responses).
//
// RESPONSIBILITIES:
//
//   1. DETECT REBALANCE TRIGGERS
//      - Consumer joins group
//      - Consumer leaves group (graceful or timeout)
//      - Topic/partition changes
//
//   2. ORCHESTRATE TWO-PHASE PROTOCOL
//      - Phase 1: Request revocations from affected consumers
//      - Wait for acknowledgments (or timeout)
//      - Phase 2: Send new assignments to all consumers
//
//   3. TRACK REBALANCE STATE
//      - Which consumers need to revoke
//      - Which have acknowledged
//      - Timeout handling
//
//   4. GENERATE METRICS
//      - Rebalance count, duration, partition moves
//
// HOW CONSUMERS LEARN ABOUT REBALANCE:
//
//   ┌────────────────────────────────────────────────────────────────────────────┐
//   │                    HEARTBEAT-BASED COMMUNICATION                           │
//   │                                                                            │
//   │   Consumer                         Coordinator                             │
//   │      │                                  │                                  │
//   │      │   Heartbeat(gen=1)               │                                  │
//   │      │ ─────────────────────────────►   │  New consumer joins              │
//   │      │                                  │  → Calculate new assignment      │
//   │      │   HeartbeatResponse {            │  → This consumer needs to        │
//   │      │     rebalanceRequired: true,     │    revoke partition 3            │
//   │      │     partitionsToRevoke: [P3],    │                                  │
//   │      │     generation: 2,               │                                  │
//   │      │     state: PENDING_REVOKE,       │                                  │
//   │      │     deadline: +60s,              │                                  │
//   │      │   }                              │                                  │
//   │      │ ◄─────────────────────────────   │                                  │
//   │      │                                  │                                  │
//   │      │   [Consumer stops processing P3] │                                  │
//   │      │   [Consumer commits offset]      │                                  │
//   │      │                                  │                                  │
//   │      │   RevokeAck(gen=2, [P3])         │                                  │
//   │      │ ─────────────────────────────►   │  All revocations complete?       │
//   │      │                                  │  Yes → Move to PENDING_ASSIGN    │
//   │      │                                  │                                  │
//   │      │   Heartbeat(gen=2)               │                                  │
//   │      │ ─────────────────────────────►   │                                  │
//   │      │                                  │                                  │
//   │      │   HeartbeatResponse {            │                                  │
//   │      │     rebalanceRequired: false,    │                                  │
//   │      │     partitionsAssigned: [P0,P1], │  ← New final assignment          │
//   │      │     generation: 3,               │                                  │
//   │      │     state: COMPLETE,             │                                  │
//   │      │   }                              │                                  │
//   │      │ ◄─────────────────────────────   │                                  │
//   │      │                                  │                                  │
//   │      │   [Consumer starts P0, P1]       │                                  │
//   │      │                                  │                                  │
//   └────────────────────────────────────────────────────────────────────────────┘
//
// TIMEOUT HANDLING:
//
//   If a consumer doesn't acknowledge revocation within the deadline:
//   1. Mark partition as force-revoked
//   2. Proceed to assignment phase
//   3. Mark consumer as suspect (may be dead)
//   4. Record timeout metric
//
// =============================================================================

package broker

import (
	"log/slog"
	"sync"
	"time"
)

// =============================================================================
// COOPERATIVE REBALANCER CONFIGURATION
// =============================================================================

// CooperativeRebalancerConfig holds configuration for the rebalancer.
type CooperativeRebalancerConfig struct {
	// Protocol is the rebalancing protocol to use.
	// Default: RebalanceProtocolCooperative
	Protocol RebalanceProtocol

	// Strategy is the partition assignment strategy.
	// Default: AssignmentStrategySticky
	Strategy AssignmentStrategy

	// RevocationTimeoutMs is how long to wait for revocation acknowledgments.
	// After this, partitions are force-revoked and rebalance continues.
	// Default: 60000 (60 seconds)
	RevocationTimeoutMs int

	// RebalanceDelayMs is how long to wait before starting a rebalance
	// after a trigger event. This allows batching multiple join/leave events.
	// Default: 500 (500ms)
	RebalanceDelayMs int

	// CheckIntervalMs is how often to check for rebalance progress.
	// Default: 500 (500ms)
	CheckIntervalMs int
}

// DefaultCooperativeRebalancerConfig returns sensible defaults.
func DefaultCooperativeRebalancerConfig() CooperativeRebalancerConfig {
	return CooperativeRebalancerConfig{
		Protocol:            RebalanceProtocolCooperative,
		Strategy:            AssignmentStrategySticky,
		RevocationTimeoutMs: 60000,
		RebalanceDelayMs:    500,
		CheckIntervalMs:     500,
	}
}

// =============================================================================
// COOPERATIVE REBALANCER
// =============================================================================

// CooperativeRebalancer orchestrates incremental rebalancing for consumer groups.
type CooperativeRebalancer struct {
	config   CooperativeRebalancerConfig
	assignor PartitionAssignor
	metrics  *RebalanceMetrics
	logger   *slog.Logger

	// rebalanceContexts maps group ID to active rebalance context
	rebalanceContexts map[string]*RebalanceContext

	// pendingRebalances tracks groups that need rebalancing
	pendingRebalances map[string]time.Time // groupID → scheduled time

	mu sync.RWMutex

	// stopCh signals the background goroutine to stop
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewCooperativeRebalancer creates a new cooperative rebalancer.
func NewCooperativeRebalancer(config CooperativeRebalancerConfig, logger *slog.Logger) *CooperativeRebalancer {
	cr := &CooperativeRebalancer{
		config:            config,
		assignor:          GetAssignor(config.Strategy),
		metrics:           NewRebalanceMetrics(),
		logger:            logger,
		rebalanceContexts: make(map[string]*RebalanceContext),
		pendingRebalances: make(map[string]time.Time),
		stopCh:            make(chan struct{}),
	}

	return cr
}

// Start begins the background rebalance checker.
func (cr *CooperativeRebalancer) Start() {
	cr.wg.Add(1)
	go cr.backgroundLoop()
}

// Stop gracefully shuts down the rebalancer.
func (cr *CooperativeRebalancer) Stop() {
	close(cr.stopCh)
	cr.wg.Wait()
}

// backgroundLoop periodically checks rebalance progress.
func (cr *CooperativeRebalancer) backgroundLoop() {
	defer cr.wg.Done()

	ticker := time.NewTicker(time.Duration(cr.config.CheckIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-cr.stopCh:
			return
		case <-ticker.C:
			cr.checkRebalanceProgress()
		}
	}
}

// checkRebalanceProgress checks for pending rebalances and timeout handling.
func (cr *CooperativeRebalancer) checkRebalanceProgress() {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	now := time.Now()

	// Check for expired revocation timeouts
	for groupID, ctx := range cr.rebalanceContexts {
		if ctx.State == RebalanceStatePendingRevoke {
			expired := ctx.GetExpiredRevocations()
			for _, memberID := range expired {
				// Force complete the revocation
				partitions := ctx.ForceCompleteRevocation(memberID)
				cr.logger.Warn("revocation timeout, force-completing",
					"group", groupID,
					"member", memberID,
					"partitions", partitions,
				)
				cr.metrics.RecordRevocationTimeout()
			}

			// Check if we can now move to assignment phase
			if ctx.AllRevocationsComplete() {
				ctx.State = RebalanceStatePendingAssign
				cr.logger.Info("all revocations complete, moving to assignment phase",
					"group", groupID,
				)
			}
		}
	}

	// Process pending rebalances that are ready
	for groupID, scheduledTime := range cr.pendingRebalances {
		if now.After(scheduledTime) {
			delete(cr.pendingRebalances, groupID)
			// Note: actual rebalance initiation happens in TriggerRebalance
			// This is just for the delay mechanism
		}
	}
}

// =============================================================================
// REBALANCE LIFECYCLE
// =============================================================================

// TriggerRebalance initiates a rebalance for a group.
// Returns immediately - actual rebalance happens via heartbeat responses.
func (cr *CooperativeRebalancer) TriggerRebalance(
	groupID string,
	reason string,
	currentGeneration int,
	members []string,
	topicPartitions map[string]int,
	currentAssignment map[string][]TopicPartition,
) (*RebalanceContext, error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	// If there's already an active rebalance, check if we should restart it
	if existingCtx, exists := cr.rebalanceContexts[groupID]; exists {
		if existingCtx.State != RebalanceStateComplete {
			// Active rebalance in progress - restart with new trigger
			cr.logger.Info("restarting active rebalance",
				"group", groupID,
				"reason", reason,
				"previous_state", existingCtx.State.String(),
			)
		}
	}

	// Calculate new assignment
	newAssignment := cr.assignor.Assign(members, topicPartitions, currentAssignment)

	// Calculate what needs to change
	diff := CalculateAssignmentDiff(currentAssignment, newAssignment)

	cr.logger.Info("calculated rebalance diff",
		"group", groupID,
		"reason", reason,
		"partitions_moved", diff.TotalPartitionsMoved,
		"members", len(members),
	)

	// Create rebalance context
	ctx := NewRebalanceContext(currentGeneration, reason)
	ctx.TargetAssignment = make(map[string]*MemberAssignment, len(newAssignment))
	ctx.PreviousAssignment = make(map[string]*MemberAssignment, len(currentAssignment))

	// Store previous assignment
	for memberID, partitions := range currentAssignment {
		ctx.PreviousAssignment[memberID] = &MemberAssignment{
			MemberID:   memberID,
			Partitions: partitions,
			Generation: currentGeneration,
		}
	}

	// Store target assignment
	for memberID, partitions := range newAssignment {
		ctx.TargetAssignment[memberID] = &MemberAssignment{
			MemberID:   memberID,
			Partitions: partitions,
			Generation: ctx.AssignGeneration,
		}
	}

	// If using cooperative protocol, set up revocations
	if cr.config.Protocol == RebalanceProtocolCooperative {
		deadline := time.Now().Add(time.Duration(cr.config.RevocationTimeoutMs) * time.Millisecond)

		// Add pending revocations for members losing partitions
		for memberID, partitions := range diff.Revocations {
			if len(partitions) > 0 {
				ctx.AddPendingRevocation(memberID, partitions, deadline)
				cr.logger.Debug("scheduling revocation",
					"group", groupID,
					"member", memberID,
					"partitions", partitions,
				)
			}
		}

		// If no revocations needed, go straight to assignment
		if ctx.AllRevocationsComplete() {
			ctx.State = RebalanceStatePendingAssign
		}
	} else {
		// Eager protocol - skip revocation phase
		ctx.State = RebalanceStatePendingAssign
	}

	cr.rebalanceContexts[groupID] = ctx
	cr.metrics.RecordRebalanceStart(reason)

	return ctx, nil
}

// AcknowledgeRevocation records a consumer's revocation acknowledgment.
func (cr *CooperativeRebalancer) AcknowledgeRevocation(
	groupID string,
	memberID string,
	generation int,
	revokedPartitions []TopicPartition,
) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	ctx, exists := cr.rebalanceContexts[groupID]
	if !exists {
		return ErrRebalanceInProgress
	}

	// Validate generation
	if generation != ctx.StartGeneration && generation != ctx.AssignGeneration {
		return ErrStaleGeneration
	}

	// Record the completion
	ctx.CompleteRevocation(memberID, revokedPartitions)

	cr.logger.Info("revocation acknowledged",
		"group", groupID,
		"member", memberID,
		"partitions", revokedPartitions,
		"pending_count", ctx.GetPendingRevocationCount(),
	)

	// Check if we can move to assignment phase
	if ctx.AllRevocationsComplete() && ctx.State == RebalanceStatePendingRevoke {
		ctx.State = RebalanceStatePendingAssign
		cr.logger.Info("all revocations complete, moving to assignment phase",
			"group", groupID,
		)
	}

	return nil
}

// CompleteRebalance marks a rebalance as complete.
func (cr *CooperativeRebalancer) CompleteRebalance(groupID string) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	ctx, exists := cr.rebalanceContexts[groupID]
	if !exists {
		return
	}

	ctx.State = RebalanceStateComplete
	ctx.CompletedAt = time.Now()

	// Calculate metrics
	durationMs := ctx.CompletedAt.Sub(ctx.StartedAt).Milliseconds()
	partitionsRevoked := 0
	partitionsAssigned := 0

	for _, partitions := range ctx.CompletedRevocations {
		partitionsRevoked += len(partitions)
	}

	for _, assignment := range ctx.TargetAssignment {
		partitionsAssigned += len(assignment.Partitions)
	}

	cr.metrics.RecordRebalanceComplete(durationMs, partitionsRevoked, partitionsAssigned, true)

	cr.logger.Info("rebalance complete",
		"group", groupID,
		"duration_ms", durationMs,
		"partitions_revoked", partitionsRevoked,
		"partitions_assigned", partitionsAssigned,
	)

	// Clean up context (keep for a bit for late heartbeats)
	// In production, we'd clean up after a delay
	delete(cr.rebalanceContexts, groupID)
}

// =============================================================================
// HEARTBEAT RESPONSE GENERATION
// =============================================================================

// GetHeartbeatResponse generates the appropriate heartbeat response for a consumer.
// This is how we communicate rebalance state to consumers.
func (cr *CooperativeRebalancer) GetHeartbeatResponse(
	groupID string,
	memberID string,
	currentGeneration int,
) *HeartbeatResponse {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	ctx, exists := cr.rebalanceContexts[groupID]
	if !exists {
		// No active rebalance
		return &HeartbeatResponse{
			RebalanceRequired: false,
			Generation:        currentGeneration,
			Protocol:          cr.config.Protocol,
			State:             RebalanceStateNone,
		}
	}

	response := &HeartbeatResponse{
		Protocol: cr.config.Protocol,
	}

	switch ctx.State {
	case RebalanceStatePendingRevoke:
		// Check if this member needs to revoke
		pending, hasPending := ctx.PendingRevocations[memberID]
		if hasPending {
			// This member needs to revoke partitions
			response.RebalanceRequired = true
			response.Generation = ctx.StartGeneration
			response.State = RebalanceStatePendingRevoke
			response.RevocationDeadline = pending.Deadline

			// Convert map to slice
			partitions := make([]TopicPartition, 0, len(pending.Partitions))
			for tp := range pending.Partitions {
				partitions = append(partitions, tp)
			}
			response.PartitionsToRevoke = partitions
		} else {
			// Member doesn't need to revoke, but rebalance is happening
			response.RebalanceRequired = true
			response.Generation = ctx.StartGeneration
			response.State = RebalanceStatePendingRevoke
			// They keep their current partitions for now
		}

	case RebalanceStatePendingAssign:
		// Revocations complete, send new assignment
		response.RebalanceRequired = false
		response.Generation = ctx.AssignGeneration
		response.State = RebalanceStatePendingAssign

		if target, exists := ctx.TargetAssignment[memberID]; exists {
			response.PartitionsAssigned = target.Partitions
		}

	case RebalanceStateComplete:
		// Rebalance finished
		response.RebalanceRequired = false
		response.Generation = ctx.AssignGeneration
		response.State = RebalanceStateComplete

		if target, exists := ctx.TargetAssignment[memberID]; exists {
			response.PartitionsAssigned = target.Partitions
		}
	}

	return response
}

// =============================================================================
// QUERY METHODS
// =============================================================================

// GetRebalanceContext returns the current rebalance context for a group.
func (cr *CooperativeRebalancer) GetRebalanceContext(groupID string) *RebalanceContext {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.rebalanceContexts[groupID]
}

// IsRebalancing returns true if a group is currently rebalancing.
func (cr *CooperativeRebalancer) IsRebalancing(groupID string) bool {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	ctx, exists := cr.rebalanceContexts[groupID]
	return exists && ctx.State != RebalanceStateComplete
}

// GetMetrics returns a snapshot of rebalance metrics.
func (cr *CooperativeRebalancer) GetMetrics() RebalanceMetrics {
	return cr.metrics.GetSnapshot()
}

// GetTargetAssignment returns the target assignment for a member during rebalance.
func (cr *CooperativeRebalancer) GetTargetAssignment(groupID, memberID string) ([]TopicPartition, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	ctx, exists := cr.rebalanceContexts[groupID]
	if !exists {
		return nil, false
	}

	target, exists := ctx.TargetAssignment[memberID]
	if !exists {
		return nil, false
	}

	return target.Partitions, true
}

// GetPendingRevocations returns partitions that member needs to revoke.
func (cr *CooperativeRebalancer) GetPendingRevocations(groupID, memberID string) ([]TopicPartition, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	ctx, exists := cr.rebalanceContexts[groupID]
	if !exists {
		return nil, false
	}

	pending, exists := ctx.PendingRevocations[memberID]
	if !exists {
		return nil, false
	}

	partitions := make([]TopicPartition, 0, len(pending.Partitions))
	for tp := range pending.Partitions {
		partitions = append(partitions, tp)
	}
	return partitions, true
}
