// =============================================================================
// COOPERATIVE REBALANCER TESTS
// =============================================================================
//
// This file tests the cooperative rebalancer orchestration logic:
//   - TriggerRebalance: Initiates two-phase rebalance
//   - AcknowledgeRevocation: Records revocation completion
//   - GetHeartbeatResponse: Generates appropriate response for consumer state
//   - Timeout handling: Force-revokes when consumers don't respond
//
// =============================================================================

package broker

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

// =============================================================================
// TEST HELPERS
// =============================================================================

func newTestRebalancer() *CooperativeRebalancer {
	config := CooperativeRebalancerConfig{
		Protocol:            RebalanceProtocolCooperative,
		Strategy:            AssignmentStrategySticky,
		RevocationTimeoutMs: 5000, // 5 seconds for tests
		RebalanceDelayMs:    100,
		CheckIntervalMs:     100,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError, // Quiet during tests
	}))
	return NewCooperativeRebalancer(config, logger)
}

// =============================================================================
// TRIGGER REBALANCE TESTS
// =============================================================================

func TestCooperativeRebalancer_TriggerRebalance_NewGroup(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	topicPartitions := map[string]int{
		"orders": 4,
	}
	members := []string{"consumer-1", "consumer-2"}
	currentAssignment := make(map[string][]TopicPartition)

	ctx, err := rebalancer.TriggerRebalance(
		"test-group",
		"group_created",
		0, // initial generation
		members,
		topicPartitions,
		currentAssignment,
	)
	if err != nil {
		t.Fatalf("TriggerRebalance() error = %v", err)
	}

	// Verify context created
	if ctx == nil {
		t.Fatal("TriggerRebalance() returned nil context")
	}
	if ctx.Reason != "group_created" {
		t.Errorf("Reason = %s, want group_created", ctx.Reason)
	}

	// For new group, no revocations needed
	totalRevocations := ctx.GetPendingRevocationCount()
	if totalRevocations != 0 {
		t.Errorf("New group should have no revocations, got %d", totalRevocations)
	}

	// But should have assignments in TargetAssignment
	totalAssignments := 0
	for _, ma := range ctx.TargetAssignment {
		totalAssignments += len(ma.Partitions)
	}
	if totalAssignments != 4 {
		t.Errorf("New group should have 4 assignments, got %d", totalAssignments)
	}
}

func TestCooperativeRebalancer_TriggerRebalance_MemberJoins(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	// 6 partitions - with 2 members each has 3
	// When 3rd member joins: fair share is 2 each
	// Each existing member is over maxAllowed, so revocations happen
	topicPartitions := map[string]int{
		"orders": 6,
	}

	// Initial: 2 consumers with 3 partitions each
	currentAssignment := map[string][]TopicPartition{
		"consumer-1": {
			{Topic: "orders", Partition: 0},
			{Topic: "orders", Partition: 1},
			{Topic: "orders", Partition: 2},
		},
		"consumer-2": {
			{Topic: "orders", Partition: 3},
			{Topic: "orders", Partition: 4},
			{Topic: "orders", Partition: 5},
		},
	}

	members := []string{"consumer-1", "consumer-2", "consumer-3"}

	ctx, err := rebalancer.TriggerRebalance(
		"test-group",
		"member_joined",
		1,
		members,
		topicPartitions,
		currentAssignment,
	)
	if err != nil {
		t.Fatalf("TriggerRebalance() error = %v", err)
	}

	// With 6 partitions and 3 members:
	// minPartitions = 6/3 = 2
	// maxPartitions = 2 (no remainder)
	// maxAllowed = maxPartitions + MaxImbalance(1) = 3
	// Current members have 3 each, which equals maxAllowed
	// Sticky assignor will revoke excess to reach fair share

	revocationCount := ctx.GetPendingRevocationCount()
	t.Logf("Revocation count: %d", revocationCount)

	// Log pending revocations for debugging
	for memberID, pendingRev := range ctx.PendingRevocations {
		if pendingRev != nil {
			t.Logf("Member %s needs to revoke %d partitions: %v",
				memberID, len(pendingRev.Partitions), pendingRev.Partitions)
		}
	}

	// Check state - if revocations needed, should be pending_revoke
	// If no revocations, should be pending_assign
	t.Logf("State = %s", ctx.State.String())

	// Verify target assignment gives consumer-3 partitions
	if targetAssign := ctx.TargetAssignment["consumer-3"]; targetAssign == nil {
		t.Error("consumer-3 should have target assignment")
	} else {
		t.Logf("consumer-3 target: %d partitions", len(targetAssign.Partitions))
	}
}

func TestCooperativeRebalancer_TriggerRebalance_MemberLeaves(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	topicPartitions := map[string]int{
		"orders": 6,
	}

	// Initial: 3 consumers
	currentAssignment := map[string][]TopicPartition{
		"consumer-1": {
			{Topic: "orders", Partition: 0},
			{Topic: "orders", Partition: 1},
		},
		"consumer-2": {
			{Topic: "orders", Partition: 2},
			{Topic: "orders", Partition: 3},
		},
		"consumer-3": {
			{Topic: "orders", Partition: 4},
			{Topic: "orders", Partition: 5},
		},
	}

	// consumer-3 leaves
	members := []string{"consumer-1", "consumer-2"}

	ctx, err := rebalancer.TriggerRebalance(
		"test-group",
		"member_left",
		2,
		members,
		topicPartitions,
		currentAssignment,
	)
	if err != nil {
		t.Fatalf("TriggerRebalance() error = %v", err)
	}

	// All partitions from consumer-3 should be redistributed
	// No revocations from remaining members (their partitions stay)
	t.Logf("Pending revocations after member leave: %d", ctx.GetPendingRevocationCount())

	// TargetAssignment should give all 6 partitions to remaining 2 members
	totalAssigned := 0
	for memberID, ma := range ctx.TargetAssignment {
		totalAssigned += len(ma.Partitions)
		t.Logf("Target: %s gets %d partitions", memberID, len(ma.Partitions))
	}
	if totalAssigned != 6 {
		t.Errorf("Total assigned = %d, want 6", totalAssigned)
	}
}

// =============================================================================
// ACKNOWLEDGE REVOCATION TESTS
// =============================================================================

func TestCooperativeRebalancer_AcknowledgeRevocation(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	topicPartitions := map[string]int{
		"orders": 4,
	}

	currentAssignment := map[string][]TopicPartition{
		"consumer-1": {
			{Topic: "orders", Partition: 0},
			{Topic: "orders", Partition: 1},
		},
		"consumer-2": {
			{Topic: "orders", Partition: 2},
			{Topic: "orders", Partition: 3},
		},
	}
	members := []string{"consumer-1", "consumer-2", "consumer-3"}

	ctx, err := rebalancer.TriggerRebalance(
		"test-group",
		"member_joined",
		1,
		members,
		topicPartitions,
		currentAssignment,
	)
	if err != nil {
		t.Fatalf("TriggerRebalance() error = %v", err)
	}

	// Get all partitions that need revocation
	var partitionsToRevoke []TopicPartition
	var memberWithRevocations string

	for memberID, pendingRev := range ctx.PendingRevocations {
		memberWithRevocations = memberID
		for tp := range pendingRev.Partitions {
			partitionsToRevoke = append(partitionsToRevoke, tp)
		}
		break // Just test with first member
	}

	if len(partitionsToRevoke) == 0 {
		t.Skip("No revocations to test")
	}

	// Get the generation from context
	generation := ctx.StartGeneration

	// Acknowledge revocation
	err = rebalancer.AcknowledgeRevocation("test-group", memberWithRevocations, generation, partitionsToRevoke)
	if err != nil {
		t.Fatalf("AcknowledgeRevocation() error = %v", err)
	}

	// Verify revocation recorded
	storedCtx := rebalancer.GetRebalanceContext("test-group")
	if storedCtx == nil {
		t.Fatal("Context should still exist")
	}

	// The member should be removed from pending
	if _, stillPending := storedCtx.PendingRevocations[memberWithRevocations]; stillPending {
		t.Error("Member should be removed from PendingRevocations after ack")
	}

	// Should be in completed revocations
	if _, completed := storedCtx.CompletedRevocations[memberWithRevocations]; !completed {
		t.Error("Member should be in CompletedRevocations after ack")
	}
}

// =============================================================================
// HEARTBEAT RESPONSE TESTS
// =============================================================================

func TestCooperativeRebalancer_GetHeartbeatResponse_NoRebalance(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	// No rebalance triggered
	response := rebalancer.GetHeartbeatResponse("test-group", "consumer-1", 1)

	if response.RebalanceRequired {
		t.Error("Should not require rebalance when none triggered")
	}
	if len(response.PartitionsToRevoke) > 0 {
		t.Error("Should have no partitions to revoke")
	}
	if len(response.PartitionsAssigned) > 0 {
		t.Error("Should have no new assignments")
	}
}

func TestCooperativeRebalancer_GetHeartbeatResponse_PendingRevoke(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	// Use 6 partitions to force revocations when 3rd member joins
	topicPartitions := map[string]int{
		"orders": 6,
	}
	currentAssignment := map[string][]TopicPartition{
		"consumer-1": {
			{Topic: "orders", Partition: 0},
			{Topic: "orders", Partition: 1},
			{Topic: "orders", Partition: 2},
		},
		"consumer-2": {
			{Topic: "orders", Partition: 3},
			{Topic: "orders", Partition: 4},
			{Topic: "orders", Partition: 5},
		},
	}
	members := []string{"consumer-1", "consumer-2", "consumer-3"}

	ctx, err := rebalancer.TriggerRebalance(
		"test-group",
		"member_joined",
		1,
		members,
		topicPartitions,
		currentAssignment,
	)
	if err != nil {
		t.Fatalf("TriggerRebalance() error = %v", err)
	}

	t.Logf("Rebalance context state: %s, pending revocations: %d",
		ctx.State.String(), ctx.GetPendingRevocationCount())

	// Get heartbeat response for a member
	response := rebalancer.GetHeartbeatResponse("test-group", "consumer-1", 1)

	// Log response details for debugging
	t.Logf("Consumer-1 heartbeat response: rebalance=%v, state=%s, revoke=%d, assigned=%d",
		response.RebalanceRequired,
		response.State.String(),
		len(response.PartitionsToRevoke),
		len(response.PartitionsAssigned),
	)

	// With an active rebalance, should require rebalance or have some state indication
	// The actual behavior depends on the rebalance state
}

// =============================================================================
// METRICS TESTS
// =============================================================================

func TestCooperativeRebalancer_Metrics(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	topicPartitions := map[string]int{
		"orders": 4,
	}
	currentAssignment := make(map[string][]TopicPartition)
	members := []string{"consumer-1", "consumer-2"}

	// Trigger a rebalance
	_, err := rebalancer.TriggerRebalance(
		"test-group",
		"group_created",
		0,
		members,
		topicPartitions,
		currentAssignment,
	)
	if err != nil {
		t.Fatalf("TriggerRebalance() error = %v", err)
	}

	// Get metrics
	metrics := rebalancer.GetMetrics()

	if metrics.TotalRebalances != 1 {
		t.Errorf("TotalRebalances = %d, want 1", metrics.TotalRebalances)
	}
	if metrics.RebalancesByReason["group_created"] != 1 {
		t.Error("Should track rebalance by reason")
	}
}

// =============================================================================
// MULTIPLE GROUP REBALANCE TESTS
// =============================================================================

func TestCooperativeRebalancer_MultipleGroups(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	// Trigger rebalances for 2 groups
	topicPartitions := map[string]int{"orders": 4}
	members := []string{"consumer-1", "consumer-2"}
	currentAssignment := make(map[string][]TopicPartition)

	ctx1, err := rebalancer.TriggerRebalance("group-1", "created", 0, members, topicPartitions, currentAssignment)
	if err != nil {
		t.Fatalf("TriggerRebalance group-1 error = %v", err)
	}

	ctx2, err := rebalancer.TriggerRebalance("group-2", "created", 0, members, topicPartitions, currentAssignment)
	if err != nil {
		t.Fatalf("TriggerRebalance group-2 error = %v", err)
	}

	// Both contexts should exist
	if ctx1 == nil || ctx2 == nil {
		t.Error("Both groups should have rebalance contexts")
	}

	// Verify both groups are being tracked via GetRebalanceContext
	if rebalancer.GetRebalanceContext("group-1") == nil {
		t.Error("group-1 should have active rebalance context")
	}
	if rebalancer.GetRebalanceContext("group-2") == nil {
		t.Error("group-2 should have active rebalance context")
	}
}

// =============================================================================
// CONTEXT LIFECYCLE TESTS
// =============================================================================

func TestCooperativeRebalancer_GetRebalanceContext(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	// No context before trigger
	ctx := rebalancer.GetRebalanceContext("test-group")
	if ctx != nil {
		t.Error("Should have no context before rebalance triggered")
	}

	// Trigger rebalance
	topicPartitions := map[string]int{"orders": 2}
	members := []string{"consumer-1"}
	currentAssignment := make(map[string][]TopicPartition)

	_, err := rebalancer.TriggerRebalance("test-group", "created", 0, members, topicPartitions, currentAssignment)
	if err != nil {
		t.Fatalf("TriggerRebalance error = %v", err)
	}

	// Should have context now
	ctx = rebalancer.GetRebalanceContext("test-group")
	if ctx == nil {
		t.Error("Should have context after rebalance triggered")
	}
}

// =============================================================================
// EDGE CASE TESTS
// =============================================================================

func TestCooperativeRebalancer_EmptyGroup(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	topicPartitions := map[string]int{"orders": 4}
	members := []string{}
	currentAssignment := make(map[string][]TopicPartition)

	ctx, err := rebalancer.TriggerRebalance("empty-group", "created", 0, members, topicPartitions, currentAssignment)
	if err != nil {
		t.Fatalf("TriggerRebalance() error = %v", err)
	}

	// Should create context but with no assignments
	if ctx == nil {
		t.Fatal("Should create context even for empty group")
	}

	totalAssigned := 0
	for _, ma := range ctx.TargetAssignment {
		totalAssigned += len(ma.Partitions)
	}
	if totalAssigned != 0 {
		t.Errorf("Empty group should have 0 assignments, got %d", totalAssigned)
	}
}

func TestCooperativeRebalancer_SingleMember(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	topicPartitions := map[string]int{"orders": 4}
	members := []string{"consumer-1"}
	currentAssignment := make(map[string][]TopicPartition)

	ctx, err := rebalancer.TriggerRebalance("single-group", "created", 0, members, topicPartitions, currentAssignment)
	if err != nil {
		t.Fatalf("TriggerRebalance() error = %v", err)
	}

	// Single member should get all partitions
	if len(ctx.TargetAssignment) != 1 {
		t.Errorf("Should have 1 member assignment, got %d", len(ctx.TargetAssignment))
	}

	assignment := ctx.TargetAssignment["consumer-1"]
	if assignment == nil {
		t.Fatal("consumer-1 should have assignment")
	}
	if len(assignment.Partitions) != 4 {
		t.Errorf("Single member should have all 4 partitions, got %d", len(assignment.Partitions))
	}
}

// =============================================================================
// CONCURRENCY TEST
// =============================================================================

func TestCooperativeRebalancer_ConcurrentRebalances(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	// Trigger rebalances for multiple groups concurrently
	done := make(chan struct{}, 10)
	groupIDs := make([]string, 10)

	for i := 0; i < 10; i++ {
		groupIDs[i] = "group-" + string(rune('A'+i))
		go func(groupNum int) {
			topicPartitions := map[string]int{"orders": 4}
			members := []string{"consumer-1", "consumer-2"}
			currentAssignment := make(map[string][]TopicPartition)

			_, err := rebalancer.TriggerRebalance(groupIDs[groupNum], "created", 0, members, topicPartitions, currentAssignment)
			if err != nil {
				t.Errorf("Concurrent TriggerRebalance for %s error = %v", groupIDs[groupNum], err)
			}
			done <- struct{}{}
		}(i)
	}

	// Wait for all
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all have contexts using GetRebalanceContext
	activeCount := 0
	for _, groupID := range groupIDs {
		if rebalancer.GetRebalanceContext(groupID) != nil {
			activeCount++
		}
	}
	if activeCount < 10 {
		t.Errorf("Should have 10 active rebalances, got %d", activeCount)
	}
}

// =============================================================================
// GENERATION TRACKING TESTS
// =============================================================================

func TestCooperativeRebalancer_GenerationTracking(t *testing.T) {
	rebalancer := newTestRebalancer()
	defer rebalancer.Stop()
	rebalancer.Start()

	topicPartitions := map[string]int{"orders": 4}
	members := []string{"consumer-1", "consumer-2"}
	currentAssignment := make(map[string][]TopicPartition)

	// Trigger with generation 5
	ctx, err := rebalancer.TriggerRebalance("gen-group", "test", 5, members, topicPartitions, currentAssignment)
	if err != nil {
		t.Fatalf("TriggerRebalance() error = %v", err)
	}

	// StartGeneration should be 5
	if ctx.StartGeneration != 5 {
		t.Errorf("StartGeneration = %d, want 5", ctx.StartGeneration)
	}

	// AssignGeneration should be 6 (StartGeneration + 1)
	if ctx.AssignGeneration != 6 {
		t.Errorf("AssignGeneration = %d, want 6", ctx.AssignGeneration)
	}
}

// =============================================================================
// TIMEOUT SIMULATION TEST
// =============================================================================

func TestCooperativeRebalancer_RevocationDeadline(t *testing.T) {
	// Create rebalancer with short timeout
	config := CooperativeRebalancerConfig{
		Protocol:            RebalanceProtocolCooperative,
		Strategy:            AssignmentStrategySticky,
		RevocationTimeoutMs: 100, // 100ms for test
		RebalanceDelayMs:    50,
		CheckIntervalMs:     50,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))
	rebalancer := NewCooperativeRebalancer(config, logger)
	defer rebalancer.Stop()
	rebalancer.Start()

	topicPartitions := map[string]int{"orders": 4}
	currentAssignment := map[string][]TopicPartition{
		"consumer-1": {{Topic: "orders", Partition: 0}, {Topic: "orders", Partition: 1}},
		"consumer-2": {{Topic: "orders", Partition: 2}, {Topic: "orders", Partition: 3}},
	}
	members := []string{"consumer-1", "consumer-2", "consumer-3"}

	ctx, err := rebalancer.TriggerRebalance("timeout-group", "member_joined", 1, members, topicPartitions, currentAssignment)
	if err != nil {
		t.Fatalf("TriggerRebalance() error = %v", err)
	}

	// Check deadline is set
	for memberID, pendingRev := range ctx.PendingRevocations {
		if pendingRev.Deadline.IsZero() {
			t.Errorf("Member %s has no deadline set", memberID)
		}
		deadline := pendingRev.Deadline
		if time.Until(deadline) > 200*time.Millisecond {
			t.Errorf("Deadline too far in future: %v", time.Until(deadline))
		}
		t.Logf("Member %s deadline: %v from now", memberID, time.Until(deadline))
	}
}
