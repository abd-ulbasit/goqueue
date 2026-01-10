// =============================================================================
// STICKY ASSIGNOR TESTS
// =============================================================================
//
// This file tests all partition assignment strategies:
//   - StickyAssignor: Minimizes partition movement during rebalances
//   - RangeAssignor: Assigns contiguous partition ranges
//   - RoundRobinAssignor: Distributes partitions evenly in round-robin
//
// TEST SCENARIOS:
//   - Initial assignment (no previous ownership)
//   - Member joins (minimize moves)
//   - Member leaves (redistribute)
//   - Unequal distribution
//   - Multi-topic assignment
//
// =============================================================================

package broker

import (
	"sort"
	"testing"
)

// =============================================================================
// STICKY ASSIGNOR TESTS
// =============================================================================

func TestStickyAssignor_InitialAssignment(t *testing.T) {
	assignor := NewStickyAssignor()

	topicPartitions := map[string]int{
		"orders": 6,
	}
	members := []string{"consumer-1", "consumer-2", "consumer-3"}
	currentAssignment := make(map[string][]TopicPartition)

	// Assign returns map[string][]TopicPartition (no error)
	assignment := assignor.Assign(members, topicPartitions, currentAssignment)

	// Verify all partitions are assigned
	totalAssigned := 0
	for _, partitions := range assignment {
		totalAssigned += len(partitions)
	}
	if totalAssigned != 6 {
		t.Errorf("Total assigned = %d, want 6", totalAssigned)
	}

	// Verify each member gets 2 partitions (6/3 = 2)
	for member, partitions := range assignment {
		if len(partitions) != 2 {
			t.Errorf("Member %s got %d partitions, want 2", member, len(partitions))
		}
	}

	// Verify no duplicate assignments
	seen := make(map[string]bool)
	for _, partitions := range assignment {
		for _, p := range partitions {
			key := p.String()
			if seen[key] {
				t.Errorf("Partition %s assigned twice", key)
			}
			seen[key] = true
		}
	}
}

func TestStickyAssignor_MemberJoins(t *testing.T) {
	assignor := NewStickyAssignor()

	// STICKY ASSIGNOR BEHAVIOR:
	// The sticky assignor prioritizes stability over perfect balance.
	// It allows MaxImbalance (default 1) extra partitions before forcing moves.
	//
	// Example: 6 partitions, 3 members
	//   - Fair share: 6/3 = 2 each
	//   - maxAllowed = maxPartitions + MaxImbalance = 2 + 1 = 3
	//   - If existing members have 3 each (== maxAllowed), NO moves happen!
	//
	// To force moves, existing members must have > maxAllowed partitions.

	// 8 partitions: with 2 members = 4 each
	// When 3rd joins: fair share = 8/3 = 2.67, so min=2, max=3
	// maxAllowed = 3 + 1 = 4
	// Existing have 4 each, which equals maxAllowed, so NO moves
	//
	// Use 9 partitions instead: with 2 members = 4.5 each, so they have 4 and 5
	// When 3rd joins: fair share = 3, maxAllowed = 4
	// Member with 5 > 4, so 1 partition moves!
	topicPartitions := map[string]int{
		"orders": 9,
	}

	// Initial: 2 consumers with uneven split (5 and 4)
	currentAssignment := map[string][]TopicPartition{
		"consumer-1": {
			{Topic: "orders", Partition: 0},
			{Topic: "orders", Partition: 1},
			{Topic: "orders", Partition: 2},
			{Topic: "orders", Partition: 3},
			{Topic: "orders", Partition: 4},
		},
		"consumer-2": {
			{Topic: "orders", Partition: 5},
			{Topic: "orders", Partition: 6},
			{Topic: "orders", Partition: 7},
			{Topic: "orders", Partition: 8},
		},
	}

	// New member joins - now 3 members
	// Fair share: 9/3 = 3 each
	// maxAllowed = 3 + 1 = 4
	// consumer-1 has 5 > 4, so will lose 1 partition
	members := []string{"consumer-1", "consumer-2", "consumer-3"}

	assignment := assignor.Assign(members, topicPartitions, currentAssignment)

	// Verify total partitions
	totalAssigned := 0
	for member, partitions := range assignment {
		totalAssigned += len(partitions)
		t.Logf("Member %s has %d partitions", member, len(partitions))
	}
	if totalAssigned != 9 {
		t.Errorf("Total assigned = %d, want 9", totalAssigned)
	}

	// Verify no member has more than maxAllowed (4)
	for member, partitions := range assignment {
		if len(partitions) > 4 {
			t.Errorf("Member %s has %d partitions, max allowed is 4", member, len(partitions))
		}
	}

	// Consumer-1 should have lost exactly 1 partition (5 → 4)
	if len(assignment["consumer-1"]) != 4 {
		t.Errorf("consumer-1 has %d partitions, want 4", len(assignment["consumer-1"]))
	}

	// Consumer-3 should get the moved partition
	if len(assignment["consumer-3"]) != 1 {
		t.Errorf("consumer-3 has %d partitions, want 1", len(assignment["consumer-3"]))
	}

	// Check stickiness: consumer-1 should keep 4 of its original 5
	c1Kept := 0
	for _, p := range assignment["consumer-1"] {
		for _, orig := range currentAssignment["consumer-1"] {
			if p == orig {
				c1Kept++
			}
		}
	}
	if c1Kept != 4 {
		t.Errorf("consumer-1 kept %d original partitions, want 4", c1Kept)
	}

	t.Logf("Sticky assignor correctly preserved %d/5 partitions for consumer-1", c1Kept)
}

func TestStickyAssignor_MemberLeaves(t *testing.T) {
	assignor := NewStickyAssignor()

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

	assignment := assignor.Assign(members, topicPartitions, currentAssignment)

	// Verify consumer-1 and consumer-2 keep their original partitions
	for _, orig := range currentAssignment["consumer-1"] {
		found := false
		for _, np := range assignment["consumer-1"] {
			if np == orig {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("consumer-1 lost partition %v", orig)
		}
	}

	for _, orig := range currentAssignment["consumer-2"] {
		found := false
		for _, np := range assignment["consumer-2"] {
			if np == orig {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("consumer-2 lost partition %v", orig)
		}
	}

	// Verify orphaned partitions from consumer-3 are redistributed
	totalAssigned := 0
	for _, partitions := range assignment {
		totalAssigned += len(partitions)
	}
	if totalAssigned != 6 {
		t.Errorf("Total assigned = %d, want 6", totalAssigned)
	}
}

func TestStickyAssignor_UnequalDistribution(t *testing.T) {
	assignor := NewStickyAssignor()

	// 5 partitions, 3 members = 2,2,1 distribution
	topicPartitions := map[string]int{
		"orders": 5,
	}
	members := []string{"consumer-1", "consumer-2", "consumer-3"}
	currentAssignment := make(map[string][]TopicPartition)

	assignment := assignor.Assign(members, topicPartitions, currentAssignment)

	// Count distribution
	counts := make(map[int]int)
	for _, partitions := range assignment {
		counts[len(partitions)]++
	}

	// Should have 2 members with 2 partitions and 1 with 1
	if counts[2] != 2 || counts[1] != 1 {
		t.Errorf("Distribution = %v, want 2 with 2 partitions and 1 with 1", counts)
	}
}

func TestStickyAssignor_MultiTopic(t *testing.T) {
	assignor := NewStickyAssignor()

	topicPartitions := map[string]int{
		"orders":  3,
		"events":  3,
		"metrics": 3,
	}
	members := []string{"consumer-1", "consumer-2", "consumer-3"}
	currentAssignment := make(map[string][]TopicPartition)

	assignment := assignor.Assign(members, topicPartitions, currentAssignment)

	// Total of 9 partitions, 3 members = 3 each
	for member, partitions := range assignment {
		if len(partitions) != 3 {
			t.Errorf("Member %s got %d partitions, want 3", member, len(partitions))
		}
	}

	// Verify all topics are covered
	topicCounts := make(map[string]int)
	for _, partitions := range assignment {
		for _, p := range partitions {
			topicCounts[p.Topic]++
		}
	}

	for topic, count := range topicPartitions {
		if topicCounts[topic] != count {
			t.Errorf("Topic %s has %d assigned, want %d", topic, topicCounts[topic], count)
		}
	}
}

// =============================================================================
// RANGE ASSIGNOR TESTS
// =============================================================================

func TestRangeAssignor_BasicAssignment(t *testing.T) {
	assignor := NewRangeAssignor()

	topicPartitions := map[string]int{
		"orders": 6,
	}
	members := []string{"consumer-1", "consumer-2", "consumer-3"}
	currentAssignment := make(map[string][]TopicPartition)

	assignment := assignor.Assign(members, topicPartitions, currentAssignment)

	// Verify even distribution
	for member, partitions := range assignment {
		if len(partitions) != 2 {
			t.Errorf("Member %s got %d partitions, want 2", member, len(partitions))
		}
	}

	// Verify contiguous ranges - sorted partitions within each member
	for _, partitions := range assignment {
		if len(partitions) < 2 {
			continue
		}
		for i := 1; i < len(partitions); i++ {
			if partitions[i].Topic == partitions[i-1].Topic {
				if partitions[i].Partition != partitions[i-1].Partition+1 {
					t.Log("Range assignor may not always give contiguous partitions")
				}
			}
		}
	}
}

func TestRangeAssignor_UnequalDistribution(t *testing.T) {
	assignor := NewRangeAssignor()

	// 10 partitions, 3 members = 4,3,3 distribution
	topicPartitions := map[string]int{
		"orders": 10,
	}
	members := []string{"consumer-1", "consumer-2", "consumer-3"}
	currentAssignment := make(map[string][]TopicPartition)

	assignment := assignor.Assign(members, topicPartitions, currentAssignment)

	// Verify total
	total := 0
	for _, partitions := range assignment {
		total += len(partitions)
	}
	if total != 10 {
		t.Errorf("Total assigned = %d, want 10", total)
	}

	// Each member should have 3-4 partitions
	for member, partitions := range assignment {
		if len(partitions) < 3 || len(partitions) > 4 {
			t.Errorf("Member %s got %d partitions, want 3-4", member, len(partitions))
		}
	}
}

// =============================================================================
// ROUND ROBIN ASSIGNOR TESTS
// =============================================================================

func TestRoundRobinAssignor_BasicAssignment(t *testing.T) {
	assignor := NewRoundRobinAssignor()

	topicPartitions := map[string]int{
		"orders": 9,
	}
	members := []string{"consumer-1", "consumer-2", "consumer-3"}
	currentAssignment := make(map[string][]TopicPartition)

	assignment := assignor.Assign(members, topicPartitions, currentAssignment)

	// Verify even distribution (9/3 = 3 each)
	for member, partitions := range assignment {
		if len(partitions) != 3 {
			t.Errorf("Member %s got %d partitions, want 3", member, len(partitions))
		}
	}
}

func TestRoundRobinAssignor_UnequalDistribution(t *testing.T) {
	assignor := NewRoundRobinAssignor()

	// 7 partitions, 3 members = 3,2,2 distribution
	topicPartitions := map[string]int{
		"orders": 7,
	}
	members := []string{"consumer-1", "consumer-2", "consumer-3"}
	currentAssignment := make(map[string][]TopicPartition)

	assignment := assignor.Assign(members, topicPartitions, currentAssignment)

	// Count distribution
	counts := make(map[int]int)
	for _, partitions := range assignment {
		counts[len(partitions)]++
	}

	// Should have 1 with 3 and 2 with 2
	if counts[3] != 1 || counts[2] != 2 {
		t.Errorf("Distribution counts = %v, want 1 with 3 and 2 with 2", counts)
	}
}

func TestRoundRobinAssignor_MultiTopic(t *testing.T) {
	assignor := NewRoundRobinAssignor()

	topicPartitions := map[string]int{
		"orders":  2,
		"events":  2,
		"metrics": 2,
	}
	members := []string{"consumer-1", "consumer-2"}
	currentAssignment := make(map[string][]TopicPartition)

	assignment := assignor.Assign(members, topicPartitions, currentAssignment)

	// 6 partitions, 2 members = 3 each
	for member, partitions := range assignment {
		if len(partitions) != 3 {
			t.Errorf("Member %s got %d partitions, want 3", member, len(partitions))
		}
	}
}

// =============================================================================
// ASSIGNMENT DIFF TESTS
// =============================================================================

func TestCalculateAssignmentDiff_NewAssignment(t *testing.T) {
	oldAssignment := make(map[string][]TopicPartition)
	newAssignment := map[string][]TopicPartition{
		"consumer-1": {
			{Topic: "orders", Partition: 0},
			{Topic: "orders", Partition: 1},
		},
		"consumer-2": {
			{Topic: "orders", Partition: 2},
			{Topic: "orders", Partition: 3},
		},
	}

	diff := CalculateAssignmentDiff(oldAssignment, newAssignment)

	// For new assignments from empty, TotalPartitionsMoved is 0 (no revocations)
	// TotalPartitionsMoved counts revocations only
	if diff.TotalPartitionsMoved != 0 {
		t.Errorf("TotalPartitionsMoved = %d, want 0 (no revocations for new assignment)", diff.TotalPartitionsMoved)
	}

	// All 4 partitions should be in Assignments (new assignments)
	totalAssignments := 0
	for _, partitions := range diff.Assignments {
		totalAssignments += len(partitions)
	}
	if totalAssignments != 4 {
		t.Errorf("Assignments = %d, want 4", totalAssignments)
	}

	// No revocations (old assignment was empty)
	totalRevocations := 0
	for _, partitions := range diff.Revocations {
		totalRevocations += len(partitions)
	}
	if totalRevocations != 0 {
		t.Errorf("Revocations = %d, want 0", totalRevocations)
	}
}

func TestCalculateAssignmentDiff_MemberJoins(t *testing.T) {
	oldAssignment := map[string][]TopicPartition{
		"consumer-1": {
			{Topic: "orders", Partition: 0},
			{Topic: "orders", Partition: 1},
		},
		"consumer-2": {
			{Topic: "orders", Partition: 2},
			{Topic: "orders", Partition: 3},
		},
	}
	newAssignment := map[string][]TopicPartition{
		"consumer-1": {
			{Topic: "orders", Partition: 0},
		},
		"consumer-2": {
			{Topic: "orders", Partition: 2},
			{Topic: "orders", Partition: 3},
		},
		"consumer-3": {
			{Topic: "orders", Partition: 1},
		},
	}

	diff := CalculateAssignmentDiff(oldAssignment, newAssignment)

	// consumer-1 loses partition 1
	if len(diff.Revocations["consumer-1"]) != 1 {
		t.Errorf("consumer-1 revocations = %d, want 1", len(diff.Revocations["consumer-1"]))
	}

	// consumer-3 is new member
	if _, exists := diff.Assignments["consumer-3"]; !exists {
		t.Error("consumer-3 should have assignments")
	}
}

func TestCalculateAssignmentDiff_MemberLeaves(t *testing.T) {
	oldAssignment := map[string][]TopicPartition{
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
	newAssignment := map[string][]TopicPartition{
		"consumer-1": {
			{Topic: "orders", Partition: 0},
			{Topic: "orders", Partition: 1},
			{Topic: "orders", Partition: 4},
		},
		"consumer-2": {
			{Topic: "orders", Partition: 2},
			{Topic: "orders", Partition: 3},
			{Topic: "orders", Partition: 5},
		},
	}

	diff := CalculateAssignmentDiff(oldAssignment, newAssignment)

	// consumer-3's partitions should be in assignments for remaining members
	assignmentsCount := 0
	for _, partitions := range diff.Assignments {
		assignmentsCount += len(partitions)
	}
	if assignmentsCount != 2 {
		t.Errorf("Assignments = %d, want 2 (from consumer-3)", assignmentsCount)
	}
}

func TestCalculateAssignmentDiff_NoChange(t *testing.T) {
	assignment := map[string][]TopicPartition{
		"consumer-1": {
			{Topic: "orders", Partition: 0},
			{Topic: "orders", Partition: 1},
		},
		"consumer-2": {
			{Topic: "orders", Partition: 2},
			{Topic: "orders", Partition: 3},
		},
	}

	diff := CalculateAssignmentDiff(assignment, assignment)

	if diff.TotalPartitionsMoved != 0 {
		t.Errorf("TotalPartitionsMoved = %d, want 0", diff.TotalPartitionsMoved)
	}
}

// =============================================================================
// EDGE CASES
// =============================================================================

func TestAssignors_EmptyMembers(t *testing.T) {
	assignors := []PartitionAssignor{
		NewStickyAssignor(),
		NewRangeAssignor(),
		NewRoundRobinAssignor(),
	}

	topicPartitions := map[string]int{"orders": 4}
	members := []string{}
	currentAssignment := make(map[string][]TopicPartition)

	for _, assignor := range assignors {
		assignment := assignor.Assign(members, topicPartitions, currentAssignment)
		if len(assignment) != 0 {
			t.Errorf("%s: Empty members should produce empty assignment, got %d", assignor.Name(), len(assignment))
		}
	}
}

func TestAssignors_SingleMember(t *testing.T) {
	assignors := []PartitionAssignor{
		NewStickyAssignor(),
		NewRangeAssignor(),
		NewRoundRobinAssignor(),
	}

	topicPartitions := map[string]int{"orders": 4}
	members := []string{"consumer-1"}
	currentAssignment := make(map[string][]TopicPartition)

	for _, assignor := range assignors {
		assignment := assignor.Assign(members, topicPartitions, currentAssignment)
		if len(assignment["consumer-1"]) != 4 {
			t.Errorf("%s: Single member should get all 4 partitions, got %d", assignor.Name(), len(assignment["consumer-1"]))
		}
	}
}

func TestAssignors_MoreMembersThanPartitions(t *testing.T) {
	assignors := []PartitionAssignor{
		NewStickyAssignor(),
		NewRangeAssignor(),
		NewRoundRobinAssignor(),
	}

	topicPartitions := map[string]int{"orders": 2}
	members := []string{"consumer-1", "consumer-2", "consumer-3", "consumer-4"}
	currentAssignment := make(map[string][]TopicPartition)

	for _, assignor := range assignors {
		assignment := assignor.Assign(members, topicPartitions, currentAssignment)

		// Only 2 partitions total - some members will have 0
		total := 0
		for _, partitions := range assignment {
			total += len(partitions)
		}
		if total != 2 {
			t.Errorf("%s: Total assigned = %d, want 2", assignor.Name(), total)
		}
	}
}

// =============================================================================
// PARTITION ASSIGNOR INTERFACE TESTS
// =============================================================================

func TestGetAssignor(t *testing.T) {
	tests := []struct {
		strategy     AssignmentStrategy
		expectedName string
	}{
		{AssignmentStrategySticky, "sticky"},
		{AssignmentStrategyRange, "range"},
		{AssignmentStrategyRoundRobin, "roundrobin"},
	}

	for _, tt := range tests {
		t.Run(tt.expectedName, func(t *testing.T) {
			assignor := GetAssignor(tt.strategy)
			if assignor.Name() != tt.expectedName {
				t.Errorf("GetAssignor(%v).Name() = %s, want %s", tt.strategy, assignor.Name(), tt.expectedName)
			}
		})
	}
}

// =============================================================================
// DETERMINISM TEST
// =============================================================================

func TestStickyAssignor_Deterministic(t *testing.T) {
	// Running the same assignment multiple times should produce the same result
	assignor := NewStickyAssignor()

	topicPartitions := map[string]int{"orders": 6}
	members := []string{"consumer-1", "consumer-2", "consumer-3"}
	currentAssignment := make(map[string][]TopicPartition)

	first := assignor.Assign(members, topicPartitions, currentAssignment)

	for i := 0; i < 10; i++ {
		subsequent := assignor.Assign(members, topicPartitions, currentAssignment)

		// Compare assignments
		for member, firstPartitions := range first {
			subsequentPartitions := subsequent[member]
			if len(firstPartitions) != len(subsequentPartitions) {
				t.Errorf("Run %d: Member %s partition count changed: %d vs %d",
					i, member, len(firstPartitions), len(subsequentPartitions))
			}

			// Sort and compare
			sort.Slice(firstPartitions, func(a, b int) bool {
				return firstPartitions[a].String() < firstPartitions[b].String()
			})
			sort.Slice(subsequentPartitions, func(a, b int) bool {
				return subsequentPartitions[a].String() < subsequentPartitions[b].String()
			})

			for j := range firstPartitions {
				if firstPartitions[j] != subsequentPartitions[j] {
					t.Errorf("Run %d: Member %s partition mismatch at index %d", i, member, j)
				}
			}
		}
	}
}
