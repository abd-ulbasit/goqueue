// =============================================================================
// STICKY ASSIGNOR - MINIMAL PARTITION MOVEMENT DURING REBALANCES
// =============================================================================
//
// WHAT IS STICKY ASSIGNMENT?
// The sticky assignor tries to keep partitions with their current owners as
// much as possible while maintaining balance across consumers. This minimizes
// partition movement during rebalances, which is critical for:
//
//   1. REDUCING DISRUPTION
//      - Fewer partitions move = fewer consumers interrupted
//      - Local state (caches, buffers) preserved when possible
//
//   2. FASTER REBALANCES
//      - Less data to transfer (offset commits, etc.)
//      - Consumers can resume faster
//
//   3. BETTER COOPERATIVE REBALANCING
//      - Cooperative protocol benefits most when few partitions move
//      - Sticky assignment is designed for cooperative rebalancing
//
// HOW IT WORKS:
//
//   ┌────────────────────────────────────────────────────────────────────────────┐
//   │                      STICKY ASSIGNMENT ALGORITHM                           │
//   │                                                                            │
//   │   INPUT:                                                                   │
//   │     - Current assignment (member → partitions)                             │
//   │     - List of active members                                               │
//   │     - List of all partitions (topic → partition count)                     │
//   │                                                                            │
//   │   STEP 1: IDENTIFY ORPHANED PARTITIONS                                     │
//   │     Partitions whose current owner is no longer in the group               │
//   │     These MUST be reassigned                                               │
//   │                                                                            │
//   │   STEP 2: IDENTIFY EXCESS PARTITIONS                                       │
//   │     If a consumer has more than fair share, mark excess for reassignment   │
//   │     Fair share = ceil(total_partitions / num_consumers)                    │
//   │                                                                            │
//   │   STEP 3: ASSIGN ORPHANED/EXCESS TO NEEDY CONSUMERS                        │
//   │     Give partitions to consumers below their fair share                    │
//   │     Prefer consumers with 0 partitions (new joiners)                       │
//   │                                                                            │
//   │   OUTPUT:                                                                  │
//   │     - New assignment (member → partitions)                                 │
//   │     - Revocations (member → partitions to give up)                         │
//   │     - Assignments (member → new partitions received)                       │
//   │                                                                            │
//   └────────────────────────────────────────────────────────────────────────────┘
//
// BALANCE GOAL:
//
//   The sticky assignor aims for "balanced enough" rather than "perfectly even":
//
//   Perfect balance (may require many moves):
//     10 partitions, 3 consumers → [4, 3, 3] (exactly one consumer has extra)
//
//   Sticky balance (preserves stickiness):
//     10 partitions, 3 consumers → [4, 4, 2] might be ok if it means fewer moves
//
//   The threshold is configurable - we allow a consumer to have up to
//   MaxImbalance more than the minimum before forcing redistribution.
//
// COMPARISON WITH OTHER STRATEGIES:
//
//   ┌────────────────────────────────────────────────────────────────────────────┐
//   │   Strategy        Movement    Balance     Ordering     Complexity          │
//   │   ───────────────────────────────────────────────────────────────────────  │
//   │   Range           High        Uneven      Contiguous   O(n)                │
//   │   RoundRobin      High        Even        Scattered    O(n)                │
//   │   Sticky          Low         Even-ish    Preserved    O(n*m)              │
//   │   CooperativeSticky  Minimal  Even-ish    Preserved    O(n*m)              │
//   └────────────────────────────────────────────────────────────────────────────┘
//
// KAFKA's STICKY ASSIGNOR (KIP-54, KIP-429):
//
//   Kafka's implementation has two versions:
//   1. StickyAssignor (KIP-54): Sticky but still eager protocol
//   2. CooperativeStickyAssignor (KIP-429): Sticky + cooperative protocol
//
//   We implement the cooperative variant as default.
//
// =============================================================================

package broker

import (
	"sort"
)

// =============================================================================
// PARTITION ASSIGNOR INTERFACE
// =============================================================================

// PartitionAssignor computes partition assignments for consumer groups.
type PartitionAssignor interface {
	// Name returns the assignor name (for logging/metrics)
	Name() string

	// Assign computes the partition assignment.
	//
	// Parameters:
	//   - members: list of member IDs in the group
	//   - topicPartitions: map of topic → partition count
	//   - currentAssignment: current assignment (nil for fresh assignment)
	//
	// Returns:
	//   - New assignment: member → list of TopicPartitions
	Assign(
		members []string,
		topicPartitions map[string]int,
		currentAssignment map[string][]TopicPartition,
	) map[string][]TopicPartition
}

// =============================================================================
// STICKY ASSIGNOR
// =============================================================================

// StickyAssignor implements the sticky assignment strategy.
type StickyAssignor struct {
	// MaxImbalance is the maximum allowed imbalance before forcing moves.
	// A consumer can have up to MinPartitions + MaxImbalance partitions
	// before we start taking partitions away.
	// Default: 1 (one extra partition is ok)
	MaxImbalance int
}

// NewStickyAssignor creates a new sticky assignor with default settings.
func NewStickyAssignor() *StickyAssignor {
	return &StickyAssignor{
		MaxImbalance: 1,
	}
}

// Name returns the assignor name.
func (s *StickyAssignor) Name() string {
	return "sticky"
}

// Assign computes a sticky partition assignment.
func (s *StickyAssignor) Assign(
	members []string,
	topicPartitions map[string]int,
	currentAssignment map[string][]TopicPartition,
) map[string][]TopicPartition {
	// Sort members for deterministic results
	sort.Strings(members)

	// Build complete list of all partitions
	allPartitions := s.getAllPartitions(topicPartitions)
	totalPartitions := len(allPartitions)
	totalMembers := len(members)

	// Handle edge cases
	if totalMembers == 0 {
		return make(map[string][]TopicPartition)
	}

	// Calculate fair share bounds
	// minPartitions: minimum any consumer should have
	// maxPartitions: maximum any consumer should have
	minPartitions := totalPartitions / totalMembers
	extraPartitions := totalPartitions % totalMembers
	maxPartitions := minPartitions
	if extraPartitions > 0 {
		maxPartitions++
	}

	// Create member set for quick lookup
	memberSet := make(map[string]struct{}, totalMembers)
	for _, m := range members {
		memberSet[m] = struct{}{}
	}

	// Initialize new assignment - preserve valid current assignments
	newAssignment := make(map[string][]TopicPartition, totalMembers)
	assignedPartitions := make(map[TopicPartition]string) // tp → memberID

	// Step 1: Preserve current assignments for active members
	for memberID, partitions := range currentAssignment {
		// Skip members no longer in group
		if _, exists := memberSet[memberID]; !exists {
			continue
		}

		// Copy over partitions this member still has
		for _, tp := range partitions {
			// Verify partition still exists in topic
			if !s.partitionExists(tp, topicPartitions) {
				continue
			}
			newAssignment[memberID] = append(newAssignment[memberID], tp)
			assignedPartitions[tp] = memberID
		}
	}

	// Ensure all members have an entry (even if empty)
	for _, memberID := range members {
		if _, exists := newAssignment[memberID]; !exists {
			newAssignment[memberID] = []TopicPartition{}
		}
	}

	// Step 2: Identify orphaned partitions (not assigned to anyone active)
	var orphanedPartitions []TopicPartition
	for _, tp := range allPartitions {
		if _, assigned := assignedPartitions[tp]; !assigned {
			orphanedPartitions = append(orphanedPartitions, tp)
		}
	}

	// Step 3: Identify excess partitions from over-assigned members
	// A member is over-assigned if they have more than maxPartitions+MaxImbalance
	maxAllowed := maxPartitions + s.MaxImbalance
	var excessPartitions []TopicPartition

	for memberID, partitions := range newAssignment {
		if len(partitions) > maxAllowed {
			// Take excess partitions away
			excess := len(partitions) - maxAllowed
			excessPartitions = append(excessPartitions, partitions[len(partitions)-excess:]...)
			newAssignment[memberID] = partitions[:len(partitions)-excess]

			// Update tracking
			for _, tp := range partitions[len(partitions)-excess:] {
				delete(assignedPartitions, tp)
			}
		}
	}

	// Step 4: Combine orphaned and excess into unassigned pool
	unassigned := make([]TopicPartition, 0, len(orphanedPartitions)+len(excessPartitions))
	unassigned = append(unassigned, orphanedPartitions...)
	unassigned = append(unassigned, excessPartitions...)
	sort.Sort(TopicPartitionList(unassigned))

	// Step 5: Assign unassigned partitions to needy members
	// Needy = below minPartitions, then below maxPartitions
	s.assignUnassignedPartitions(newAssignment, unassigned, minPartitions, maxPartitions, members)

	// Step 6: Sort each member's partitions for determinism
	for memberID := range newAssignment {
		sort.Sort(TopicPartitionList(newAssignment[memberID]))
	}

	return newAssignment
}

// getAllPartitions builds a sorted list of all partitions.
func (s *StickyAssignor) getAllPartitions(topicPartitions map[string]int) []TopicPartition {
	var all []TopicPartition

	// Get sorted topic names for determinism
	topics := make([]string, 0, len(topicPartitions))
	for topic := range topicPartitions {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	for _, topic := range topics {
		count := topicPartitions[topic]
		for p := 0; p < count; p++ {
			all = append(all, TopicPartition{Topic: topic, Partition: p})
		}
	}
	return all
}

// partitionExists checks if a partition exists in the topic map.
func (s *StickyAssignor) partitionExists(tp TopicPartition, topicPartitions map[string]int) bool {
	count, exists := topicPartitions[tp.Topic]
	if !exists {
		return false
	}
	return tp.Partition >= 0 && tp.Partition < count
}

// assignUnassignedPartitions distributes unassigned partitions to needy members.
func (s *StickyAssignor) assignUnassignedPartitions(
	assignment map[string][]TopicPartition,
	unassigned []TopicPartition,
	minPartitions, maxPartitions int,
	members []string,
) {
	if len(unassigned) == 0 {
		return
	}

	// Create a priority queue of members sorted by partition count (ascending)
	type memberCount struct {
		memberID string
		count    int
	}

	getQueue := func() []memberCount {
		queue := make([]memberCount, 0, len(members))
		for _, m := range members {
			queue = append(queue, memberCount{m, len(assignment[m])})
		}
		// Sort by count ascending, then by member ID for determinism
		sort.Slice(queue, func(i, j int) bool {
			if queue[i].count != queue[j].count {
				return queue[i].count < queue[j].count
			}
			return queue[i].memberID < queue[j].memberID
		})
		return queue
	}

	// Assign partitions one at a time to the most needy member
	for _, tp := range unassigned {
		queue := getQueue()

		// Find the first member below maxPartitions
		assigned := false
		for _, mc := range queue {
			if mc.count < maxPartitions {
				assignment[mc.memberID] = append(assignment[mc.memberID], tp)
				assigned = true
				break
			}
		}

		// If everyone is at max, give to the first member (shouldn't happen in balanced case)
		if !assigned && len(queue) > 0 {
			assignment[queue[0].memberID] = append(assignment[queue[0].memberID], tp)
		}
	}
}

// =============================================================================
// RANGE ASSIGNOR (for backward compatibility)
// =============================================================================

// RangeAssignor implements the range assignment strategy.
// Partitions are divided into contiguous ranges per consumer.
type RangeAssignor struct{}

// NewRangeAssignor creates a new range assignor.
func NewRangeAssignor() *RangeAssignor {
	return &RangeAssignor{}
}

// Name returns the assignor name.
func (r *RangeAssignor) Name() string {
	return "range"
}

// Assign computes a range-based partition assignment.
func (r *RangeAssignor) Assign(
	members []string,
	topicPartitions map[string]int,
	_ map[string][]TopicPartition, // current assignment ignored
) map[string][]TopicPartition {
	// Sort members for deterministic results
	sort.Strings(members)

	totalMembers := len(members)
	if totalMembers == 0 {
		return make(map[string][]TopicPartition)
	}

	newAssignment := make(map[string][]TopicPartition, totalMembers)
	for _, m := range members {
		newAssignment[m] = []TopicPartition{}
	}

	// Get sorted topic names for determinism
	topics := make([]string, 0, len(topicPartitions))
	for topic := range topicPartitions {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	// Assign partitions per topic using range strategy
	for _, topic := range topics {
		partitionCount := topicPartitions[topic]
		partitionsPerMember := partitionCount / totalMembers
		extraPartitions := partitionCount % totalMembers

		currentPartition := 0
		for i, memberID := range members {
			// Calculate how many partitions this member gets
			count := partitionsPerMember
			if i < extraPartitions {
				count++
			}

			// Assign contiguous range
			for j := 0; j < count; j++ {
				newAssignment[memberID] = append(newAssignment[memberID], TopicPartition{
					Topic:     topic,
					Partition: currentPartition,
				})
				currentPartition++
			}
		}
	}

	return newAssignment
}

// =============================================================================
// ROUND ROBIN ASSIGNOR
// =============================================================================

// RoundRobinAssignor implements the round-robin assignment strategy.
// Partitions are distributed in interleaved fashion.
type RoundRobinAssignor struct{}

// NewRoundRobinAssignor creates a new round-robin assignor.
func NewRoundRobinAssignor() *RoundRobinAssignor {
	return &RoundRobinAssignor{}
}

// Name returns the assignor name.
func (rr *RoundRobinAssignor) Name() string {
	return "roundrobin"
}

// Assign computes a round-robin partition assignment.
func (rr *RoundRobinAssignor) Assign(
	members []string,
	topicPartitions map[string]int,
	_ map[string][]TopicPartition, // current assignment ignored
) map[string][]TopicPartition {
	// Sort members for deterministic results
	sort.Strings(members)

	totalMembers := len(members)
	if totalMembers == 0 {
		return make(map[string][]TopicPartition)
	}

	newAssignment := make(map[string][]TopicPartition, totalMembers)
	for _, m := range members {
		newAssignment[m] = []TopicPartition{}
	}

	// Build sorted list of all partitions
	var allPartitions []TopicPartition
	topics := make([]string, 0, len(topicPartitions))
	for topic := range topicPartitions {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	for _, topic := range topics {
		count := topicPartitions[topic]
		for p := 0; p < count; p++ {
			allPartitions = append(allPartitions, TopicPartition{Topic: topic, Partition: p})
		}
	}

	// Distribute round-robin
	for i, tp := range allPartitions {
		memberIdx := i % totalMembers
		newAssignment[members[memberIdx]] = append(newAssignment[members[memberIdx]], tp)
	}

	return newAssignment
}

// =============================================================================
// ASSIGNMENT DIFF CALCULATOR
// =============================================================================

// AssignmentDiff represents the changes between old and new assignments.
type AssignmentDiff struct {
	// Revocations: partitions that need to be taken away
	Revocations map[string][]TopicPartition

	// Assignments: partitions that need to be given
	Assignments map[string][]TopicPartition

	// Unchanged: partitions that stay with current owner
	Unchanged map[string][]TopicPartition

	// TotalPartitionsMoved: number of partitions that changed owner
	TotalPartitionsMoved int
}

// CalculateAssignmentDiff computes the difference between two assignments.
func CalculateAssignmentDiff(
	oldAssignment map[string][]TopicPartition,
	newAssignment map[string][]TopicPartition,
) *AssignmentDiff {
	diff := &AssignmentDiff{
		Revocations: make(map[string][]TopicPartition),
		Assignments: make(map[string][]TopicPartition),
		Unchanged:   make(map[string][]TopicPartition),
	}

	// Build old assignment lookup
	oldOwnership := make(map[TopicPartition]string)
	for memberID, partitions := range oldAssignment {
		for _, tp := range partitions {
			oldOwnership[tp] = memberID
		}
	}

	// Build new assignment lookup
	newOwnership := make(map[TopicPartition]string)
	for memberID, partitions := range newAssignment {
		for _, tp := range partitions {
			newOwnership[tp] = memberID
		}
	}

	// Find revocations (in old, not in new for same member, or owner changed)
	for memberID, oldPartitions := range oldAssignment {
		for _, tp := range oldPartitions {
			newOwner, inNew := newOwnership[tp]
			if !inNew || newOwner != memberID {
				// This member lost this partition
				diff.Revocations[memberID] = append(diff.Revocations[memberID], tp)
				diff.TotalPartitionsMoved++
			} else {
				// Partition stays with same member
				diff.Unchanged[memberID] = append(diff.Unchanged[memberID], tp)
			}
		}
	}

	// Find assignments (in new, not in old, or owner changed)
	for memberID, newPartitions := range newAssignment {
		for _, tp := range newPartitions {
			oldOwner, inOld := oldOwnership[tp]
			if !inOld || oldOwner != memberID {
				// This member gained this partition
				diff.Assignments[memberID] = append(diff.Assignments[memberID], tp)
			}
		}
	}

	return diff
}

// =============================================================================
// GET ASSIGNOR BY STRATEGY
// =============================================================================

// GetAssignor returns an assignor for the given strategy.
func GetAssignor(strategy AssignmentStrategy) PartitionAssignor {
	switch strategy {
	case AssignmentStrategyRange:
		return NewRangeAssignor()
	case AssignmentStrategyRoundRobin:
		return NewRoundRobinAssignor()
	case AssignmentStrategySticky:
		return NewStickyAssignor()
	default:
		return NewStickyAssignor()
	}
}
