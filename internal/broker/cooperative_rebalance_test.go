// =============================================================================
// COOPERATIVE REBALANCING TESTS
// =============================================================================
//
// This file tests the core cooperative rebalancing types and utilities.
//
// TEST CATEGORIES:
//   - TopicPartition operations (equality, diff, union, intersect)
//   - MemberAssignment operations
//   - RebalanceContext lifecycle
//   - RebalanceMetrics recording
//   - HeartbeatResponse generation
//
// =============================================================================

package broker

import (
	"testing"
	"time"
)

// =============================================================================
// TOPIC PARTITION TESTS
// =============================================================================

func TestTopicPartition_String(t *testing.T) {
	tests := []struct {
		name     string
		tp       TopicPartition
		expected string
	}{
		{
			name:     "basic topic partition",
			tp:       TopicPartition{Topic: "orders", Partition: 0},
			expected: "orders-0",
		},
		{
			name:     "multi-digit partition",
			tp:       TopicPartition{Topic: "events", Partition: 15},
			expected: "events-15",
		},
		{
			name:     "topic with hyphens",
			tp:       TopicPartition{Topic: "user-events", Partition: 3},
			expected: "user-events-3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.tp.String()
			if result != tt.expected {
				t.Errorf("TopicPartition.String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestTopicPartition_Equality(t *testing.T) {
	tests := []struct {
		name     string
		a        TopicPartition
		b        TopicPartition
		expected bool
	}{
		{
			name:     "equal partitions",
			a:        TopicPartition{Topic: "orders", Partition: 0},
			b:        TopicPartition{Topic: "orders", Partition: 0},
			expected: true,
		},
		{
			name:     "different topic",
			a:        TopicPartition{Topic: "orders", Partition: 0},
			b:        TopicPartition{Topic: "events", Partition: 0},
			expected: false,
		},
		{
			name:     "different partition",
			a:        TopicPartition{Topic: "orders", Partition: 0},
			b:        TopicPartition{Topic: "orders", Partition: 1},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.a == tt.b // struct equality works for comparable types
			if result != tt.expected {
				t.Errorf("TopicPartition equality = %v, want %v", result, tt.expected)
			}
		})
	}
}

// =============================================================================
// PARTITION SET OPERATIONS TESTS
// =============================================================================

func TestDiffPartitions(t *testing.T) {
	tests := []struct {
		name     string
		current  []TopicPartition
		new      []TopicPartition
		wantDiff []TopicPartition
	}{
		{
			name: "some removed",
			current: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
				{Topic: "orders", Partition: 2},
			},
			new: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 2},
			},
			wantDiff: []TopicPartition{
				{Topic: "orders", Partition: 1},
			},
		},
		{
			name: "all removed",
			current: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
			},
			new: []TopicPartition{},
			wantDiff: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
			},
		},
		{
			name: "none removed",
			current: []TopicPartition{
				{Topic: "orders", Partition: 0},
			},
			new: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
			},
			wantDiff: []TopicPartition{},
		},
		{
			name:     "empty current",
			current:  []TopicPartition{},
			new:      []TopicPartition{{Topic: "orders", Partition: 0}},
			wantDiff: []TopicPartition{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DiffPartitions(tt.current, tt.new)
			if len(result) != len(tt.wantDiff) {
				t.Errorf("DiffPartitions() len = %d, want %d", len(result), len(tt.wantDiff))
				return
			}
			for i, r := range result {
				if r != tt.wantDiff[i] {
					t.Errorf("DiffPartitions()[%d] = %v, want %v", i, r, tt.wantDiff[i])
				}
			}
		})
	}
}

func TestIntersectPartitions(t *testing.T) {
	tests := []struct {
		name   string
		a      []TopicPartition
		b      []TopicPartition
		expect []TopicPartition
	}{
		{
			name: "partial overlap",
			a: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
			},
			b: []TopicPartition{
				{Topic: "orders", Partition: 1},
				{Topic: "orders", Partition: 2},
			},
			expect: []TopicPartition{
				{Topic: "orders", Partition: 1},
			},
		},
		{
			name: "no overlap",
			a: []TopicPartition{
				{Topic: "orders", Partition: 0},
			},
			b: []TopicPartition{
				{Topic: "orders", Partition: 1},
			},
			expect: []TopicPartition{},
		},
		{
			name: "full overlap",
			a: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
			},
			b: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
			},
			expect: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IntersectPartitions(tt.a, tt.b)
			if len(result) != len(tt.expect) {
				t.Errorf("IntersectPartitions() len = %d, want %d", len(result), len(tt.expect))
				return
			}
			// Check all expected items are in result
			for _, exp := range tt.expect {
				found := false
				for _, r := range result {
					if r == exp {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("IntersectPartitions() missing %v", exp)
				}
			}
		})
	}
}

func TestUnionPartitions(t *testing.T) {
	tests := []struct {
		name   string
		a      []TopicPartition
		b      []TopicPartition
		expect int // expected length (union removes duplicates)
	}{
		{
			name: "no overlap",
			a: []TopicPartition{
				{Topic: "orders", Partition: 0},
			},
			b: []TopicPartition{
				{Topic: "orders", Partition: 1},
			},
			expect: 2,
		},
		{
			name: "with overlap",
			a: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
			},
			b: []TopicPartition{
				{Topic: "orders", Partition: 1},
				{Topic: "orders", Partition: 2},
			},
			expect: 3,
		},
		{
			name:   "empty inputs",
			a:      []TopicPartition{},
			b:      []TopicPartition{},
			expect: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := UnionPartitions(tt.a, tt.b)
			if len(result) != tt.expect {
				t.Errorf("UnionPartitions() len = %d, want %d", len(result), tt.expect)
			}
		})
	}
}

// =============================================================================
// MEMBER ASSIGNMENT TESTS
// =============================================================================

func TestMemberAssignment_HasPartition(t *testing.T) {
	ma := &MemberAssignment{
		MemberID: "consumer-1",
		Partitions: []TopicPartition{
			{Topic: "orders", Partition: 0},
			{Topic: "orders", Partition: 1},
		},
	}

	t.Run("HasPartition_found", func(t *testing.T) {
		tp := TopicPartition{Topic: "orders", Partition: 0}
		if !ma.HasPartition(tp) {
			t.Error("HasPartition() should return true for existing partition")
		}
	})

	t.Run("HasPartition_not_found", func(t *testing.T) {
		tp := TopicPartition{Topic: "orders", Partition: 2}
		if ma.HasPartition(tp) {
			t.Error("HasPartition() should return false for non-existing partition")
		}
	})
}

// =============================================================================
// REBALANCE CONTEXT TESTS
// =============================================================================

func TestRebalanceContext_Lifecycle(t *testing.T) {
	ctx := NewRebalanceContext(1, "member_joined")

	t.Run("Initial state", func(t *testing.T) {
		if ctx.StartGeneration != 1 {
			t.Errorf("StartGeneration = %d, want 1", ctx.StartGeneration)
		}
		if ctx.AssignGeneration != 2 {
			t.Errorf("AssignGeneration = %d, want 2", ctx.AssignGeneration)
		}
		if ctx.State != RebalanceStatePendingRevoke {
			t.Errorf("State = %s, want pending_revoke", ctx.State)
		}
		if ctx.Reason != "member_joined" {
			t.Errorf("Reason = %s, want member_joined", ctx.Reason)
		}
	})

	t.Run("AddPendingRevocation", func(t *testing.T) {
		deadline := time.Now().Add(60 * time.Second)
		ctx.AddPendingRevocation("consumer-1", []TopicPartition{
			{Topic: "orders", Partition: 0},
		}, deadline)

		if ctx.AllRevocationsComplete() {
			t.Error("AllRevocationsComplete() should return false after adding revocation")
		}
		if ctx.GetPendingRevocationCount() != 1 {
			t.Errorf("GetPendingRevocationCount() = %d, want 1", ctx.GetPendingRevocationCount())
		}
	})

	t.Run("CompleteRevocation", func(t *testing.T) {
		ctx.CompleteRevocation("consumer-1", []TopicPartition{
			{Topic: "orders", Partition: 0},
		})

		if !ctx.AllRevocationsComplete() {
			t.Error("AllRevocationsComplete() should return true after completing all revocations")
		}
		if len(ctx.CompletedRevocations["consumer-1"]) != 1 {
			t.Error("CompletedRevocations should record the revoked partitions")
		}
	})
}

func TestRebalanceContext_ForceRevocation(t *testing.T) {
	ctx := NewRebalanceContext(1, "test")

	deadline := time.Now().Add(-1 * time.Second) // Already expired
	ctx.AddPendingRevocation("consumer-1", []TopicPartition{
		{Topic: "orders", Partition: 0},
		{Topic: "orders", Partition: 1},
	}, deadline)

	forced := ctx.ForceCompleteRevocation("consumer-1")
	if len(forced) != 2 {
		t.Errorf("ForceCompleteRevocation() returned %d partitions, want 2", len(forced))
	}
	if !ctx.AllRevocationsComplete() {
		t.Error("Should be complete after force revocation")
	}
}

// =============================================================================
// REBALANCE METRICS TESTS
// =============================================================================

func TestRebalanceMetrics_Recording(t *testing.T) {
	metrics := &RebalanceMetrics{
		RebalancesByReason: make(map[string]int64),
	}

	t.Run("RecordRebalanceStart", func(t *testing.T) {
		metrics.RecordRebalanceStart("member_joined")

		if metrics.TotalRebalances != 1 {
			t.Errorf("TotalRebalances = %d, want 1", metrics.TotalRebalances)
		}
		if metrics.RebalancesByReason["member_joined"] != 1 {
			t.Error("RebalancesByReason should record reason")
		}
	})

	t.Run("RecordRebalanceComplete_success", func(t *testing.T) {
		// RecordRebalanceComplete signature: (durationMs int64, partitionsRevoked, partitionsAssigned int, success bool)
		metrics.RecordRebalanceComplete(100, 5, 3, true)

		if metrics.SuccessfulRebalances != 1 {
			t.Errorf("SuccessfulRebalances = %d, want 1", metrics.SuccessfulRebalances)
		}
		if metrics.TotalPartitionsRevoked != 5 {
			t.Errorf("TotalPartitionsRevoked = %d, want 5", metrics.TotalPartitionsRevoked)
		}
		if metrics.TotalPartitionsAssigned != 3 {
			t.Errorf("TotalPartitionsAssigned = %d, want 3", metrics.TotalPartitionsAssigned)
		}
		if metrics.LastRebalanceDurationMs < 100 {
			t.Errorf("LastRebalanceDurationMs = %d, should be >= 100", metrics.LastRebalanceDurationMs)
		}
	})

	t.Run("RecordRebalanceComplete_failure", func(t *testing.T) {
		// RecordRebalanceComplete signature: (durationMs int64, partitionsRevoked, partitionsAssigned int, success bool)
		metrics.RecordRebalanceComplete(50, 0, 0, false)

		if metrics.FailedRebalances != 1 {
			t.Errorf("FailedRebalances = %d, want 1", metrics.FailedRebalances)
		}
	})

	t.Run("RecordRevocationTimeout", func(t *testing.T) {
		metrics.RecordRevocationTimeout()
		metrics.RecordRevocationTimeout()

		if metrics.TotalRevocationTimeouts != 2 {
			t.Errorf("TotalRevocationTimeouts = %d, want 2", metrics.TotalRevocationTimeouts)
		}
	})

	t.Run("AverageRebalanceDuration", func(t *testing.T) {
		// We had 2 complete rebalances (1 success + 1 failure)
		if metrics.AverageRebalanceDurationMs == 0 {
			t.Error("AverageRebalanceDurationMs should be calculated")
		}
	})
}

// =============================================================================
// HEARTBEAT RESPONSE TESTS
// =============================================================================

func TestHeartbeatResponse_States(t *testing.T) {
	tests := []struct {
		name     string
		response HeartbeatResponse
		wantRev  bool
		wantNew  bool
	}{
		{
			name: "no rebalance",
			response: HeartbeatResponse{
				RebalanceRequired: false,
				State:             RebalanceStateNone,
			},
			wantRev: false,
			wantNew: false,
		},
		{
			name: "pending revoke",
			response: HeartbeatResponse{
				RebalanceRequired: true,
				State:             RebalanceStatePendingRevoke,
				PartitionsToRevoke: []TopicPartition{
					{Topic: "orders", Partition: 0},
				},
			},
			wantRev: true,
			wantNew: false,
		},
		{
			name: "pending assign",
			response: HeartbeatResponse{
				RebalanceRequired: true,
				State:             RebalanceStatePendingAssign,
				PartitionsAssigned: []TopicPartition{
					{Topic: "orders", Partition: 1},
				},
			},
			wantRev: false,
			wantNew: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasRevocations := len(tt.response.PartitionsToRevoke) > 0
			hasAssignments := len(tt.response.PartitionsAssigned) > 0

			if hasRevocations != tt.wantRev {
				t.Errorf("has revocations = %v, want %v", hasRevocations, tt.wantRev)
			}
			if hasAssignments != tt.wantNew {
				t.Errorf("has assignments = %v, want %v", hasAssignments, tt.wantNew)
			}
		})
	}
}

// =============================================================================
// REBALANCE PROTOCOL/STRATEGY/STATE STRING TESTS
// =============================================================================

func TestRebalanceProtocol_String(t *testing.T) {
	tests := []struct {
		protocol RebalanceProtocol
		expected string
	}{
		{RebalanceProtocolEager, "eager"},
		{RebalanceProtocolCooperative, "cooperative"},
		{RebalanceProtocol(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.protocol.String(); got != tt.expected {
				t.Errorf("RebalanceProtocol.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestAssignmentStrategy_String(t *testing.T) {
	tests := []struct {
		strategy AssignmentStrategy
		expected string
	}{
		{AssignmentStrategyRange, "range"},
		{AssignmentStrategyRoundRobin, "roundrobin"},
		{AssignmentStrategySticky, "sticky"},
		{AssignmentStrategy(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.strategy.String(); got != tt.expected {
				t.Errorf("AssignmentStrategy.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestRebalanceState_String(t *testing.T) {
	tests := []struct {
		state    RebalanceState
		expected string
	}{
		{RebalanceStateNone, "none"},
		{RebalanceStatePendingRevoke, "pending_revoke"},
		{RebalanceStatePendingAssign, "pending_assign"},
		{RebalanceStateComplete, "complete"},
		{RebalanceState(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.state.String(); got != tt.expected {
				t.Errorf("RebalanceState.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// =============================================================================
// HELPER: PARTITION INT ARRAY CONVERSION TESTS
// =============================================================================

func TestIntsToPartitions(t *testing.T) {
	tests := []struct {
		name       string
		topic      string
		partitions []int
		expected   []TopicPartition
	}{
		{
			name:       "single partition",
			topic:      "orders",
			partitions: []int{0},
			expected:   []TopicPartition{{Topic: "orders", Partition: 0}},
		},
		{
			name:       "multiple partitions",
			topic:      "orders",
			partitions: []int{0, 1, 2},
			expected: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
				{Topic: "orders", Partition: 2},
			},
		},
		{
			name:       "empty",
			topic:      "orders",
			partitions: []int{},
			expected:   []TopicPartition{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IntsToPartitions(tt.partitions, tt.topic)
			if len(result) != len(tt.expected) {
				t.Errorf("IntsToPartitions() len = %d, want %d", len(result), len(tt.expected))
				return
			}
			for i, r := range result {
				if r != tt.expected[i] {
					t.Errorf("IntsToPartitions()[%d] = %v, want %v", i, r, tt.expected[i])
				}
			}
		})
	}
}

func TestPartitionsToInts(t *testing.T) {
	tests := []struct {
		name       string
		topic      string
		partitions []TopicPartition
		expected   []int
	}{
		{
			name:  "mixed topics - filter by topic",
			topic: "orders",
			partitions: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "events", Partition: 1},
				{Topic: "orders", Partition: 2},
			},
			expected: []int{0, 2},
		},
		{
			name:  "all same topic",
			topic: "orders",
			partitions: []TopicPartition{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
			},
			expected: []int{0, 1},
		},
		{
			name:       "empty",
			topic:      "orders",
			partitions: []TopicPartition{},
			expected:   []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PartitionsToInts(tt.partitions, tt.topic)
			if len(result) != len(tt.expected) {
				t.Errorf("PartitionsToInts() len = %d, want %d", len(result), len(tt.expected))
				return
			}
			for i, r := range result {
				if r != tt.expected[i] {
					t.Errorf("PartitionsToInts()[%d] = %d, want %d", i, r, tt.expected[i])
				}
			}
		})
	}
}
