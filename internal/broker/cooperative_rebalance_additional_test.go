package broker

import (
	"testing"
	"time"
)

func TestCooperativeRebalance_ParseEnumsAndHelpers(t *testing.T) {
	// =========================================================================
	// COOPERATIVE REBALANCE: PARSING + SMALL HELPERS
	// =========================================================================
	//
	// WHY:
	//   These helpers look trivial, but they're part of the broker's public-ish
	//   API surface (HTTP params, config parsing, JSON responses). Small bugs
	//   here turn into confusing UX ("unknown protocol") or hidden defaults.
	// =========================================================================

	// Protocol parsing.
	if got, err := ParseRebalanceProtocol("eager"); err != nil || got != RebalanceProtocolEager {
		t.Fatalf("ParseRebalanceProtocol(eager)=%v,%v", got, err)
	}
	if got, err := ParseRebalanceProtocol("cooperative"); err != nil || got != RebalanceProtocolCooperative {
		t.Fatalf("ParseRebalanceProtocol(cooperative)=%v,%v", got, err)
	}
	if _, err := ParseRebalanceProtocol("nope"); err == nil {
		t.Fatalf("expected unknown protocol to error")
	}

	// Strategy parsing.
	if got, err := ParseAssignmentStrategy("range"); err != nil || got != AssignmentStrategyRange {
		t.Fatalf("ParseAssignmentStrategy(range)=%v,%v", got, err)
	}
	if got, err := ParseAssignmentStrategy("roundrobin"); err != nil || got != AssignmentStrategyRoundRobin {
		t.Fatalf("ParseAssignmentStrategy(roundrobin)=%v,%v", got, err)
	}
	if got, err := ParseAssignmentStrategy("sticky"); err != nil || got != AssignmentStrategySticky {
		t.Fatalf("ParseAssignmentStrategy(sticky)=%v,%v", got, err)
	}
	if _, err := ParseAssignmentStrategy("nope"); err == nil {
		t.Fatalf("expected unknown strategy to error")
	}

	// MemberAssignment.TopicPartitionSet + HasPartition.
	ma := &MemberAssignment{
		MemberID: "m1",
		Partitions: []TopicPartition{
			{Topic: "t", Partition: 1},
			{Topic: "t", Partition: 2},
		},
	}
	set := ma.TopicPartitionSet()
	if len(set) != 2 {
		t.Fatalf("TopicPartitionSet len=%d want=2", len(set))
	}
	if !ma.HasPartition(TopicPartition{Topic: "t", Partition: 2}) {
		t.Fatalf("expected HasPartition to find assigned partition")
	}
	if ma.HasPartition(TopicPartition{Topic: "t", Partition: 99}) {
		t.Fatalf("expected HasPartition to be false for unassigned partition")
	}

	// RebalanceContext expired revocations.
	rc := NewRebalanceContext(10, "test")
	deadlinePast := time.Now().Add(-10 * time.Millisecond)
	deadlineFuture := time.Now().Add(10 * time.Second)

	rc.AddPendingRevocation("m-expired", []TopicPartition{{Topic: "t", Partition: 0}}, deadlinePast)
	rc.AddPendingRevocation("m-ok", []TopicPartition{{Topic: "t", Partition: 1}}, deadlineFuture)

	expired := rc.GetExpiredRevocations()
	if len(expired) != 1 || expired[0] != "m-expired" {
		t.Fatalf("GetExpiredRevocations=%v want=[m-expired]", expired)
	}

	forced := rc.ForceCompleteRevocation("m-expired")
	if len(forced) != 1 {
		t.Fatalf("ForceCompleteRevocation=%v want 1 partition", forced)
	}

	if rc.GetPendingRevocationCount() != 1 {
		t.Fatalf("pending revocations=%d want=1", rc.GetPendingRevocationCount())
	}
}
