package broker

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestCooperativeGroupCoordinator_RebalanceLifecycle_TwoMembers(t *testing.T) {
	// =========================================================================
	// COOPERATIVE REBALANCING: TWO-MEMBER LIFECYCLE (JOIN → REVOKE → ASSIGN)
	// =========================================================================
	//
	// WHY THIS TEST EXISTS:
	//   Cooperative rebalancing is one of goqueue's differentiators:
	//     - we avoid Kafka's "stop-the-world" eager rebalances
	//     - only the partitions that must move are revoked
	//
	// This file (`cooperative_group.go`) is an integration layer that is easy to
	// miss in tests because many lower-level pieces are already tested elsewhere.
	//
	// WHAT WE VERIFY (HIGH SIGNAL):
	//   1) First member joins an empty group → gets an assignment immediately
	//      (no revocations needed).
	//   2) Second member joins → causes revocation of some partitions from member1.
	//   3) Member1 acknowledges revocation → coordinator can complete rebalance.
	//   4) The group becomes stable and exposes consistent assignment info.
	//
	// NOTE:
	//   The code builds the "members" slice from a map iteration, so we avoid
	//   asserting an exact partition-to-member mapping. Instead we assert
	//   correctness invariants (no duplicates, full coverage of partitions).
	// =========================================================================

	dataDir := t.TempDir()

	// Base (eager) group coordinator used for storage + topic registration.
	cfg := DefaultCoordinatorConfig(dataDir)
	cfg.SessionCheckIntervalMs = 10
	gc, err := NewGroupCoordinator(cfg)
	if err != nil {
		t.Fatalf("NewGroupCoordinator: %v", err)
	}
	defer func() {
		_ = gc.Close()
	}()

	// Tell the coordinator how many partitions exist for our topic.
	gc.RegisterTopic("orders", 2)

	// Cooperative rebalancer with very small time constants so tests are fast.
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	rbCfg := CooperativeRebalancerConfig{
		Protocol:            RebalanceProtocolCooperative,
		Strategy:            AssignmentStrategyRoundRobin,
		RevocationTimeoutMs: 200, // keep revocation deadline short
		RebalanceDelayMs:    0,   // TriggerRebalance computes diff immediately
		CheckIntervalMs:     10,
	}
	rb := NewCooperativeRebalancer(rbCfg, logger)
	rb.Start()
	defer rb.Stop()

	// We construct the coordinator directly so we can inject our fast rebalancer.
	cgc := &CooperativeGroupCoordinator{
		GroupCoordinator:  gc,
		rebalancer:        rb,
		cooperativeGroups: make(map[string]*CooperativeGroup),
		defaultConfig:     DefaultCooperativeGroupConfig(),
	}

	const groupID = "g-orders"

	// -------------------------------------------------------------------------
	// 1) First member joins.
	// -------------------------------------------------------------------------
	join1, err := cgc.JoinCooperativeGroup(groupID, "client-1", []string{"orders"})
	if err != nil {
		t.Fatalf("JoinCooperativeGroup(member1): %v", err)
	}
	if join1.MemberID == "" {
		t.Fatalf("expected non-empty memberID")
	}
	if join1.Generation <= 0 {
		t.Fatalf("expected generation to advance, got %d", join1.Generation)
	}
	if !join1.RebalanceRequired {
		// Join responses currently always say "rebalance required" because the
		// actual assignment is communicated via heartbeats.
		t.Fatalf("expected Join to require rebalance")
	}

	hb1, err := cgc.HeartbeatCooperative(groupID, join1.MemberID, join1.Generation)
	if err != nil {
		t.Fatalf("HeartbeatCooperative(member1): %v", err)
	}
	if hb1.RebalanceRequired {
		t.Fatalf("expected no revocations on first join")
	}
	if hb1.State != RebalanceStatePendingAssign {
		t.Fatalf("state=%s want=%s", hb1.State.String(), RebalanceStatePendingAssign.String())
	}
	if len(hb1.PartitionsAssigned) != 2 {
		t.Fatalf("assigned=%v want 2 partitions", hb1.PartitionsAssigned)
	}

	// -------------------------------------------------------------------------
	// 2) Second member joins → should trigger revocation(s) from member1.
	// -------------------------------------------------------------------------
	join2, err := cgc.JoinCooperativeGroup(groupID, "client-2", []string{"orders"})
	if err != nil {
		t.Fatalf("JoinCooperativeGroup(member2): %v", err)
	}
	if join2.Generation <= join1.Generation {
		t.Fatalf("expected generation to increase (join1=%d join2=%d)", join1.Generation, join2.Generation)
	}

	// Member1 is still on the previous generation; in cooperative mode the
	// coordinator replies with StartGeneration when revocations are needed.
	hb1Revoke, err := cgc.HeartbeatCooperative(groupID, join1.MemberID, join1.Generation)
	if err != nil {
		t.Fatalf("HeartbeatCooperative(member1 during rebalance): %v", err)
	}
	if !hb1Revoke.RebalanceRequired {
		t.Fatalf("expected rebalance to be required after member2 joins")
	}
	if hb1Revoke.State != RebalanceStatePendingRevoke {
		t.Fatalf("state=%s want=%s", hb1Revoke.State.String(), RebalanceStatePendingRevoke.String())
	}
	if len(hb1Revoke.PartitionsToRevoke) == 0 {
		// With 2 partitions and 2 members, we expect at least one partition move.
		t.Fatalf("expected at least one partition to revoke; got none")
	}
	if time.Until(hb1Revoke.RevocationDeadline) <= 0 {
		t.Fatalf("expected a future revocation deadline")
	}

	// -------------------------------------------------------------------------
	// 3) Acknowledge revocation and complete rebalance.
	// -------------------------------------------------------------------------
	if err := cgc.AcknowledgeRevocation(groupID, join1.MemberID, hb1Revoke.Generation, hb1Revoke.PartitionsToRevoke); err != nil {
		t.Fatalf("AcknowledgeRevocation: %v", err)
	}

	// Coordinator-level completion updates in-memory assignments and flips
	// group state to stable.
	cgc.CompleteRebalance(groupID)

	// Heartbeats after completion should no longer require rebalance.
	if _, err := cgc.HeartbeatCooperative(groupID, join1.MemberID, join2.Generation); err != nil {
		t.Fatalf("HeartbeatCooperative(member1 post-complete): %v", err)
	}
	if _, err := cgc.HeartbeatCooperative(groupID, join2.MemberID, join2.Generation); err != nil {
		t.Fatalf("HeartbeatCooperative(member2 post-complete): %v", err)
	}

	group, ok := cgc.GetCooperativeGroup(groupID)
	if !ok {
		t.Fatalf("expected cooperative group to exist")
	}

	// Cooperative info should be internally consistent.
	info := group.GetCooperativeInfo()
	if info.Protocol == "" || info.AssignmentStrategy == "" {
		t.Fatalf("expected protocol/strategy to be populated")
	}
	if info.RebalanceState == "" {
		t.Fatalf("expected rebalance state to be populated")
	}

	// Assignment invariants:
	//   - total assigned partitions == 2
	//   - each (topic,partition) assigned to at most one member
	assign := group.GetCurrentAssignment()
	seen := make(map[TopicPartition]string)
	total := 0
	for memberID, parts := range assign {
		if memberID == "" {
			t.Fatalf("unexpected empty memberID in assignment")
		}
		for _, tp := range parts {
			total++
			if prev, exists := seen[tp]; exists {
				t.Fatalf("duplicate assignment for %s: %s and %s", tp.String(), prev, memberID)
			}
			seen[tp] = memberID
		}
	}
	if total != 2 {
		t.Fatalf("total assigned partitions=%d want=2 (assign=%v)", total, assign)
	}

	// Cover a couple of helper methods and validate they behave as expected.
	// (We don't care WHICH member owns partition 0, just that exactly one does.)
	p0 := TopicPartition{Topic: "orders", Partition: 0}
	m1HasP0 := group.IsPartitionAssignedCoop(join1.MemberID, p0)
	m2HasP0 := group.IsPartitionAssignedCoop(join2.MemberID, p0)
	if m1HasP0 == m2HasP0 {
		t.Fatalf("expected partition %s to be assigned to exactly one member", p0.String())
	}

	if _, exists := group.GetMemberAssignment(join1.MemberID); !exists {
		t.Fatalf("expected GetMemberAssignment to find member1")
	}

	// Ensure the underlying ConsumerGroup helper is wired (coverage target).
	if _, err := group.ConsumerGroup.GetMember(join1.MemberID); err != nil {
		t.Fatalf("expected GetMember to find member1: %v", err)
	}
}
