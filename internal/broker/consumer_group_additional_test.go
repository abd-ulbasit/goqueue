package broker

import (
	"testing"
	"time"
)

// =============================================================================
// CONSUMER GROUP UNIT TESTS (BROKER PACKAGE)
// =============================================================================
//
// These tests focus on the *core invariants* of the eager consumer group model:
//   - Joining increases generation and assigns partitions.
//   - Heartbeats reject stale generations (zombie protection).
//   - Leaving triggers rebalance / transitions to Empty.
//   - Session eviction removes expired members deterministically.
//
// Even though GoQueue later adds cooperative rebalancing, these eager-group
// semantics are still used by parts of the system and (importantly) are the
// foundation for correctness.
// =============================================================================

func TestConsumerGroup_JoinHeartbeatLeave_AssignmentAndState(t *testing.T) {
	cfg := DefaultConsumerGroupConfig()
	g := NewConsumerGroup("g1", []string{"orders"}, cfg)

	if got, want := g.GetState(), GroupStateEmpty; got != want {
		t.Fatalf("initial state=%v, want %v", got, want)
	}
	if g.MemberCount() != 0 {
		t.Fatalf("initial MemberCount=%d, want 0", g.MemberCount())
	}

	// -------------------------------------------------------------------------
	// JOIN #1: first member should become leader and get all partitions.
	// -------------------------------------------------------------------------
	res1, err := g.Join("c1", 6)
	if err != nil {
		t.Fatalf("Join(c1) failed: %v", err)
	}
	if res1.Generation != g.GetGeneration() {
		t.Fatalf("Join generation=%d, g.GetGeneration()=%d", res1.Generation, g.GetGeneration())
	}
	if g.MemberCount() != 1 {
		t.Fatalf("MemberCount after join=%d, want 1", g.MemberCount())
	}
	if g.GetState() != GroupStateStable {
		t.Fatalf("state after join=%v, want %v", g.GetState(), GroupStateStable)
	}
	if res1.LeaderID != res1.MemberID {
		// With a single member, that member must be the leader.
		t.Fatalf("LeaderID=%q, want %q", res1.LeaderID, res1.MemberID)
	}
	if len(res1.Partitions) != 6 {
		t.Fatalf("partitions assigned=%d, want 6", len(res1.Partitions))
	}
	for i := 0; i < 6; i++ {
		if !g.IsPartitionAssigned(res1.MemberID, i) {
			t.Fatalf("partition %d should be assigned to member1", i)
		}
	}

	// -------------------------------------------------------------------------
	// JOIN #2: generation increments, partitions are range-assigned.
	// -------------------------------------------------------------------------
	res2, err := g.Join("c2", 6)
	if err != nil {
		t.Fatalf("Join(c2) failed: %v", err)
	}
	if res2.Generation != g.GetGeneration() {
		t.Fatalf("Join2 generation=%d, g.GetGeneration()=%d", res2.Generation, g.GetGeneration())
	}
	if res2.Generation <= res1.Generation {
		t.Fatalf("generation did not increase: gen1=%d gen2=%d", res1.Generation, res2.Generation)
	}

	// With 6 partitions and 2 members, range assignment should be 3 + 3.
	p1, gen, err := g.GetAssignment(res1.MemberID)
	if err != nil {
		t.Fatalf("GetAssignment(member1) failed: %v", err)
	}
	if gen != g.GetGeneration() {
		t.Fatalf("GetAssignment generation=%d, want %d", gen, g.GetGeneration())
	}
	p2, _, err := g.GetAssignment(res2.MemberID)
	if err != nil {
		t.Fatalf("GetAssignment(member2) failed: %v", err)
	}
	if len(p1) != 3 || len(p2) != 3 {
		t.Fatalf("assignment sizes=%d,%d want 3,3", len(p1), len(p2))
	}

	// -------------------------------------------------------------------------
	// HEARTBEAT: stale generation must be rejected (zombie consumer protection).
	// -------------------------------------------------------------------------
	if err := g.Heartbeat(res1.MemberID, g.GetGeneration()); err != nil {
		t.Fatalf("Heartbeat(current generation) failed: %v", err)
	}
	if err := g.Heartbeat(res1.MemberID, g.GetGeneration()-1); err != ErrStaleGeneration {
		t.Fatalf("Heartbeat(stale generation) err=%v, want %v", err, ErrStaleGeneration)
	}
	if err := g.Heartbeat("no-such-member", g.GetGeneration()); err != ErrMemberNotFound {
		t.Fatalf("Heartbeat(unknown member) err=%v, want %v", err, ErrMemberNotFound)
	}

	// -------------------------------------------------------------------------
	// LEAVE: leaving a member rebalances; leaving last member empties group.
	// -------------------------------------------------------------------------
	if err := g.Leave(res2.MemberID, 6); err != nil {
		t.Fatalf("Leave(member2) failed: %v", err)
	}
	if g.MemberCount() != 1 {
		t.Fatalf("MemberCount after leave=%d, want 1", g.MemberCount())
	}
	if g.GetState() != GroupStateStable {
		t.Fatalf("state after member2 leave=%v, want %v", g.GetState(), GroupStateStable)
	}

	if err := g.Leave(res1.MemberID, 6); err != nil {
		t.Fatalf("Leave(member1) failed: %v", err)
	}
	if g.MemberCount() != 0 {
		t.Fatalf("MemberCount after leaving last member=%d, want 0", g.MemberCount())
	}
	if g.GetState() != GroupStateEmpty {
		t.Fatalf("state after leaving last member=%v, want %v", g.GetState(), GroupStateEmpty)
	}

	// Leave of non-existent member should return ErrMemberNotFound.
	if err := g.Leave("nope", 6); err != ErrMemberNotFound {
		t.Fatalf("Leave(unknown) err=%v, want %v", err, ErrMemberNotFound)
	}
}

func TestConsumerGroup_SessionExpiration_CheckAndEvict(t *testing.T) {
	cfg := DefaultConsumerGroupConfig()
	cfg.SessionTimeoutMs = 10 // tiny timeout for deterministic test

	g := NewConsumerGroup("g1", []string{"orders"}, cfg)
	res, err := g.Join("c1", 2)
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	// Force-expire the member by setting the heartbeat far in the past.
	g.mu.Lock()
	g.Members[res.MemberID].LastHeartbeat = time.Now().Add(-1 * time.Hour)
	g.mu.Unlock()

	expired := g.CheckSessions()
	if len(expired) != 1 || expired[0] != res.MemberID {
		t.Fatalf("CheckSessions=%v, want [%s]", expired, res.MemberID)
	}

	evicted := g.EvictExpiredMembers(2)
	if evicted != 1 {
		t.Fatalf("EvictExpiredMembers=%d, want 1", evicted)
	}
	if g.MemberCount() != 0 {
		t.Fatalf("MemberCount after eviction=%d, want 0", g.MemberCount())
	}
	if g.GetState() != GroupStateEmpty {
		t.Fatalf("state after eviction=%v, want %v", g.GetState(), GroupStateEmpty)
	}

	// GetInfo should be safe on empty groups.
	info := g.GetInfo()
	if info.ID != "g1" {
		t.Fatalf("GetInfo.ID=%q, want %q", info.ID, "g1")
	}
	if info.State != GroupStateEmpty.String() {
		t.Fatalf("GetInfo.State=%q, want %q", info.State, GroupStateEmpty.String())
	}
	if len(info.Members) != 0 {
		t.Fatalf("GetInfo.Members len=%d, want 0", len(info.Members))
	}
}
