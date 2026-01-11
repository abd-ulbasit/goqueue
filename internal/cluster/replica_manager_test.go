package cluster

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestReplicaManager_LeaderLifecycle_StateAndISR(t *testing.T) {
	// ============================================================================
	// REPLICA MANAGER (LEADER PATH)
	// ============================================================================
	//
	// WHY:
	//   ReplicaManager is the local authority for partition role/state.
	//   ReplicationServer depends on it to decide "am I the leader?" and to
	//   track high watermark (commit point).
	//
	// WHAT WE EXERCISE:
	//   - BecomeLeader initializes LocalReplica state + ISRManager
	//   - Accessors return safe snapshots (copies)
	//   - Leader-only operations enforce role checks
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := DefaultReplicationConfig()
	cfg.SnapshotEnabled = false // keep tests deterministic (no filesystem snapshot manager needed)

	rm := NewReplicaManager("n1", cfg, nil, t.TempDir(), logger)
	t.Cleanup(func() { _ = rm.Stop() })

	topic := "orders"
	partition := 0

	if err := rm.BecomeLeader(topic, partition, 7, []NodeID{"n1", "n2"}, 0); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}

	if !rm.IsLeader(topic, partition) {
		t.Fatalf("expected IsLeader=true")
	}

	// GetReplicaState should return a copy, not a pointer to internal state.
	s1 := rm.GetReplicaState(topic, partition)
	if s1 == nil {
		t.Fatalf("expected replica state")
	}
	s1.Role = ReplicaRoleFollower

	s2 := rm.GetReplicaState(topic, partition)
	if s2 == nil {
		t.Fatalf("expected replica state")
	}
	if s2.Role != ReplicaRoleLeader {
		t.Fatalf("expected internal role to remain leader; got %s", s2.Role)
	}

	// Leader-only operation: UpdateLogEndOffset should succeed.
	if err := rm.UpdateLogEndOffset(topic, partition, 10); err != nil {
		t.Fatalf("UpdateLogEndOffset: %v", err)
	}

	if got := rm.GetLogEndOffset(topic, partition); got != 10 {
		t.Fatalf("GetLogEndOffset=%d want=10", got)
	}
	if got := rm.GetHighWatermark(topic, partition); got != 0 {
		t.Fatalf("GetHighWatermark=%d want=0 (HW advances only when ISR confirms)", got)
	}

	// ISR accessors should return non-empty ISR (initially optimistic: all replicas are in ISR).
	isr := rm.GetISR(topic, partition)
	if len(isr) == 0 {
		t.Fatalf("expected non-empty ISR")
	}
}

func TestReplicaManager_WaitForAcks_TimeoutAndSatisfied(t *testing.T) {
	// ============================================================================
	// ACK-ALL WAITING (PENDING ACKS)
	// ============================================================================
	//
	// WHY:
	//   AckAll semantics require the leader to wait for ISR confirmation.
	//   Bugs here can cause:
	//     - false success (producer thinks committed, but followers didn't replicate)
	//     - false failure/timeouts (kills throughput)
	//     - goroutine leaks (time.After misuse)
	//
	// WHAT WE COVER:
	//   - Timeout path cleans up pending acks
	//   - Happy path completes when high watermark advances
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := DefaultReplicationConfig()
	cfg.SnapshotEnabled = false
	cfg.AckTimeoutMs = 20

	rm := NewReplicaManager("n1", cfg, nil, t.TempDir(), logger)
	t.Cleanup(func() { _ = rm.Stop() })

	topic := "orders"
	partition := 0

	if err := rm.BecomeLeader(topic, partition, 1, []NodeID{"n1", "n2"}, 0); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	if err := rm.UpdateLogEndOffset(topic, partition, 10); err != nil {
		t.Fatalf("UpdateLogEndOffset: %v", err)
	}

	// 1) Timeout path.
	ctx := context.Background()
	if err := rm.WaitForAcks(ctx, topic, partition, 9); err == nil {
		t.Fatalf("expected timeout waiting for acks")
	}

	// 2) Satisfied path.
	cfg2 := DefaultReplicationConfig()
	cfg2.SnapshotEnabled = false
	cfg2.AckTimeoutMs = 250

	rm2 := NewReplicaManager("n1", cfg2, nil, t.TempDir(), logger)
	t.Cleanup(func() { _ = rm2.Stop() })

	if err := rm2.BecomeLeader(topic, partition, 1, []NodeID{"n1", "n2"}, 0); err != nil {
		t.Fatalf("BecomeLeader(rm2): %v", err)
	}
	if err := rm2.UpdateLogEndOffset(topic, partition, 10); err != nil {
		t.Fatalf("UpdateLogEndOffset(rm2): %v", err)
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- rm2.WaitForAcks(context.Background(), topic, partition, 9)
	}()

	// Advance HW by recording a follower fetch that implies the follower has caught up.
	// fetchOffset=10 means follower has appended up to 9 (matched offset = 9).
	if err := rm2.RecordFollowerFetch(topic, partition, "n2", 10); err != nil {
		t.Fatalf("RecordFollowerFetch: %v", err)
	}

	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("WaitForAcks should succeed after HW advances: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("WaitForAcks did not return (possible goroutine leak / HW not advancing)")
	}

	if got := rm2.GetHighWatermark(topic, partition); got < 10 {
		t.Fatalf("expected HW to advance to >=10, got %d", got)
	}
}

func TestReplicaManager_WaitForAcks_FailsWhenMinISRNotMet(t *testing.T) {
	// ============================================================================
	// MIN ISR ENFORCEMENT
	// ============================================================================
	//
	// WHY:
	//   Kafka-style MinInSyncReplicas protects durability during partial outages.
	//   If ISR is below this threshold, AckAll writes must fail.
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := DefaultReplicationConfig()
	cfg.SnapshotEnabled = false
	cfg.MinInSyncReplicas = 3 // impossible with only 2 replicas

	rm := NewReplicaManager("n1", cfg, nil, t.TempDir(), logger)
	t.Cleanup(func() { _ = rm.Stop() })

	if err := rm.BecomeLeader("orders", 0, 1, []NodeID{"n1", "n2"}, 0); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}

	if err := rm.WaitForAcks(context.Background(), "orders", 0, 1); err == nil {
		t.Fatalf("expected error when MinInSyncReplicas cannot be met")
	}
}

func TestReplicaManager_ApplyFetchedMessages_FollowerStateUpdates(t *testing.T) {
	// ============================================================================
	// FOLLOWER APPLY PATH
	// ============================================================================
	//
	// WHY:
	//   Followers must advance local LEO/HW based on leader responses.
	//   Bugs here lead to stuck replicas (never catching up) or inconsistent reads.
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := DefaultReplicationConfig()
	cfg.SnapshotEnabled = false

	rm := NewReplicaManager("n2", cfg, nil, t.TempDir(), logger)
	t.Cleanup(func() { _ = rm.Stop() })

	// Avoid BecomeFollower() because it starts a background fetcher. For this unit test,
	// we insert a follower replica directly and validate ApplyFetchedMessages logic.
	key := partitionKey("orders", 0)
	rm.mu.Lock()
	rm.replicas[key] = &LocalReplica{
		State: &ReplicaState{
			Topic:         "orders",
			Partition:     0,
			Role:          ReplicaRoleFollower,
			LeaderID:      "n1",
			LeaderEpoch:   1,
			LogEndOffset:  0,
			HighWatermark: 0,
		},
	}
	rm.mu.Unlock()

	msgs := []ReplicatedMessage{
		{Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 1, Timestamp: time.Now().UnixMilli(), Key: []byte("k2"), Value: []byte("v2")},
	}

	if err := rm.ApplyFetchedMessages("orders", 0, msgs, 2, 2); err != nil {
		t.Fatalf("ApplyFetchedMessages: %v", err)
	}

	st := rm.GetReplicaState("orders", 0)
	if st == nil {
		t.Fatalf("expected state")
	}
	if st.LogEndOffset != 2 {
		t.Fatalf("LEO=%d want=2", st.LogEndOffset)
	}
	if st.HighWatermark != 2 {
		t.Fatalf("HW=%d want=2", st.HighWatermark)
	}
}
