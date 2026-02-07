package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"goqueue/internal/cluster"
)

func newTestMetadataStore(t *testing.T) *cluster.MetadataStore {
	t.Helper()

	ms := cluster.NewMetadataStore(t.TempDir())
	return ms
}

func mustCreateTopicMeta(t *testing.T, ms *cluster.MetadataStore, name string, partitions int, rf int) {
	t.Helper()

	if err := ms.CreateTopic(name, partitions, rf, cluster.DefaultTopicConfig()); err != nil {
		t.Fatalf("CreateTopic(%q) failed: %v", name, err)
	}
}

func TestReassignmentManager_StartReassignment_PopulatesDerivedReplicaSets(t *testing.T) {
	ms := newTestMetadataStore(t)
	mustCreateTopicMeta(t, ms, "orders", 1, 2)

	// We pass broker=nil because the derived replica-set computation happens
	// during validation, before any log/high-watermark logic is needed.
	rm := NewReassignmentManager(ms, nil, cluster.NodeID("n1"))

	req := &ReassignmentRequest{
		Partitions: []PartitionReassignment{
			{
				Topic:       "orders",
				Partition:   0,
				OldReplicas: []cluster.NodeID{"n1", "n2"},
				NewReplicas: []cluster.NodeID{"n1", "n3"},
			},
		},
		ThrottleBytesPerSec: 123,
		TimeoutPerPartition: 50 * time.Millisecond,
	}

	reassignID, err := rm.StartReassignment(req)
	if err != nil {
		t.Fatalf("StartReassignment failed: %v", err)
	}

	// =====================================================================
	// WHY THIS TEST EXISTS
	// =====================================================================
	// AddingReplicas/RemovingReplicas are derived fields used by the catch-up
	// loop (Phase 2). If StartReassignment validates a COPY of each partition
	// reassignment (for _, pr := range ...), those derived fields are computed
	// on the copy and then silently discarded.
	//
	// That would cause Phase 2 to think there are zero new replicas and to skip
	// waiting for catch-up entirely.
	if got := req.Partitions[0].AddingReplicas; len(got) != 1 || got[0] != "n3" {
		t.Fatalf("AddingReplicas=%v, want [n3]", got)
	}
	if got := req.Partitions[0].RemovingReplicas; len(got) != 1 || got[0] != "n2" {
		t.Fatalf("RemovingReplicas=%v, want [n2]", got)
	}

	// The manager runs reassignment asynchronously and persists metadata to disk.
	// Waiting for completion avoids teardown racing with background writes.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, err := rm.GetStatus(reassignID)
		if err != nil {
			t.Fatalf("GetStatus(%q) failed: %v", reassignID, err)
		}
		if st.CompletedAt != nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("reassignment %q did not complete in time", reassignID)
}

func TestReassignmentManager_StartReassignment_ValidationErrors(t *testing.T) {
	ms := newTestMetadataStore(t)
	mustCreateTopicMeta(t, ms, "orders", 2, 2)

	rm := NewReassignmentManager(ms, nil, cluster.NodeID("n1"))

	tests := []struct {
		name string
		req  *ReassignmentRequest
		want error
	}{
		{
			name: "no partitions",
			req:  &ReassignmentRequest{},
			want: ErrInvalidReassignment,
		},
		{
			name: "topic not found",
			req: &ReassignmentRequest{Partitions: []PartitionReassignment{{
				Topic:       "missing",
				Partition:   0,
				OldReplicas: []cluster.NodeID{"n1", "n2"},
				NewReplicas: []cluster.NodeID{"n1", "n3"},
			}}},
			want: ErrInvalidReassignment,
		},
		{
			name: "partition out of range",
			req: &ReassignmentRequest{Partitions: []PartitionReassignment{{
				Topic:       "orders",
				Partition:   99,
				OldReplicas: []cluster.NodeID{"n1", "n2"},
				NewReplicas: []cluster.NodeID{"n1", "n3"},
			}}},
			want: ErrInvalidReassignment,
		},
		{
			name: "old replicas empty",
			req: &ReassignmentRequest{Partitions: []PartitionReassignment{{
				Topic:       "orders",
				Partition:   0,
				OldReplicas: nil,
				NewReplicas: []cluster.NodeID{"n1", "n2"},
			}}},
			want: ErrInvalidReassignment,
		},
		{
			name: "new replicas empty",
			req: &ReassignmentRequest{Partitions: []PartitionReassignment{{
				Topic:       "orders",
				Partition:   0,
				OldReplicas: []cluster.NodeID{"n1", "n2"},
				NewReplicas: nil,
			}}},
			want: ErrInvalidReassignment,
		},
		{
			name: "replication factor mismatch",
			req: &ReassignmentRequest{Partitions: []PartitionReassignment{{
				Topic:       "orders",
				Partition:   0,
				OldReplicas: []cluster.NodeID{"n1", "n2"},
				NewReplicas: []cluster.NodeID{"n1"},
			}}},
			want: ErrInvalidReassignment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := rm.StartReassignment(tt.req)
			if err == nil {
				t.Fatalf("StartReassignment expected error")
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("StartReassignment error=%v, want %v", err, tt.want)
			}
		})
	}
}

func TestReassignmentManager_WaitForCatchup_DeadlineExceededMapsToSentinel(t *testing.T) {
	ms := newTestMetadataStore(t)
	mustCreateTopicMeta(t, ms, "orders", 1, 1)

	rm := NewReassignmentManager(ms, nil, cluster.NodeID("n1"))

	status := &ReassignmentStatus{
		ID:      "reassign-1",
		Request: &ReassignmentRequest{TimeoutPerPartition: 10 * time.Millisecond},
		Progress: map[string]*PartitionReassignmentProgress{
			"orders-0": {
				Topic:          "orders",
				Partition:      0,
				State:          ReassignmentStateCatchingUp,
				ReplicaOffsets: map[cluster.NodeID]int64{},
				StartedAt:      time.Now(),
			},
		},
	}

	pr := &PartitionReassignment{Topic: "orders", Partition: 0, AddingReplicas: []cluster.NodeID{"n2"}}

	// We want checkCatchup() to return false so waitForCatchup goes into the
	// timer wait, and then we want the context to expire so we take the
	// DeadlineExceeded path. Leaving metadata assignment absent makes
	// checkCatchup return (false, 0) deterministically.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	err := rm.waitForCatchup(ctx, status, pr)
	if err == nil {
		t.Fatalf("waitForCatchup expected error")
	}
	if !errors.Is(err, ErrReplicaCatchupTimeout) {
		t.Fatalf("waitForCatchup error=%v, want %v", err, ErrReplicaCatchupTimeout)
	}
}

func TestReassignmentManager_RunReassignment_CanceledStateIsNotFailed(t *testing.T) {
	ms := newTestMetadataStore(t)
	mustCreateTopicMeta(t, ms, "orders", 1, 2)

	rm := NewReassignmentManager(ms, nil, cluster.NodeID("n1"))

	status := &ReassignmentStatus{
		ID: "reassign-1",
		Request: &ReassignmentRequest{Partitions: []PartitionReassignment{{
			Topic:       "orders",
			Partition:   0,
			OldReplicas: []cluster.NodeID{"n1", "n2"},
			NewReplicas: []cluster.NodeID{"n1", "n3"},
		}}},
		Progress: map[string]*PartitionReassignmentProgress{
			"orders-0": {
				Topic:          "orders",
				Partition:      0,
				State:          ReassignmentStatePending,
				ReplicaOffsets: map[cluster.NodeID]int64{},
				StartedAt:      time.Now(),
			},
		},
		StartedAt: time.Now(),
	}

	// Mirror what StartReassignment would have recorded.
	rm.activeReassignments[status.ID] = status
	rm.partitionReassignments["orders-0"] = status.ID
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled before the worker starts

	// Pretend there is a cancel function registered.
	rm.cancelFuncs[status.ID] = func() {}

	rm.runReassignment(ctx, status)

	if status.State != ReassignmentStateCanceled {
		t.Fatalf("status.State=%q, want %q", status.State, ReassignmentStateCanceled)
	}
	if status.CompletedAt == nil {
		t.Fatalf("CompletedAt expected to be set")
	}
	if _, stillMapped := rm.partitionReassignments["orders-0"]; stillMapped {
		t.Fatalf("partitionReassignments mapping expected to be removed")
	}
}

func TestReassignmentManager_GetStatus_ReturnsDeepCopy(t *testing.T) {
	ms := newTestMetadataStore(t)
	rm := NewReassignmentManager(ms, nil, cluster.NodeID("n1"))

	completedAt := time.Now()
	status := &ReassignmentStatus{
		ID:    "reassign-1",
		State: ReassignmentStateCompleted,
		Request: &ReassignmentRequest{Partitions: []PartitionReassignment{{
			Topic: "orders", Partition: 0,
		}}},
		Progress: map[string]*PartitionReassignmentProgress{
			"orders-0": {
				Topic:     "orders",
				Partition: 0,
				State:     ReassignmentStateCompleted,
				ReplicaOffsets: map[cluster.NodeID]int64{
					"n1": 10,
				},
				CompletedAt: &completedAt,
			},
		},
		StartedAt:   time.Now(),
		CompletedAt: &completedAt,
	}
	rm.activeReassignments[status.ID] = status

	copy1, err := rm.GetStatus(status.ID)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	// Mutate the returned copy; the original should not change.
	copy1.Progress["orders-0"].ReplicaOffsets["n1"] = 999
	if status.Progress["orders-0"].ReplicaOffsets["n1"] == 999 {
		t.Fatalf("GetStatus did not deep-copy ReplicaOffsets")
	}
}
