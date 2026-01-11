package broker

import (
	"errors"
	"testing"
	"time"

	"goqueue/internal/cluster"
)

func TestPartitionScaler_AddPartitions_ValidationAndGuards(t *testing.T) {
	ms := newTestMetadataStore(t)
	mustCreateTopicMeta(t, ms, "orders", 2, 2)

	ps := NewPartitionScaler(ms, nil, cluster.NodeID("n1"), []cluster.NodeID{"n1", "n2"})

	// Internal topic guard.
	if _, err := ps.AddPartitions(&PartitionScaleRequest{TopicName: "__consumer_offsets", NewPartitionCount: 2}); !errors.Is(err, ErrInternalTopicScaling) {
		t.Fatalf("internal topic error=%v, want %v", err, ErrInternalTopicScaling)
	}

	// Topic not found.
	if _, err := ps.AddPartitions(&PartitionScaleRequest{TopicName: "missing", NewPartitionCount: 2}); err == nil {
		t.Fatalf("missing topic expected error")
	}

	// Cannot reduce.
	if _, err := ps.AddPartitions(&PartitionScaleRequest{TopicName: "orders", NewPartitionCount: 1}); !errors.Is(err, ErrCannotReducePartitions) {
		t.Fatalf("reduce error=%v, want %v", err, ErrCannotReducePartitions)
	}

	// No change.
	if _, err := ps.AddPartitions(&PartitionScaleRequest{TopicName: "orders", NewPartitionCount: 2}); !errors.Is(err, ErrNoChange) {
		t.Fatalf("no change error=%v, want %v", err, ErrNoChange)
	}

	// Insufficient nodes for RF=2.
	ps.UpdateNodeList([]cluster.NodeID{"n1"})
	if _, err := ps.AddPartitions(&PartitionScaleRequest{TopicName: "orders", NewPartitionCount: 3}); !errors.Is(err, ErrInsufficientNodes) {
		t.Fatalf("insufficient nodes error=%v, want %v", err, ErrInsufficientNodes)
	}

	// Scaling-in-progress guard is deterministic (we pre-mark the topic).
	ps.UpdateNodeList([]cluster.NodeID{"n1", "n2"})
	ps.mu.Lock()
	ps.scalingInProgress["orders"] = time.Now()
	ps.mu.Unlock()
	if _, err := ps.AddPartitions(&PartitionScaleRequest{TopicName: "orders", NewPartitionCount: 3}); !errors.Is(err, ErrScalingInProgress) {
		t.Fatalf("in-progress error=%v, want %v", err, ErrScalingInProgress)
	}
}

func TestPartitionScaler_AddPartitions_CreateLocalPartitionError_CleansMarker(t *testing.T) {
	ms := newTestMetadataStore(t)
	mustCreateTopicMeta(t, ms, "orders", 1, 1)

	// Broker exists, but we intentionally do NOT create the topic in broker storage.
	// That should cause createLocalPartition -> broker.GetTopic to fail.
	b := newTestBroker(t)
	ps := NewPartitionScaler(ms, b, cluster.NodeID("n1"), []cluster.NodeID{"n1"})

	_, err := ps.AddPartitions(&PartitionScaleRequest{TopicName: "orders", NewPartitionCount: 2})
	if err == nil {
		t.Fatalf("expected error")
	}

	if ps.IsScalingInProgress("orders") {
		t.Fatalf("scalingInProgress marker should be cleaned up on error")
	}
}

func TestPartitionScaler_AddPartitions_Success_CreatesPartitions_UpdatesMetadata_NotifiesListeners(t *testing.T) {
	ms := newTestMetadataStore(t)
	mustCreateTopicMeta(t, ms, "orders", 1, 1)

	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 1)

	ps := NewPartitionScaler(ms, b, cluster.NodeID("n1"), []cluster.NodeID{"n1"})

	// Listener should be called asynchronously; use a channel to make this deterministic.
	ch := make(chan struct {
		topic       string
		oldCount    int
		newCount    int
		newPartsLen int
	}, 1)
	ps.AddListener(func(topic string, oldCount, newCount int, newPartitions []int) {
		ch <- struct {
			topic       string
			oldCount    int
			newCount    int
			newPartsLen int
		}{topic: topic, oldCount: oldCount, newCount: newCount, newPartsLen: len(newPartitions)}
	})

	res, err := ps.AddPartitions(&PartitionScaleRequest{TopicName: "orders", NewPartitionCount: 3})
	if err != nil {
		t.Fatalf("AddPartitions failed: %v", err)
	}
	if !res.Success {
		t.Fatalf("Success=false, want true")
	}
	if res.OldPartitionCount != 1 || res.NewPartitionCount != 3 {
		t.Fatalf("counts old=%d new=%d, want old=1 new=3", res.OldPartitionCount, res.NewPartitionCount)
	}
	if len(res.PartitionsAdded) != 2 {
		t.Fatalf("PartitionsAdded=%v, want length 2", res.PartitionsAdded)
	}

	// Broker should have created the new partitions.
	topic, err := b.GetTopic("orders")
	if err != nil {
		t.Fatalf("GetTopic failed: %v", err)
	}
	if got := topic.NumPartitions(); got != 3 {
		t.Fatalf("topic.NumPartitions()=%d, want 3", got)
	}
	if _, err := topic.Partition(2); err != nil {
		t.Fatalf("expected new partition 2 to exist: %v", err)
	}

	// Metadata store should reflect the new partition count.
	meta := ms.GetTopic("orders")
	if meta == nil || meta.PartitionCount != 3 {
		t.Fatalf("metadata partition count=%v, want 3", meta)
	}

	// Metadata should include assignments for new partitions.
	if a := ms.GetAssignment("orders", 1); a == nil || a.Leader != "n1" {
		t.Fatalf("assignment for orders/1=%v, want leader n1", a)
	}
	if a := ms.GetAssignment("orders", 2); a == nil || a.Leader != "n1" {
		t.Fatalf("assignment for orders/2=%v, want leader n1", a)
	}

	select {
	case got := <-ch:
		if got.topic != "orders" || got.oldCount != 1 || got.newCount != 3 || got.newPartsLen != 2 {
			t.Fatalf("listener got=%+v, want topic=orders old=1 new=3 newPartsLen=2", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for listener notification")
	}
}

func TestPartitionScaler_AutoAssignReplicas_IsRoundRobin(t *testing.T) {
	ms := newTestMetadataStore(t)
	ps := NewPartitionScaler(ms, nil, cluster.NodeID("n1"), []cluster.NodeID{"n1", "n2", "n3"})

	// Partition 4 with RF=2 should start at nodeIDs[4%3]=nodeIDs[1]=n2.
	reps := ps.autoAssignReplicas(4, 2)
	if len(reps) != 2 {
		t.Fatalf("replicas=%v, want 2 entries", reps)
	}
	if reps[0] != "n2" || reps[1] != "n3" {
		t.Fatalf("replicas=%v, want [n2 n3]", reps)
	}
}
