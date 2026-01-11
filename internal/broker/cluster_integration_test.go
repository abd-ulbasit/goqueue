package broker

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"goqueue/internal/cluster"
)

// =============================================================================
// BROKER ↔ CLUSTER INTEGRATION (WRAPPER) TESTS
// =============================================================================
//
// The broker’s `clusterCoordinator` type is a thin wrapper around the cluster
// package. It’s easy for this file to sit at 0% coverage because most cluster
// behavior is exercised in `internal/cluster` tests.
//
// WHY TEST THE WRAPPER?
//   - These methods are what the broker/API layer calls.
//   - Wrapper-level error mapping and “single-node defaults” can hide bugs.
//   - We want confidence that enabling cluster mode doesn’t silently break
//     basic metadata operations (CreateTopicMeta, leader queries, etc.).
// =============================================================================

func TestClusterCoordinator_WrapperLifecycleAndMetadata(t *testing.T) {
	b := newTestBroker(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	cc, err := newClusterCoordinator(b, &ClusterModeConfig{
		ClientAddress:    "127.0.0.1:0",
		ClusterAddress:   "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Peers:            nil,
		QuorumSize:       1,
	}, logger)
	if err != nil {
		t.Fatalf("newClusterCoordinator failed: %v", err)
	}

	// RegisterRoutes should be safe before Start() (handlers registration only).
	mux := http.NewServeMux()
	cc.RegisterRoutes(mux)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cc.Start(ctx); err != nil {
		t.Fatalf("cc.Start failed: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = cc.Stop(stopCtx)
	}()

	// Basic accessors.
	_ = cc.NodeID()
	_ = cc.ControllerID()
	_ = cc.ClusterSize()
	_ = cc.Stats()

	// In a single-node cluster with quorumSize=1, we expect to become controller.
	// NOTE: Election/leadership notifications can be async (goroutine-based),
	// so we poll briefly instead of assuming it becomes controller immediately.
	deadline := time.Now().Add(2 * time.Second)
	for !cc.IsController() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !cc.IsController() {
		t.Fatalf("expected node to become controller in single-node cluster within timeout")
	}

	// Create topic metadata and verify assignments exist.
	if err := cc.CreateTopicMeta("orders", 2, 1); err != nil {
		t.Fatalf("CreateTopicMeta failed: %v", err)
	}

	meta := cc.coordinator.MetadataStore().GetTopic("orders")
	if meta == nil {
		t.Fatalf("expected metadata store to contain topic")
	}

	if a := cc.coordinator.MetadataStore().GetAssignment("orders", 0); a == nil {
		t.Fatalf("expected assignment for orders/0")
	}

	// Leadership helpers should resolve to self for self-led partitions.
	if !cc.IsLeaderFor("orders", 0) {
		t.Fatalf("expected IsLeaderFor(orders,0)=true")
	}
	if leader := cc.GetLeader("orders", 0); leader != cc.NodeID() {
		t.Fatalf("GetLeader=%s, want %s", leader, cc.NodeID())
	}
	if reps := cc.GetReplicas("orders", 0); len(reps) != 1 || reps[0] != cc.NodeID() {
		t.Fatalf("GetReplicas=%v, want [%s]", reps, cc.NodeID())
	}

	// Delete metadata as controller.
	if err := cc.DeleteTopicMeta("orders"); err != nil {
		t.Fatalf("DeleteTopicMeta failed: %v", err)
	}

	// Exercise wrapper event handlers (log-only behavior today).
	cc.handleMetadataChange(&cluster.ClusterMeta{Version: 1, Topics: map[string]*cluster.TopicMeta{}})
	cc.handleCoordinatorEvent(cluster.CoordinatorEvent{Type: cluster.EventBecameController, Details: "test"})
	cc.handleCoordinatorEvent(cluster.CoordinatorEvent{Type: cluster.EventLostController, Details: "test"})
	cc.handleCoordinatorEvent(cluster.CoordinatorEvent{Type: cluster.EventQuorumLost, Details: "test"})
}
