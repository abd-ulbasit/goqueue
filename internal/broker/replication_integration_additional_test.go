package broker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"goqueue/internal/cluster"
)

func newClusterEnabledTestBroker(t *testing.T) *Broker {
	t.Helper()

	dir := t.TempDir()

	b, err := NewBroker(BrokerConfig{
		DataDir:        dir,
		NodeID:         "node-1",
		LogLevel:       slog.LevelError,
		ClusterEnabled: true,
		ClusterConfig: &ClusterModeConfig{
			ClientAddress:    "127.0.0.1:0",
			ClusterAddress:   "127.0.0.1:0",
			AdvertiseAddress: "127.0.0.1:0",
			Peers:            nil,
			QuorumSize:       1,
		},
		// Replication is wired separately in these tests; we don't enable it on
		// the broker config yet.
	})
	if err != nil {
		t.Fatalf("NewBroker (cluster enabled) failed: %v", err)
	}

	// Ensure we shut everything down even if the test fails early.
	t.Cleanup(func() {
		_ = b.Close()
	})

	// Start cluster coordinator - this is normally done after HTTP server starts,
	// but for unit tests we can start it immediately since we don't need peer connections.
	if b.clusterCoordinator != nil {
		if err := b.StartCluster(); err != nil {
			t.Fatalf("StartCluster() failed: %v", err)
		}
	}

	// In a single-node quorum, leadership should be acquired quickly but can be
	// async (background goroutines). Poll briefly to reduce flake.
	// NOTE: Increased timeout from 2s to 5s after M11 changes added partition
	// elector initialization which can add latency to startup.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.clusterCoordinator != nil && b.clusterCoordinator.IsController() {
			return b
		}
		time.Sleep(50 * time.Millisecond)
	}

	// If we can't become controller, something is badly wrong and many cluster
	// behaviors (metadata creation) won't work.
	t.Fatalf("broker did not become controller in time")
	return nil
}

func TestReplicationCoordinator_NewRequiresClusterCoordinator(t *testing.T) {
	b := newTestBroker(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	if _, err := newReplicationCoordinator(b, logger); err == nil {
		t.Fatalf("newReplicationCoordinator expected error")
	}
}

func TestReplicationCoordinator_StorageCallbacks_ReadMessagesAndApplyFetched(t *testing.T) {
	b := newClusterEnabledTestBroker(t)

	// Create broker storage topic and write a couple messages.
	mustCreateTopic(t, b, "orders", 1)
	if _, _, err := b.Publish("orders", nil, []byte("v0")); err != nil {
		t.Fatalf("Publish(v0) failed: %v", err)
	}
	if _, _, err := b.Publish("orders", nil, []byte("v1")); err != nil {
		t.Fatalf("Publish(v1) failed: %v", err)
	}

	// Create matching cluster metadata so replication components have something
	// to enumerate.
	if err := b.clusterCoordinator.CreateTopicMeta("orders", 1, 1); err != nil {
		t.Fatalf("CreateTopicMeta failed: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	rc, err := newReplicationCoordinator(b, logger)
	if err != nil {
		t.Fatalf("newReplicationCoordinator failed: %v", err)
	}
	defer func() {
		// Ensure any background fetchers are stopped (BecomeFollower starts one).
		_ = rc.Stop(context.Background())
	}()

	// ---------------------------------------------------------------------
	// readMessagesFromStorage: error paths
	// ---------------------------------------------------------------------
	if _, _, err := rc.readMessagesFromStorage("missing", 0, 0, 1024); err == nil {
		t.Fatalf("readMessagesFromStorage(missing) expected error")
	}
	if _, _, err := rc.readMessagesFromStorage("orders", 99, 0, 1024); err == nil {
		t.Fatalf("readMessagesFromStorage(bad partition) expected error")
	}
	if _, _, err := rc.readMessagesFromStorage("orders", 0, -1, 1024); err == nil {
		t.Fatalf("readMessagesFromStorage(fromOffset<earliest) expected error")
	}

	// ---------------------------------------------------------------------
	// readMessagesFromStorage: happy path + bounds
	// ---------------------------------------------------------------------
	msgs, leo, err := rc.readMessagesFromStorage("orders", 0, 0, 2048)
	if err != nil {
		t.Fatalf("readMessagesFromStorage failed: %v", err)
	}
	if leo < 2 {
		t.Fatalf("logEndOffset=%d, want >=2", leo)
	}
	if len(msgs) == 0 {
		t.Fatalf("expected at least one replicated message")
	}
	if msgs[0].Offset != 0 {
		t.Fatalf("first offset=%d, want 0", msgs[0].Offset)
	}

	// If we ask to read starting at the current log end offset, we should get an
	// empty slice and no error.
	msgs, leo2, err := rc.readMessagesFromStorage("orders", 0, leo, 1024)
	if err != nil {
		t.Fatalf("readMessagesFromStorage(fromOffset=LEO) failed: %v", err)
	}
	if leo2 != leo {
		t.Fatalf("logEndOffset=%d, want %d", leo2, leo)
	}
	if len(msgs) != 0 {
		t.Fatalf("messages=%d, want 0", len(msgs))
	}

	// ---------------------------------------------------------------------
	// toStorageMessage conversion (ms -> ns)
	// ---------------------------------------------------------------------
	sm := rc.toStorageMessage(&cluster.ReplicatedMessage{Timestamp: 2})
	const msToNs = int64(1_000_000)
	if sm.Timestamp != 2*msToNs {
		t.Fatalf("storage timestamp=%d, want %d", sm.Timestamp, 2*msToNs)
	}

	// ---------------------------------------------------------------------
	// ApplyFetchedMessages: success path requires the local replica to be a
	// follower in the replica manager.
	// ---------------------------------------------------------------------
	if err := rc.replicaManager.BecomeFollower(
		"orders",
		0,
		cluster.NodeID("leader-1"),
		"127.0.0.1:0",
		1,
		0,
	); err != nil {
		t.Fatalf("BecomeFollower failed: %v", err)
	}

	// Apply a replicated message at offset 0 to the broker's storage.
	if err := rc.ApplyFetchedMessages(
		"orders",
		0,
		[]cluster.ReplicatedMessage{{Offset: 0, Timestamp: 10, Key: []byte("k"), Value: []byte("v")}},
		0,
		1,
	); err != nil {
		t.Fatalf("ApplyFetchedMessages failed: %v", err)
	}

	// Error mapping: missing topic / partition should be caught before calling
	// into ReplicaManager.
	if err := rc.ApplyFetchedMessages("missing", 0, nil, 0, 0); err == nil {
		t.Fatalf("ApplyFetchedMessages(missing topic) expected error")
	}
	if err := rc.ApplyFetchedMessages("orders", 99, nil, 0, 0); err == nil {
		t.Fatalf("ApplyFetchedMessages(missing partition) expected error")
	}
}
