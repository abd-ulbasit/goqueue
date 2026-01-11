package cluster

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCoordinator_StartAndStop_SingleNodeLifecycle(t *testing.T) {
	// ============================================================================
	// COORDINATOR LIFECYCLE (START/STOP)
	// ============================================================================
	//
	// WHY:
	//   The coordinator is the orchestration "brain" for cluster mode.
	//   We want confidence that:
	//     - bootstrap sequence runs
	//     - background goroutines start
	//     - Stop() cancels cleanly (no deadlocks)
	//
	// NOTE:
	//   We intentionally do NOT start a ClusterServer here; Stop() will attempt
	//   a best-effort leave that may fail quickly (and should be handled).
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(ioDiscard{}, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := (&ClusterConfig{
		NodeID:         "node-A",
		ClientAddress:  "127.0.0.1:18080",
		ClusterAddress: "127.0.0.1:19000",
		QuorumSize:     1,
		// Keep timeouts small so tests run fast.
		HeartbeatInterval:  50 * time.Millisecond,
		SuspectTimeout:     150 * time.Millisecond,
		DeadTimeout:        250 * time.Millisecond,
		LeaseTimeout:       300 * time.Millisecond,
		LeaseRenewInterval: 100 * time.Millisecond,
		BootstrapTimeout:   100 * time.Millisecond,
	}).WithDefaults()

	c, err := NewCoordinator(cfg, t.TempDir(), logger)
	if err != nil {
		t.Fatalf("NewCoordinator failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	select {
	case <-c.Ready():
		// ok
	case <-time.After(time.Second):
		t.Fatalf("expected Ready() to be closed")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stopCancel()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestCoordinator_Start_TwiceReturnsError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(ioDiscard{}, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := (&ClusterConfig{
		NodeID:             "node-A",
		ClientAddress:      "127.0.0.1:18080",
		ClusterAddress:     "127.0.0.1:19000",
		QuorumSize:         1,
		HeartbeatInterval:  50 * time.Millisecond,
		SuspectTimeout:     150 * time.Millisecond,
		DeadTimeout:        250 * time.Millisecond,
		LeaseTimeout:       300 * time.Millisecond,
		LeaseRenewInterval: 100 * time.Millisecond,
	}).WithDefaults()

	c, err := NewCoordinator(cfg, t.TempDir(), logger)
	if err != nil {
		t.Fatalf("NewCoordinator failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer stopCancel()
		_ = c.Stop(stopCtx)
	}()

	if err := c.Start(ctx); err == nil {
		t.Fatalf("expected second Start() to return error")
	}
}

func TestCoordinator_syncMetadataToFollowers_PushesToAliveFollowers(t *testing.T) {
	// ============================================================================
	// CONTROLLER METADATA SYNC
	// ============================================================================
	//
	// WHY:
	//   Controllers are responsible for pushing cluster metadata to followers.
	//   If this breaks, followers will serve stale topic/partition assignments.
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(ioDiscard{}, &slog.HandlerOptions{Level: slog.LevelError}))

	// Follower server that records the metadata push.
	got := make(chan *ClusterMeta, 1)
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/metadata" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		var req struct {
			Metadata *ClusterMeta `json:"metadata"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		got <- req.Metadata
		_ = json.NewEncoder(w).Encode(map[string]bool{"success": true})
	}))
	defer follower.Close()

	followerAddr := strings.TrimPrefix(follower.URL, "http://")

	cfg := (&ClusterConfig{
		NodeID:             "controller-1",
		ClientAddress:      "127.0.0.1:18080",
		ClusterAddress:     "127.0.0.1:19000",
		QuorumSize:         1,
		HeartbeatInterval:  50 * time.Millisecond,
		SuspectTimeout:     150 * time.Millisecond,
		DeadTimeout:        250 * time.Millisecond,
		LeaseTimeout:       300 * time.Millisecond,
		LeaseRenewInterval: 100 * time.Millisecond,
	}).WithDefaults()

	c, err := NewCoordinator(cfg, t.TempDir(), logger)
	if err != nil {
		t.Fatalf("NewCoordinator failed: %v", err)
	}
	if err := c.membership.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf failed: %v", err)
	}

	// Add a follower node as alive.
	followerInfo := mustNodeInfo(t, "follower-1", "127.0.0.1:28080", followerAddr)
	if err := c.membership.AddNode(followerInfo); err != nil {
		t.Fatalf("AddNode(follower) failed: %v", err)
	}

	// Mark self as controller so IsController() branch triggers sync.
	setElectorLeader(t, c.elector)
	if err := c.membership.SetController(c.node.ID(), 1); err != nil {
		t.Fatalf("SetController failed: %v", err)
	}

	// Seed metadata.
	meta := NewClusterMeta()
	meta.Version = 42
	meta.Topics["orders"] = &TopicMeta{Name: "orders", PartitionCount: 3, ReplicationFactor: 1, CreatedAt: time.Now(), UpdatedAt: time.Now(), Config: DefaultTopicConfig()}
	if err := c.metadataStore.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta failed: %v", err)
	}

	// We need a non-nil context for the per-node push timeout.
	c.ctx = context.Background()

	c.syncMetadataToFollowers()

	select {
	case pushed := <-got:
		if pushed == nil || pushed.Version != 42 {
			t.Fatalf("pushed meta=%v, want version 42", pushed)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected metadata push to follower")
	}
}

func TestCoordinator_joinViaDiscovery_AppliesStateAndEmitsEvent(t *testing.T) {
	// ============================================================================
	// JOIN VIA DISCOVERY (HAPPY PATH)
	// ============================================================================
	//
	// WHY:
	//   `Coordinator.joinViaDiscovery` is a critical bootstrap step: it is what
	//   turns a process from "standalone" into "cluster member".
	//
	// BUG CLASS WE CARE ABOUT:
	//   - Join succeeds but membership state is not applied.
	//   - Join succeeds but no event is emitted (operators lose visibility).
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(ioDiscard{}, &slog.HandlerOptions{Level: slog.LevelError}))

	// Peer server always accepts join and returns a well-formed cluster state.
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/join" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		var req JoinRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		state := &ClusterState{
			Version:         10,
			ControllerID:    "controller-1",
			ControllerEpoch: 7,
			Nodes: map[NodeID]*NodeInfo{
				"controller-1": mustNodeInfo(t, "controller-1", "127.0.0.1:28080", "127.0.0.1:29000"),
				req.NodeID:     mustNodeInfo(t, req.NodeID, "127.0.0.1:18080", "127.0.0.1:19000"),
			},
		}

		resp := JoinResponse{
			Success:          true,
			ClusterState:     state,
			ControllerID:     state.ControllerID,
			ControllerAddr:   "",
			RedirectRequired: false,
		}
		_ = json.NewEncoder(w).Encode(&resp)
	}))
	defer peer.Close()

	peerAddr := strings.TrimPrefix(peer.URL, "http://")

	cfg := (&ClusterConfig{
		NodeID:         "node-A",
		ClientAddress:  "127.0.0.1:18080",
		ClusterAddress: "127.0.0.1:19000",
		Peers:          []string{peerAddr},
		QuorumSize:     1,
		// Satisfy validation but keep any timers short.
		LeaseTimeout:       300 * time.Millisecond,
		LeaseRenewInterval: 100 * time.Millisecond,
	}).WithDefaults()

	c, err := NewCoordinator(cfg, t.TempDir(), logger)
	if err != nil {
		t.Fatalf("NewCoordinator failed: %v", err)
	}
	if err := c.membership.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf failed: %v", err)
	}

	c.ctx = context.Background()

	events := make(chan CoordinatorEvent, 1)
	c.AddEventListener(func(ev CoordinatorEvent) {
		if ev.Type == EventJoinedCluster {
			events <- ev
		}
	})

	if err := c.joinViaDiscovery(); err != nil {
		t.Fatalf("joinViaDiscovery failed: %v", err)
	}

	// Assert membership state was applied.
	if got := c.membership.ControllerID(); got != "controller-1" {
		t.Fatalf("ControllerID=%s, want controller-1", got)
	}
	if got := c.membership.ControllerEpoch(); got != 7 {
		t.Fatalf("ControllerEpoch=%d, want 7", got)
	}
	if got := c.membership.GetNode("controller-1"); got == nil {
		t.Fatalf("expected controller node in membership")
	}

	select {
	case <-events:
		// ok
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected EventJoinedCluster")
	}
}

func TestCoordinator_joinViaDiscovery_RedirectToController(t *testing.T) {
	// ============================================================================
	// JOIN VIA DISCOVERY (REDIRECT)
	// ============================================================================
	//
	// WHY:
	//   A node may attempt to join via a non-controller peer.
	//   The peer should redirect it to the controller.
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(ioDiscard{}, &slog.HandlerOptions{Level: slog.LevelError}))

	var controller *httptest.Server
	controller = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/join" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		var req JoinRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		state := &ClusterState{
			Version:         10,
			ControllerID:    "controller-1",
			ControllerEpoch: 9,
			Nodes: map[NodeID]*NodeInfo{
				"controller-1": mustNodeInfo(t, "controller-1", "127.0.0.1:28080", strings.TrimPrefix(controller.URL, "http://")),
				req.NodeID:     mustNodeInfo(t, req.NodeID, "127.0.0.1:18080", "127.0.0.1:19000"),
			},
		}

		resp := JoinResponse{Success: true, ClusterState: state, ControllerID: state.ControllerID}
		_ = json.NewEncoder(w).Encode(&resp)
	}))
	defer controller.Close()

	controllerAddr := strings.TrimPrefix(controller.URL, "http://")

	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/join" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		resp := JoinResponse{
			Success:          false,
			Error:            "not controller",
			ControllerID:     "controller-1",
			ControllerAddr:   controllerAddr,
			RedirectRequired: true,
		}
		_ = json.NewEncoder(w).Encode(&resp)
	}))
	defer peer.Close()

	peerAddr := strings.TrimPrefix(peer.URL, "http://")

	cfg := (&ClusterConfig{
		NodeID:             "node-A",
		ClientAddress:      "127.0.0.1:18080",
		ClusterAddress:     "127.0.0.1:19000",
		Peers:              []string{peerAddr},
		QuorumSize:         1,
		LeaseTimeout:       300 * time.Millisecond,
		LeaseRenewInterval: 100 * time.Millisecond,
	}).WithDefaults()

	c, err := NewCoordinator(cfg, t.TempDir(), logger)
	if err != nil {
		t.Fatalf("NewCoordinator failed: %v", err)
	}
	if err := c.membership.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf failed: %v", err)
	}

	c.ctx = context.Background()

	if err := c.joinViaDiscovery(); err != nil {
		t.Fatalf("joinViaDiscovery failed: %v", err)
	}

	if got := c.membership.ControllerID(); got != "controller-1" {
		t.Fatalf("ControllerID=%s, want controller-1", got)
	}
}

func TestCoordinator_joinViaDiscovery_SuccessWithoutClusterStateIsRejected(t *testing.T) {
	// ============================================================================
	// JOIN VIA DISCOVERY (MALFORMED SUCCESS RESPONSE)
	// ============================================================================
	//
	// WHY:
	//   We should not panic if a peer responds with Success=true but omits the
	//   ClusterState. That could happen due to a buggy peer version or partial
	//   upgrades.
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(ioDiscard{}, &slog.HandlerOptions{Level: slog.LevelError}))

	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := JoinResponse{Success: true, ClusterState: nil, ControllerID: "controller-1"}
		_ = json.NewEncoder(w).Encode(&resp)
	}))
	defer peer.Close()

	peerAddr := strings.TrimPrefix(peer.URL, "http://")

	cfg := (&ClusterConfig{
		NodeID:             "node-A",
		ClientAddress:      "127.0.0.1:18080",
		ClusterAddress:     "127.0.0.1:19000",
		Peers:              []string{peerAddr},
		QuorumSize:         1,
		LeaseTimeout:       300 * time.Millisecond,
		LeaseRenewInterval: 100 * time.Millisecond,
	}).WithDefaults()

	c, err := NewCoordinator(cfg, t.TempDir(), logger)
	if err != nil {
		t.Fatalf("NewCoordinator failed: %v", err)
	}
	if err := c.membership.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf failed: %v", err)
	}

	c.ctx = context.Background()

	if err := c.joinViaDiscovery(); err == nil {
		t.Fatalf("expected joinViaDiscovery to fail for missing ClusterState")
	}
}

func TestCoordinator_waitForQuorum_TimesOutQuickly(t *testing.T) {
	// ============================================================================
	// WAIT FOR QUORUM (TIMEOUT PATH)
	// ============================================================================
	//
	// WHY:
	//   When starting a multi-node cluster, bootstrap must not hang forever.
	//   This test covers the timeout branch deterministically.
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(ioDiscard{}, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := (&ClusterConfig{
		NodeID:             "node-A",
		ClientAddress:      "127.0.0.1:18080",
		ClusterAddress:     "127.0.0.1:19000",
		Peers:              []string{"127.0.0.1:9999"},
		QuorumSize:         2,
		BootstrapTimeout:   50 * time.Millisecond,
		LeaseTimeout:       300 * time.Millisecond,
		LeaseRenewInterval: 100 * time.Millisecond,
	}).WithDefaults()

	c, err := NewCoordinator(cfg, t.TempDir(), logger)
	if err != nil {
		t.Fatalf("NewCoordinator failed: %v", err)
	}
	if err := c.membership.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf failed: %v", err)
	}

	c.ctx = context.Background()

	err = c.waitForQuorum()
	if err == nil {
		t.Fatalf("expected quorum wait to timeout")
	}
}
