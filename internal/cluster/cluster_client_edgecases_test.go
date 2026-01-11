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

func TestClusterClient_RequestLeave_NoController_ReturnsError(t *testing.T) {
	h := newTestClusterHarnessNoHTTP(t, "client-1")
	cc := NewClusterClient(h.node, h.membership, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := cc.RequestLeave(ctx)
	if err == nil {
		t.Fatalf("expected error when no controller is known")
	}
}

func TestClusterClient_RequestLeave_ControllerNotFound_ReturnsError(t *testing.T) {
	h := newTestClusterHarnessNoHTTP(t, "client-1")

	// Simulate a stale/partial membership view where we *think* there is a
	// controller, but we don't have its NodeInfo (e.g., disk corruption or a bug
	// in state application). We can't use SetController here because it validates
	// that the controller node exists.
	h.membership.mu.Lock()
	h.membership.state.ControllerID = "controller-1"
	h.membership.state.ControllerEpoch = 1
	h.membership.mu.Unlock()

	cc := NewClusterClient(h.node, h.membership, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := cc.RequestLeave(ctx)
	if err == nil {
		t.Fatalf("expected error when controller info is missing")
	}
}

func TestClusterClient_RequestVote_TargetNotFound_ReturnsError(t *testing.T) {
	h := newTestClusterHarnessNoHTTP(t, "client-1")
	cc := NewClusterClient(h.node, h.membership, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := cc.RequestVote(ctx, "unknown-node", 1)
	if err == nil {
		t.Fatalf("expected error when target node is missing")
	}
}

func TestClusterClient_doRequest_Non200IncludesBody(t *testing.T) {
	// ============================================================================
	// doRequest ERROR SHAPE
	// ============================================================================
	//
	// WHY:
	//   When requests fail, having the body in the error dramatically improves
	//   debuggability (operators see the reason, not just the HTTP code).
	// ============================================================================

	h := newTestClusterHarnessNoHTTP(t, "client-1")
	cc := NewClusterClient(h.node, h.membership, slog.Default())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()

	peerAddr := strings.TrimPrefix(srv.URL, "http://")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := cc.RequestJoin(ctx, peerAddr)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "request failed") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("error=%q, want to include status and body", err.Error())
	}
}

func TestClusterClient_doRequest_DecodeError(t *testing.T) {
	h := newTestClusterHarnessNoHTTP(t, "client-1")
	cc := NewClusterClient(h.node, h.membership, slog.Default())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not-json"))
	}))
	defer srv.Close()

	peerAddr := strings.TrimPrefix(srv.URL, "http://")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := cc.RequestJoin(ctx, peerAddr)
	if err == nil {
		t.Fatalf("expected decode error")
	}
	if !strings.Contains(err.Error(), "decode response") {
		t.Fatalf("error=%q, want decode response", err.Error())
	}
}

func TestClusterClient_BroadcastHeartbeats_MergesStateFromPeer(t *testing.T) {
	// ============================================================================
	// GOSSIP MERGE VIA HEARTBEAT
	// ============================================================================
	//
	// WHY:
	//   BroadcastHeartbeats implements a lightweight gossip mechanism.
	//   The property we need is: if a peer knows about a node we don't, we learn
	//   about it by merging the peer's heartbeat response.
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(ioDiscard{}, &slog.HandlerOptions{Level: slog.LevelError}))

	// Remote node returns a heartbeat response that includes a new node we don't
	// currently know about.
	hit := make(chan struct{}, 1)
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/heartbeat" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		resp := HeartbeatResponse{
			NodeID:          "node-2",
			Version:         10,
			ControllerID:    "controller-1",
			ControllerEpoch: 3,
			Nodes: map[NodeID]*NodeInfo{
				"node-2": mustNodeInfo(t, "node-2", "127.0.0.1:28080", strings.TrimPrefix(srv.URL, "http://")),
				"node-3": mustNodeInfo(t, "node-3", "127.0.0.1:38080", "127.0.0.1:39000"),
			},
		}

		_ = json.NewEncoder(w).Encode(&resp)
		hit <- struct{}{}
	}))
	defer srv.Close()

	remoteAddr := strings.TrimPrefix(srv.URL, "http://")

	cfg := (&ClusterConfig{
		NodeID:             "node-1",
		ClientAddress:      "127.0.0.1:18080",
		ClusterAddress:     "127.0.0.1:19000",
		LeaseTimeout:       300 * time.Millisecond,
		LeaseRenewInterval: 100 * time.Millisecond,
	}).WithDefaults()

	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	m := NewMembership(n, cfg, t.TempDir())
	if err := m.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf failed: %v", err)
	}

	// Add remote node as alive so BroadcastHeartbeats targets it.
	remoteInfo := mustNodeInfo(t, "node-2", "127.0.0.1:28080", remoteAddr)
	if err := m.AddNode(remoteInfo); err != nil {
		t.Fatalf("AddNode(remote) failed: %v", err)
	}

	cc := NewClusterClient(n, m, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cc.BroadcastHeartbeats(ctx)

	select {
	case <-hit:
		// Remote was contacted.
	case <-time.After(time.Second):
		t.Fatalf("expected heartbeat request to remote")
	}

	// BroadcastHeartbeats performs merges in goroutines; wait briefly for the merge.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if m.GetNode("node-3") != nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("expected merged membership to contain node-3")
}
