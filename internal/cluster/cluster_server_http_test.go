package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// testClusterHarness wires up an in-memory ClusterServer with real Membership,
// FailureDetector, ControllerElector, and MetadataStore.
//
// WHY THIS STYLE?
// - We want high behavioral coverage of the HTTP handlers and client methods.
// - We keep the components real (not mocks) so tests catch architectural flaws
//   at integration boundaries (e.g., controller redirects, membership updates).
// - We avoid starting long-lived background loops (failure detector / elector
//   Start) to keep tests deterministic.
//
// NOTE: Some elector methods create timers even if Start() wasn't called.
// We always Stop() it in cleanup to prevent timer goroutine leaks.

type testClusterHarness struct {
	cfg        *ClusterConfig
	node       *Node
	membership *Membership
	elector    *ControllerElector
	fd         *FailureDetector
	meta       *MetadataStore
	server     *ClusterServer
	httpServer *httptest.Server
	addr       string // host:port
}

func newTestClusterHarness(t *testing.T, nodeID string) *testClusterHarness {
	t.Helper()

	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(ioDiscard{}, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := (&ClusterConfig{
		NodeID:         nodeID,
		ClientAddress:  "127.0.0.1:18080",
		ClusterAddress: "127.0.0.1:19000",
		DataDir:        dir,
		// Keep any created lease timers short, but satisfy config validation
		// (LeaseRenewInterval must be less than LeaseTimeout).
		LeaseTimeout:       300 * time.Millisecond,
		LeaseRenewInterval: 100 * time.Millisecond,
	}).WithDefaults()

	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	m := NewMembership(n, cfg, dir)
	if err := m.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf failed: %v", err)
	}

	fd := NewFailureDetector(m, cfg)
	meta := NewMetadataStore(dir)
	ce := NewControllerElector(n, m, cfg)
	t.Cleanup(func() { ce.Stop() })

	cs := NewClusterServer(n, m, fd, ce, meta, logger)

	mux := http.NewServeMux()
	cs.RegisterRoutes(mux)

	hs := httptest.NewServer(mux)
	t.Cleanup(hs.Close)

	addr := strings.TrimPrefix(hs.URL, "http://")

	return &testClusterHarness{
		cfg:        cfg,
		node:       n,
		membership: m,
		elector:    ce,
		fd:         fd,
		meta:       meta,
		server:     cs,
		httpServer: hs,
		addr:       addr,
	}
}

// newTestClusterHarnessNoHTTP creates a harness without starting an HTTP server.
//
// We use this for ClusterClient-only tests where we need a Node + Membership but
// don't want to consume ports / start extra HTTP listeners.
func newTestClusterHarnessNoHTTP(t *testing.T, nodeID string) *testClusterHarness {
	t.Helper()

	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(ioDiscard{}, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := (&ClusterConfig{
		NodeID:             nodeID,
		ClientAddress:      "127.0.0.1:18080",
		ClusterAddress:     "127.0.0.1:19000",
		DataDir:            dir,
		LeaseTimeout:       300 * time.Millisecond,
		LeaseRenewInterval: 100 * time.Millisecond,
	}).WithDefaults()

	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	m := NewMembership(n, cfg, dir)
	if err := m.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf failed: %v", err)
	}

	fd := NewFailureDetector(m, cfg)
	meta := NewMetadataStore(dir)
	ce := NewControllerElector(n, m, cfg)
	t.Cleanup(func() { ce.Stop() })

	cs := NewClusterServer(n, m, fd, ce, meta, logger)

	return &testClusterHarness{
		cfg:        cfg,
		node:       n,
		membership: m,
		elector:    ce,
		fd:         fd,
		meta:       meta,
		server:     cs,
		httpServer: nil,
		addr:       "",
	}
}

// ioDiscard is a tiny io.Writer to silence slog output in tests.
// We keep it here to avoid pulling in a logging dependency.
type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

func postJSON(t *testing.T, url string, body any, out any) int {
	t.Helper()

	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}

	return resp.StatusCode
}

func getJSON(t *testing.T, url string, out any) int {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("http get: %v", err)
	}
	defer resp.Body.Close()

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}

	return resp.StatusCode
}

func mustNodeInfo(t *testing.T, id NodeID, clientAddr, clusterAddr string) *NodeInfo {
	t.Helper()

	ca, err := ParseNodeAddress(clientAddr)
	if err != nil {
		t.Fatalf("ParseNodeAddress(client): %v", err)
	}
	ia, err := ParseNodeAddress(clusterAddr)
	if err != nil {
		t.Fatalf("ParseNodeAddress(cluster): %v", err)
	}

	return &NodeInfo{
		ID:             id,
		ClientAddress:  ca,
		ClusterAddress: ia,
		Status:         NodeStatusAlive,
		Role:           NodeRoleFollower,
		JoinedAt:       time.Now(),
		LastHeartbeat:  time.Now(),
		Version:        "test",
	}
}

func setElectorLeader(t *testing.T, ce *ControllerElector) {
	t.Helper()
	ce.mu.Lock()
	defer ce.mu.Unlock()
	ce.state = ControllerStateLeader
	ce.currentControllerID = ce.localNode.ID()
}

func TestClusterServer_Heartbeat_RecordsAndReturnsState(t *testing.T) {
	h := newTestClusterHarness(t, "node-1")

	// Make sure membership has a controller to report.
	setElectorLeader(t, h.elector)
	if err := h.membership.SetController(h.node.ID(), 1); err != nil {
		t.Fatalf("SetController failed: %v", err)
	}

	var resp HeartbeatResponse
	status := postJSON(t,
		h.httpServer.URL+"/cluster/heartbeat",
		HeartbeatRequest{NodeID: "peer-1", Timestamp: time.Now(), ControllerID: h.membership.ControllerID(), Epoch: h.membership.ControllerEpoch()},
		&resp,
	)
	if status != http.StatusOK {
		t.Fatalf("status=%d, want %d", status, http.StatusOK)
	}
	if resp.NodeID != h.node.ID() {
		t.Fatalf("resp.NodeID=%s, want %s", resp.NodeID, h.node.ID())
	}
	if resp.ControllerID != h.node.ID() {
		t.Fatalf("resp.ControllerID=%s, want %s", resp.ControllerID, h.node.ID())
	}

	// The server should have recorded a heartbeat for the sender.
	if _, ok := h.fd.LastHeartbeat("peer-1"); !ok {
		t.Fatalf("expected failure detector to record heartbeat")
	}
}

func TestClusterServer_Join_AsNonController_ReturnsRedirect(t *testing.T) {
	h := newTestClusterHarness(t, "node-1")

	controllerID := NodeID("controller-1")
	controllerInfo := mustNodeInfo(t, controllerID, "127.0.0.1:28080", "127.0.0.1:29000")
	if err := h.membership.AddNode(controllerInfo); err != nil {
		t.Fatalf("AddNode(controller) failed: %v", err)
	}
	if err := h.membership.SetController(controllerID, 10); err != nil {
		t.Fatalf("SetController failed: %v", err)
	}

	joiner := mustNodeInfo(t, "joiner-1", "127.0.0.1:38080", "127.0.0.1:39000")
	var resp JoinResponse
	status := postJSON(t, h.httpServer.URL+"/cluster/join", JoinRequest{NodeID: joiner.ID, NodeInfo: *joiner}, &resp)
	if status != http.StatusOK {
		t.Fatalf("status=%d, want %d", status, http.StatusOK)
	}
	if resp.Success {
		t.Fatalf("expected Success=false for non-controller")
	}
	if !resp.RedirectRequired {
		t.Fatalf("expected RedirectRequired=true")
	}
	if resp.ControllerID != controllerID {
		t.Fatalf("ControllerID=%s, want %s", resp.ControllerID, controllerID)
	}
	if resp.ControllerAddr != controllerInfo.ClusterAddress.String() {
		t.Fatalf("ControllerAddr=%q, want %q", resp.ControllerAddr, controllerInfo.ClusterAddress.String())
	}

	// Non-controller should not mutate membership.
	if got := h.membership.GetNode(joiner.ID); got != nil {
		t.Fatalf("joiner unexpectedly added to membership")
	}
}

func TestClusterServer_Join_AsController_AddsNode(t *testing.T) {
	h := newTestClusterHarness(t, "node-1")

	setElectorLeader(t, h.elector)
	if err := h.membership.SetController(h.node.ID(), 1); err != nil {
		t.Fatalf("SetController failed: %v", err)
	}

	joiner := mustNodeInfo(t, "joiner-1", "127.0.0.1:38080", "127.0.0.1:39000")
	var resp JoinResponse
	status := postJSON(t, h.httpServer.URL+"/cluster/join", JoinRequest{NodeID: joiner.ID, NodeInfo: *joiner}, &resp)
	if status != http.StatusOK {
		t.Fatalf("status=%d, want %d", status, http.StatusOK)
	}
	if !resp.Success {
		t.Fatalf("expected join success")
	}
	if got := h.membership.GetNode(joiner.ID); got == nil {
		t.Fatalf("expected joiner to be added")
	}
}

func TestClusterServer_Leave_AsController_RemovesNode(t *testing.T) {
	h := newTestClusterHarness(t, "node-1")

	setElectorLeader(t, h.elector)
	if err := h.membership.SetController(h.node.ID(), 1); err != nil {
		t.Fatalf("SetController failed: %v", err)
	}

	leaver := mustNodeInfo(t, "leaver-1", "127.0.0.1:48080", "127.0.0.1:49000")
	if err := h.membership.AddNode(leaver); err != nil {
		t.Fatalf("AddNode(leaver) failed: %v", err)
	}

	var resp LeaveResponse
	status := postJSON(t, h.httpServer.URL+"/cluster/leave", LeaveRequest{NodeID: leaver.ID}, &resp)
	if status != http.StatusOK {
		t.Fatalf("status=%d, want %d", status, http.StatusOK)
	}
	if !resp.Success {
		t.Fatalf("expected leave success")
	}
	if got := h.membership.GetNode(leaver.ID); got != nil {
		t.Fatalf("expected node to be removed")
	}
}

func TestClusterServer_GetStateAndMetadata(t *testing.T) {
	h := newTestClusterHarness(t, "node-1")

	setElectorLeader(t, h.elector)
	if err := h.membership.SetController(h.node.ID(), 42); err != nil {
		t.Fatalf("SetController failed: %v", err)
	}

	// Apply some metadata so we can assert it round-trips.
	meta := NewClusterMeta()
	meta.Version = 7
	meta.Topics["orders"] = &TopicMeta{Name: "orders", PartitionCount: 3, ReplicationFactor: 1, CreatedAt: time.Now(), UpdatedAt: time.Now(), Config: DefaultTopicConfig()}
	if err := h.meta.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta failed: %v", err)
	}

	var resp StateSyncResponse
	status := getJSON(t, h.httpServer.URL+"/cluster/state", &resp)
	if status != http.StatusOK {
		t.Fatalf("status=%d, want %d", status, http.StatusOK)
	}
	if resp.ControllerID != h.node.ID() {
		t.Fatalf("ControllerID=%s, want %s", resp.ControllerID, h.node.ID())
	}
	if resp.ClusterMeta == nil || resp.ClusterMeta.Version != 7 {
		t.Fatalf("ClusterMeta.Version=%v, want 7", resp.ClusterMeta)
	}
}

func TestClusterServer_VoteEndpoint_GrantsAndDenies(t *testing.T) {
	h := newTestClusterHarness(t, "node-1")

	// Vote request should grant the first vote in an epoch.
	var resp1 ControllerVoteResponse
	status := postJSON(t, h.httpServer.URL+"/cluster/vote", ControllerVoteRequest{CandidateID: "cand-1", Epoch: 1}, &resp1)
	if status != http.StatusOK {
		t.Fatalf("status=%d, want %d", status, http.StatusOK)
	}
	if !resp1.VoteGranted {
		t.Fatalf("expected vote granted")
	}

	// A different candidate in the same epoch should be denied.
	var resp2 ControllerVoteResponse
	_ = postJSON(t, h.httpServer.URL+"/cluster/vote", ControllerVoteRequest{CandidateID: "cand-2", Epoch: 1}, &resp2)
	if resp2.VoteGranted {
		t.Fatalf("expected vote denied after voting for someone else")
	}
}

func TestClusterServer_MetadataEndpoint_AppliesMeta(t *testing.T) {
	h := newTestClusterHarness(t, "node-1")

	meta := NewClusterMeta()
	meta.Version = 123

	var resp map[string]bool
	status := postJSON(t, h.httpServer.URL+"/cluster/metadata", struct {
		Metadata *ClusterMeta `json:"metadata"`
	}{Metadata: meta}, &resp)
	if status != http.StatusOK {
		t.Fatalf("status=%d, want %d", status, http.StatusOK)
	}

	got := h.meta.Meta()
	if got.Version != 123 {
		t.Fatalf("meta.Version=%d, want 123", got.Version)
	}
}

func TestClusterClient_RoundTrip_ServerEndpoints(t *testing.T) {
	h := newTestClusterHarness(t, "server-1")

	// Mark server as controller for leave redirect semantics.
	setElectorLeader(t, h.elector)
	if err := h.membership.SetController(h.node.ID(), 1); err != nil {
		t.Fatalf("SetController failed: %v", err)
	}

	// Create a client node + membership that knows how to reach the server.
	clientHarness := newTestClusterHarnessNoHTTP(t, "client-1")

	serverInfo := mustNodeInfo(t, h.node.ID(), "127.0.0.1:18080", h.addr)
	if err := clientHarness.membership.AddNode(serverInfo); err != nil {
		t.Fatalf("AddNode(server) failed: %v", err)
	}
	if err := clientHarness.membership.SetController(h.node.ID(), 1); err != nil {
		t.Fatalf("SetController failed: %v", err)
	}

	cc := NewClusterClient(clientHarness.node, clientHarness.membership, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// SendHeartbeat hits /cluster/heartbeat.
	hb, err := cc.SendHeartbeat(ctx, h.node.ID())
	if err != nil {
		t.Fatalf("SendHeartbeat failed: %v", err)
	}
	if hb.NodeID != h.node.ID() {
		t.Fatalf("hb.NodeID=%s, want %s", hb.NodeID, h.node.ID())
	}

	// FetchState hits /cluster/state.
	state, err := cc.FetchState(ctx, h.node.ID())
	if err != nil {
		t.Fatalf("FetchState failed: %v", err)
	}
	if state.ControllerID != h.node.ID() {
		t.Fatalf("state.ControllerID=%s, want %s", state.ControllerID, h.node.ID())
	}

	// PushMetadata hits /cluster/metadata.
	meta := NewClusterMeta()
	meta.Version = 999
	if err := cc.PushMetadata(ctx, h.node.ID(), meta); err != nil {
		t.Fatalf("PushMetadata failed: %v", err)
	}
	if got := h.meta.Meta(); got.Version != 999 {
		t.Fatalf("server meta.Version=%d, want 999", got.Version)
	}
}

func TestClusterServer_HealthEndpoint_AndJSONError(t *testing.T) {
	// ============================================================================
	// HEALTH + ERROR SHAPE
	// ============================================================================
	//
	// WHY:
	//   - /cluster/health is what operators hit first when debugging a cluster.
	//   - jsonError() is the shared error serializer; if it breaks, clients get
	//     unparseable error bodies.
	// ============================================================================

	h := newTestClusterHarness(t, "node-1")

	// Health should always succeed (even without controller election started).
	var health map[string]any
	status := getJSON(t, h.httpServer.URL+"/cluster/health", &health)
	if status != http.StatusOK {
		t.Fatalf("health status=%d want=%d", status, http.StatusOK)
	}
	if health["node_id"] == nil {
		t.Fatalf("expected node_id in health response")
	}

	// Trigger a handler JSON decode error to ensure jsonError() is exercised.
	resp, err := http.Post(h.httpServer.URL+"/cluster/heartbeat", "application/json", bytes.NewBufferString("{"))
	if err != nil {
		t.Fatalf("POST invalid heartbeat: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid heartbeat status=%d want=%d", resp.StatusCode, http.StatusBadRequest)
	}

	var errBody map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&errBody); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if errBody["error"] == "" {
		t.Fatalf("expected error field in jsonError response")
	}
}
