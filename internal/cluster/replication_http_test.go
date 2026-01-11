package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReplication_HTTP_RoundTrip_ServerAndClients(t *testing.T) {
	// ============================================================================
	// REPLICATION: SERVER + CLIENT ROUND-TRIP (HTTP)
	// ============================================================================
	//
	// WHY THIS TEST EXISTS:
	//   The replication stack is large and is easy to break in subtle ways.
	//   Pure unit tests don't catch protocol mismatches, JSON shape drift, or
	//   handler wiring bugs. This integration-style test runs real HTTP handlers
	//   via httptest.NewServer.
	//
	// WHAT THIS TEST COVERS:
	//   - ReplicationServer handlers: fetch, snapshot create/download, ISR update,
	//     leader election, partition info.
	//   - ReplicationClient calls (leader/controller address resolution)
	//   - ClusterClient replication helpers in cluster_server.go (Fetch/RequestSnapshot/
	//     DownloadSnapshot) which were previously 0% covered.
	//
	// ARCHITECTURAL FLAW WE TRY TO CATCH:
	//   "The cluster package compiles, but the endpoints are never exercised".
	//   That leads to production-only failures when a follower starts replicating.
	// ============================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()

	// --------------------------------------------------------------------------
	// Build Membership + Metadata
	// --------------------------------------------------------------------------
	cfg := DefaultClusterConfig()
	cfg.NodeID = "n1"
	cfg.ClusterAddress = "127.0.0.1:9001"
	cfg.ClientAddress = "127.0.0.1:8001"

	localNode, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	membership := NewMembership(localNode, cfg, dataDir)
	if err := membership.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf: %v", err)
	}

	// Add follower node.
	followerInfo := &NodeInfo{
		ID:            "n2",
		ClientAddress:  NodeAddress{Host: "127.0.0.1", Port: 8002},
		ClusterAddress: NodeAddress{Host: "127.0.0.1", Port: 9002},
		Status:        NodeStatusAlive,
		Role:          NodeRoleFollower,
		JoinedAt:      time.Now(),
		LastHeartbeat: time.Now(),
		Version:       "test",
	}
	if err := membership.AddNode(followerInfo); err != nil {
		t.Fatalf("AddNode(n2): %v", err)
	}

	// Controller is n1 (same as leader in this test).
	if err := membership.SetController("n1", 1); err != nil {
		t.Fatalf("SetController: %v", err)
	}

	metaStore := NewMetadataStore("")
	if err := metaStore.SetAssignment(&PartitionAssignment{
		Topic:     "orders",
		Partition: 0,
		Leader:    "n1",
		Replicas:  []NodeID{"n1", "n2"},
		ISR:       []NodeID{"n1", "n2"},
		Version:   1,
	}); err != nil {
		t.Fatalf("SetAssignment: %v", err)
	}

	replCfg := DefaultReplicationConfig()
	replCfg.SnapshotEnabled = true

	// --------------------------------------------------------------------------
	// Create ReplicaManager (leader) + SnapshotManager + Elector
	// --------------------------------------------------------------------------
	// NOTE: We intentionally avoid BecomeFollower() in this test because it starts
	// a background fetch loop. We'll exercise fetch paths through HTTP endpoints.
	rm := NewReplicaManager("n1", replCfg, nil, dataDir, logger)
	t.Cleanup(func() { _ = rm.Stop() })

	if err := rm.BecomeLeader("orders", 0, 5, []NodeID{"n1", "n2"}, 3); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}

	// Create an on-disk log directory for snapshots to archive.
	logDir := filepath.Join(dataDir, "logs", "orders", "0")
	if err := os.MkdirAll(filepath.Join(logDir, "segments"), 0o755); err != nil {
		t.Fatalf("mkdir logDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "00000000000000000000.log"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write log file: %v", err)
	}

	snapMgr := NewSnapshotManager(dataDir, logger)
	partitionElector := NewPartitionLeaderElector(metaStore, membership, replCfg, logger)

	rs := NewReplicationServer(rm, snapMgr, partitionElector, metaStore, replCfg, logger)
	rs.SetLogDirFn(func(topic string, partition int) string { return logDir })
	rs.SetLogReader(func(topic string, partition int, fromOffset int64, maxBytes int64) ([]ReplicatedMessage, int64, error) {
		// Minimal fake log:
		//   offsets: 0,1,2
		//   LEO: 3 (next offset)
		if fromOffset < 0 {
			return nil, 0, errors.New("offset out of range")
		}
		logEnd := int64(3)
		if fromOffset > logEnd {
			return nil, logEnd, errors.New("offset out of range")
		}

		if fromOffset > 2 {
			return []ReplicatedMessage{}, logEnd, nil
		}

		msgs := []ReplicatedMessage{
			{Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("k1"), Value: []byte("v1")},
			{Offset: 1, Timestamp: time.Now().UnixMilli(), Key: []byte("k2"), Value: []byte("v2")},
			{Offset: 2, Timestamp: time.Now().UnixMilli(), Key: []byte("k3"), Value: []byte("v3")},
		}
		// Return fromOffset..end.
		start := int(fromOffset)
		return msgs[start:], logEnd, nil
	})

	mux := http.NewServeMux()
	rs.RegisterRoutes(mux)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	addr := strings.TrimPrefix(srv.URL, "http://")

	// Make membership leader/controller point to the test server address.
	// Membership stores NodeInfo by pointer internally; UpdateNodeStatus persists
	// but doesn't let us update addresses, so we mutate the stored NodeInfo directly
	// (same package test).
	membership.mu.Lock()
	if info := membership.state.Nodes["n1"]; info != nil {
		info.ClusterAddress, _ = ParseNodeAddress(addr)
	}
	if info := membership.state.Nodes["n2"]; info != nil {
		info.ClusterAddress, _ = ParseNodeAddress(addr)
	}
	membership.mu.Unlock()

	ctx := context.Background()

	// --------------------------------------------------------------------------
	// Fetch handler sanity (unknown partition, not leader, epoch fenced)
	// --------------------------------------------------------------------------
	{
		// Unknown partition: hit handler directly so the client doesn't fail earlier.
		reqBody, _ := json.Marshal(FetchRequest{Topic: "orders", Partition: 999, FromOffset: 0, FollowerID: "n2"})
		resp, err := http.Post(srv.URL+"/cluster/fetch", "application/json", bytes.NewReader(reqBody))
		if err != nil {
			t.Fatalf("POST /cluster/fetch: %v", err)
		}
		defer resp.Body.Close()

		var fr FetchResponse
		if err := json.NewDecoder(resp.Body).Decode(&fr); err != nil {
			t.Fatalf("decode fetch response: %v", err)
		}
		if fr.ErrorCode != FetchErrorUnknownPartition {
			t.Fatalf("expected unknown partition error, got=%d msg=%q", fr.ErrorCode, fr.ErrorMessage)
		}
	}

	{
		// Not leader: temporarily flip role.
		rep := rm.GetLocalReplica("orders", 0)
		rep.mu.Lock()
		oldRole := rep.State.Role
		rep.State.Role = ReplicaRoleFollower
		rep.mu.Unlock()

		rc := NewReplicationClient("n2", membership, metaStore, replCfg, logger)
		resp, err := rc.Fetch(ctx, "orders", 0, 0, 5)
		if err != nil {
			t.Fatalf("ReplicationClient.Fetch: %v", err)
		}
		if resp.ErrorCode != FetchErrorNotLeader {
			t.Fatalf("expected not leader error, got=%d msg=%q", resp.ErrorCode, resp.ErrorMessage)
		}

		// Restore role.
		rep.mu.Lock()
		rep.State.Role = oldRole
		rep.mu.Unlock()
	}

	{
		// Epoch fenced: request with stale epoch.
		rc := NewReplicationClient("n2", membership, metaStore, replCfg, logger)
		resp, err := rc.Fetch(ctx, "orders", 0, 0, 1) // leader epoch is 5
		if err != nil {
			t.Fatalf("ReplicationClient.Fetch(stale epoch): %v", err)
		}
		if resp.ErrorCode != FetchErrorEpochFenced {
			t.Fatalf("expected epoch fenced error, got=%d msg=%q", resp.ErrorCode, resp.ErrorMessage)
		}
	}

	// --------------------------------------------------------------------------
	// Happy path fetch and out-of-range fetch
	// --------------------------------------------------------------------------
	{
		rc := NewReplicationClient("n2", membership, metaStore, replCfg, logger)
		resp, err := rc.Fetch(ctx, "orders", 0, 0, 5)
		if err != nil {
			t.Fatalf("ReplicationClient.Fetch: %v", err)
		}
		if resp.ErrorCode != FetchErrorNone {
			t.Fatalf("expected fetch success, got=%d msg=%q", resp.ErrorCode, resp.ErrorMessage)
		}
		if len(resp.Messages) == 0 {
			t.Fatalf("expected messages")
		}
		if resp.LogEndOffset <= 0 {
			t.Fatalf("expected LogEndOffset > 0")
		}

		// Offset out of range -> triggers snapshot-based catch-up in real follower.
		resp2, err := rc.Fetch(ctx, "orders", 0, 100, 5)
		if err != nil {
			t.Fatalf("ReplicationClient.Fetch(out of range): %v", err)
		}
		if resp2.ErrorCode != FetchErrorOffsetOutOfRange {
			t.Fatalf("expected offset out of range, got=%d msg=%q", resp2.ErrorCode, resp2.ErrorMessage)
		}
	}

	// --------------------------------------------------------------------------
	// Snapshot creation + download via ReplicationClient
	// --------------------------------------------------------------------------
	var snapResp *SnapshotResponse
	{
		rc := NewReplicationClient("n2", membership, metaStore, replCfg, logger)
		sr, err := rc.RequestSnapshot(ctx, "orders", 0)
		if err != nil {
			t.Fatalf("RequestSnapshot: %v", err)
		}
		if sr.ErrorCode != SnapshotErrorNone {
			t.Fatalf("expected SnapshotErrorNone, got=%d msg=%q", sr.ErrorCode, sr.ErrorMessage)
		}
		if sr.Snapshot == nil || sr.DownloadURL == "" {
			t.Fatalf("expected snapshot metadata + download url")
		}
		snapResp = sr

		destDir := filepath.Join(dataDir, "downloads")
		path, err := rc.DownloadSnapshot(ctx, "orders", 0, sr.DownloadURL, destDir)
		if err != nil {
			t.Fatalf("DownloadSnapshot: %v", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("downloaded snapshot missing: %v", err)
		}
	}

	// Snapshot download mismatch (offset mismatch => 404).
	{
		badURL := "/cluster/snapshot/orders/0/999999"
		resp, err := http.Get(srv.URL + badURL)
		if err != nil {
			t.Fatalf("GET snapshot mismatch: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", resp.StatusCode)
		}
	}

	// --------------------------------------------------------------------------
	// ISR updates via ReplicationClient
	// --------------------------------------------------------------------------
	{
		rc := NewReplicationClient("n2", membership, metaStore, replCfg, logger)

		if err := rc.ReportISRShrink(ctx, "orders", 0, "n2", "test shrink"); err != nil {
			t.Fatalf("ReportISRShrink: %v", err)
		}
		assign := metaStore.GetAssignment("orders", 0)
		if assign == nil {
			t.Fatalf("expected assignment")
		}
		for _, id := range assign.ISR {
			if id == "n2" {
				t.Fatalf("expected n2 removed from ISR")
			}
		}

		if err := rc.ReportISRExpand(ctx, "orders", 0, 3); err != nil {
			t.Fatalf("ReportISRExpand: %v", err)
		}
		assign = metaStore.GetAssignment("orders", 0)
		found := false
		for _, id := range assign.ISR {
			if id == "n2" {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected n2 added back to ISR")
		}
	}

	// --------------------------------------------------------------------------
	// Leader election via ReplicationClient (controller endpoint)
	// --------------------------------------------------------------------------
	{
		// Kill current leader so election chooses a new leader from alive replicas.
		if err := membership.UpdateNodeStatus("n1", NodeStatusDead); err != nil {
			t.Fatalf("UpdateNodeStatus(n1 dead): %v", err)
		}

		rc := NewReplicationClient("n2", membership, metaStore, replCfg, logger)
		lr, err := rc.RequestLeaderElection(ctx, "orders", 0, "test election", false)
		if err != nil {
			t.Fatalf("RequestLeaderElection: %v", err)
		}
		if lr.ErrorCode != LeaderElectionSuccess {
			t.Fatalf("expected leader election success, got=%d msg=%q", lr.ErrorCode, lr.ErrorMessage)
		}
		if lr.NewLeader != "n2" {
			t.Fatalf("expected new leader n2, got %s", lr.NewLeader)
		}
	}

	// --------------------------------------------------------------------------
	// Partition info lookup
	// --------------------------------------------------------------------------
	{
		rc := NewReplicationClient("n2", membership, metaStore, replCfg, logger)
		info, err := rc.GetPartitionInfo(ctx, "n1", "orders", 0)
		if err != nil {
			t.Fatalf("GetPartitionInfo: %v", err)
		}
		if info.Topic != "orders" || info.Partition != 0 {
			t.Fatalf("unexpected partition info: %+v", info)
		}
		if info.LocalReplica == nil {
			t.Fatalf("expected LocalReplica info to be present")
		}
	}

	// --------------------------------------------------------------------------
	// ClusterClient replication helpers (cluster_server.go)
	// --------------------------------------------------------------------------
	{
		cc := NewClusterClient(localNode, membership, logger)

		// Fetch wrapper.
		fr, err := cc.Fetch(ctx, addr, &FetchRequest{Topic: "orders", Partition: 0, FromOffset: 0, FollowerID: "n2", LeaderEpoch: 5})
		if err != nil {
			t.Fatalf("ClusterClient.Fetch: %v", err)
		}
		if fr.ErrorCode != FetchErrorNone {
			t.Fatalf("expected fetch success from ClusterClient, got=%d msg=%q", fr.ErrorCode, fr.ErrorMessage)
		}

		// RequestSnapshot wrapper.
		sr, err := cc.RequestSnapshot(ctx, addr, "orders", 0, "n2")
		if err != nil {
			t.Fatalf("ClusterClient.RequestSnapshot: %v", err)
		}
		if sr.ErrorCode != SnapshotErrorNone {
			t.Fatalf("expected snapshot success from ClusterClient, got=%d", sr.ErrorCode)
		}
		if sr.DownloadURL == "" || sr.Snapshot == nil {
			t.Fatalf("expected download url and snapshot")
		}

		// DownloadSnapshot wrapper.
		destDir := filepath.Join(dataDir, "cluster-client-download")
		path, err := cc.DownloadSnapshot(ctx, addr, sr.DownloadURL, destDir)
		if err != nil {
			t.Fatalf("ClusterClient.DownloadSnapshot: %v", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("cluster-client snapshot missing: %v", err)
		}

		// Ensure snapshot response pointer from earlier still usable.
		_ = snapResp
	}
}
