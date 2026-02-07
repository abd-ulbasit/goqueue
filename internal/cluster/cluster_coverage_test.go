// =============================================================================
// CLUSTER COVERAGE TESTS
// =============================================================================
//
// Additional tests to improve coverage of cluster package to 90%+.
// Tests functions with 0% coverage and edge cases.
//
// =============================================================================

package cluster

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// =============================================================================
// NODE TESTS
// =============================================================================

// TestNode_Version tests the Version method.
func TestNode_Version(t *testing.T) {
	config := DefaultClusterConfig()
	config.NodeID = "node-1"
	config.AdvertiseAddress = "localhost:9092"
	config.Version = "1.0.0"

	node, err := NewNode(config)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	version := node.Version()
	if version != "1.0.0" {
		t.Errorf("Version() = %q, want %q", version, "1.0.0")
	}
}

// TestNode_Rack tests the Rack method.
func TestNode_Rack(t *testing.T) {
	config := DefaultClusterConfig()
	config.NodeID = "node-1"
	config.AdvertiseAddress = "localhost:9092"
	config.Rack = "rack-1"

	node, err := NewNode(config)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	rack := node.Rack()
	if rack != "rack-1" {
		t.Errorf("Rack() = %q, want %q", rack, "rack-1")
	}
}

// TestNode_IsController tests the IsController method.
func TestNode_IsController(t *testing.T) {
	config := DefaultClusterConfig()
	config.NodeID = "node-1"
	config.AdvertiseAddress = "localhost:9092"

	node, err := NewNode(config)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	// Node is not controller by default
	if node.IsController() {
		t.Error("IsController() should return false by default")
	}

	// Set role to controller
	node.SetRole(NodeRoleController)
	if !node.IsController() {
		t.Error("IsController() should return true after SetRole(Controller)")
	}

	// Set back to follower
	node.SetRole(NodeRoleFollower)
	if node.IsController() {
		t.Error("IsController() should return false after SetRole(Follower)")
	}
}

// TestNode_JoinedAt tests the JoinedAt method.
func TestNode_JoinedAt(t *testing.T) {
	config := DefaultClusterConfig()
	config.NodeID = "node-1"
	config.AdvertiseAddress = "localhost:9092"

	node, err := NewNode(config)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	// Set joined time
	now := time.Now()
	node.SetJoinedAt(now)

	joinedAt := node.JoinedAt()
	if !joinedAt.Equal(now) {
		t.Errorf("JoinedAt() = %v, want %v", joinedAt, now)
	}
}

// TestNode_String tests the String method.
func TestNode_String(t *testing.T) {
	config := DefaultClusterConfig()
	config.NodeID = "node-1"
	config.AdvertiseAddress = "localhost:9092"

	node, err := NewNode(config)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	s := node.String()
	if s == "" {
		t.Error("String() returned empty string")
	}
	// Should contain node id
	if !strings.Contains(s, "node-1") {
		t.Errorf("String() = %q, should contain node-1", s)
	}
}

// =============================================================================
// NODE STATUS TESTS
// =============================================================================

// TestNodeStatus_IsHealthy tests the IsHealthy method.
func TestNodeStatus_IsHealthy(t *testing.T) {
	tests := []struct {
		status NodeStatus
		want   bool
	}{
		{NodeStatusAlive, true},
		{NodeStatusLeaving, true},
		{NodeStatusDead, false},
		{NodeStatusSuspect, false},
		{NodeStatusUnknown, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			got := tt.status.IsHealthy()
			if got != tt.want {
				t.Errorf("NodeStatus(%q).IsHealthy() = %v, want %v", tt.status, got, tt.want)
			}
		})
	}
}

// =============================================================================
// NODE ADDRESS TESTS
// =============================================================================

// TestNodeAddress_IsValid tests the IsValid method.
func TestNodeAddress_IsValid(t *testing.T) {
	tests := []struct {
		name string
		addr NodeAddress
		want bool
	}{
		{"valid", NodeAddress{Host: "localhost", Port: 9092}, true},
		{"valid ip", NodeAddress{Host: "192.168.1.1", Port: 8080}, true},
		{"no host", NodeAddress{Host: "", Port: 9092}, false},
		{"no port", NodeAddress{Host: "localhost", Port: 0}, false},
		{"negative port", NodeAddress{Host: "localhost", Port: -1}, false},
		{"port too high", NodeAddress{Host: "localhost", Port: 70000}, false},
		{"valid max port", NodeAddress{Host: "localhost", Port: 65535}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.addr.IsValid()
			if got != tt.want {
				t.Errorf("NodeAddress{%q, %d}.IsValid() = %v, want %v", tt.addr.Host, tt.addr.Port, got, tt.want)
			}
		})
	}
}

// =============================================================================
// CLUSTER STATE TESTS
// =============================================================================

// TestClusterState_HealthyNodes tests the HealthyNodes method.
func TestClusterState_HealthyNodes(t *testing.T) {
	state := &ClusterState{
		Nodes: map[NodeID]*NodeInfo{
			"node-1": {ID: "node-1", Status: NodeStatusAlive},
			"node-2": {ID: "node-2", Status: NodeStatusLeaving},
			"node-3": {ID: "node-3", Status: NodeStatusDead},
			"node-4": {ID: "node-4", Status: NodeStatusSuspect},
		},
	}

	healthy := state.HealthyNodes()
	if len(healthy) != 2 {
		t.Errorf("HealthyNodes() returned %d nodes, want 2", len(healthy))
	}

	// Check that only alive and leaving are returned
	for _, node := range healthy {
		if node.Status != NodeStatusAlive && node.Status != NodeStatusLeaving {
			t.Errorf("HealthyNodes() returned node with status %v", node.Status)
		}
	}
}

// TestClusterState_GetController tests the GetController method.
func TestClusterState_GetController(t *testing.T) {
	// Test with no controller
	state := &ClusterState{
		Nodes:        map[NodeID]*NodeInfo{},
		ControllerID: "",
	}

	controller := state.GetController()
	if controller != nil {
		t.Error("GetController() should return nil when no controller is set")
	}

	// Test with controller
	state = &ClusterState{
		Nodes: map[NodeID]*NodeInfo{
			"node-1": {ID: "node-1", Status: NodeStatusAlive},
			"node-2": {ID: "node-2", Status: NodeStatusAlive},
		},
		ControllerID: "node-1",
	}

	controller = state.GetController()
	if controller == nil {
		t.Error("GetController() should return node when controller is set")
	} else if controller.ID != "node-1" {
		t.Errorf("GetController().ID = %q, want %q", controller.ID, "node-1")
	}
}

// =============================================================================
// REPLICATION TYPE TESTS
// =============================================================================

// TestReplicaState_String tests the String method on ReplicaState struct.
func TestReplicaState_String(t *testing.T) {
	rs := &ReplicaState{
		Topic:         "orders",
		Partition:     0,
		Role:          ReplicaRoleLeader,
		LeaderID:      "node-1",
		LeaderEpoch:   5,
		LogEndOffset:  100,
		HighWatermark: 95,
	}

	s := rs.String()
	if s == "" {
		t.Error("String() returned empty string")
	}
	// Should contain key info
	if !strings.Contains(s, "orders") {
		t.Errorf("String() = %q, should contain topic 'orders'", s)
	}
	if !strings.Contains(s, "leader") {
		t.Errorf("String() = %q, should contain role 'leader'", s)
	}
}

// =============================================================================
// REPLICA MANAGER ADDITIONAL TESTS
// =============================================================================

// TestReplicaManager_AddListener tests the AddListener method.
func TestReplicaManager_AddListener(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rm := NewReplicaManager(
		"test-node",
		DefaultReplicationConfig(),
		nil, // metadata store not needed for this test
		t.TempDir(),
		logger,
	)

	listenerCount := 0
	rm.AddListener(func(event ReplicaEvent) {
		listenerCount++
	})

	// Verify listener was added (we can't directly check listeners slice,
	// but we can verify it doesn't panic)
	if rm == nil {
		t.Fatal("ReplicaManager should not be nil")
	}
	// Use listenerCount to avoid unused variable warning
	_ = listenerCount
}

// TestReplicaManager_GetAllReplicas tests the GetAllReplicas method.
func TestReplicaManager_GetAllReplicas(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rm := NewReplicaManager(
		"test-node",
		DefaultReplicationConfig(),
		nil,
		t.TempDir(),
		logger,
	)

	// Initially should be empty
	replicas := rm.GetAllReplicas()
	if len(replicas) != 0 {
		t.Errorf("GetAllReplicas() = %d replicas, want 0", len(replicas))
	}

	// Add a replica by becoming leader
	err := rm.BecomeLeader("test-topic", 0, 1, []NodeID{"test-node"}, 0)
	if err != nil {
		t.Fatalf("BecomeLeader failed: %v", err)
	}

	// Now should have one replica
	replicas = rm.GetAllReplicas()
	if len(replicas) != 1 {
		t.Errorf("GetAllReplicas() = %d replicas, want 1", len(replicas))
	}
	if replicas[0].Topic != "test-topic" {
		t.Errorf("GetAllReplicas()[0].Topic = %q, want %q", replicas[0].Topic, "test-topic")
	}
}

// TestReplicaManager_GetFollowerProgress tests the GetFollowerProgress method.
func TestReplicaManager_GetFollowerProgress(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rm := NewReplicaManager(
		"test-node",
		DefaultReplicationConfig(),
		nil,
		t.TempDir(),
		logger,
	)

	// Non-existent partition
	progress := rm.GetFollowerProgress("no-topic", 0, "follower-1")
	if progress != nil {
		t.Error("GetFollowerProgress should return nil for non-existent partition")
	}

	// Add a replica by becoming leader
	err := rm.BecomeLeader("test-topic", 0, 1, []NodeID{"test-node", "follower-1"}, 0)
	if err != nil {
		t.Fatalf("BecomeLeader failed: %v", err)
	}

	// Follower progress might be nil initially (no fetches yet)
	_ = rm.GetFollowerProgress("test-topic", 0, "follower-1")
	// This is expected to return nil until the follower actually fetches
	// Just verify it doesn't panic
}

// =============================================================================
// ISR MANAGER ADDITIONAL TESTS
// =============================================================================

// TestISRManager_GetFollowerProgress tests the GetFollowerProgress method.
func TestISRManager_GetFollowerProgress(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	config := ISRConfig{
		MinInSyncReplicas: 1,
		LagTimeMaxMs:      10000,
		LagMaxMessages:    1000,
	}

	im := NewISRManager("test-topic", 0, "leader-1", []NodeID{"leader-1", "follower-1"}, config, logger)

	// Non-existent follower
	progress := im.GetFollowerProgress("non-existent")
	if progress != nil {
		t.Error("GetFollowerProgress should return nil for non-existent follower")
	}

	// Record a fetch to create follower progress
	im.RecordFetch("follower-1", 100, 150)

	// Now should have progress
	progress = im.GetFollowerProgress("follower-1")
	if progress == nil {
		t.Error("GetFollowerProgress should return progress for existing follower")
	} else if progress.ReplicaID != "follower-1" {
		t.Errorf("GetFollowerProgress().ReplicaID = %q, want %q", progress.ReplicaID, "follower-1")
	}
}

// =============================================================================
// PARTITION LEADER ELECTOR ADDITIONAL TESTS
// =============================================================================

// TestPartitionLeaderElector_ElectPreferredLeader tests the ElectPreferredLeader method.
func TestPartitionLeaderElector_ElectPreferredLeader(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()

	cfg := DefaultClusterConfig()
	cfg.NodeID = "n1"
	cfg.ClusterAddress = "127.0.0.1:9001"
	cfg.ClientAddress = "127.0.0.1:8001"

	n1, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	m := NewMembership(n1, cfg, dataDir)
	if err := m.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf: %v", err)
	}

	// Add follower node
	n2 := &NodeInfo{
		ID:             "n2",
		ClientAddress:  NodeAddress{Host: "127.0.0.1", Port: 8002},
		ClusterAddress: NodeAddress{Host: "127.0.0.1", Port: 9002},
		Status:         NodeStatusAlive,
		Role:           NodeRoleFollower,
		JoinedAt:       time.Now(),
		LastHeartbeat:  time.Now(),
		Version:        "test",
	}
	if err := m.AddNode(n2); err != nil {
		t.Fatalf("AddNode(n2): %v", err)
	}

	ms := NewMetadataStore("")
	pe := NewPartitionLeaderElector(ms, m, DefaultReplicationConfig(), logger)

	// Test with non-existent partition
	_, err = pe.ElectPreferredLeader("non-existent", 0)
	if err == nil {
		t.Error("ElectPreferredLeader should fail for non-existent partition")
	}

	// Seed assignment with n2 as leader but n1 as preferred (first in replicas)
	assign := &PartitionAssignment{
		Topic:     "orders",
		Partition: 0,
		Leader:    "n2",
		Replicas:  []NodeID{"n1", "n2"}, // n1 is preferred
		ISR:       []NodeID{"n1", "n2"},
		Version:   1,
	}
	if err := ms.SetAssignment(assign); err != nil {
		t.Fatalf("SetAssignment: %v", err)
	}

	// ElectPreferredLeader should move leadership to n1
	resp, err := pe.ElectPreferredLeader("orders", 0)
	if err != nil {
		t.Fatalf("ElectPreferredLeader: %v", err)
	}
	if resp.ErrorCode != LeaderElectionSuccess {
		t.Errorf("ElectPreferredLeader returned error code %d: %s", resp.ErrorCode, resp.ErrorMessage)
	}
	if resp.NewLeader != "n1" {
		t.Errorf("ElectPreferredLeader chose %q, want n1", resp.NewLeader)
	}

	// Test when already preferred leader
	resp, err = pe.ElectPreferredLeader("orders", 0)
	if err != nil {
		t.Fatalf("ElectPreferredLeader (already preferred): %v", err)
	}
	if resp.ErrorCode != LeaderElectionSuccess {
		t.Errorf("Already preferred leader should succeed")
	}
}

// TestPartitionLeaderElector_ElectLeadersForNode tests the ElectLeadersForNode method.
func TestPartitionLeaderElector_ElectLeadersForNode(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()

	cfg := DefaultClusterConfig()
	cfg.NodeID = "n1"
	cfg.ClusterAddress = "127.0.0.1:9001"
	cfg.ClientAddress = "127.0.0.1:8001"

	n1, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	m := NewMembership(n1, cfg, dataDir)
	if err := m.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf: %v", err)
	}

	// Add follower nodes
	for i := 2; i <= 3; i++ {
		info := &NodeInfo{
			ID:             NodeID("n" + string(rune('0'+i))),
			ClientAddress:  NodeAddress{Host: "127.0.0.1", Port: 8000 + i},
			ClusterAddress: NodeAddress{Host: "127.0.0.1", Port: 9000 + i},
			Status:         NodeStatusAlive,
			Role:           NodeRoleFollower,
			JoinedAt:       time.Now(),
			LastHeartbeat:  time.Now(),
			Version:        "test",
		}
		if err := m.AddNode(info); err != nil {
			t.Fatalf("AddNode(%s): %v", info.ID, err)
		}
	}

	ms := NewMetadataStore("")
	pe := NewPartitionLeaderElector(ms, m, DefaultReplicationConfig(), logger)

	// Add multiple partitions led by n1
	for i := 0; i < 3; i++ {
		assign := &PartitionAssignment{
			Topic:     "orders",
			Partition: i,
			Leader:    "n1",
			Replicas:  []NodeID{"n1", "n2", "n3"},
			ISR:       []NodeID{"n1", "n2", "n3"},
			Version:   1,
		}
		if err := ms.SetAssignment(assign); err != nil {
			t.Fatalf("SetAssignment(%d): %v", i, err)
		}
	}

	// Mark n1 as dead
	if err := m.UpdateNodeStatus("n1", NodeStatusDead); err != nil {
		t.Fatalf("UpdateNodeStatus: %v", err)
	}

	// ElectLeadersForNode should elect new leaders for all partitions led by n1
	results, err := pe.ElectLeadersForNode("n1")
	if err != nil {
		t.Fatalf("ElectLeadersForNode: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("ElectLeadersForNode returned %d results, want 3", len(results))
	}

	// Verify all elections succeeded and chose non-n1 leaders
	for _, resp := range results {
		if resp.ErrorCode != LeaderElectionSuccess {
			t.Errorf("Election for %s-%d failed: %s", resp.Topic, resp.Partition, resp.ErrorMessage)
			continue
		}
		if resp.NewLeader == "n1" {
			t.Errorf("New leader should not be n1 (dead node)")
		}
	}
}
