package cluster

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// =============================================================================
// MEMBERSHIP TESTS
// =============================================================================

func TestNewMembership(t *testing.T) {
	// Create temp directory for persistence
	tmpDir, err := os.MkdirTemp("", "membership-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := &ClusterConfig{
		NodeID:         "node1",
		ClusterAddress: "localhost:9000",
		QuorumSize:     1,
	}
	config = config.WithDefaults()

	node, err := NewNode(config)
	if err != nil {
		t.Fatal(err)
	}

	membership := NewMembership(node, config, tmpDir)
	if membership == nil {
		t.Fatal("NewMembership returned nil")
	}

	// RegisterSelf must be called explicitly to add local node
	if err := membership.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf failed: %v", err)
	}

	// Verify local node is registered by using GetNode
	info := membership.GetNode(node.ID())
	if info == nil {
		t.Fatal("local node not registered in membership")
	}
	if info.Status != NodeStatusAlive {
		t.Errorf("local node status = %v, want %v", info.Status, NodeStatusAlive)
	}
}

func TestMembership_AddRemoveNode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "membership-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:         "node1",
		ClusterAddress: "localhost:9000",
		QuorumSize:     1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf() // Add local node first

	// Add another node
	remoteNode := &NodeInfo{
		ID:             "node2",
		ClusterAddress: NodeAddress{Host: "localhost", Port: 9001},
		Status:         NodeStatusAlive,
		JoinedAt:       time.Now(),
		LastHeartbeat:  time.Now(),
	}

	if err := membership.AddNode(remoteNode); err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}

	// Verify node was added
	if membership.GetNode("node2") == nil {
		t.Error("node2 not found after AddNode")
	}

	// Check node count (local + remote)
	if got := membership.NodeCount(); got != 2 {
		t.Errorf("NodeCount() = %v, want 2", got)
	}

	// Remove node (gracefully)
	if err := membership.RemoveNode("node2", true); err != nil {
		t.Fatalf("RemoveNode failed: %v", err)
	}

	// Verify node was removed
	if membership.GetNode("node2") != nil {
		t.Error("node2 still exists after RemoveNode")
	}
}

func TestMembership_UpdateNodeStatus(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "membership-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:         "node1",
		ClusterAddress: "localhost:9000",
		QuorumSize:     1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf()

	// Add a node
	remoteNode := &NodeInfo{
		ID:             "node2",
		ClusterAddress: NodeAddress{Host: "localhost", Port: 9001},
		Status:         NodeStatusAlive,
		JoinedAt:       time.Now(),
		LastHeartbeat:  time.Now(),
	}
	membership.AddNode(remoteNode)

	// Update status to suspect
	if err := membership.UpdateNodeStatus("node2", NodeStatusSuspect); err != nil {
		t.Fatalf("UpdateNodeStatus failed: %v", err)
	}

	// Verify status changed
	info := membership.GetNode("node2")
	if info.Status != NodeStatusSuspect {
		t.Errorf("node status = %v, want %v", info.Status, NodeStatusSuspect)
	}
}

func TestMembership_SetController(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "membership-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:         "node1",
		ClusterAddress: "localhost:9000",
		QuorumSize:     1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf()

	// Initially no controller
	if !membership.ControllerID().IsEmpty() {
		t.Error("controller should be empty initially")
	}

	// Set controller
	if err := membership.SetController("node1", 1); err != nil {
		t.Fatalf("SetController failed: %v", err)
	}

	// Verify controller
	if membership.ControllerID() != "node1" {
		t.Errorf("ControllerID() = %v, want node1", membership.ControllerID())
	}

	if membership.ControllerEpoch() != 1 {
		t.Errorf("ControllerEpoch() = %v, want 1", membership.ControllerEpoch())
	}
}

func TestMembership_AliveNodes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "membership-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:         "node1",
		ClusterAddress: "localhost:9000",
		QuorumSize:     1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf() // node1 is alive

	// Add more nodes
	membership.AddNode(&NodeInfo{
		ID:             "node2",
		ClusterAddress: NodeAddress{Host: "localhost", Port: 9001},
		Status:         NodeStatusAlive,
		JoinedAt:       time.Now(),
		LastHeartbeat:  time.Now(),
	})
	membership.AddNode(&NodeInfo{
		ID:             "node3",
		ClusterAddress: NodeAddress{Host: "localhost", Port: 9002},
		Status:         NodeStatusAlive, // AddNode sets to alive anyway
		JoinedAt:       time.Now(),
		LastHeartbeat:  time.Now(),
	})

	// Now mark node3 as dead using UpdateNodeStatus
	membership.UpdateNodeStatus("node3", NodeStatusDead)

	aliveNodes := membership.AliveNodes()

	// Should have 2 alive nodes (node1 and node2)
	if len(aliveNodes) != 2 {
		t.Errorf("AliveNodes() count = %v, want 2", len(aliveNodes))
	}

	// Verify the dead node is not in the list
	for _, n := range aliveNodes {
		if n.ID == "node3" {
			t.Error("dead node3 should not be in alive nodes list")
		}
	}
}

func TestMembership_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "membership-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:         "node1",
		ClusterAddress: "localhost:9000",
		QuorumSize:     1,
	}).WithDefaults()

	node, _ := NewNode(config)

	// Create membership and add nodes
	membership1 := NewMembership(node, config, tmpDir)
	membership1.RegisterSelf()
	membership1.AddNode(&NodeInfo{
		ID:             "node2",
		ClusterAddress: NodeAddress{Host: "localhost", Port: 9001},
		Status:         NodeStatusAlive,
		JoinedAt:       time.Now(),
		LastHeartbeat:  time.Now(),
	})
	membership1.SetController("node1", 1)

	// AddNode already persists, so just verify the file exists
	stateFile := filepath.Join(tmpDir, "cluster", "state.json")
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Error("state file not created")
	}

	// Load state in new membership
	membership2 := NewMembership(node, config, tmpDir)
	if err := membership2.LoadState(); err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}

	// Verify nodes persisted
	if membership2.GetNode("node2") == nil {
		t.Error("node2 not found after load")
	}

	// Verify controller persisted
	if membership2.ControllerID() != "node1" {
		t.Errorf("controller not persisted, got %v", membership2.ControllerID())
	}
}

func TestMembership_EventListener(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "membership-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:         "node1",
		ClusterAddress: "localhost:9000",
		QuorumSize:     1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf()

	// Track events
	events := make([]MembershipEventType, 0)
	membership.AddListener(func(event MembershipEvent) {
		events = append(events, event.Type)
	})

	// Trigger events
	membership.AddNode(&NodeInfo{
		ID:             "node2",
		ClusterAddress: NodeAddress{Host: "localhost", Port: 9001},
		Status:         NodeStatusAlive,
		JoinedAt:       time.Now(),
		LastHeartbeat:  time.Now(),
	})
	membership.RemoveNode("node2", true)

	// Verify events
	if len(events) < 2 {
		t.Errorf("expected at least 2 events, got %d", len(events))
	}

	foundJoin := false
	foundLeave := false
	for _, e := range events {
		if e == EventNodeJoined {
			foundJoin = true
		}
		if e == EventNodeLeft {
			foundLeave = true
		}
	}

	if !foundJoin {
		t.Error("EventNodeJoined not emitted")
	}
	if !foundLeave {
		t.Error("EventNodeLeft not emitted")
	}
}
