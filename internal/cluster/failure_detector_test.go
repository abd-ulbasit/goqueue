package cluster

import (
	"os"
	"testing"
	"time"
)

// =============================================================================
// FAILURE DETECTOR TESTS
// =============================================================================

func TestFailureDetector_RecordHeartbeat(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fd-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:            "node1",
		ClusterAddress:    "localhost:9000",
		HeartbeatInterval: 100 * time.Millisecond,
		SuspectTimeout:    200 * time.Millisecond,
		DeadTimeout:       300 * time.Millisecond,
		QuorumSize:        1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf()

	fd := NewFailureDetector(membership, config)

	// Add a remote node
	membership.AddNode(&NodeInfo{
		ID:             "node2",
		ClusterAddress: NodeAddress{Host: "localhost", Port: 9001},
		Status:         NodeStatusAlive,
	})

	// Record heartbeat for node2
	fd.RecordHeartbeat("node2")

	// Should have recorded the heartbeat
	lastHB, ok := fd.LastHeartbeat("node2")
	if !ok || lastHB.IsZero() {
		t.Error("heartbeat not recorded")
	}
}

func TestFailureDetector_DetectsFailure(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fd-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:            "node1",
		ClusterAddress:    "localhost:9000",
		HeartbeatInterval: 50 * time.Millisecond,
		SuspectTimeout:    100 * time.Millisecond,
		DeadTimeout:       150 * time.Millisecond,
		QuorumSize:        1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf()

	fd := NewFailureDetector(membership, config)

	// Add a remote node
	membership.AddNode(&NodeInfo{
		ID:             "node2",
		ClusterAddress: NodeAddress{Host: "localhost", Port: 9001},
		Status:         NodeStatusAlive,
	})

	// Record initial heartbeat
	fd.RecordHeartbeat("node2")

	// Start detector
	fd.Start()
	defer fd.Stop()

	// Wait for suspect timeout
	time.Sleep(150 * time.Millisecond)

	// Node should be suspect by now
	info := membership.GetNode("node2")
	if info.Status != NodeStatusSuspect && info.Status != NodeStatusDead {
		t.Errorf("node2 status = %v, want suspect or dead", info.Status)
	}

	// Wait for dead timeout
	time.Sleep(100 * time.Millisecond)

	// Node should be dead
	info = membership.GetNode("node2")
	if info.Status != NodeStatusDead {
		t.Errorf("node2 status = %v, want dead", info.Status)
	}
}

func TestFailureDetector_RecoveredNode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fd-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:            "node1",
		ClusterAddress:    "localhost:9000",
		HeartbeatInterval: 50 * time.Millisecond,
		SuspectTimeout:    100 * time.Millisecond,
		DeadTimeout:       150 * time.Millisecond,
		QuorumSize:        1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf()

	fd := NewFailureDetector(membership, config)

	// Add a suspect node
	membership.AddNode(&NodeInfo{
		ID:             "node2",
		ClusterAddress: NodeAddress{Host: "localhost", Port: 9001},
		Status:         NodeStatusAlive,
	})
	membership.UpdateNodeStatus("node2", NodeStatusSuspect)

	// Record heartbeat - should recover node
	fd.RecordHeartbeat("node2")

	// Node should be alive again
	info := membership.GetNode("node2")
	if info.Status != NodeStatusAlive {
		t.Errorf("node2 status = %v, want alive after heartbeat", info.Status)
	}
}

// =============================================================================
// CONTROLLER ELECTOR TESTS
// =============================================================================

func TestControllerElector_InitialState(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "elector-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:             "node1",
		ClusterAddress:     "localhost:9000",
		LeaseTimeout:       1 * time.Second,
		LeaseRenewInterval: 300 * time.Millisecond,
		QuorumSize:         1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf()

	elector := NewControllerElector(node, membership, config)

	// Initially not controller
	if elector.IsController() {
		t.Error("should not be controller initially")
	}

	// Initial state should be follower
	if elector.State() != ControllerStateFollower {
		t.Errorf("initial state = %v, want follower", elector.State())
	}
}

func TestControllerElector_VoteRequest(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "elector-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:             "node1",
		ClusterAddress:     "localhost:9000",
		LeaseTimeout:       1 * time.Second,
		LeaseRenewInterval: 300 * time.Millisecond,
		QuorumSize:         1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf()

	elector := NewControllerElector(node, membership, config)

	// Receive vote request for epoch 1
	req := &ControllerVoteRequest{
		CandidateID: "node2",
		Epoch:       1,
	}

	resp := elector.HandleVoteRequest(req)

	// Should grant vote (no previous vote, epoch is higher)
	if !resp.VoteGranted {
		t.Error("should grant vote for first request")
	}

	// Second request for same epoch should not grant (already voted)
	req2 := &ControllerVoteRequest{
		CandidateID: "node3",
		Epoch:       1,
	}

	resp2 := elector.HandleVoteRequest(req2)
	if resp2.VoteGranted {
		t.Error("should not grant second vote for same epoch")
	}
}

func TestControllerElector_HigherEpochRequest(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "elector-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:             "node1",
		ClusterAddress:     "localhost:9000",
		LeaseTimeout:       1 * time.Second,
		LeaseRenewInterval: 300 * time.Millisecond,
		QuorumSize:         1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf()

	elector := NewControllerElector(node, membership, config)

	// Vote for epoch 1
	req1 := &ControllerVoteRequest{
		CandidateID: "node2",
		Epoch:       1,
	}
	elector.HandleVoteRequest(req1)

	// Higher epoch should grant vote
	req2 := &ControllerVoteRequest{
		CandidateID: "node3",
		Epoch:       2,
	}

	resp := elector.HandleVoteRequest(req2)
	if !resp.VoteGranted {
		t.Error("should grant vote for higher epoch")
	}
}

func TestControllerElector_AcknowledgeController(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "elector-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := (&ClusterConfig{
		NodeID:             "node1",
		ClusterAddress:     "localhost:9000",
		LeaseTimeout:       1 * time.Second,
		LeaseRenewInterval: 300 * time.Millisecond,
		QuorumSize:         1,
	}).WithDefaults()

	node, _ := NewNode(config)
	membership := NewMembership(node, config, tmpDir)
	membership.RegisterSelf()

	elector := NewControllerElector(node, membership, config)

	// Acknowledge a controller
	elector.AcknowledgeController("node2", 1)

	// Elector should have updated its internal state
	// (Membership update is done via separate SetController call in coordinator)
	if elector.Epoch() != 1 {
		t.Errorf("elector epoch = %v, want 1", elector.Epoch())
	}
}
