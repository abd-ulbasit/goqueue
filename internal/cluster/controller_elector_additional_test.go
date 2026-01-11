package cluster

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestControllerElector_TriggerElection_BecomeAndStepDownOnQuorumLoss(t *testing.T) {
	// =========================================================================
	// CONTROLLER ELECTOR: ELECTION SUCCESS + LEASE/QUORUM STEP-DOWN
	// =========================================================================
	//
	// WHY:
	//   The controller is the cluster's "brain" (metadata + decisions).
	//   Election bugs are catastrophic:
	//     - no controller → cluster stuck
	//     - split-brain → conflicting metadata updates
	//
	// WHAT WE VERIFY:
	//   1) TriggerElection + granted votes leads to controller role.
	//   2) renewLease() steps down immediately when quorum is lost.
	//   3) Callbacks are invoked (become/lose) so the coordinator can react.
	// =========================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := (&ClusterConfig{
		NodeID:             "node1",
		ClusterAddress:     "127.0.0.1:0",
		LeaseTimeout:       200 * time.Millisecond,
		LeaseRenewInterval: 50 * time.Millisecond,
	}).WithDefaults()

	node, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	m := NewMembership(node, cfg, t.TempDir())
	if err := m.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf: %v", err)
	}

	// Add two other alive nodes so majority quorum is 2/3.
	if err := m.AddNode(&NodeInfo{ID: "node2", ClusterAddress: NodeAddress{Host: "127.0.0.1", Port: 9002}, Status: NodeStatusAlive, JoinedAt: time.Now(), LastHeartbeat: time.Now()}); err != nil {
		t.Fatalf("AddNode(node2): %v", err)
	}
	if err := m.AddNode(&NodeInfo{ID: "node3", ClusterAddress: NodeAddress{Host: "127.0.0.1", Port: 9003}, Status: NodeStatusAlive, JoinedAt: time.Now(), LastHeartbeat: time.Now()}); err != nil {
		t.Fatalf("AddNode(node3): %v", err)
	}

	ce := NewControllerElector(node, m, cfg)
	ce.logger = logger
	defer ce.Stop()

	became := make(chan struct{}, 1)
	lost := make(chan struct{}, 1)

	ce.SetOnBecomeController(func() { became <- struct{}{} })
	ce.SetOnLoseController(func() { lost <- struct{}{} })

	// Stub out network vote requests: everyone grants our vote.
	ce.SetRequestVoteFunc(func(nodeID NodeID, req *ControllerVoteRequest) (*ControllerVoteResponse, error) {
		return &ControllerVoteResponse{VoteGranted: true, Epoch: req.Epoch}, nil
	})

	ce.Start()
	ce.TriggerElection()

	select {
	case <-became:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting to become controller")
	}

	if !ce.IsController() {
		t.Fatalf("expected IsController=true")
	}
	if ce.CurrentController() != "node1" {
		t.Fatalf("CurrentController=%s want=node1", ce.CurrentController())
	}

	// Now force quorum loss (only 1 alive out of 3).
	if err := m.UpdateNodeStatus("node2", NodeStatusDead); err != nil {
		t.Fatalf("UpdateNodeStatus(node2): %v", err)
	}
	if err := m.UpdateNodeStatus("node3", NodeStatusDead); err != nil {
		t.Fatalf("UpdateNodeStatus(node3): %v", err)
	}

	// renewLease() should detect quorum loss and step down.
	ce.renewLease()

	select {
	case <-lost:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting to lose controller")
	}

	if ce.IsController() {
		t.Fatalf("expected IsController=false after quorum loss")
	}
	if st := ce.Stats(); st.IsController {
		t.Fatalf("Stats().IsController=true want=false")
	}
}

func TestControllerElector_VoteDeniedWithHigherEpochUpdatesControllerView(t *testing.T) {
	// =========================================================================
	// CONTROLLER ELECTOR: FENCING ON HIGHER EPOCH
	// =========================================================================
	//
	// WHY:
	//   Epoch fencing prevents a stale candidate from "winning" after a newer
	//   controller has already been elected elsewhere.
	// =========================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := (&ClusterConfig{NodeID: "node1", ClusterAddress: "127.0.0.1:0"}).WithDefaults()
	node, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	m := NewMembership(node, cfg, t.TempDir())
	if err := m.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf: %v", err)
	}
	_ = m.AddNode(&NodeInfo{ID: "node2", ClusterAddress: NodeAddress{Host: "127.0.0.1", Port: 9002}, Status: NodeStatusAlive, JoinedAt: time.Now(), LastHeartbeat: time.Now()})

	ce := NewControllerElector(node, m, cfg)
	ce.logger = logger
	defer ce.Stop()

	ce.Start()

	// Become a candidate.
	ce.TriggerElection()

	// Simulate a vote denial that carries a higher epoch and announces a controller.
	ce.handleVoteResult(VoteResult{NodeID: "node2", Response: &ControllerVoteResponse{
		VoteGranted:         false,
		Epoch:               ce.Epoch() + 5,
		CurrentControllerID: "node2",
	}})

	if ce.State() != ControllerStateFollower {
		t.Fatalf("state=%s want=%s", ce.State(), ControllerStateFollower)
	}
	if ce.CurrentController() != "node2" {
		t.Fatalf("CurrentController=%s want=node2", ce.CurrentController())
	}
}
