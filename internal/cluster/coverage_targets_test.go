package cluster

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestCoordinator_StatsIncludesUptime(t *testing.T) {
	// =========================================================================
	// COORDINATOR STATS: UPTIME SHOULD NOT ALWAYS BE ZERO
	// =========================================================================
	//
	// WHY THIS TEST EXISTS:
	//   CoordinatorStats exposes an Uptime field for observability (admin APIs,
	//   debugging, dashboards). Prior to this test, Stats() never populated
	//   Uptime, so it was always 0 even when the coordinator was running.
	//
	// This test ensures:
	//   - Start() marks the coordinator as started (uptime > 0)
	//   - Stop() resets uptime to 0 (deterministic + reflects non-running)
	// =========================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := (&ClusterConfig{NodeID: "n1", ClusterAddress: "127.0.0.1:0", QuorumSize: 1}).WithDefaults()

	c, err := NewCoordinator(cfg, "", logger)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = c.Stop(context.Background()) }()

	stats := c.Stats()
	if stats.NodeID != "n1" {
		t.Fatalf("NodeID=%q want=%q", stats.NodeID, "n1")
	}
	if stats.Uptime <= 0 {
		t.Fatalf("Uptime=%v want > 0", stats.Uptime)
	}

	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	statsAfter := c.Stats()
	if statsAfter.Uptime != 0 {
		t.Fatalf("Uptime after Stop=%v want=0", statsAfter.Uptime)
	}
}

func TestFailureDetector_HealthQueriesAndString(t *testing.T) {
	// =========================================================================
	// FAILURE DETECTOR: HEALTH QUERIES + DEBUG STRING
	// =========================================================================
	//
	// WHY:
	//   The FailureDetector is used for cluster liveness decisions, but the
	//   "health query" helpers (IsAlive/IsSuspect/IsDead/GetNodeHealth/String)
	//   were previously uncovered. These helpers are also what operators/debug
	//   tooling call first.
	//
	// This test exercises:
	//   - Status query helpers for present + missing nodes
	//   - GetNodeHealth behavior when heartbeat is missing vs present
	//   - String() output (used in logging/debugging)
	// =========================================================================

	cfg := (&ClusterConfig{NodeID: "n1", ClusterAddress: "127.0.0.1:0", QuorumSize: 1}).WithDefaults()
	n1, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	m := NewMembership(n1, cfg, "")
	if err := m.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf: %v", err)
	}

	// Add a second node and drive it into SUSPECT then DEAD to cover all status checks.
	cfg2 := (&ClusterConfig{NodeID: "n2", ClusterAddress: "127.0.0.1:9002", ClientAddress: "127.0.0.1:8082", QuorumSize: 1}).WithDefaults()
	n2, err := NewNode(cfg2)
	if err != nil {
		t.Fatalf("NewNode(n2): %v", err)
	}
	if err := m.AddNode(n2.Info()); err != nil {
		t.Fatalf("AddNode(n2): %v", err)
	}
	if err := m.UpdateNodeStatus("n2", NodeStatusSuspect); err != nil {
		t.Fatalf("UpdateNodeStatus(n2,SUSPECT): %v", err)
	}
	if err := m.UpdateNodeStatus("n2", NodeStatusDead); err != nil {
		t.Fatalf("UpdateNodeStatus(n2,DEAD): %v", err)
	}

	fd := NewFailureDetector(m, cfg)

	if !fd.IsAlive("n1") {
		t.Fatalf("expected n1 alive")
	}
	if fd.IsAlive("missing") {
		t.Fatalf("expected missing node not alive")
	}
	if !fd.IsDead("n2") {
		t.Fatalf("expected n2 dead")
	}
	if fd.IsSuspect("n2") {
		t.Fatalf("expected n2 not suspect once dead")
	}

	if _, err := fd.GetNodeHealth("missing"); err == nil {
		t.Fatalf("expected error for missing node")
	}

	// Seed a heartbeat so GetNodeHealth returns non-zero timestamps.
	fd.mu.Lock()
	fd.lastHeartbeats["n2"] = time.Now().Add(-250 * time.Millisecond)
	fd.mu.Unlock()

	rep, err := fd.GetNodeHealth("n2")
	if err != nil {
		t.Fatalf("GetNodeHealth(n2): %v", err)
	}
	if rep.NodeID != "n2" {
		t.Fatalf("NodeID=%q want=%q", rep.NodeID, "n2")
	}
	if rep.SinceHeartbeat <= 0 {
		t.Fatalf("SinceHeartbeat=%v want > 0", rep.SinceHeartbeat)
	}

	// String() should be stable enough for humans to read and include counts.
	s := fd.String()
	if !strings.Contains(s, "FailureDetector{") {
		t.Fatalf("String()=%q missing prefix", s)
	}
}

func TestMembership_AccessorsAndString(t *testing.T) {
	// =========================================================================
	// MEMBERSHIP ACCESSORS: VERSION / ISCONTROLLER / LOCALNODE / STRING
	// =========================================================================

	cfg := (&ClusterConfig{NodeID: "n1", ClusterAddress: "127.0.0.1:0", QuorumSize: 1}).WithDefaults()
	n1, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	m := NewMembership(n1, cfg, "")
	if err := m.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf: %v", err)
	}

	if m.LocalNode() != n1 {
		t.Fatalf("LocalNode mismatch")
	}

	// After RegisterSelf, state version must be non-zero.
	if v := m.Version(); v <= 0 {
		t.Fatalf("Version=%d want > 0", v)
	}

	// Setting controller to ourselves should make IsController() true.
	if err := m.SetController(n1.ID(), 1); err != nil {
		t.Fatalf("SetController(self): %v", err)
	}
	if !m.IsController() {
		t.Fatalf("expected IsController() true")
	}

	s := m.String()
	if !strings.Contains(s, "Membership{") {
		t.Fatalf("String()=%q missing prefix", s)
	}
}

func TestMetadataStore_UpdatePartitionCount_ValidatesNotifiesAndStats(t *testing.T) {
	// =========================================================================
	// METADATA STORE: PARTITION SCALING (KAFKA-STYLE) + LISTENERS + STATS
	// =========================================================================
	//
	// WHY:
	//   Partition scaling is a correctness-sensitive operation (only increases).
	//   We also rely on listeners to push metadata updates to followers.
	// =========================================================================

	ms := NewMetadataStore("")

	updates := make(chan *ClusterMeta, 10)
	ms.AddListener(func(meta *ClusterMeta) {
		updates <- meta
	})

	if err := ms.CreateTopic("orders", 1, 1, TopicConfig{}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Drain the create-topic notification.
	select {
	case <-updates:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected listener notification on CreateTopic")
	}

	if err := ms.UpdatePartitionCount("missing", 2); err == nil {
		t.Fatalf("expected error for missing topic")
	}

	// Decrease is forbidden.
	if err := ms.UpdatePartitionCount("orders", 0); err == nil {
		t.Fatalf("expected error when decreasing partitions")
	}

	// No-op increase should not error.
	if err := ms.UpdatePartitionCount("orders", 1); err != nil {
		t.Fatalf("expected no-op update to succeed: %v", err)
	}

	// A real increase should notify listeners.
	if err := ms.UpdatePartitionCount("orders", 3); err != nil {
		t.Fatalf("UpdatePartitionCount: %v", err)
	}

	select {
	case meta := <-updates:
		if meta.Topics["orders"].PartitionCount != 3 {
			t.Fatalf("listener saw partitionCount=%d want=3", meta.Topics["orders"].PartitionCount)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected listener notification on UpdatePartitionCount")
	}

	topic := ms.GetTopic("orders")
	if topic == nil || topic.PartitionCount != 3 {
		t.Fatalf("GetTopic().PartitionCount=%v want=3", topic)
	}

	st := ms.Stats()
	if st.TopicCount != 1 {
		t.Fatalf("Stats.TopicCount=%d want=1", st.TopicCount)
	}
	if st.PartitionCount != 3 {
		t.Fatalf("Stats.PartitionCount=%d want=3", st.PartitionCount)
	}
}

func TestNode_AddressAccessorsAndTagsCopy(t *testing.T) {
	// =========================================================================
	// NODE ACCESSORS: CLIENT/CLUSTER/ADVERTISE ADDRESS + TAGS COPY
	// =========================================================================

	cfg := (&ClusterConfig{
		NodeID:           "n1",
		ClientAddress:    "127.0.0.1:8081",
		ClusterAddress:   "127.0.0.1:9001",
		AdvertiseAddress: "10.0.0.1:9001",
		Rack:             "rack-a",
		Tags:             map[string]string{"az": "us-east-1a"},
		QuorumSize:       1,
	}).WithDefaults()

	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	if got := n.ClientAddress().String(); got != "127.0.0.1:8081" {
		t.Fatalf("ClientAddress=%q want=%q", got, "127.0.0.1:8081")
	}
	if got := n.ClusterAddress().String(); got != "127.0.0.1:9001" {
		t.Fatalf("ClusterAddress=%q want=%q", got, "127.0.0.1:9001")
	}
	if got := n.AdvertiseAddress().String(); got != "10.0.0.1:9001" {
		t.Fatalf("AdvertiseAddress=%q want=%q", got, "10.0.0.1:9001")
	}

	// Tags() must return a copy so callers can't mutate internal state.
	tags := n.Tags()
	if tags["az"] != "us-east-1a" {
		t.Fatalf("Tags[az]=%q want=%q", tags["az"], "us-east-1a")
	}
	tags["az"] = "evil-mutator"
	if n.Tags()["az"] != "us-east-1a" {
		t.Fatalf("Tags() appears to share map backing (unexpected mutation)")
	}

	// Config() should return the same pointer we were constructed with.
	if n.Config() == nil {
		t.Fatalf("Config() returned nil")
	}
}
