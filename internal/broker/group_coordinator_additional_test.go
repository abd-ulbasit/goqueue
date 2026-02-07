package broker

import (
	"errors"
	"testing"
	"time"
)

func TestGroupCoordinator_RegisterTopicAndListGroups(t *testing.T) {
	dataDir := t.TempDir()
	gc, err := NewGroupCoordinator(DefaultCoordinatorConfig(dataDir))
	if err != nil {
		t.Fatalf("NewGroupCoordinator failed: %v", err)
	}
	defer func() { _ = gc.Close() }()

	gc.RegisterTopic("orders", 3)
	if got, ok := gc.GetPartitionCount("orders"); !ok || got != 3 {
		t.Fatalf("GetPartitionCount(orders)=(%d,%v), want (3,true)", got, ok)
	}
	if got, ok := gc.GetPartitionCount("missing"); ok || got != 0 {
		t.Fatalf("GetPartitionCount(missing)=(%d,%v), want (0,false)", got, ok)
	}

	// Create a group by joining.
	resp, err := gc.JoinGroup("g1", "client-1", []string{"orders"})
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}
	if resp.MemberID == "" {
		t.Fatalf("JoinGroup returned empty memberID")
	}

	groups := gc.ListGroups()
	if len(groups) != 1 || groups[0] != "g1" {
		t.Fatalf("ListGroups=%v, want [g1]", groups)
	}
}

func TestGroupCoordinator_JoinLeaveAndDeleteGroup_Errors(t *testing.T) {
	dataDir := t.TempDir()
	gc, err := NewGroupCoordinator(DefaultCoordinatorConfig(dataDir))
	if err != nil {
		t.Fatalf("NewGroupCoordinator failed: %v", err)
	}
	defer func() { _ = gc.Close() }()

	// LeaveGroup should fail for unknown group.
	if err := gc.LeaveGroup("missing", "m"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("LeaveGroup unknown group err=%v, want %v", err, ErrGroupNotFound)
	}

	// DeleteGroup should fail for unknown group.
	if err := gc.DeleteGroup("missing"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("DeleteGroup unknown group err=%v, want %v", err, ErrGroupNotFound)
	}

	// Create group and then delete it.
	gc.RegisterTopic("orders", 1)
	joinResp, err := gc.JoinGroup("g1", "client-1", []string{"orders"})
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}
	if joinResp.MemberID == "" {
		t.Fatalf("JoinGroup returned empty memberID")
	}

	if err := gc.DeleteGroup("g1"); err != nil {
		t.Fatalf("DeleteGroup failed: %v", err)
	}

	if groups := gc.ListGroups(); len(groups) != 0 {
		t.Fatalf("ListGroups after delete=%v, want empty", groups)
	}
}

func TestGroupCoordinator_HeartbeatAndCommitAndFetchOffsets(t *testing.T) {
	dataDir := t.TempDir()
	gc, err := NewGroupCoordinator(DefaultCoordinatorConfig(dataDir))
	if err != nil {
		t.Fatalf("NewGroupCoordinator failed: %v", err)
	}
	defer func() { _ = gc.Close() }()

	gc.RegisterTopic("orders", 2)
	joinResp, err := gc.JoinGroup("g1", "client-1", []string{"orders"})
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}

	// A heartbeat should succeed for an active member.
	if err := gc.Heartbeat("g1", joinResp.MemberID, joinResp.Generation); err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	// Commit offsets and fetch them back.
	if err := gc.CommitOffset("g1", "orders", 0, 100, joinResp.MemberID); err != nil {
		t.Fatalf("CommitOffset p0 failed: %v", err)
	}
	if err := gc.CommitOffset("g1", "orders", 1, 200, joinResp.MemberID); err != nil {
		t.Fatalf("CommitOffset p1 failed: %v", err)
	}

	if got, err := gc.GetOffset("g1", "orders", 0); err != nil || got != 100 {
		t.Fatalf("GetOffset orders[0] got=%d err=%v, want 100 nil", got, err)
	}
	if got, err := gc.GetOffset("g1", "orders", 1); err != nil || got != 200 {
		t.Fatalf("GetOffset orders[1] got=%d err=%v, want 200 nil", got, err)
	}

	// Unknown group returns ErrOffsetNotFound because offsets are stored independently
	// from group membership state.
	if _, err := gc.GetOffset("missing", "orders", 0); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("GetOffset missing group err=%v, want %v", err, ErrOffsetNotFound)
	}
}

func TestGroupCoordinator_CheckExpiredSessions_EvictsMember(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultCoordinatorConfig(dataDir)
	// Make eviction deterministic: a very short session timeout.
	cfg.DefaultGroupConfig.SessionTimeoutMs = 5
	cfg.DefaultGroupConfig.RebalanceTimeoutMs = 50

	gc, err := NewGroupCoordinator(cfg)
	if err != nil {
		t.Fatalf("NewGroupCoordinator failed: %v", err)
	}
	defer func() { _ = gc.Close() }()

	gc.RegisterTopic("orders", 1)
	joinResp, err := gc.JoinGroup("g1", "client-1", []string{"orders"})
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}

	// Force the member to look expired without sleeping.
	g, err := gc.GetGroup("g1")
	if err != nil {
		t.Fatalf("GetGroup failed: %v", err)
	}

	g.mu.Lock()
	m := g.Members[joinResp.MemberID]
	if m == nil {
		g.mu.Unlock()
		t.Fatalf("expected member to exist")
	}
	m.LastHeartbeat = time.Now().Add(-1 * time.Second)
	g.mu.Unlock()

	// checkExpiredSessions is normally timer-driven; calling it directly makes the
	// test deterministic while still covering the coordinator logic.
	gc.checkExpiredSessions()

	g.mu.RLock()
	_, stillThere := g.Members[joinResp.MemberID]
	g.mu.RUnlock()
	if stillThere {
		t.Fatalf("expected member to be evicted")
	}
}

func TestGroupCoordinator_CreateGroupAndInfo_APIs(t *testing.T) {
	dataDir := t.TempDir()
	gc, err := NewGroupCoordinator(DefaultCoordinatorConfig(dataDir))
	if err != nil {
		t.Fatalf("NewGroupCoordinator failed: %v", err)
	}
	defer func() { _ = gc.Close() }()

	// CreateGroup should create once, and return the same group on subsequent calls.
	g1, err := gc.CreateGroup("g1", []string{"orders"})
	if err != nil {
		t.Fatalf("CreateGroup failed: %v", err)
	}
	g2, err := gc.CreateGroup("g1", []string{"orders"})
	if err != nil {
		t.Fatalf("CreateGroup (2nd) failed: %v", err)
	}
	if g1 != g2 {
		t.Fatalf("CreateGroup did not return the same instance")
	}

	info, err := gc.GetGroupInfo("g1")
	if err != nil {
		t.Fatalf("GetGroupInfo failed: %v", err)
	}
	if info.ID != "g1" {
		t.Fatalf("GetGroupInfo.ID=%q, want %q", info.ID, "g1")
	}

	infos := gc.GetAllGroupsInfo()
	if len(infos) != 1 {
		t.Fatalf("GetAllGroupsInfo len=%d, want 1", len(infos))
	}
	if infos[0].ID != "g1" {
		t.Fatalf("GetAllGroupsInfo[0].ID=%q, want %q", infos[0].ID, "g1")
	}
}

func TestGroupCoordinator_CommitOffsetsAndGetGroupOffsets(t *testing.T) {
	dataDir := t.TempDir()
	gc, err := NewGroupCoordinator(DefaultCoordinatorConfig(dataDir))
	if err != nil {
		t.Fatalf("NewGroupCoordinator failed: %v", err)
	}
	defer func() { _ = gc.Close() }()

	gc.RegisterTopic("orders", 2)
	joinResp, err := gc.JoinGroup("g1", "client-1", []string{"orders"})
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}

	// With a single member, the range assignor gives all partitions.
	if err := gc.CommitOffsets("g1", map[string]map[int]int64{
		"orders": {0: 10, 1: 20},
	}, joinResp.MemberID); err != nil {
		t.Fatalf("CommitOffsets failed: %v", err)
	}

	goff, err := gc.GetGroupOffsets("g1")
	if err != nil {
		t.Fatalf("GetGroupOffsets failed: %v", err)
	}
	if got := goff.Topics["orders"].Partitions[0].Offset; got != 10 {
		t.Fatalf("GetGroupOffsets orders[0]=%d, want 10", got)
	}
	if got := goff.Topics["orders"].Partitions[1].Offset; got != 20 {
		t.Fatalf("GetGroupOffsets orders[1]=%d, want 20", got)
	}
}

func TestGroupCoordinator_MarkForAutoCommit_WrapperStagesOffsets(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultCoordinatorConfig(dataDir)

	// Enable auto-commit *staging* but keep the ticker disabled so the test is
	// deterministic and doesn't rely on wall-clock timers.
	cfg.DefaultGroupConfig.AutoCommit = true
	cfg.DefaultGroupConfig.AutoCommitIntervalMs = 0

	gc, err := NewGroupCoordinator(cfg)
	if err != nil {
		t.Fatalf("NewGroupCoordinator failed: %v", err)
	}
	defer func() { _ = gc.Close() }()

	gc.MarkForAutoCommit("g1", "orders", 0, 123)
	gc.offsetManager.flushPendingCommits()

	got, err := gc.GetOffset("g1", "orders", 0)
	if err != nil {
		t.Fatalf("GetOffset failed: %v", err)
	}
	if got != 123 {
		t.Fatalf("GetOffset=%d, want 123", got)
	}
}
