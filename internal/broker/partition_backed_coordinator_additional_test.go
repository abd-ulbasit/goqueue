package broker

import (
	"errors"
	"testing"
)

func TestPartitionBackedCoordinator_PersistsAndRebuildsState(t *testing.T) {
	dataDir := t.TempDir()

	// Use a single offsets partition so group→partition routing is deterministic
	// and we avoid accidental "not coordinator" failures.
	itmCfg := DefaultInternalTopicConfig(dataDir)
	itmCfg.OffsetsPartitionCount = 1
	itmCfg.EnableReplication = false

	itm, err := NewInternalTopicManager(itmCfg)
	if err != nil {
		t.Fatalf("NewInternalTopicManager failed: %v", err)
	}
	if err := itm.Start(); err != nil {
		t.Fatalf("InternalTopicManager.Start failed: %v", err)
	}
	defer func() { _ = itm.Stop() }()

	coordCfg := DefaultPartitionBackedCoordinatorConfig()
	// Slow down background ticking in tests; we'll call checkExpiredSessions()
	// directly when we want to cover that logic.
	coordCfg.SessionCheckIntervalMs = 60 * 60 * 1000

	c1, err := NewPartitionBackedCoordinator(coordCfg, itm)
	if err != nil {
		t.Fatalf("NewPartitionBackedCoordinator failed: %v", err)
	}
	if err := c1.Start(); err != nil {
		t.Fatalf("coordinator.Start failed: %v", err)
	}
	defer func() { _ = c1.Stop() }()

	// Join as a brand-new member. With a new group, we must become leader.
	join, err := c1.JoinGroup(&JoinGroupRequest{
		GroupID:            "g1",
		ClientID:           "client-1",
		ClientHost:         "host-1",
		SessionTimeoutMs:   5000,
		RebalanceTimeoutMs: 5000,
		ProtocolType:       "consumer",
		Protocols: map[string][]byte{
			"range": []byte("protocol-metadata"),
		},
	})
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}
	if join.MemberID == "" || !join.IsLeader {
		t.Fatalf("JoinGroup memberID=%q isLeader=%v, want non-empty + true", join.MemberID, join.IsLeader)
	}

	// Leader provides assignment for itself.
	assignment, _ := EncodeAssignment([]TopicPartition{{Topic: "orders", Partition: 0}})
	syncRes, err := c1.SyncGroup(&SyncGroupRequest{
		GroupID:      "g1",
		MemberID:     join.MemberID,
		GenerationID: join.GenerationID,
		Assignments:  map[string][]byte{join.MemberID: assignment},
	})
	if err != nil {
		t.Fatalf("SyncGroup failed: %v", err)
	}
	decoded, err := DecodeAssignment(syncRes.Assignment)
	if err != nil {
		t.Fatalf("DecodeAssignment failed: %v", err)
	}
	if len(decoded) != 1 || decoded[0].Topic != "orders" || decoded[0].Partition != 0 {
		t.Fatalf("SyncGroup assignment=%v, want orders-0", decoded)
	}

	// Commit and fetch an offset to cover the internal-topic-backed offset cache.
	if err := c1.CommitOffset(&OffsetCommitRequest{
		GroupID:   "g1",
		Topic:     "orders",
		Partition: 0,
		Offset:    123,
		Metadata:  "m",
	}); err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}
	fetched, err := c1.FetchOffset(&OffsetFetchRequest{GroupID: "g1", Topic: "orders", Partition: 0})
	if err != nil {
		t.Fatalf("FetchOffset failed: %v", err)
	}
	if fetched.Offset != 123 {
		t.Fatalf("FetchOffset.Offset=%d, want 123", fetched.Offset)
	}

	// Describe + list should see the group.
	desc, err := c1.DescribeGroup("g1")
	if err != nil {
		t.Fatalf("DescribeGroup failed: %v", err)
	}
	if desc.GroupID != "g1" || desc.Leader != join.MemberID || desc.Generation != join.GenerationID {
		t.Fatalf("DescribeGroup=%+v, want g1 leader=%q gen=%d", desc, join.MemberID, join.GenerationID)
	}
	if len(desc.Members) != 1 {
		t.Fatalf("DescribeGroup members=%d, want 1", len(desc.Members))
	}

	groups, err := c1.ListGroups()
	if err != nil {
		t.Fatalf("ListGroups failed: %v", err)
	}
	if len(groups) != 1 || groups[0] != "g1" {
		t.Fatalf("ListGroups=%v, want [g1]", groups)
	}

	// Restart just the coordinator (same internal topic manager, same logs) and
	// ensure state rebuild rehydrates offsets and group metadata.
	if err := c1.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	c2, err := NewPartitionBackedCoordinator(coordCfg, itm)
	if err != nil {
		t.Fatalf("NewPartitionBackedCoordinator (2) failed: %v", err)
	}
	if err := c2.Start(); err != nil {
		t.Fatalf("coordinator.Start (2) failed: %v", err)
	}
	defer func() { _ = c2.Stop() }()

	fetched2, err := c2.FetchOffset(&OffsetFetchRequest{GroupID: "g1", Topic: "orders", Partition: 0})
	if err != nil {
		t.Fatalf("FetchOffset (rebuild) failed: %v", err)
	}
	if fetched2.Offset != 123 {
		t.Fatalf("FetchOffset (rebuild).Offset=%d, want 123", fetched2.Offset)
	}

	desc2, err := c2.DescribeGroup("g1")
	if err != nil {
		t.Fatalf("DescribeGroup (rebuild) failed: %v", err)
	}
	if desc2.GroupID != "g1" {
		t.Fatalf("DescribeGroup (rebuild).GroupID=%q, want %q", desc2.GroupID, "g1")
	}

	stats := c2.Stats()
	if !stats.StateRebuilt || stats.Groups != 1 || stats.OffsetsStored < 1 {
		t.Fatalf("Stats=%+v, want rebuilt=true groups=1 offsets>=1", stats)
	}
}

func TestPartitionBackedCoordinator_NotCoordinatorErrors(t *testing.T) {
	dataDir := t.TempDir()

	// In replication-enabled mode (without leadership being assigned), the
	// internal topic manager has NO local partitions. That makes IsCoordinatorFor
	// return false, which should translate to ErrNotCoordinator from the
	// partition-backed coordinator.
	itmCfg := DefaultInternalTopicConfig(dataDir)
	itmCfg.OffsetsPartitionCount = 1
	itmCfg.EnableReplication = true

	itm, err := NewInternalTopicManager(itmCfg)
	if err != nil {
		t.Fatalf("NewInternalTopicManager failed: %v", err)
	}
	if err := itm.Start(); err != nil {
		t.Fatalf("InternalTopicManager.Start failed: %v", err)
	}
	defer func() { _ = itm.Stop() }()

	c, err := NewPartitionBackedCoordinator(DefaultPartitionBackedCoordinatorConfig(), itm)
	if err != nil {
		t.Fatalf("NewPartitionBackedCoordinator failed: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = c.Stop() }()

	if _, err := c.JoinGroup(&JoinGroupRequest{GroupID: "g1", ClientID: "c1"}); !errors.Is(err, ErrNotCoordinator) {
		t.Fatalf("JoinGroup err=%v, want %v", err, ErrNotCoordinator)
	}
	if err := c.CommitOffset(&OffsetCommitRequest{GroupID: "g1", Topic: "orders", Partition: 0, Offset: 1}); !errors.Is(err, ErrNotCoordinator) {
		t.Fatalf("CommitOffset err=%v, want %v", err, ErrNotCoordinator)
	}
	if _, err := c.FetchOffset(&OffsetFetchRequest{GroupID: "g1", Topic: "orders", Partition: 0}); !errors.Is(err, ErrNotCoordinator) {
		t.Fatalf("FetchOffset err=%v, want %v", err, ErrNotCoordinator)
	}
	if _, err := c.DescribeGroup("g1"); !errors.Is(err, ErrNotCoordinator) {
		t.Fatalf("DescribeGroup err=%v, want %v", err, ErrNotCoordinator)
	}
}

func TestPartitionBackedCoordinator_ApplyTombstones(t *testing.T) {
	dataDir := t.TempDir()

	itmCfg := DefaultInternalTopicConfig(dataDir)
	itmCfg.OffsetsPartitionCount = 1
	itmCfg.EnableReplication = false
	itm, err := NewInternalTopicManager(itmCfg)
	if err != nil {
		t.Fatalf("NewInternalTopicManager failed: %v", err)
	}
	if err := itm.Start(); err != nil {
		t.Fatalf("InternalTopicManager.Start failed: %v", err)
	}
	defer func() { _ = itm.Stop() }()

	c, err := NewPartitionBackedCoordinator(DefaultPartitionBackedCoordinatorConfig(), itm)
	if err != nil {
		t.Fatalf("NewPartitionBackedCoordinator failed: %v", err)
	}

	// Seed an offset commit via applyRecord.
	commit := NewOffsetCommitRecord("g1", "orders", 0, 10, "m")
	if err := c.applyRecord(commit); err != nil {
		t.Fatalf("applyRecord(commit) failed: %v", err)
	}

	if _, err := c.FetchOffset(&OffsetFetchRequest{GroupID: "g1", Topic: "orders", Partition: 0}); err != nil {
		t.Fatalf("FetchOffset after commit failed: %v", err)
	}

	tomb := NewTombstoneRecord(RecordTypeOffsetCommit, commit.Key)
	if err := c.applyRecord(tomb); err != nil {
		t.Fatalf("applyRecord(tombstone) failed: %v", err)
	}
	if _, err := c.FetchOffset(&OffsetFetchRequest{GroupID: "g1", Topic: "orders", Partition: 0}); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("FetchOffset after tombstone err=%v, want %v", err, ErrOffsetNotFound)
	}

	// Seed group metadata and then tombstone it.
	gm := NewGroupMetadataRecord("g1", GroupStateStable, "range", "leader-1", 1, []InternalMemberMetadata{{
		MemberID:         "leader-1",
		ClientID:         "c1",
		ClientHost:       "h1",
		SessionTimeout:   1000,
		RebalanceTimeout: 1000,
	}})
	if err := c.applyRecord(gm); err != nil {
		t.Fatalf("applyRecord(groupmeta) failed: %v", err)
	}
	if _, err := c.DescribeGroup("g1"); err != nil {
		t.Fatalf("DescribeGroup after groupmeta failed: %v", err)
	}

	gmKey, err := DecodeGroupMetadataKey(gm.Key)
	if err != nil {
		t.Fatalf("DecodeGroupMetadataKey failed: %v", err)
	}
	_ = gmKey

	gmTomb := NewTombstoneRecord(RecordTypeGroupMetadata, gm.Key)
	if err := c.applyRecord(gmTomb); err != nil {
		t.Fatalf("applyRecord(groupmeta tombstone) failed: %v", err)
	}
	if _, err := c.DescribeGroup("g1"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("DescribeGroup after tombstone err=%v, want %v", err, ErrGroupNotFound)
	}
}
