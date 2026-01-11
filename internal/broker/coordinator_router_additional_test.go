package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func mustGroupIDForPartition(t *testing.T, itm *InternalTopicManager, wantPartition int, prefix string) string {
	// WHY: In cluster mode, routing is based on groupID -> partition hash.
	// To test forwarding and 404-mapping deterministically, we need a groupID
	// that we *know* hashes to the partition we set a remote leader for.
	for i := 0; i < 10_000; i++ {
		candidate := fmt.Sprintf("%s-%d", prefix, i)
		if itm.GetPartitionForGroup(candidate) == wantPartition {
			return candidate
		}
	}
	t.Fatalf("failed to find groupID for partition=%d (prefix=%q)", wantPartition, prefix)
	return ""
}

// fakeCoordinator is a lightweight in-memory coordinator used to prove that the
// router correctly chooses "local" vs "forward" paths.
//
// WHY NOT USE THE REAL COORDINATOR HERE?
// The real coordinator pulls in background goroutines, timeouts, and
// persistence. Those are valuable to test elsewhere, but here we want very
// targeted tests around the router's decision logic and forwarding glue.
//
// By using a fake we can:
//   - deterministically assert which methods are called
//   - cover all router methods without fragile timing
//   - keep the tests fast
//
// NOTE: This type lives in _test.go so it can't leak into production builds.
// It implements GroupCoordinatorI.
//go:generate true

type fakeCoordinator struct {
	joinCalled       int
	leaveCalled      int
	heartbeatCalled  int
	syncCalled       int
	commitCalled     int
	fetchCalled      int
	describeCalled   int
	listCalled       int
	registerCalled   int
	unregisterCalled int
	startCalled      int
	stopCalled       int
	lastRegistered   map[string]int
	lastUnregistered []string
}

func newFakeCoordinator() *fakeCoordinator {
	return &fakeCoordinator{lastRegistered: make(map[string]int)}
}

func (f *fakeCoordinator) JoinGroup(req *JoinGroupRequest) (*JoinGroupResult, error) {
	f.joinCalled++
	if req.GroupID == "" {
		return nil, ErrGroupIDRequired
	}
	return &JoinGroupResult{MemberID: "member-1", GenerationID: 1, LeaderID: "member-1", IsLeader: true}, nil
}

func (f *fakeCoordinator) LeaveGroup(req *LeaveGroupRequest) error {
	f.leaveCalled++
	return nil
}

func (f *fakeCoordinator) Heartbeat(req *HeartbeatRequest) (*HeartbeatResult, error) {
	f.heartbeatCalled++
	return &HeartbeatResult{ShouldRejoin: false}, nil
}

func (f *fakeCoordinator) SyncGroup(req *SyncGroupRequest) (*SyncGroupResult, error) {
	f.syncCalled++
	return &SyncGroupResult{Assignment: []byte("assignment"), Protocol: "range"}, nil
}

func (f *fakeCoordinator) CommitOffset(req *OffsetCommitRequest) error {
	f.commitCalled++
	return nil
}

func (f *fakeCoordinator) FetchOffset(req *OffsetFetchRequest) (*OffsetFetchResult, error) {
	f.fetchCalled++
	return &OffsetFetchResult{Offset: 123, Metadata: "m"}, nil
}

func (f *fakeCoordinator) FetchAllOffsets(groupID string) (map[string]map[int]int64, error) {
	// Not used by CoordinatorRouter today.
	return map[string]map[int]int64{}, nil
}

func (f *fakeCoordinator) DescribeGroup(groupID string) (*GroupDescription, error) {
	f.describeCalled++
	return &GroupDescription{GroupID: groupID}, nil
}

func (f *fakeCoordinator) ListGroups() ([]string, error) {
	f.listCalled++
	return []string{"g1"}, nil
}

func (f *fakeCoordinator) RegisterTopic(topicName string, partitionCount int) {
	f.registerCalled++
	f.lastRegistered[topicName] = partitionCount
}

func (f *fakeCoordinator) UnregisterTopic(topicName string) {
	f.unregisterCalled++
	f.lastUnregistered = append(f.lastUnregistered, topicName)
}

func (f *fakeCoordinator) Start() error {
	f.startCalled++
	return nil
}

func (f *fakeCoordinator) Stop() error {
	f.stopCalled++
	return nil
}

func mustHostPortFromURL(t *testing.T, raw string) (string, int) {
	t.Helper()

	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("url.Parse failed: %v", err)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("net.SplitHostPort failed for %q: %v", u.Host, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("strconv.Atoi failed for %q: %v", portStr, err)
	}
	return host, port
}

func TestCoordinatorRouter_SingleNodeRoutesLocally(t *testing.T) {
	dir := t.TempDir()

	itm, err := NewInternalTopicManager(InternalTopicConfig{
		DataDir:               dir,
		OffsetsPartitionCount: 3,
		ReplicationFactor:     1,
		SegmentBytes:          1 << 20,
		RetentionBytes:        -1,
		EnableReplication:     false,
	})
	if err != nil {
		t.Fatalf("NewInternalTopicManager failed: %v", err)
	}
	defer itm.Stop()
	if err := itm.Start(); err != nil {
		t.Fatalf("InternalTopicManager.Start failed: %v", err)
	}

	fake := newFakeCoordinator()

	r := NewCoordinatorRouter(CoordinatorRouterConfig{
		LocalCoordinator:     fake,
		InternalTopicManager: itm,
		ClusterEnabled:       false,
		NodeID:               "node-1",
		ForwardTimeoutMs:     2000,
	})

	coord, err := r.FindCoordinator("g1")
	if err != nil {
		t.Fatalf("FindCoordinator failed: %v", err)
	}
	if !coord.IsLocal || coord.NodeID != "node-1" {
		t.Fatalf("FindCoordinator = %+v, want local node-1", coord)
	}

	// Exercise routing methods; they should call the local coordinator.
	ctx := context.Background()
	if _, err := r.JoinGroup(ctx, &JoinGroupRequest{GroupID: "g1"}); err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}
	if err := r.LeaveGroup(ctx, &LeaveGroupRequest{GroupID: "g1", MemberID: "m"}); err != nil {
		t.Fatalf("LeaveGroup failed: %v", err)
	}
	if _, err := r.Heartbeat(ctx, &HeartbeatRequest{GroupID: "g1", MemberID: "m", GenerationID: 1}); err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}
	if _, err := r.SyncGroup(ctx, &SyncGroupRequest{GroupID: "g1", MemberID: "m", GenerationID: 1}); err != nil {
		t.Fatalf("SyncGroup failed: %v", err)
	}
	if err := r.CommitOffset(ctx, &OffsetCommitRequest{GroupID: "g1", Topic: "orders", Partition: 0, Offset: 10}); err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}
	if _, err := r.FetchOffset(ctx, &OffsetFetchRequest{GroupID: "g1", Topic: "orders", Partition: 0}); err != nil {
		t.Fatalf("FetchOffset failed: %v", err)
	}
	if _, err := r.DescribeGroup(ctx, "g1"); err != nil {
		t.Fatalf("DescribeGroup failed: %v", err)
	}
	if _, err := r.ListGroups(ctx); err != nil {
		t.Fatalf("ListGroups failed: %v", err)
	}

	// Topic registration is pass-through.
	r.RegisterTopic("orders", 3)
	r.UnregisterTopic("orders")

	if fake.joinCalled == 0 || fake.commitCalled == 0 || fake.describeCalled == 0 {
		t.Fatalf("expected local coordinator to be used; calls=%+v", fake)
	}
	if fake.registerCalled != 1 || fake.lastRegistered["orders"] != 3 {
		t.Fatalf("RegisterTopic calls unexpected: calls=%d registered=%v", fake.registerCalled, fake.lastRegistered)
	}
	if fake.unregisterCalled != 1 {
		t.Fatalf("UnregisterTopic calls=%d, want 1", fake.unregisterCalled)
	}
}

func TestCoordinatorRouter_ClusterModeCoordinatorNotFound(t *testing.T) {
	dir := t.TempDir()

	itm, err := NewInternalTopicManager(InternalTopicConfig{
		DataDir:               dir,
		OffsetsPartitionCount: 3,
		ReplicationFactor:     1,
		SegmentBytes:          1 << 20,
		RetentionBytes:        -1,
		EnableReplication:     false,
	})
	if err != nil {
		t.Fatalf("NewInternalTopicManager failed: %v", err)
	}
	defer itm.Stop()
	if err := itm.Start(); err != nil {
		t.Fatalf("InternalTopicManager.Start failed: %v", err)
	}

	// Simulate that this node is NOT the leader for the group's partition.
	groupID := "g1"
	partition := itm.GetPartitionForGroup(groupID)
	itm.LostLeadership(partition)

	r := NewCoordinatorRouter(CoordinatorRouterConfig{
		LocalCoordinator:     newFakeCoordinator(),
		InternalTopicManager: itm,
		ClusterEnabled:       true,
		NodeID:               "node-1",
		ForwardTimeoutMs:     2000,
	})

	_, err = r.FindCoordinator(groupID)
	if !errors.Is(err, ErrCoordinatorNotFound) {
		t.Fatalf("FindCoordinator error=%v, want %v", err, ErrCoordinatorNotFound)
	}
}

func TestCoordinatorRouter_ForwardsToPartitionLeader(t *testing.T) {
	dir := t.TempDir()

	itm, err := NewInternalTopicManager(InternalTopicConfig{
		DataDir:               dir,
		OffsetsPartitionCount: 3,
		ReplicationFactor:     1,
		SegmentBytes:          1 << 20,
		RetentionBytes:        -1,
		EnableReplication:     false,
	})
	if err != nil {
		t.Fatalf("NewInternalTopicManager failed: %v", err)
	}
	defer itm.Stop()
	if err := itm.Start(); err != nil {
		t.Fatalf("InternalTopicManager.Start failed: %v", err)
	}

	// A minimal "remote coordinator" server used to exercise forwarding paths.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/internal/coordinator/join":
			var jr JoinGroupRequest
			_ = json.NewDecoder(req.Body).Decode(&jr)
			_ = json.NewEncoder(w).Encode(JoinGroupResult{MemberID: "m-remote", GenerationID: 7, LeaderID: "m-remote", IsLeader: true})
		case "/internal/coordinator/leave":
			w.WriteHeader(http.StatusNoContent)
		case "/internal/coordinator/heartbeat":
			_ = json.NewEncoder(w).Encode(HeartbeatResult{ShouldRejoin: false})
		case "/internal/coordinator/sync":
			_ = json.NewEncoder(w).Encode(SyncGroupResult{Assignment: []byte("a"), Protocol: "range"})
		case "/internal/coordinator/commit":
			w.WriteHeader(http.StatusNoContent)
		case "/internal/coordinator/fetch-offset":
			if strings.HasPrefix(req.URL.Query().Get("group"), "missing") {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(OffsetFetchResult{Offset: 42, Metadata: "", Timestamp: time.Now()})
		case "/internal/coordinator/describe":
			if strings.HasPrefix(req.URL.Query().Get("group"), "missing") {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(GroupDescription{GroupID: req.URL.Query().Get("group")})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	host, port := mustHostPortFromURL(t, server.URL)

	groupID := "g1"
	partition := itm.GetPartitionForGroup(groupID)
	itm.LostLeadership(partition)

	missingGroupID := mustGroupIDForPartition(t, itm, partition, "missing")

	r := NewCoordinatorRouter(CoordinatorRouterConfig{
		LocalCoordinator:     newFakeCoordinator(),
		InternalTopicManager: itm,
		ClusterEnabled:       true,
		NodeID:               "node-1",
		ForwardTimeoutMs:     2000,
	})

	r.UpdatePartitionLeader(partition, &CoordinatorInfo{NodeID: "node-2", Host: host, Port: port, IsLocal: false})

	ctx := context.Background()

	if _, err := r.JoinGroup(ctx, &JoinGroupRequest{GroupID: groupID}); err != nil {
		t.Fatalf("JoinGroup (forwarded) failed: %v", err)
	}
	if err := r.LeaveGroup(ctx, &LeaveGroupRequest{GroupID: groupID, MemberID: "m"}); err != nil {
		t.Fatalf("LeaveGroup (forwarded) failed: %v", err)
	}
	if _, err := r.Heartbeat(ctx, &HeartbeatRequest{GroupID: groupID, MemberID: "m", GenerationID: 1}); err != nil {
		t.Fatalf("Heartbeat (forwarded) failed: %v", err)
	}
	if _, err := r.SyncGroup(ctx, &SyncGroupRequest{GroupID: groupID, MemberID: "m", GenerationID: 1}); err != nil {
		t.Fatalf("SyncGroup (forwarded) failed: %v", err)
	}
	if err := r.CommitOffset(ctx, &OffsetCommitRequest{GroupID: groupID, Topic: "orders", Partition: 0, Offset: 10}); err != nil {
		t.Fatalf("CommitOffset (forwarded) failed: %v", err)
	}
	if _, err := r.FetchOffset(ctx, &OffsetFetchRequest{GroupID: groupID, Topic: "orders", Partition: 0}); err != nil {
		t.Fatalf("FetchOffset (forwarded) failed: %v", err)
	}
	if _, err := r.DescribeGroup(ctx, groupID); err != nil {
		t.Fatalf("DescribeGroup (forwarded) failed: %v", err)
	}

	// Cover the NotFound mappings.
	if _, err := r.FetchOffset(ctx, &OffsetFetchRequest{GroupID: missingGroupID, Topic: "orders", Partition: 0}); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("FetchOffset missing error=%v, want %v", err, ErrOffsetNotFound)
	}
	if _, err := r.DescribeGroup(ctx, missingGroupID); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("DescribeGroup missing error=%v, want %v", err, ErrGroupNotFound)
	}

	stats := r.Stats()
	if !stats.ClusterEnabled {
		t.Fatalf("Stats.ClusterEnabled=false, want true")
	}
	if stats.CachedLeaders != 1 {
		t.Fatalf("Stats.CachedLeaders=%d, want 1", stats.CachedLeaders)
	}
	if stats.LocalPartitions != 2 {
		t.Fatalf("Stats.LocalPartitions=%d, want 2 (3 total - 1 lost)", stats.LocalPartitions)
	}
}
