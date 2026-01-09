// =============================================================================
// REPLICATION TYPES - DATA STRUCTURES FOR LOG REPLICATION
// =============================================================================
//
// WHAT: Types used for leader-follower log replication.
//
// CORE CONCEPTS:
//
//   1. REPLICA STATE - Is this node leader or follower for a partition?
//      ┌─────────────────────────────────────────────────────────────────┐
//      │                    REPLICA STATE MACHINE                        │
//      │                                                                 │
//      │      ┌──────────┐  elected   ┌────────┐                         │
//      │      │ FOLLOWER │───────────►│ LEADER │                         │
//      │      └────┬─────┘            └────┬───┘                         │
//      │           │                       │                             │
//      │           │                       │  lost election / demoted    │
//      │           │                       │                             │
//      │           ▼                       ▼                             │
//      │      ┌──────────┐            ┌────────┐                         │
//      │      │ OFFLINE  │◄───────────│FOLLOWER│                         │
//      │      └──────────┘            └────────┘                         │
//      └─────────────────────────────────────────────────────────────────┘
//
//   2. FETCH PROTOCOL - How followers get data from leader
//      ┌─────────────────────────────────────────────────────────────────┐
//      │ Follower                    Leader                              │
//      │    │                          │                                 │
//      │    │  FetchRequest{           │                                 │
//      │    │    topic: "orders",      │                                 │
//      │    │    partition: 0,         │                                 │
//      │    │    fetchOffset: 1000,    │ "Give me everything from 1000"  │
//      │    │    maxBytes: 1MB         │                                 │
//      │    │  }                       │                                 │
//      │    │─────────────────────────►│                                 │
//      │    │                          │                                 │
//      │    │  FetchResponse{          │                                 │
//      │    │    messages: [...],      │                                 │
//      │    │    highWatermark: 1050,  │ "Committed up to 1050"          │
//      │    │    leaderEpoch: 5        │                                 │
//      │    │  }                       │                                 │
//      │    │◄─────────────────────────│                                 │
//      └─────────────────────────────────────────────────────────────────┘
//
//   3. HIGH WATERMARK (HW) - What's safe to read
//      ┌─────────────────────────────────────────────────────────────────┐
//      │                                                                 │
//      │  Leader log:    [0] [1] [2] [3] [4] [5] [6] [7]                 │
//      │                              │           │                      │
//      │                              HW=4        LEO=7                  │
//      │                                                                 │
//      │  HW = "High Watermark" = last offset replicated to ALL ISR      │
//      │  LEO = "Log End Offset" = next offset to be written             │
//      │                                                                 │
//      │  RULE: Consumers can only read up to HW                         │
//      │  WHY: If leader dies, messages after HW might be lost           │
//      │                                                                 │
//      └─────────────────────────────────────────────────────────────────┘
//
//   4. ISR (In-Sync Replicas) - Who's caught up
//      ┌─────────────────────────────────────────────────────────────────┐
//      │                                                                 │
//      │  REPLICAS = [A, B, C]     (all nodes that SHOULD have data)     │
//      │  ISR      = [A, B]        (nodes that ACTUALLY have latest)     │
//      │                                                                 │
//      │  C falls out of ISR when:                                       │
//      │    - Time: No fetch for 10 seconds, OR                          │
//      │    - Offset: More than 1000 messages behind                     │
//      │                                                                 │
//      │  C rejoins ISR when:                                            │
//      │    - Catches up to leader's LEO                                 │
//      │                                                                 │
//      └─────────────────────────────────────────────────────────────────┘
//
// COMPARISON:
//   - Kafka: Same concepts (ISR, HW, LEO), Raft-like replication in KRaft
//   - MongoDB: Similar with replica sets, oplog replication
//   - PostgreSQL: Streaming replication with WAL, synchronous replicas
//
// =============================================================================

package cluster

import (
	"fmt"
	"time"
)

// =============================================================================
// REPLICA STATE
// =============================================================================

// ReplicaRole indicates whether this replica is leader or follower.
type ReplicaRole string

const (
	// ReplicaRoleLeader means this node handles reads/writes for the partition.
	ReplicaRoleLeader ReplicaRole = "leader"

	// ReplicaRoleFollower means this node replicates from the leader.
	ReplicaRoleFollower ReplicaRole = "follower"

	// ReplicaRoleOffline means this replica is not active.
	ReplicaRoleOffline ReplicaRole = "offline"
)

// ReplicaState tracks the current state of a local replica.
type ReplicaState struct {
	// Topic is the topic name.
	Topic string `json:"topic"`

	// Partition is the partition number.
	Partition int `json:"partition"`

	// Role is whether this node is leader or follower.
	Role ReplicaRole `json:"role"`

	// LeaderID is the current leader's node ID.
	// If this node is leader, LeaderID == local node ID.
	LeaderID NodeID `json:"leader_id"`

	// LeaderEpoch increments each time leadership changes.
	// Used to detect stale leaders (zombie fencing).
	LeaderEpoch int64 `json:"leader_epoch"`

	// LogEndOffset (LEO) is the next offset to be written.
	LogEndOffset int64 `json:"log_end_offset"`

	// HighWatermark is the last offset replicated to ALL ISR members.
	// Consumers can only read up to this point.
	HighWatermark int64 `json:"high_watermark"`

	// UpdatedAt is when this state was last modified.
	UpdatedAt time.Time `json:"updated_at"`
}

// String returns a human-readable representation.
func (rs *ReplicaState) String() string {
	return fmt.Sprintf("%s-%d [%s] leader=%s epoch=%d LEO=%d HW=%d",
		rs.Topic, rs.Partition, rs.Role, rs.LeaderID, rs.LeaderEpoch,
		rs.LogEndOffset, rs.HighWatermark)
}

// =============================================================================
// FETCH PROTOCOL
// =============================================================================
//
// WHY PULL (NOT PUSH)?
//   - Follower controls its own pace (backpressure built-in)
//   - If follower is slow, it fetches in batches
//   - Simpler failure handling (follower reconnects and resumes)
//   - Same protocol works for consumers (with different semantics)
//
// =============================================================================

// FetchRequest is sent by followers to fetch data from leader.
type FetchRequest struct {
	// Topic is the topic to fetch from.
	Topic string `json:"topic"`

	// Partition is the partition to fetch from.
	Partition int `json:"partition"`

	// FromOffset is the offset to start fetching from.
	// "Give me all messages starting at this offset."
	FromOffset int64 `json:"from_offset"`

	// MaxBytes limits the response size.
	// Default: 1MB. Leader won't return more than this.
	MaxBytes int `json:"max_bytes"`

	// FollowerID identifies the follower making the request.
	// Used by leader to track ISR status.
	FollowerID NodeID `json:"follower_id"`

	// LeaderEpoch is the follower's expected leader epoch.
	// If stale, leader rejects the request (epoch fencing).
	// TODO: then this rapilica would be good for nothing ?
	LeaderEpoch int64 `json:"leader_epoch"`
}

// FetchResponse is returned by leader with messages and metadata.
type FetchResponse struct {
	// Topic is the topic fetched.
	Topic string `json:"topic"`

	// Partition is the partition fetched.
	Partition int `json:"partition"`

	// ErrorCode indicates any error (0 = success).
	ErrorCode int `json:"error_code"`

	// ErrorMessage is human-readable error description.
	ErrorMessage string `json:"error_message,omitempty"`

	// HighWatermark is the leader's current HW.
	// Follower uses this to know what's "committed".
	HighWatermark int64 `json:"high_watermark"`

	// LogEndOffset is the leader's current LEO.
	// Tells follower how far behind it is.
	LogEndOffset int64 `json:"log_end_offset"`

	// LeaderEpoch is the current leader epoch.
	LeaderEpoch int64 `json:"leader_epoch"`

	// Messages is the batch of messages fetched.
	// Each message includes offset, key, value, etc.
	Messages []ReplicatedMessage `json:"messages"`

	// NextFetchOffset is where follower should fetch next.
	// Typically = last message offset + 1.
	NextFetchOffset int64 `json:"next_fetch_offset"`
}

// ReplicatedMessage is a message in the fetch response.
// Simplified format for replication (full message bytes).
// TODO: We have some missing fields here (Compression, flags, etc.)
type ReplicatedMessage struct {
	// Offset is the message's offset in the partition.
	Offset int64 `json:"offset"`

	// Timestamp is when the message was produced.
	Timestamp int64 `json:"timestamp"`

	// Key is the message key (may be empty).
	Key []byte `json:"key,omitempty"`

	// Value is the message payload.
	Value []byte `json:"value"`

	// Priority is the message priority (0=critical, 4=background).
	Priority uint8 `json:"priority"`

	// Headers contains optional metadata.
	Headers map[string]string `json:"headers,omitempty"`

	// IsControlRecord indicates COMMIT/ABORT markers.
	IsControlRecord bool `json:"is_control_record,omitempty"`
}

// =============================================================================
// FETCH ERROR CODES
// =============================================================================

const (
	// FetchErrorNone indicates success.
	FetchErrorNone = 0

	// FetchErrorUnknownTopic means the topic doesn't exist.
	FetchErrorUnknownTopic = 1

	// FetchErrorUnknownPartition means the partition doesn't exist.
	FetchErrorUnknownPartition = 2

	// FetchErrorNotLeader means this node is not the leader.
	FetchErrorNotLeader = 3

	// FetchErrorOffsetOutOfRange means requested offset is invalid.
	FetchErrorOffsetOutOfRange = 4

	// FetchErrorEpochFenced means the replica's epoch is stale.
	FetchErrorEpochFenced = 5

	// FetchErrorInternal means an unexpected error occurred.
	FetchErrorInternal = 100
)

// =============================================================================
// ISR UPDATE MESSAGES
// =============================================================================

// ISRUpdate is sent to controller when ISR changes.
// ISRUpdateType indicates whether a node is joining or leaving ISR.
type ISRUpdateType string

const (
	// ISRShrink indicates a node is being removed from ISR.
	ISRShrink ISRUpdateType = "shrink"

	// ISRExpand indicates a node is being added to ISR.
	ISRExpand ISRUpdateType = "expand"
)

// ISRUpdate is sent to controller when ISR changes.
// Used by both leader (reporting shrink) and follower (reporting expansion).
type ISRUpdate struct {
	// Topic is the affected topic.
	Topic string `json:"topic"`

	// Partition is the affected partition.
	Partition int `json:"partition"`

	// NodeID is the node being added/removed from ISR.
	NodeID NodeID `json:"node_id"`

	// Type indicates shrink or expand.
	Type ISRUpdateType `json:"type"`

	// Offset is the node's current offset (for expand requests).
	Offset int64 `json:"offset,omitempty"`

	// Reason describes why ISR changed.
	Reason string `json:"reason"`

	// Timestamp when the update was generated.
	Timestamp time.Time `json:"timestamp"`
}

// =============================================================================
// LEADER ELECTION MESSAGES
// =============================================================================

// LeaderElectionRequest is sent to controller to elect a new leader.
type LeaderElectionRequest struct {
	// Topic is the topic needing new leader.
	Topic string `json:"topic"`

	// Partition is the partition needing new leader.
	Partition int `json:"partition"`

	// Reason is why election is needed.
	Reason string `json:"reason"`

	// PreferredLeader is a hint for who should be leader (optional).
	PreferredLeader NodeID `json:"preferred_leader,omitempty"`

	// AllowUnclean if true, allows electing non-ISR member.
	AllowUnclean bool `json:"allow_unclean"`
}

// LeaderElectionResponse is the controller's response.
type LeaderElectionResponse struct {
	// Topic is the topic.
	Topic string `json:"topic"`

	// Partition is the partition.
	Partition int `json:"partition"`

	// NewLeader is the elected leader.
	NewLeader NodeID `json:"new_leader"`

	// NewEpoch is the new leader epoch.
	NewEpoch int64 `json:"new_epoch"`

	// ErrorCode indicates any error.
	ErrorCode int `json:"error_code"`

	// ErrorMessage describes the error.
	ErrorMessage string `json:"error_message,omitempty"`
}

// Leader election error codes.
const (
	// LeaderElectionSuccess indicates successful election.
	LeaderElectionSuccess = 0

	// LeaderElectionNoISR means no in-sync replica available.
	LeaderElectionNoISR = 1

	// LeaderElectionNotController means request went to non-controller.
	LeaderElectionNotController = 2

	// LeaderElectionUncleanDisabled means unclean election was needed but disabled.
	LeaderElectionUncleanDisabled = 3
)

// =============================================================================
// REPLICATION CONFIGURATION
// =============================================================================

// ReplicationConfig controls replication behavior.
type ReplicationConfig struct {
	// FetchIntervalMs is how often followers fetch from leader.
	// Default: 500ms (balanced)
	FetchIntervalMs int `json:"fetch_interval_ms" yaml:"fetch_interval_ms"`

	// FetchMaxBytes is max bytes per fetch request.
	// Default: 1MB
	FetchMaxBytes int `json:"fetch_max_bytes" yaml:"fetch_max_bytes"`

	// FetchTimeoutMs is the HTTP timeout for fetch requests.
	// Default: 5000ms
	FetchTimeoutMs int `json:"fetch_timeout_ms" yaml:"fetch_timeout_ms"`

	// ISRLagTimeMaxMs is max time before replica is removed from ISR.
	// Default: 10000 (10 seconds)
	// Combined with ISRLagMaxMessages for ISR shrink decision.
	ISRLagTimeMaxMs int `json:"isr_lag_time_max_ms" yaml:"isr_lag_time_max_ms"`

	// ISRLagMaxMessages is max messages behind before ISR removal.
	// Default: 1000 messages
	// Combined with ISRLagTimeMaxMs for ISR shrink decision.
	ISRLagMaxMessages int64 `json:"isr_lag_max_messages" yaml:"isr_lag_max_messages"`

	// MinInSyncReplicas is minimum ISR size for writes to succeed.
	// If ISR < this value, writes are rejected.
	// Default: 1
	MinInSyncReplicas int `json:"min_in_sync_replicas" yaml:"min_in_sync_replicas"`

	// UncleanLeaderElection allows electing non-ISR member as leader.
	// Default: false (prefer consistency over availability)
	UncleanLeaderElection bool `json:"unclean_leader_election" yaml:"unclean_leader_election"`

	// LeaderFollowerReadEnabled allows reading from followers.
	// Default: false (all reads go to leader)
	LeaderFollowerReadEnabled bool `json:"leader_follower_read_enabled" yaml:"leader_follower_read_enabled"`

	// SnapshotEnabled enables snapshot-based catch-up for followers.
	// Default: true (fast catch-up)
	SnapshotEnabled bool `json:"snapshot_enabled" yaml:"snapshot_enabled"`

	// SnapshotThresholdMessages triggers snapshot when follower is this far behind.
	// Default: 10000 messages
	SnapshotThresholdMessages int64 `json:"snapshot_threshold_messages" yaml:"snapshot_threshold_messages"`

	// AckTimeout is how long to wait for ISR acks on AckAll writes.
	// Default: 5000ms
	AckTimeoutMs int `json:"ack_timeout_ms" yaml:"ack_timeout_ms"`
}

// DefaultReplicationConfig returns sensible defaults.
func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		FetchIntervalMs:           500,         // 500ms fetch interval
		FetchMaxBytes:             1024 * 1024, // 1MB max per fetch
		FetchTimeoutMs:            5000,        // 5s HTTP timeout
		ISRLagTimeMaxMs:           10000,       // 10s max lag time
		ISRLagMaxMessages:         1000,        // 1000 messages max lag
		MinInSyncReplicas:         1,           // Allow single-replica writes
		UncleanLeaderElection:     false,       // Prefer consistency
		LeaderFollowerReadEnabled: false,       // Read from leader only
		SnapshotEnabled:           true,        // Enable fast catch-up
		SnapshotThresholdMessages: 10000,       // Snapshot if 10k behind
		AckTimeoutMs:              5000,        // 5s ack timeout
	}
}

// =============================================================================
// REPLICA ACKNOWLEDGMENT
// =============================================================================
//
// When producer uses AckAll, leader must wait for ISR acknowledgment.
// This tracks pending acknowledgments.
//
// FLOW:
//   Producer ─► Leader writes message at offset 100
//            ─► Leader records pendingAck{offset: 100, waitCh: ch}
//            ─► Followers fetch and ack offset 100
//            ─► Leader updates HW, closes waitCh
//            ─► Producer gets success
//
// =============================================================================

// PendingAck tracks a message waiting for ISR acknowledgment.
type PendingAck struct {
	// Offset is the message offset waiting for acks.
	Offset int64

	// RequiredAcks is how many ISR members must ack.
	RequiredAcks int

	// ReceivedAcks tracks which replicas have acked.
	ReceivedAcks map[NodeID]bool

	// WaitCh is closed when ack requirements are met.
	WaitCh chan struct{}

	// CreatedAt is when this pending ack was created.
	CreatedAt time.Time

	// TimeoutAt is when this ack times out.
	TimeoutAt time.Time
}

// =============================================================================
// FOLLOWER STATE (LEADER'S VIEW)
// =============================================================================
//
// Leader tracks each follower's progress for ISR management.
//
// =============================================================================

// FollowerProgress tracks a follower's replication progress (from leader's POV).
type FollowerProgress struct {
	// ReplicaID is the follower's node ID.
	ReplicaID NodeID `json:"replica_id"`

	// LastFetchTime is when follower last fetched.
	LastFetchTime time.Time `json:"last_fetch_time"`

	// LastFetchOffset is the offset follower requested.
	LastFetchOffset int64 `json:"last_fetch_offset"`

	// MatchedOffset is the highest offset follower has confirmed.
	// This is what follower reported it successfully appended.
	MatchedOffset int64 `json:"matched_offset"`

	// IsInSync indicates if follower is in ISR.
	IsInSync bool `json:"is_in_sync"`

	// LagMessages is how far behind the follower is.
	LagMessages int64 `json:"lag_messages"`

	// ConsecutiveErrors counts fetch errors (for health).
	ConsecutiveErrors int `json:"consecutive_errors"`
}

// =============================================================================
// SNAPSHOT TYPES
// =============================================================================
//
// WHY SNAPSHOTS?
//   When a follower is very far behind, fetching message-by-message is slow.
//   Snapshot = compressed point-in-time copy of the log.
//   Follower loads snapshot, then catches up from snapshot offset.
//
// EXAMPLE:
//   Leader LEO: 100,000
//   Follower offset: 5,000 (95k behind!)
//
//   Without snapshot: Fetch 95,000 messages individually = slow
//   With snapshot: Load snapshot at 90,000, fetch 10,000 = fast
//
// =============================================================================

// Snapshot represents a point-in-time copy of partition data.
type Snapshot struct {
	// Topic is the topic name.
	Topic string `json:"topic"`

	// Partition is the partition number.
	Partition int `json:"partition"`

	// LastIncludedOffset is the highest offset in the snapshot.
	// Follower should fetch from LastIncludedOffset + 1 after loading.
	LastIncludedOffset int64 `json:"last_included_offset"`

	// LastIncludedEpoch is the leader epoch at snapshot time.
	LastIncludedEpoch int64 `json:"last_included_epoch"`

	// SizeBytes is the snapshot size.
	SizeBytes int64 `json:"size_bytes"`

	// Checksum is SHA256 of snapshot data.
	Checksum string `json:"checksum"`

	// CreatedAt is when snapshot was created.
	CreatedAt time.Time `json:"created_at"`

	// FilePath is where snapshot is stored.
	FilePath string `json:"file_path"`
}

// SnapshotRequest asks leader for a snapshot.
type SnapshotRequest struct {
	// Topic is the topic.
	Topic string `json:"topic"`

	// Partition is the partition.
	Partition int `json:"partition"`

	// RequestedBy is the requesting follower.
	RequestedBy NodeID `json:"requested_by"`
}

// SnapshotResponse provides snapshot metadata.
type SnapshotResponse struct {
	// ErrorCode indicates any error.
	ErrorCode int `json:"error_code"`

	// ErrorMessage describes the error.
	ErrorMessage string `json:"error_message,omitempty"`

	// Snapshot is the snapshot metadata.
	Snapshot *Snapshot `json:"snapshot,omitempty"`

	// DownloadURL is where to download the snapshot data.
	// Format: http://{leader}/cluster/snapshot/{topic}/{partition}/data
	DownloadURL string `json:"download_url,omitempty"`
}

// Snapshot error codes.
const (
	SnapshotErrorNone         = 0
	SnapshotErrorNotLeader    = 1
	SnapshotErrorNotAvailable = 2
	SnapshotErrorInternal     = 100
)
