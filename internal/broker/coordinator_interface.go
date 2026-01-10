// =============================================================================
// GROUP COORDINATOR INTERFACE - ABSTRACTION FOR COORDINATOR IMPLEMENTATIONS
// =============================================================================
//
// WHY AN INTERFACE?
// We have two coordinator implementations:
//   1. EagerCoordinator: Stop-the-world rebalancing (M3 - original)
//   2. CooperativeCoordinator: Incremental rebalancing (M12 - Kafka KIP-429)
//
// Both need to:
//   - Store state in __consumer_offsets topic (M13)
//   - Handle failover via partition leader election
//   - Support the same HTTP API
//
// The interface allows us to:
//   - Switch implementations without changing API code
//   - Share the same persistence layer
//   - Route requests to the correct coordinator based on group protocol
//
// ┌──────────────────────────────────────────────────────────────────────────┐
// │                    COORDINATOR INTERFACE DESIGN                          │
// │                                                                          │
// │   ┌─────────────────────────────────────────────────────────────────┐    │
// │   │                   GroupCoordinator Interface                    │    │
// │   │                                                                 │    │
// │   │  - JoinGroup(...)     → Add member, trigger rebalance           │    │
// │   │  - LeaveGroup(...)    → Remove member, trigger rebalance        │    │
// │   │  - Heartbeat(...)     → Keep session alive                      │    │
// │   │  - SyncGroup(...)     → Get assignment (after rebalance)        │    │
// │   │  - CommitOffset(...)  → Store consumer offset                   │    │
// │   │  - FetchOffset(...)   → Get consumer offset                     │    │
// │   │  - DescribeGroup(...) → Get group info                          │    │
// │   │                                                                 │    │
// │   └─────────────────────────────────────────────────────────────────┘    │
// │                              ▲                                           │
// │                              │ implements                                │
// │            ┌─────────────────┴─────────────────┐                         │
// │            │                                   │                         │
// │   ┌────────┴─────────┐              ┌──────────┴───────────┐             │
// │   │ EagerCoordinator │              │CooperativeCoordinator│             │
// │   │                  │              │                      │             │
// │   │ Stop-the-world   │              │ Incremental          │             │
// │   │ rebalancing      │              │ rebalancing          │             │
// │   └──────────────────┘              └──────────────────────┘             │
// │            │                                   │                         │
// │            └─────────────────┬─────────────────┘                         │
// │                              │                                           │
// │                              ▼                                           │
// │   ┌─────────────────────────────────────────────────────────────────┐    │
// │   │              Shared Persistence Layer                           │    │
// │   │              (InternalTopicManager)                             │    │
// │   │                                                                 │    │
// │   │  - WriteOffsetCommit() → __consumer_offsets                     │    │
// │   │  - WriteGroupMetadata() → __consumer_offsets                    │    │
// │   │  - ReadPartition() → State rebuild                              │    │
// │   └─────────────────────────────────────────────────────────────────┘    │
// │                                                                          │
// └──────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package broker

import (
	"encoding/json"
	"errors"
	"time"
)

// =============================================================================
// ERRORS
// =============================================================================

var (
	// ErrUnknownMember means the member ID is not recognized.
	ErrUnknownMember = errors.New("unknown member")

	// ErrInvalidGeneration means the generation ID doesn't match.
	ErrInvalidGeneration = errors.New("invalid generation")

	// Note: ErrRebalanceInProgress is defined in consumer_group.go

	// ErrNotLeader means this member is not the group leader.
	ErrNotLeader = errors.New("not group leader")

	// ErrMemberIDRequired means member ID is required for this operation.
	ErrMemberIDRequired = errors.New("member ID required")

	// ErrGroupIDRequired means group ID is required.
	ErrGroupIDRequired = errors.New("group ID required")

	// ErrSessionExpired means the member's session has expired.
	ErrSessionExpired = errors.New("session expired")

	// ErrIllegalGeneration means the generation is invalid for this operation.
	ErrIllegalGeneration = errors.New("illegal generation")
)

// =============================================================================
// GROUP COORDINATOR INTERFACE
// =============================================================================

// GroupCoordinatorI is the interface for consumer group coordination.
// Both eager and cooperative coordinators implement this.
//
// NAMING: GroupCoordinatorI (with I suffix) because GroupCoordinator
// struct already exists. This interface abstracts over implementations.
type GroupCoordinatorI interface {
	// =========================================================================
	// GROUP LIFECYCLE
	// =========================================================================

	// JoinGroup adds a member to the group and triggers rebalance if needed.
	//
	// FLOW:
	//   1. Member sends JoinGroup request
	//   2. Coordinator adds member to group
	//   3. If first member or rebalance needed → start rebalance
	//   4. Wait for all members to join (or timeout)
	//   5. Return with generation ID and leader info
	//
	// PARAMETERS:
	//   - groupID: Consumer group identifier
	//   - memberID: Empty string for new members, existing ID for rejoin
	//   - protocolType: Usually "consumer" for consumer groups
	//   - protocols: Supported assignment protocols (range, roundrobin, etc.)
	//   - sessionTimeout: How long before member is considered dead
	//   - rebalanceTimeout: How long to wait for members during rebalance
	//
	// RETURNS:
	//   - JoinGroupResult with memberID, generation, leader info
	//   - error if join fails
	JoinGroup(req *JoinGroupRequest) (*JoinGroupResult, error)

	// LeaveGroup removes a member from the group.
	// Triggers rebalance if group is not empty.
	LeaveGroup(req *LeaveGroupRequest) error

	// Heartbeat keeps the member's session alive.
	// Also returns rebalance status if one is pending.
	//
	// FLOW:
	//   1. Member sends heartbeat at regular intervals
	//   2. Coordinator resets session timer
	//   3. If rebalance pending → return rebalance flag
	//   4. Member should rejoin if rebalance flag set
	Heartbeat(req *HeartbeatRequest) (*HeartbeatResult, error)

	// SyncGroup gets the partition assignment for a member.
	// Called after JoinGroup completes rebalance.
	//
	// FLOW (for leader):
	//   1. Leader computes assignment based on chosen protocol
	//   2. Leader sends assignment in SyncGroup request
	//   3. Coordinator stores assignment
	//   4. All members get their assignment
	//
	// FLOW (for followers):
	//   1. Follower sends empty assignment
	//   2. Waits for leader to provide assignment
	//   3. Gets own assignment back
	SyncGroup(req *SyncGroupRequest) (*SyncGroupResult, error)

	// =========================================================================
	// OFFSET MANAGEMENT
	// =========================================================================

	// CommitOffset stores the consumer's offset for a topic-partition.
	// Persisted to __consumer_offsets internal topic.
	CommitOffset(req *OffsetCommitRequest) error

	// FetchOffset retrieves the last committed offset.
	FetchOffset(req *OffsetFetchRequest) (*OffsetFetchResult, error)

	// FetchAllOffsets retrieves all committed offsets for a group.
	FetchAllOffsets(groupID string) (map[string]map[int]int64, error)

	// =========================================================================
	// GROUP QUERIES
	// =========================================================================

	// DescribeGroup returns detailed information about a group.
	DescribeGroup(groupID string) (*GroupDescription, error)

	// ListGroups returns all known groups.
	ListGroups() ([]string, error)

	// =========================================================================
	// TOPIC MANAGEMENT
	// =========================================================================

	// RegisterTopic registers a topic for partition assignment.
	// Called when topics are created.
	RegisterTopic(topicName string, partitionCount int)

	// UnregisterTopic removes a topic from assignment.
	UnregisterTopic(topicName string)

	// =========================================================================
	// LIFECYCLE
	// =========================================================================

	// Start starts the coordinator (background goroutines, etc.).
	Start() error

	// Stop stops the coordinator gracefully.
	Stop() error
}

// =============================================================================
// REQUEST/RESPONSE TYPES
// =============================================================================

// JoinGroupRequest is the request to join a consumer group.
type JoinGroupRequest struct {
	// GroupID is the consumer group identifier.
	GroupID string

	// MemberID is empty for new members, or existing ID for rejoin.
	MemberID string

	// ProtocolType is usually "consumer".
	ProtocolType string

	// Protocols is the list of supported assignment protocols.
	// Format: name → metadata (protocol-specific)
	Protocols map[string][]byte

	// SessionTimeoutMs is how long before member is considered dead.
	SessionTimeoutMs int

	// RebalanceTimeoutMs is how long to wait during rebalance.
	RebalanceTimeoutMs int

	// ClientID identifies the client (for debugging).
	ClientID string

	// ClientHost is the client's hostname/IP.
	ClientHost string
}

// JoinGroupResult is the response from joining a group.
type JoinGroupResult struct {
	// MemberID is the assigned member ID (new or existing).
	MemberID string

	// GenerationID is the group's generation (increments on each rebalance).
	GenerationID int

	// LeaderID is the member ID of the group leader.
	LeaderID string

	// Protocol is the chosen assignment protocol.
	Protocol string

	// Members is only populated for the leader.
	// Contains all members' metadata for assignment.
	Members []JoinGroupMember

	// IsLeader indicates if this member is the leader.
	IsLeader bool
}

// JoinGroupMember is a member's info for the leader to compute assignment.
type JoinGroupMember struct {
	MemberID string
	Metadata []byte // Protocol-specific metadata
}

// LeaveGroupRequest is the request to leave a group.
type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

// HeartbeatRequest is a keep-alive request.
type HeartbeatRequest struct {
	GroupID      string
	MemberID     string
	GenerationID int
}

// HeartbeatResult is the heartbeat response.
type HeartbeatResult struct {
	// ShouldRejoin indicates the member should rejoin (rebalance pending).
	ShouldRejoin bool

	// RebalanceState provides details about rebalance (for cooperative).
	RebalanceState string

	// PendingRevocations are partitions to stop processing (cooperative).
	PendingRevocations []TopicPartition
}

// SyncGroupRequest is the request to sync assignment.
type SyncGroupRequest struct {
	GroupID      string
	MemberID     string
	GenerationID int

	// Assignments is only populated by the leader.
	// Maps memberID → serialized assignment.
	Assignments map[string][]byte
}

// SyncGroupResult is the sync response with the member's assignment.
type SyncGroupResult struct {
	// Assignment is this member's partition assignment.
	Assignment []byte

	// Protocol is the assignment protocol used.
	Protocol string
}

// OffsetCommitRequest is the request to commit an offset.
type OffsetCommitRequest struct {
	GroupID      string
	MemberID     string
	GenerationID int
	Topic        string
	Partition    int
	Offset       int64
	Metadata     string
}

// OffsetFetchRequest is the request to fetch committed offset.
type OffsetFetchRequest struct {
	GroupID   string
	Topic     string
	Partition int
}

// OffsetFetchResult is the offset fetch response.
type OffsetFetchResult struct {
	Offset   int64
	Metadata string
	// Timestamp is when the offset was committed.
	Timestamp time.Time
}

// GroupDescription contains detailed group information.
type GroupDescription struct {
	GroupID      string
	State        GroupState
	Protocol     string
	ProtocolType string
	Leader       string
	Members      []GroupMemberDescription
	Generation   int
}

// GroupMemberDescription contains member information.
type GroupMemberDescription struct {
	MemberID   string
	ClientID   string
	ClientHost string
	Assignment []TopicPartition
}

// Note: TopicPartition is defined in cooperative_rebalance.go
// Note: AssignmentStrategy is defined in cooperative_rebalance.go

// =============================================================================
// ASSIGNMENT ENCODING
// =============================================================================
//
// Partition assignments are serialized for storage and transport.
// Format is simple: JSON array of TopicPartition.
//
// EXAMPLE:
//   [{"topic":"orders","partition":0},{"topic":"orders","partition":1}]
//
// WHY JSON?
//   - Human readable for debugging
//   - Easy to parse in any language
//   - Small overhead for typical assignments
//
// =============================================================================

// EncodeAssignment serializes a partition assignment.
func EncodeAssignment(partitions []TopicPartition) ([]byte, error) {
	return json.Marshal(partitions)
}

// DecodeAssignment deserializes a partition assignment.
func DecodeAssignment(data []byte) ([]TopicPartition, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var partitions []TopicPartition
	err := json.Unmarshal(data, &partitions)
	return partitions, err
}

// =============================================================================
// COORDINATOR PROTOCOL TYPES
// =============================================================================

// CoordinatorProtocol identifies the rebalancing protocol.
type CoordinatorProtocol string

const (
	// CoordinatorProtocolEager is stop-the-world rebalancing.
	CoordinatorProtocolEager CoordinatorProtocol = "eager"

	// CoordinatorProtocolCooperative is incremental rebalancing (KIP-429).
	CoordinatorProtocolCooperative CoordinatorProtocol = "cooperative"
)
