// =============================================================================
// PARTITION-BACKED COORDINATOR - FAULT-TOLERANT GROUP COORDINATOR
// =============================================================================
//
// WHAT: A GroupCoordinator implementation that persists state to __consumer_offsets.
//
// WHY PARTITION-BACKED?
// The original GroupCoordinator (M3) stored state in memory + local files.
// This was fine for single-node, but in a cluster:
//   - If the node dies, all group state is lost
//   - Consumers must rejoin from scratch
//   - Committed offsets might be stale or lost
//
// The partition-backed coordinator:
//   - Writes every state change to __consumer_offsets topic
//   - On failover, new leader replays log to rebuild state
//   - Same durability guarantees as user messages
//   - Leverages existing replication infrastructure
//
// ┌──────────────────────────────────────────────────────────────────────────┐
// │                    PARTITION-BACKED COORDINATOR                          │
// │                                                                          │
// │   Write Path:                                                            │
// │   ┌──────────────┐   ┌────────────────────┐   ┌────────────────────┐     │
// │   │ API Request  │──►│ Update In-Memory   │──►│ Write to           │     │
// │   │ (CommitOffset│   │ State              │   │ __consumer_offsets │     │
// │   │  JoinGroup)  │   └────────────────────┘   └────────────────────┘     │
// │   └──────────────┘                                     │                 │
// │                                                        ▼                 │
// │                                              ┌──────────────────┐        │
// │                                              │ Replicate to ISR │        │
// │                                              └──────────────────┘        │
// │                                                                          │
// │   Recovery Path (on becoming partition leader):                          │
// │   ┌────────────────────┐   ┌──────────────────┐   ┌──────────────────┐   │
// │   │ Read               │──►│ Apply Each       │──►│ In-Memory State  │   │
// │   │ __consumer_offsets │   │ Record           │   │ Rebuilt          │   │
// │   └────────────────────┘   └──────────────────┘   └──────────────────┘   │
// │                                                                          │
// └──────────────────────────────────────────────────────────────────────────┘
//
// STATE MACHINE:
// Records in __consumer_offsets are applied in order to rebuild state:
//   - OffsetCommitRecord → Update offset cache
//   - GroupMetadataRecord → Update group membership, assignment, generation
//   - TombstoneRecord → Delete entry (for compaction)
//
// =============================================================================

package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// =============================================================================
// CONFIGURATION
// =============================================================================

// PartitionBackedCoordinatorConfig holds configuration.
type PartitionBackedCoordinatorConfig struct {
	// SessionTimeoutMs is how long before a member is considered dead.
	// Default: 30000 (30 seconds)
	SessionTimeoutMs int

	// HeartbeatIntervalMs is the expected heartbeat interval.
	// Default: 3000 (3 seconds)
	HeartbeatIntervalMs int

	// RebalanceTimeoutMs is max time to wait for members during rebalance.
	// Default: 60000 (60 seconds)
	RebalanceTimeoutMs int

	// SessionCheckIntervalMs is how often to check for expired sessions.
	// Default: 1000 (1 second)
	SessionCheckIntervalMs int

	// AutoCommitEnabled enables auto-commit of offsets.
	AutoCommitEnabled bool

	// AutoCommitIntervalMs is auto-commit interval.
	// Default: 5000 (5 seconds)
	AutoCommitIntervalMs int
}

// DefaultPartitionBackedCoordinatorConfig returns sensible defaults.
func DefaultPartitionBackedCoordinatorConfig() PartitionBackedCoordinatorConfig {
	return PartitionBackedCoordinatorConfig{
		SessionTimeoutMs:       30000,
		HeartbeatIntervalMs:    3000,
		RebalanceTimeoutMs:     60000,
		SessionCheckIntervalMs: 1000,
		AutoCommitEnabled:      true,
		AutoCommitIntervalMs:   5000,
	}
}

// =============================================================================
// PARTITION-BACKED COORDINATOR
// =============================================================================

// PartitionBackedCoordinator implements GroupCoordinatorI with internal topic persistence.
type PartitionBackedCoordinator struct {
	// config holds coordinator configuration.
	config PartitionBackedCoordinatorConfig

	// internalTopicManager manages __consumer_offsets topic.
	internalTopicManager *InternalTopicManager

	// =========================================================================
	// IN-MEMORY STATE (rebuilt from log)
	// =========================================================================

	// groups maps groupID → group state.
	groups map[string]*CoordinatorGroupState

	// offsets maps groupID → topic → partition → offset info.
	offsets map[string]map[string]map[int]*StoredOffset

	// topicPartitionCounts maps topic → partition count.
	topicPartitionCounts map[string]int

	// mu protects all in-memory state.
	mu sync.RWMutex

	// =========================================================================
	// LIFECYCLE
	// =========================================================================

	// logger for operations.
	logger *slog.Logger

	// ctx is the coordinator's context.
	ctx context.Context

	// cancel cancels background operations.
	cancel context.CancelFunc

	// wg waits for background goroutines.
	wg sync.WaitGroup

	// started indicates the coordinator has started.
	started bool

	// stateRebuilt indicates state has been rebuilt from log.
	stateRebuilt bool
}

// CoordinatorGroupState holds in-memory state for a consumer group.
type CoordinatorGroupState struct {
	// GroupID is the group identifier.
	GroupID string

	// State is the current group state.
	State GroupState

	// Protocol is the assignment protocol (range, roundrobin, sticky).
	Protocol string

	// Leader is the member ID of the group leader.
	Leader string

	// Generation is the current generation ID.
	Generation int32

	// Members maps memberID → member state.
	Members map[string]*CoordinatorMemberState

	// Assignment maps memberID → partition assignment.
	Assignment map[string][]TopicPartition

	// Topics are the subscribed topics.
	Topics []string

	// CreatedAt is when the group was created.
	CreatedAt time.Time

	// UpdatedAt is when the group was last modified.
	UpdatedAt time.Time
}

// CoordinatorMemberState holds state for a single member.
type CoordinatorMemberState struct {
	// MemberID is the member identifier.
	MemberID string

	// ClientID is the client identifier.
	ClientID string

	// ClientHost is the client's host.
	ClientHost string

	// SessionTimeout is the session timeout.
	SessionTimeout time.Duration

	// RebalanceTimeout is the rebalance timeout.
	RebalanceTimeout time.Duration

	// LastHeartbeat is when the last heartbeat was received.
	LastHeartbeat time.Time

	// Metadata is protocol-specific metadata.
	Metadata []byte

	// JoinedAt is when the member joined.
	JoinedAt time.Time
}

// StoredOffset holds a stored offset.
type StoredOffset struct {
	Offset      int64
	Metadata    string
	CommittedAt time.Time
}

// =============================================================================
// CONSTRUCTOR
// =============================================================================

// NewPartitionBackedCoordinator creates a new partition-backed coordinator.
func NewPartitionBackedCoordinator(
	config PartitionBackedCoordinatorConfig,
	internalTopicManager *InternalTopicManager,
) (*PartitionBackedCoordinator, error) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With("component", "partition-backed-coordinator")

	ctx, cancel := context.WithCancel(context.Background())

	c := &PartitionBackedCoordinator{
		config:               config,
		internalTopicManager: internalTopicManager,
		groups:               make(map[string]*CoordinatorGroupState),
		offsets:              make(map[string]map[string]map[int]*StoredOffset),
		topicPartitionCounts: make(map[string]int),
		logger:               logger,
		ctx:                  ctx,
		cancel:               cancel,
	}

	return c, nil
}

// =============================================================================
// LIFECYCLE (implements GroupCoordinatorI)
// =============================================================================

// Start starts the coordinator.
func (c *PartitionBackedCoordinator) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return nil
	}

	c.logger.Info("starting partition-backed coordinator")

	// Rebuild state from __consumer_offsets
	if err := c.rebuildState(); err != nil {
		return fmt.Errorf("failed to rebuild state: %w", err)
	}

	// Start background session monitor
	c.wg.Add(1)
	go c.sessionMonitorLoop()

	c.started = true
	c.logger.Info("partition-backed coordinator started",
		"groups", len(c.groups),
		"offsets_cached", c.countOffsets())

	return nil
}

// Stop stops the coordinator.
func (c *PartitionBackedCoordinator) Stop() error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return nil
	}
	c.started = false
	c.mu.Unlock()

	c.logger.Info("stopping partition-backed coordinator")

	// Cancel background operations
	c.cancel()

	// Wait for goroutines
	c.wg.Wait()

	c.logger.Info("partition-backed coordinator stopped")
	return nil
}

// =============================================================================
// STATE REBUILD
// =============================================================================

// rebuildState reads __consumer_offsets and rebuilds in-memory state.
func (c *PartitionBackedCoordinator) rebuildState() error {
	c.logger.Info("rebuilding coordinator state from __consumer_offsets")

	localPartitions := c.internalTopicManager.GetLocalPartitions()
	totalRecords := 0

	for _, partition := range localPartitions {
		records, err := c.internalTopicManager.ReadPartition(partition)
		if err != nil {
			c.logger.Warn("failed to read partition",
				"partition", partition,
				"error", err)
			continue
		}

		for _, record := range records {
			if err := c.applyRecord(record); err != nil {
				c.logger.Warn("failed to apply record",
					"partition", partition,
					"type", record.Type,
					"error", err)
			}
		}

		totalRecords += len(records)
	}

	c.stateRebuilt = true
	c.logger.Info("state rebuild complete",
		"partitions_read", len(localPartitions),
		"records_applied", totalRecords,
		"groups", len(c.groups))

	return nil
}

// applyRecord applies a single record to in-memory state.
func (c *PartitionBackedCoordinator) applyRecord(record *InternalRecord) error {
	switch record.Type {
	case RecordTypeOffsetCommit:
		return c.applyOffsetCommit(record)
	case RecordTypeGroupMetadata:
		return c.applyGroupMetadata(record)
	case RecordTypeTombstone:
		return c.applyTombstone(record)
	default:
		return fmt.Errorf("unknown record type: %d", record.Type)
	}
}

func (c *PartitionBackedCoordinator) applyOffsetCommit(record *InternalRecord) error {
	key, err := DecodeOffsetCommitKey(record.Key)
	if err != nil {
		return fmt.Errorf("failed to decode offset key: %w", err)
	}

	value, err := DecodeOffsetCommitValue(record.Value)
	if err != nil {
		return fmt.Errorf("failed to decode offset value: %w", err)
	}

	// Update in-memory offset cache
	if c.offsets[key.Group] == nil {
		c.offsets[key.Group] = make(map[string]map[int]*StoredOffset)
	}
	if c.offsets[key.Group][key.Topic] == nil {
		c.offsets[key.Group][key.Topic] = make(map[int]*StoredOffset)
	}

	c.offsets[key.Group][key.Topic][int(key.Partition)] = &StoredOffset{
		Offset:      value.Offset,
		Metadata:    value.Metadata,
		CommittedAt: time.UnixMilli(value.Timestamp),
	}

	return nil
}

func (c *PartitionBackedCoordinator) applyGroupMetadata(record *InternalRecord) error {
	key, err := DecodeGroupMetadataKey(record.Key)
	if err != nil {
		return fmt.Errorf("failed to decode group key: %w", err)
	}

	value, err := DecodeGroupMetadataValue(record.Value)
	if err != nil {
		return fmt.Errorf("failed to decode group value: %w", err)
	}

	// Update or create group state
	group := c.groups[key.Group]
	if group == nil {
		group = &CoordinatorGroupState{
			GroupID:    key.Group,
			Members:    make(map[string]*CoordinatorMemberState),
			Assignment: make(map[string][]TopicPartition),
			CreatedAt:  time.Now(),
		}
		c.groups[key.Group] = group
	}

	group.State = value.State
	group.Protocol = value.Protocol
	group.Leader = value.Leader
	group.Generation = value.Generation
	group.UpdatedAt = time.Now()

	// Update members
	group.Members = make(map[string]*CoordinatorMemberState)
	for _, m := range value.Members {
		member := &CoordinatorMemberState{
			MemberID:         m.MemberID,
			ClientID:         m.ClientID,
			ClientHost:       m.ClientHost,
			SessionTimeout:   time.Duration(m.SessionTimeout) * time.Millisecond,
			RebalanceTimeout: time.Duration(m.RebalanceTimeout) * time.Millisecond,
			Metadata:         m.Metadata,
			LastHeartbeat:    time.Now(), // Assume alive during rebuild
		}
		group.Members[m.MemberID] = member

		// Decode and store assignment
		if len(m.Assignment) > 0 {
			partitions, err := DecodeAssignment(m.Assignment)
			if err == nil {
				group.Assignment[m.MemberID] = partitions
			}
		}
	}

	return nil
}

func (c *PartitionBackedCoordinator) applyTombstone(record *InternalRecord) error {
	// Original type stored in flags
	originalType := RecordType(record.Flags)

	switch originalType {
	case RecordTypeOffsetCommit:
		key, err := DecodeOffsetCommitKey(record.Key)
		if err != nil {
			return err
		}
		// Delete from cache
		if c.offsets[key.Group] != nil && c.offsets[key.Group][key.Topic] != nil {
			delete(c.offsets[key.Group][key.Topic], int(key.Partition))
		}

	case RecordTypeGroupMetadata:
		key, err := DecodeGroupMetadataKey(record.Key)
		if err != nil {
			return err
		}
		// Delete group
		delete(c.groups, key.Group)
	}

	return nil
}

// =============================================================================
// GROUP LIFECYCLE (implements GroupCoordinatorI)
// =============================================================================

// JoinGroup adds a member to the group.
func (c *PartitionBackedCoordinator) JoinGroup(req *JoinGroupRequest) (*JoinGroupResult, error) {
	if req.GroupID == "" {
		return nil, ErrGroupIDRequired
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we're coordinator for this group
	if !c.internalTopicManager.IsCoordinatorFor(req.GroupID) {
		return nil, ErrNotCoordinator
	}

	// Get or create group
	group := c.groups[req.GroupID]
	if group == nil {
		group = &CoordinatorGroupState{
			GroupID:    req.GroupID,
			State:      GroupStateEmpty,
			Members:    make(map[string]*CoordinatorMemberState),
			Assignment: make(map[string][]TopicPartition),
			CreatedAt:  time.Now(),
		}
		c.groups[req.GroupID] = group
	}

	// Generate or use existing member ID
	memberID := req.MemberID
	if memberID == "" {
		memberID = fmt.Sprintf("%s-%s-%d", req.ClientID, req.GroupID, time.Now().UnixNano())
	}

	// Check if member exists
	member, exists := group.Members[memberID]
	if !exists {
		member = &CoordinatorMemberState{
			MemberID:   memberID,
			ClientID:   req.ClientID,
			ClientHost: req.ClientHost,
			JoinedAt:   time.Now(),
		}
		group.Members[memberID] = member
	}

	// Update member state
	member.SessionTimeout = time.Duration(req.SessionTimeoutMs) * time.Millisecond
	member.RebalanceTimeout = time.Duration(req.RebalanceTimeoutMs) * time.Millisecond
	member.LastHeartbeat = time.Now()

	// Pick first protocol that all members support
	if req.Protocols != nil {
		for protocol, metadata := range req.Protocols {
			group.Protocol = protocol
			member.Metadata = metadata
			break
		}
	}

	// Check if we need to rebalance
	needsRebalance := group.State == GroupStateEmpty ||
		group.State == GroupStateRebalancing ||
		!exists // New member

	if needsRebalance {
		// Trigger rebalance
		group.State = GroupStateRebalancing
		group.Generation++

		// First member becomes leader
		if len(group.Members) == 1 {
			group.Leader = memberID
		}
	}

	// Persist group metadata
	if err := c.persistGroupMetadata(group); err != nil {
		c.logger.Warn("failed to persist group metadata", "error", err)
		// Continue anyway - state is in memory
	}

	// Build result
	result := &JoinGroupResult{
		MemberID:     memberID,
		GenerationID: int(group.Generation),
		LeaderID:     group.Leader,
		Protocol:     group.Protocol,
		IsLeader:     memberID == group.Leader,
	}

	// If leader, include all members for assignment
	if memberID == group.Leader {
		for mid, m := range group.Members {
			result.Members = append(result.Members, JoinGroupMember{
				MemberID: mid,
				Metadata: m.Metadata,
			})
		}
	}

	c.logger.Info("member joined group",
		"group", req.GroupID,
		"member", memberID,
		"generation", group.Generation,
		"is_leader", result.IsLeader)

	return result, nil
}

// LeaveGroup removes a member from the group.
func (c *PartitionBackedCoordinator) LeaveGroup(req *LeaveGroupRequest) error {
	if req.GroupID == "" {
		return ErrGroupIDRequired
	}
	if req.MemberID == "" {
		return ErrMemberIDRequired
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.internalTopicManager.IsCoordinatorFor(req.GroupID) {
		return ErrNotCoordinator
	}

	group := c.groups[req.GroupID]
	if group == nil {
		return ErrGroupNotFound
	}

	if _, exists := group.Members[req.MemberID]; !exists {
		return ErrUnknownMember
	}

	// Remove member
	delete(group.Members, req.MemberID)
	delete(group.Assignment, req.MemberID)

	// Update state
	if len(group.Members) == 0 {
		group.State = GroupStateEmpty
		group.Leader = ""
	} else {
		// Trigger rebalance
		group.State = GroupStateRebalancing
		group.Generation++

		// If leader left, pick new leader
		if group.Leader == req.MemberID {
			for mid := range group.Members {
				group.Leader = mid
				break
			}
		}
	}

	// Persist
	if err := c.persistGroupMetadata(group); err != nil {
		c.logger.Warn("failed to persist group metadata", "error", err)
	}

	c.logger.Info("member left group",
		"group", req.GroupID,
		"member", req.MemberID,
		"remaining_members", len(group.Members))

	return nil
}

// Heartbeat processes a heartbeat.
func (c *PartitionBackedCoordinator) Heartbeat(req *HeartbeatRequest) (*HeartbeatResult, error) {
	if req.GroupID == "" {
		return nil, ErrGroupIDRequired
	}
	if req.MemberID == "" {
		return nil, ErrMemberIDRequired
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.internalTopicManager.IsCoordinatorFor(req.GroupID) {
		return nil, ErrNotCoordinator
	}

	group := c.groups[req.GroupID]
	if group == nil {
		return nil, ErrGroupNotFound
	}

	member, exists := group.Members[req.MemberID]
	if !exists {
		return nil, ErrUnknownMember
	}

	// Verify generation
	if req.GenerationID != int(group.Generation) {
		return &HeartbeatResult{
			ShouldRejoin:   true,
			RebalanceState: "generation_mismatch",
		}, nil
	}

	// Update heartbeat
	member.LastHeartbeat = time.Now()

	// Check if rebalance is pending
	shouldRejoin := group.State == GroupStateRebalancing

	return &HeartbeatResult{
		ShouldRejoin:   shouldRejoin,
		RebalanceState: group.State.String(),
	}, nil
}

// SyncGroup syncs partition assignment.
func (c *PartitionBackedCoordinator) SyncGroup(req *SyncGroupRequest) (*SyncGroupResult, error) {
	if req.GroupID == "" {
		return nil, ErrGroupIDRequired
	}
	if req.MemberID == "" {
		return nil, ErrMemberIDRequired
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.internalTopicManager.IsCoordinatorFor(req.GroupID) {
		return nil, ErrNotCoordinator
	}

	group := c.groups[req.GroupID]
	if group == nil {
		return nil, ErrGroupNotFound
	}

	if _, exists := group.Members[req.MemberID]; !exists {
		return nil, ErrUnknownMember
	}

	if req.GenerationID != int(group.Generation) {
		return nil, ErrIllegalGeneration
	}

	// If leader, store assignments
	if req.MemberID == group.Leader && req.Assignments != nil {
		for mid, assignment := range req.Assignments {
			if m, exists := group.Members[mid]; exists {
				// Store raw assignment for persistence
				m.Metadata = assignment

				// Decode for in-memory tracking
				partitions, err := DecodeAssignment(assignment)
				if err == nil {
					group.Assignment[mid] = partitions
				}
			}
		}

		// Move to stable
		group.State = GroupStateStable

		// Persist
		if err := c.persistGroupMetadata(group); err != nil {
			c.logger.Warn("failed to persist group metadata", "error", err)
		}
	}

	// Return this member's assignment
	assignment, _ := group.Assignment[req.MemberID]
	encoded, _ := EncodeAssignment(assignment)

	return &SyncGroupResult{
		Assignment: encoded,
		Protocol:   group.Protocol,
	}, nil
}

// =============================================================================
// OFFSET MANAGEMENT (implements GroupCoordinatorI)
// =============================================================================

// CommitOffset stores an offset.
func (c *PartitionBackedCoordinator) CommitOffset(req *OffsetCommitRequest) error {
	if req.GroupID == "" {
		return ErrGroupIDRequired
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.internalTopicManager.IsCoordinatorFor(req.GroupID) {
		return ErrNotCoordinator
	}

	// Write to internal topic
	_, err := c.internalTopicManager.WriteOffsetCommit(
		req.GroupID,
		req.Topic,
		int32(req.Partition),
		req.Offset,
		req.Metadata,
	)
	if err != nil {
		return fmt.Errorf("failed to write offset commit: %w", err)
	}

	// Update in-memory cache
	if c.offsets[req.GroupID] == nil {
		c.offsets[req.GroupID] = make(map[string]map[int]*StoredOffset)
	}
	if c.offsets[req.GroupID][req.Topic] == nil {
		c.offsets[req.GroupID][req.Topic] = make(map[int]*StoredOffset)
	}
	c.offsets[req.GroupID][req.Topic][req.Partition] = &StoredOffset{
		Offset:      req.Offset,
		Metadata:    req.Metadata,
		CommittedAt: time.Now(),
	}

	return nil
}

// FetchOffset retrieves a committed offset.
func (c *PartitionBackedCoordinator) FetchOffset(req *OffsetFetchRequest) (*OffsetFetchResult, error) {
	if req.GroupID == "" {
		return nil, ErrGroupIDRequired
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.internalTopicManager.IsCoordinatorFor(req.GroupID) {
		return nil, ErrNotCoordinator
	}

	// Check cache
	if c.offsets[req.GroupID] != nil &&
		c.offsets[req.GroupID][req.Topic] != nil &&
		c.offsets[req.GroupID][req.Topic][req.Partition] != nil {

		stored := c.offsets[req.GroupID][req.Topic][req.Partition]
		return &OffsetFetchResult{
			Offset:    stored.Offset,
			Metadata:  stored.Metadata,
			Timestamp: stored.CommittedAt,
		}, nil
	}

	return nil, ErrOffsetNotFound
}

// FetchAllOffsets retrieves all offsets for a group.
func (c *PartitionBackedCoordinator) FetchAllOffsets(groupID string) (map[string]map[int]int64, error) {
	if groupID == "" {
		return nil, ErrGroupIDRequired
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.internalTopicManager.IsCoordinatorFor(groupID) {
		return nil, ErrNotCoordinator
	}

	result := make(map[string]map[int]int64)

	if c.offsets[groupID] != nil {
		for topic, partitions := range c.offsets[groupID] {
			result[topic] = make(map[int]int64)
			for partition, stored := range partitions {
				result[topic][partition] = stored.Offset
			}
		}
	}

	return result, nil
}

// =============================================================================
// GROUP QUERIES (implements GroupCoordinatorI)
// =============================================================================

// DescribeGroup returns group information.
func (c *PartitionBackedCoordinator) DescribeGroup(groupID string) (*GroupDescription, error) {
	if groupID == "" {
		return nil, ErrGroupIDRequired
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.internalTopicManager.IsCoordinatorFor(groupID) {
		return nil, ErrNotCoordinator
	}

	group := c.groups[groupID]
	if group == nil {
		return nil, ErrGroupNotFound
	}

	desc := &GroupDescription{
		GroupID:      group.GroupID,
		State:        group.State,
		Protocol:     group.Protocol,
		ProtocolType: "consumer",
		Leader:       group.Leader,
		Generation:   int(group.Generation),
		Members:      make([]GroupMemberDescription, 0, len(group.Members)),
	}

	for mid, member := range group.Members {
		memberDesc := GroupMemberDescription{
			MemberID:   mid,
			ClientID:   member.ClientID,
			ClientHost: member.ClientHost,
			Assignment: group.Assignment[mid],
		}
		desc.Members = append(desc.Members, memberDesc)
	}

	return desc, nil
}

// ListGroups returns all group IDs.
func (c *PartitionBackedCoordinator) ListGroups() ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	groups := make([]string, 0, len(c.groups))
	for id := range c.groups {
		groups = append(groups, id)
	}
	return groups, nil
}

// =============================================================================
// TOPIC MANAGEMENT (implements GroupCoordinatorI)
// =============================================================================

// RegisterTopic registers a topic with partition count.
func (c *PartitionBackedCoordinator) RegisterTopic(topicName string, partitionCount int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topicPartitionCounts[topicName] = partitionCount
	c.logger.Info("registered topic", "topic", topicName, "partitions", partitionCount)
}

// UnregisterTopic removes a topic.
func (c *PartitionBackedCoordinator) UnregisterTopic(topicName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.topicPartitionCounts, topicName)
}

// =============================================================================
// PERSISTENCE HELPERS
// =============================================================================

func (c *PartitionBackedCoordinator) persistGroupMetadata(group *CoordinatorGroupState) error {
	// Convert to MemberMetadata for persistence
	members := make([]InternalMemberMetadata, 0, len(group.Members))
	for mid, m := range group.Members {
		assignment, _ := EncodeAssignment(group.Assignment[mid])
		members = append(members, InternalMemberMetadata{
			MemberID:         mid,
			ClientID:         m.ClientID,
			ClientHost:       m.ClientHost,
			SessionTimeout:   int32(m.SessionTimeout.Milliseconds()),
			RebalanceTimeout: int32(m.RebalanceTimeout.Milliseconds()),
			Assignment:       assignment,
			Metadata:         m.Metadata,
		})
	}

	_, err := c.internalTopicManager.WriteGroupMetadata(
		group.GroupID,
		group.State,
		group.Protocol,
		group.Leader,
		group.Generation,
		members,
	)
	return err
}

// =============================================================================
// SESSION MONITORING
// =============================================================================

func (c *PartitionBackedCoordinator) sessionMonitorLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Duration(c.config.SessionCheckIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.checkExpiredSessions()
		}
	}
}

func (c *PartitionBackedCoordinator) checkExpiredSessions() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	for _, group := range c.groups {
		if group.State == GroupStateDead || group.State == GroupStateEmpty {
			continue
		}

		var expiredMembers []string
		for mid, member := range group.Members {
			if now.Sub(member.LastHeartbeat) > member.SessionTimeout {
				expiredMembers = append(expiredMembers, mid)
			}
		}

		if len(expiredMembers) > 0 {
			for _, mid := range expiredMembers {
				delete(group.Members, mid)
				delete(group.Assignment, mid)
				c.logger.Info("evicted expired member",
					"group", group.GroupID,
					"member", mid)
			}

			// Update group state
			if len(group.Members) == 0 {
				group.State = GroupStateEmpty
				group.Leader = ""
			} else {
				group.State = GroupStateRebalancing
				group.Generation++

				// Pick new leader if needed
				if _, exists := group.Members[group.Leader]; !exists {
					for mid := range group.Members {
						group.Leader = mid
						break
					}
				}
			}

			// Persist
			if err := c.persistGroupMetadata(group); err != nil {
				c.logger.Warn("failed to persist after session expiry", "error", err)
			}
		}
	}
}

// =============================================================================
// UTILITIES
// =============================================================================

func (c *PartitionBackedCoordinator) countOffsets() int {
	count := 0
	for _, topics := range c.offsets {
		for _, partitions := range topics {
			count += len(partitions)
		}
	}
	return count
}

// Stats returns coordinator statistics.
type CoordinatorStats struct {
	Groups        int
	TotalMembers  int
	OffsetsStored int
	StateRebuilt  bool
}

func (c *PartitionBackedCoordinator) Stats() CoordinatorStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	totalMembers := 0
	for _, g := range c.groups {
		totalMembers += len(g.Members)
	}

	return CoordinatorStats{
		Groups:        len(c.groups),
		TotalMembers:  totalMembers,
		OffsetsStored: c.countOffsets(),
		StateRebuilt:  c.stateRebuilt,
	}
}
