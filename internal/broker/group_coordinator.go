// =============================================================================
// GROUP COORDINATOR - THE BRAIN OF CONSUMER GROUP MANAGEMENT
// =============================================================================
//
// WHAT IS A GROUP COORDINATOR?
// The group coordinator is responsible for:
//   - Managing consumer group membership (join, leave, heartbeat)
//   - Triggering and managing rebalances
//   - Storing and retrieving committed offsets
//   - Detecting failed consumers (session timeouts)
//
// WHY A SEPARATE COORDINATOR?
//
//   ┌────────────────────────────────────────────────────────────────────────────┐
//   │                    GROUP COORDINATOR ARCHITECTURE                          │
//   │                                                                            │
//   │   ┌─────────────────────────────────────────────────────────────────────┐  │
//   │   │                      GroupCoordinator                               │  │
//   │   │                                                                     │  │
//   │   │   ┌─────────────────┐  ┌─────────────────┐  ┌──────────────────┐    │  │
//   │   │   │  ConsumerGroup  │  │  ConsumerGroup  │  │  OffsetManager   │    │  │
//   │   │   │  "orders-proc"  │  │  "logs-proc"    │  │                  │    │  │
//   │   │   │                 │  │                 │  │  Tracks offsets  │    │  │
//   │   │   │  - Members      │  │  - Members      │  │  for all groups  │    │  │
//   │   │   │  - Assignment   │  │  - Assignment   │  │                  │    │  │
//   │   │   │  - Generation   │  │  - Generation   │  │                  │    │  │
//   │   │   └─────────────────┘  └─────────────────┘  └──────────────────┘    │  │
//   │   │                                                                     │  │
//   │   │   ┌────────────────────────────────────────────────────────────┐    │  │
//   │   │   │                  Session Monitor                           │    │  │
//   │   │   │  Background goroutine checking for expired sessions        │    │  │
//   │   │   └────────────────────────────────────────────────────────────┘    │  │
//   │   │                                                                     │  │
//   │   └─────────────────────────────────────────────────────────────────────┘  │
//   │                                                                            │
//   └────────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON - How other systems coordinate consumer groups:
//
//   - Kafka (Old): ZooKeeper stored group metadata and coordinated leaders
//                  Complex, required separate ZK cluster
//
//   - Kafka (New): Group coordinator is a broker. One broker per group.
//                  __consumer_offsets topic stores offsets.
//                  Group coordinator selected by hashing group ID.
//
//   - RabbitMQ: No consumer groups. Competing consumers pattern.
//               Each consumer gets different messages from same queue.
//
//   - SQS: No explicit coordination. Visibility timeout handles it.
//          Multiple consumers can poll, message goes invisible.
//
//   - goqueue: Single broker is coordinator for all groups (M3).
//              In M11 (clustering), partition leader becomes coordinator.
//
// =============================================================================

package broker

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// =============================================================================
// COORDINATOR CONFIGURATION
// =============================================================================

// CoordinatorConfig holds configuration for the group coordinator.
type CoordinatorConfig struct {
	// OffsetsDir is where offset files are stored
	OffsetsDir string

	// SessionCheckIntervalMs is how often to check for expired sessions
	// Default: 1000 (1 second)
	SessionCheckIntervalMs int

	// DefaultGroupConfig is the default config for new consumer groups
	DefaultGroupConfig ConsumerGroupConfig
}

// DefaultCoordinatorConfig returns sensible defaults.
func DefaultCoordinatorConfig(dataDir string) CoordinatorConfig {
	return CoordinatorConfig{
		OffsetsDir:             filepath.Join(dataDir, "offsets"),
		SessionCheckIntervalMs: 1000,
		DefaultGroupConfig:     DefaultConsumerGroupConfig(),
	}
}

// =============================================================================
// GROUP COORDINATOR
// =============================================================================

// GroupCoordinator manages all consumer groups and their offsets.
type GroupCoordinator struct {
	// config holds coordinator configuration
	config CoordinatorConfig

	// groups maps group ID to ConsumerGroup
	groups map[string]*ConsumerGroup

	// offsetManager handles offset storage
	offsetManager *OffsetManager

	// topicPartitionCounts maps topic name to partition count
	// Used during partition assignment
	topicPartitionCounts map[string]int

	// mu protects groups map and topicPartitionCounts
	mu sync.RWMutex

	// logger for coordinator operations
	logger *slog.Logger

	// stopCh signals background goroutines to stop
	stopCh chan struct{}

	// wg tracks background goroutines
	wg sync.WaitGroup
}

// NewGroupCoordinator creates a new group coordinator.
func NewGroupCoordinator(config CoordinatorConfig) (*GroupCoordinator, error) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create offset manager
	offsetManager, err := NewOffsetManager(
		config.OffsetsDir,
		config.DefaultGroupConfig.AutoCommit,
		config.DefaultGroupConfig.AutoCommitIntervalMs,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create offset manager: %w", err)
	}

	gc := &GroupCoordinator{
		config:               config,
		groups:               make(map[string]*ConsumerGroup),
		offsetManager:        offsetManager,
		topicPartitionCounts: make(map[string]int),
		logger:               logger,
		stopCh:               make(chan struct{}),
	}

	// Start session monitor
	gc.startSessionMonitor()

	return gc, nil
}

// =============================================================================
// TOPIC REGISTRATION
// =============================================================================

// RegisterTopic registers a topic with its partition count.
// Must be called when topics are created or loaded.
func (gc *GroupCoordinator) RegisterTopic(topic string, partitionCount int) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.topicPartitionCounts[topic] = partitionCount
	gc.logger.Info("registered topic with coordinator",
		"topic", topic,
		"partitions", partitionCount,
	)
}

// UnregisterTopic removes a topic from the coordinator.
func (gc *GroupCoordinator) UnregisterTopic(topic string) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	delete(gc.topicPartitionCounts, topic)
}

// GetPartitionCount returns the partition count for a topic.
func (gc *GroupCoordinator) GetPartitionCount(topic string) (int, bool) {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	count, exists := gc.topicPartitionCounts[topic]
	return count, exists
}

// =============================================================================
// GROUP OPERATIONS
// =============================================================================

// CreateGroup creates a new consumer group if it doesn't exist.
func (gc *GroupCoordinator) CreateGroup(groupID string, topics []string) (*ConsumerGroup, error) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	if existing, exists := gc.groups[groupID]; exists {
		return existing, nil // Return existing group
	}

	group := NewConsumerGroup(groupID, topics, gc.config.DefaultGroupConfig)
	gc.groups[groupID] = group

	gc.logger.Info("created consumer group",
		"group", groupID,
		"topics", topics,
	)

	return group, nil
}

// GetGroup returns a consumer group by ID.
func (gc *GroupCoordinator) GetGroup(groupID string) (*ConsumerGroup, error) {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	group, exists := gc.groups[groupID]
	if !exists {
		return nil, ErrGroupNotFound
	}
	return group, nil
}

// GetOrCreateGroup returns existing group or creates new one.
func (gc *GroupCoordinator) GetOrCreateGroup(groupID string, topics []string) (*ConsumerGroup, error) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	if existing, exists := gc.groups[groupID]; exists {
		return existing, nil
	}

	group := NewConsumerGroup(groupID, topics, gc.config.DefaultGroupConfig)
	gc.groups[groupID] = group

	gc.logger.Info("created consumer group",
		"group", groupID,
		"topics", topics,
	)

	return group, nil
}

// DeleteGroup removes a consumer group.
func (gc *GroupCoordinator) DeleteGroup(groupID string) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	group, exists := gc.groups[groupID]
	if !exists {
		return ErrGroupNotFound
	}

	// Mark as dead
	group.mu.Lock()
	group.State = GroupStateDead
	group.mu.Unlock()

	// Remove from map
	delete(gc.groups, groupID)

	// Delete offsets
	if err := gc.offsetManager.DeleteGroup(groupID); err != nil {
		gc.logger.Warn("failed to delete group offsets",
			"group", groupID,
			"error", err,
		)
	}

	gc.logger.Info("deleted consumer group", "group", groupID)
	return nil
}

// ListGroups returns all consumer group IDs.
func (gc *GroupCoordinator) ListGroups() []string {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	groups := make([]string, 0, len(gc.groups))
	for id := range gc.groups {
		groups = append(groups, id)
	}
	return groups
}

// =============================================================================
// JOIN/LEAVE OPERATIONS
// =============================================================================

// JoinGroup adds a consumer to a group.
//
// FLOW:
//  1. Get or create the consumer group
//  2. Determine partition count for the subscribed topic
//  3. Add member to group (triggers rebalance)
//  4. Return assignment to the consumer
//
// PARAMETERS:
//   - groupID: The consumer group to join
//   - clientID: Client-provided identifier (for debugging)
//   - topics: Topics the consumer wants to subscribe to
func (gc *GroupCoordinator) JoinGroup(groupID, clientID string, topics []string) (*JoinResult, error) {
	// Get or create group
	group, err := gc.GetOrCreateGroup(groupID, topics)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create group: %w", err)
	}

	// Get total partition count across all subscribed topics
	// For M3, we simplify by using the first topic's partition count
	// Multi-topic subscription will be improved in M12
	gc.mu.RLock()
	partitionCount := 0
	if len(topics) > 0 {
		if count, exists := gc.topicPartitionCounts[topics[0]]; exists {
			partitionCount = count
		}
	}
	gc.mu.RUnlock()

	if partitionCount == 0 {
		return nil, fmt.Errorf("topic not found or has no partitions: %v", topics)
	}

	// Join the group
	result, err := group.Join(clientID, partitionCount)
	if err != nil {
		return nil, fmt.Errorf("failed to join group: %w", err)
	}

	gc.logger.Info("consumer joined group",
		"group", groupID,
		"member", result.MemberID,
		"generation", result.Generation,
		"partitions", result.Partitions,
	)

	return result, nil
}

// LeaveGroup removes a consumer from a group.
func (gc *GroupCoordinator) LeaveGroup(groupID, memberID string) error {
	group, err := gc.GetGroup(groupID)
	if err != nil {
		return err
	}

	// Get partition count for rebalance
	gc.mu.RLock()
	partitionCount := 0
	if len(group.Topics) > 0 {
		if count, exists := gc.topicPartitionCounts[group.Topics[0]]; exists {
			partitionCount = count
		}
	}
	gc.mu.RUnlock()

	if err := group.Leave(memberID, partitionCount); err != nil {
		return err
	}

	gc.logger.Info("consumer left group",
		"group", groupID,
		"member", memberID,
	)

	return nil
}

// Heartbeat processes a heartbeat from a consumer.
func (gc *GroupCoordinator) Heartbeat(groupID, memberID string, generation int) error {
	group, err := gc.GetGroup(groupID)
	if err != nil {
		return err
	}

	return group.Heartbeat(memberID, generation)
}

// =============================================================================
// OFFSET OPERATIONS
// =============================================================================

// CommitOffset commits an offset for a consumer.
func (gc *GroupCoordinator) CommitOffset(groupID, topic string, partition int, offset int64, memberID string) error {
	group, err := gc.GetGroup(groupID)
	if err != nil {
		return err
	}

	// Verify member owns this partition
	if !group.IsPartitionAssigned(memberID, partition) {
		return ErrNotAssigned
	}

	generation := group.GetGeneration()
	return gc.offsetManager.Commit(groupID, topic, partition, offset, generation, memberID)
}

// CommitOffsets commits multiple offsets for a consumer.
func (gc *GroupCoordinator) CommitOffsets(groupID string, offsets map[string]map[int]int64, memberID string) error {
	group, err := gc.GetGroup(groupID)
	if err != nil {
		return err
	}

	// Verify member owns all partitions
	for _, partitions := range offsets {
		for partition := range partitions {
			if !group.IsPartitionAssigned(memberID, partition) {
				return ErrNotAssigned
			}
		}
	}

	generation := group.GetGeneration()
	return gc.offsetManager.CommitBatch(groupID, offsets, generation)
}

// GetOffset retrieves the committed offset for a partition.
func (gc *GroupCoordinator) GetOffset(groupID, topic string, partition int) (int64, error) {
	return gc.offsetManager.GetOffset(groupID, topic, partition)
}

// GetGroupOffsets retrieves all committed offsets for a group.
func (gc *GroupCoordinator) GetGroupOffsets(groupID string) (*GroupOffsets, error) {
	return gc.offsetManager.GetGroupOffsets(groupID)
}

// MarkForAutoCommit stages an offset for auto-commit.
func (gc *GroupCoordinator) MarkForAutoCommit(groupID, topic string, partition int, offset int64) {
	gc.offsetManager.MarkForAutoCommit(groupID, topic, partition, offset)
}

// =============================================================================
// SESSION MONITORING
// =============================================================================

// startSessionMonitor starts the background session monitoring goroutine.
//
// WHY BACKGROUND MONITORING?
// Consumers can crash without sending a leave request. We need to detect
// this by checking if heartbeats have stopped (session timeout).
//
// FLOW:
//  1. Every SessionCheckIntervalMs, iterate all groups
//  2. For each group, check each member's last heartbeat
//  3. If heartbeat older than SessionTimeoutMs → evict member
//  4. Eviction triggers rebalance
func (gc *GroupCoordinator) startSessionMonitor() {
	gc.wg.Add(1)
	go gc.sessionMonitorLoop()
}

func (gc *GroupCoordinator) sessionMonitorLoop() {
	defer gc.wg.Done()

	ticker := time.NewTicker(time.Duration(gc.config.SessionCheckIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-gc.stopCh:
			return
		case <-ticker.C:
			gc.checkExpiredSessions()
		}
	}
}

func (gc *GroupCoordinator) checkExpiredSessions() {
	gc.mu.RLock()
	groups := make([]*ConsumerGroup, 0, len(gc.groups))
	partitionCounts := make(map[string]int)
	for _, group := range gc.groups {
		groups = append(groups, group)
		if len(group.Topics) > 0 {
			if count, exists := gc.topicPartitionCounts[group.Topics[0]]; exists {
				partitionCounts[group.ID] = count
			}
		}
	}
	gc.mu.RUnlock()

	for _, group := range groups {
		partitionCount := partitionCounts[group.ID]
		evicted := group.EvictExpiredMembers(partitionCount)
		if evicted > 0 {
			gc.logger.Info("evicted expired members",
				"group", group.ID,
				"count", evicted,
				"newGeneration", group.GetGeneration(),
			)
		}
	}
}

// =============================================================================
// GROUP INFO
// =============================================================================

// GetGroupInfo returns detailed information about a consumer group.
func (gc *GroupCoordinator) GetGroupInfo(groupID string) (*GroupInfo, error) {
	group, err := gc.GetGroup(groupID)
	if err != nil {
		return nil, err
	}

	info := group.GetInfo()
	return &info, nil
}

// GetAllGroupsInfo returns information about all consumer groups.
func (gc *GroupCoordinator) GetAllGroupsInfo() []GroupInfo {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	infos := make([]GroupInfo, 0, len(gc.groups))
	for _, group := range gc.groups {
		infos = append(infos, group.GetInfo())
	}
	return infos
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Close shuts down the coordinator gracefully.
func (gc *GroupCoordinator) Close() error {
	// Signal goroutines to stop
	close(gc.stopCh)

	// Wait for goroutines
	gc.wg.Wait()

	// Close offset manager
	if err := gc.offsetManager.Close(); err != nil {
		return fmt.Errorf("failed to close offset manager: %w", err)
	}

	gc.logger.Info("group coordinator closed")
	return nil
}
