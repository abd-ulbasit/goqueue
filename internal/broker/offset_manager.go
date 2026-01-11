// =============================================================================
// OFFSET MANAGER - TRACKING CONSUMER PROGRESS
// =============================================================================
//
// WHAT IS OFFSET MANAGEMENT?
// The offset manager tracks where each consumer group has read up to in each
// partition. When a consumer restarts, it can resume from the last committed
// offset instead of re-reading from the beginning.
//
// WHY IS THIS IMPORTANT?
//
//   ┌───────────────────────────────────────────────────────────────────────────┐
//   │              WITHOUT OFFSET TRACKING                                      │
//   │                                                                           │
//   │   1. Consumer reads messages 0-99, processes them                         │
//   │   2. Consumer crashes                                                     │
//   │   3. Consumer restarts → WHERE DO I START?                                │
//   │      - From 0? → Re-process 0-99 (DUPLICATES!)                            │
//   │      - From 100? → How do we know it's 100?                               │
//   │                                                                           │
//   │              WITH OFFSET TRACKING                                         │
//   │                                                                           │
//   │   1. Consumer reads messages 0-99, processes them                         │
//   │   2. Consumer commits offset 100 (means "processed through 99")           │
//   │   3. Consumer crashes                                                     │
//   │   4. Consumer restarts → check committed offset → start at 100            │
//   │                                                                           │
//   └───────────────────────────────────────────────────────────────────────────┘
//
// OFFSET SEMANTICS:
// The committed offset represents the NEXT offset to be read, not the last
// offset that was processed. This is Kafka's convention:
//
//   - Committed offset 100 means: "I've processed everything up to AND INCLUDING
//     offset 99. Next time I poll, give me starting from offset 100."
//
// COMPARISON - How other systems track consumer progress:
//
//   - Kafka: Commits to internal __consumer_offsets topic
//            Benefits: Replicated, compacted, transactional
//            Drawback: Complexity of internal topic
//
//   - SQS: No offset concept. Uses visibility timeout + delete.
//          After processing, you delete the message.
//          Simple but no replay capability.
//
//   - RabbitMQ: Acknowledgments. Message removed from queue on ack.
//               No offset concept, no replay.
//
//   - Redis Streams: XACK command, consumer group tracks last-delivered-id
//                    Similar to Kafka but simpler.
//
//   - goqueue (M3): File-based JSON storage per group
//                   Simple, debuggable, good enough for single-node
//                   Migration to internal topic planned for M11
//
// AUTO-COMMIT vs MANUAL-COMMIT:
//
//   AUTO-COMMIT:
//   - Offsets committed automatically every N milliseconds
//   - Simpler code (no explicit commit calls)
//   - Risk: Message processed but commit not yet done → crash → re-delivery
//   - Use when: Idempotent processing, can handle duplicates
//
//   MANUAL-COMMIT:
//   - Consumer explicitly calls Commit() after processing
//   - Full control over when offsets are persisted
//   - Risk: Forget to commit → restart reprocesses everything
//   - Use when: Need exactly-once semantics, critical data
//
// =============================================================================

package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrOffsetNotFound means no committed offset exists for the partition
	ErrOffsetNotFound = errors.New("offset not found")

	// ErrInvalidOffset means the offset is negative or otherwise invalid
	ErrInvalidOffset = errors.New("invalid offset")
)

// =============================================================================
// OFFSET DATA STRUCTURES
// =============================================================================

// PartitionOffset represents the committed offset for a single partition.
type PartitionOffset struct {
	Partition   int       `json:"partition"`
	Offset      int64     `json:"offset"`       // Next offset to read
	CommittedAt time.Time `json:"committed_at"` // When this offset was committed
	Metadata    string    `json:"metadata"`     // Optional metadata (e.g., consumer ID)
}

// TopicOffsets holds offsets for all partitions of a topic.
type TopicOffsets struct {
	Topic      string                   `json:"topic"`
	Partitions map[int]*PartitionOffset `json:"partitions"`
}

// GroupOffsets holds all offsets for a consumer group.
type GroupOffsets struct {
	GroupID    string                   `json:"group_id"`
	Topics     map[string]*TopicOffsets `json:"topics"`
	Generation int                      `json:"generation"` // Last known generation
	UpdatedAt  time.Time                `json:"updated_at"`
}

// =============================================================================
// OFFSET MANAGER
// =============================================================================

// OffsetManager handles offset storage and retrieval for consumer groups.
//
// STORAGE FORMAT (JSON file per group):
//
//	data/offsets/{group_id}/offsets.json
//
// Example content:
//
//	{
//	  "group_id": "order-processors",
//	  "topics": {
//	    "orders": {
//	      "topic": "orders",
//	      "partitions": {
//	        "0": {"partition": 0, "offset": 1523, "committed_at": "..."},
//	        "1": {"partition": 1, "offset": 892, "committed_at": "..."}
//	      }
//	    }
//	  },
//	  "generation": 5,
//	  "updated_at": "2025-01-01T12:00:00Z"
//	}
type OffsetManager struct {
	// baseDir is where offset files are stored
	// Structure: baseDir/{group_id}/offsets.json
	baseDir string

	// cache holds in-memory copy of offsets for fast access
	// Writes go to both cache and disk
	cache map[string]*GroupOffsets

	// mu protects cache access
	mu sync.RWMutex

	// closed tracks whether Close() has been called (prevents double-close panic)
	closed bool

	// autoCommitEnabled globally enables/disables auto-commit
	autoCommitEnabled bool

	// autoCommitIntervalMs is how often to flush auto-commits
	autoCommitIntervalMs int

	// pendingCommits tracks uncommitted offsets for auto-commit
	// Key: groupID, Value: map of topic -> partition -> offset
	pendingCommits map[string]map[string]map[int]int64

	// pendingMu protects pendingCommits
	pendingMu sync.Mutex

	// stopCh signals the auto-commit goroutine to stop
	stopCh chan struct{}

	// wg tracks the auto-commit goroutine
	wg sync.WaitGroup
}

// NewOffsetManager creates a new offset manager.
func NewOffsetManager(baseDir string, autoCommitEnabled bool, autoCommitIntervalMs int) (*OffsetManager, error) {
	// Create base directory
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create offset directory: %w", err)
	}

	om := &OffsetManager{
		baseDir:              baseDir,
		cache:                make(map[string]*GroupOffsets),
		autoCommitEnabled:    autoCommitEnabled,
		autoCommitIntervalMs: autoCommitIntervalMs,
		pendingCommits:       make(map[string]map[string]map[int]int64),
		stopCh:               make(chan struct{}),
	}

	// Load existing offsets into cache
	if err := om.loadAllOffsets(); err != nil {
		return nil, fmt.Errorf("failed to load offsets: %w", err)
	}

	// Start auto-commit goroutine if enabled
	if autoCommitEnabled && autoCommitIntervalMs > 0 {
		om.startAutoCommit()
	}

	return om, nil
}

// =============================================================================
// OFFSET COMMIT OPERATIONS
// =============================================================================

// Commit saves the offset for a specific partition.
//
// PARAMETERS:
//   - groupID: The consumer group ID
//   - topic: The topic name
//   - partition: The partition number
//   - offset: The next offset to read (processed through offset-1)
//   - generation: The consumer group generation (for staleness check)
//   - metadata: Optional metadata string
//
// FLOW:
//  1. Validate inputs
//  2. Update in-memory cache
//  3. Persist to disk (fsync)
//  4. Return success
//
// COMPARISON:
//   - Kafka: Commits to internal topic, can batch commits, async option
//   - goqueue: Direct file write, always sync for durability
func (om *OffsetManager) Commit(groupID, topic string, partition int, offset int64, generation int, metadata string) error {
	if offset < 0 {
		return ErrInvalidOffset
	}

	om.mu.Lock()
	defer om.mu.Unlock()

	// Get or create group offsets
	groupOffsets, exists := om.cache[groupID]
	if !exists {
		groupOffsets = &GroupOffsets{
			GroupID: groupID,
			Topics:  make(map[string]*TopicOffsets),
		}
		om.cache[groupID] = groupOffsets
	}

	// Get or create topic offsets
	topicOffsets, exists := groupOffsets.Topics[topic]
	if !exists {
		topicOffsets = &TopicOffsets{
			Topic:      topic,
			Partitions: make(map[int]*PartitionOffset),
		}
		groupOffsets.Topics[topic] = topicOffsets
	}

	// Update partition offset
	topicOffsets.Partitions[partition] = &PartitionOffset{
		Partition:   partition,
		Offset:      offset,
		CommittedAt: time.Now(),
		Metadata:    metadata,
	}

	// Update group metadata
	groupOffsets.Generation = generation
	groupOffsets.UpdatedAt = time.Now()

	// Persist to disk
	return om.persistGroup(groupID)
}

// CommitBatch saves multiple offsets atomically.
//
// BATCH COMMIT STRUCTURE:
//
//	map[topic]map[partition]offset
//
// WHY BATCH?
// When consuming from multiple partitions, committing each separately
// would cause multiple disk writes. Batching reduces I/O.
func (om *OffsetManager) CommitBatch(groupID string, offsets map[string]map[int]int64, generation int) error {
	om.mu.Lock()
	defer om.mu.Unlock()

	// Get or create group offsets
	groupOffsets, exists := om.cache[groupID]
	if !exists {
		groupOffsets = &GroupOffsets{
			GroupID: groupID,
			Topics:  make(map[string]*TopicOffsets),
		}
		om.cache[groupID] = groupOffsets
	}

	now := time.Now()

	// Update all offsets
	for topic, partitions := range offsets {
		topicOffsets, exists := groupOffsets.Topics[topic]
		if !exists {
			topicOffsets = &TopicOffsets{
				Topic:      topic,
				Partitions: make(map[int]*PartitionOffset),
			}
			groupOffsets.Topics[topic] = topicOffsets
		}

		for partition, offset := range partitions {
			if offset < 0 {
				continue // Skip invalid offsets
			}
			topicOffsets.Partitions[partition] = &PartitionOffset{
				Partition:   partition,
				Offset:      offset,
				CommittedAt: now,
			}
		}
	}

	// Update group metadata
	groupOffsets.Generation = generation
	groupOffsets.UpdatedAt = now

	// Single persist for all changes
	return om.persistGroup(groupID)
}

// =============================================================================
// OFFSET RETRIEVAL OPERATIONS
// =============================================================================

// GetOffset retrieves the committed offset for a specific partition.
//
// RETURNS:
//   - offset: The next offset to read
//   - error: ErrOffsetNotFound if no offset committed
func (om *OffsetManager) GetOffset(groupID, topic string, partition int) (int64, error) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	groupOffsets, exists := om.cache[groupID]
	if !exists {
		return 0, ErrOffsetNotFound
	}

	topicOffsets, exists := groupOffsets.Topics[topic]
	if !exists {
		return 0, ErrOffsetNotFound
	}

	partitionOffset, exists := topicOffsets.Partitions[partition]
	if !exists {
		return 0, ErrOffsetNotFound
	}

	return partitionOffset.Offset, nil
}

// GetTopicOffsets retrieves all committed offsets for a topic.
func (om *OffsetManager) GetTopicOffsets(groupID, topic string) (map[int]int64, error) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	groupOffsets, exists := om.cache[groupID]
	if !exists {
		return nil, ErrOffsetNotFound
	}

	topicOffsets, exists := groupOffsets.Topics[topic]
	if !exists {
		return nil, ErrOffsetNotFound
	}

	result := make(map[int]int64, len(topicOffsets.Partitions))
	for partition, po := range topicOffsets.Partitions {
		result[partition] = po.Offset
	}
	return result, nil
}

// GetGroupOffsets retrieves all committed offsets for a consumer group.
func (om *OffsetManager) GetGroupOffsets(groupID string) (*GroupOffsets, error) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	groupOffsets, exists := om.cache[groupID]
	if !exists {
		return nil, ErrOffsetNotFound
	}

	// Return a copy to prevent external modification
	return om.copyGroupOffsets(groupOffsets), nil
}

// =============================================================================
// OFFSET RESET OPERATIONS
// =============================================================================

// ResetOffset type for specifying reset behavior.
type ResetOffset int

const (
	// ResetOffsetEarliest resets to the beginning of the partition
	ResetOffsetEarliest ResetOffset = iota

	// ResetOffsetLatest resets to the end of the partition (skip all existing)
	ResetOffsetLatest

	// ResetOffsetTimestamp resets to a specific timestamp (not implemented in M3)
	ResetOffsetTimestamp
)

// ResetToEarliest resets offset to 0 (beginning).
//
// USE CASE:
//   - Reprocessing all historical data
//   - New consumer group starting from scratch
//   - Disaster recovery
func (om *OffsetManager) ResetToEarliest(groupID, topic string, partition int, generation int) error {
	return om.Commit(groupID, topic, partition, 0, generation, "reset:earliest")
}

// ResetToLatest resets offset to the provided latest offset.
//
// USE CASE:
//   - Skip all existing messages
//   - Consumer only cares about new messages
//   - Catching up quickly without processing backlog
func (om *OffsetManager) ResetToLatest(groupID, topic string, partition int, latestOffset int64, generation int) error {
	return om.Commit(groupID, topic, partition, latestOffset, generation, "reset:latest")
}

// ResetToOffset resets to a specific offset.
//
// USE CASE:
//   - Point-in-time recovery
//   - Reprocessing from specific point after bug fix
func (om *OffsetManager) ResetToOffset(groupID, topic string, partition int, offset int64, generation int) error {
	return om.Commit(groupID, topic, partition, offset, generation, fmt.Sprintf("reset:offset:%d", offset))
}

// =============================================================================
// GROUP MANAGEMENT
// =============================================================================

// DeleteGroup removes all offsets for a consumer group.
//
// WHEN TO USE:
//   - Consumer group no longer needed
//   - Clean slate for reprocessing
//   - Cleanup after testing
func (om *OffsetManager) DeleteGroup(groupID string) error {
	om.mu.Lock()
	defer om.mu.Unlock()

	// Remove from cache
	delete(om.cache, groupID)

	// Remove from disk
	groupDir := filepath.Join(om.baseDir, groupID)
	if err := os.RemoveAll(groupDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete group offsets: %w", err)
	}

	return nil
}

// ListGroups returns all consumer groups with committed offsets.
func (om *OffsetManager) ListGroups() []string {
	om.mu.RLock()
	defer om.mu.RUnlock()

	groups := make([]string, 0, len(om.cache))
	for groupID := range om.cache {
		groups = append(groups, groupID)
	}
	return groups
}

// =============================================================================
// AUTO-COMMIT
// =============================================================================

// MarkForAutoCommit stages an offset for auto-commit.
// The offset will be committed during the next auto-commit cycle.
//
// WHY STAGED COMMITS?
// Auto-commit happens on an interval (e.g., every 5 seconds). Between
// intervals, we track which offsets need to be committed.
//
// FLOW:
//  1. Consumer polls messages (offsets 100-109)
//  2. Consumer processes messages
//  3. Consumer calls MarkForAutoCommit(topic, partition, 110)
//  4. Auto-commit goroutine wakes up, commits 110
//  5. If crash before step 4 → messages 100-109 redelivered (at-least-once)
func (om *OffsetManager) MarkForAutoCommit(groupID, topic string, partition int, offset int64) {
	if !om.autoCommitEnabled {
		return
	}

	om.pendingMu.Lock()
	defer om.pendingMu.Unlock()

	// Get or create group pending commits
	groupPending, exists := om.pendingCommits[groupID]
	if !exists {
		groupPending = make(map[string]map[int]int64)
		om.pendingCommits[groupID] = groupPending
	}

	// Get or create topic pending commits
	topicPending, exists := groupPending[topic]
	if !exists {
		topicPending = make(map[int]int64)
		groupPending[topic] = topicPending
	}

	// Only update if higher than current pending
	// (handles out-of-order processing)
	if current, exists := topicPending[partition]; !exists || offset > current {
		topicPending[partition] = offset
	}
}

// startAutoCommit starts the background auto-commit goroutine.
func (om *OffsetManager) startAutoCommit() {
	om.wg.Add(1)
	go om.autoCommitLoop()
}

// autoCommitLoop periodically commits pending offsets.
func (om *OffsetManager) autoCommitLoop() {
	defer om.wg.Done()

	ticker := time.NewTicker(time.Duration(om.autoCommitIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-om.stopCh:
			// Final commit before shutdown
			om.flushPendingCommits()
			return
		case <-ticker.C:
			om.flushPendingCommits()
		}
	}
}

// flushPendingCommits commits all staged offsets.
func (om *OffsetManager) flushPendingCommits() {
	om.pendingMu.Lock()
	// Swap pending commits to avoid holding lock during commit
	pending := om.pendingCommits
	om.pendingCommits = make(map[string]map[string]map[int]int64)
	om.pendingMu.Unlock()

	for groupID, topics := range pending {
		offsets := make(map[string]map[int]int64)
		for topic, partitions := range topics {
			offsets[topic] = partitions
		}
		// Best-effort commit (ignore errors in auto-commit)
		_ = om.CommitBatch(groupID, offsets, 0)
	}
}

// =============================================================================
// PERSISTENCE
// =============================================================================

// persistGroup writes a group's offsets to disk.
func (om *OffsetManager) persistGroup(groupID string) error {
	groupOffsets := om.cache[groupID]
	if groupOffsets == nil {
		return nil
	}

	// Create group directory
	groupDir := filepath.Join(om.baseDir, groupID)
	if err := os.MkdirAll(groupDir, 0755); err != nil {
		return fmt.Errorf("failed to create group directory: %w", err)
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(groupOffsets, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal offsets: %w", err)
	}

	// Write to temp file first (atomic write pattern)
	offsetFile := filepath.Join(groupDir, "offsets.json")
	tempFile := offsetFile + ".tmp"

	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// Rename for atomic replace
	if err := os.Rename(tempFile, offsetFile); err != nil {
		return fmt.Errorf("failed to rename offset file: %w", err)
	}

	return nil
}

// loadAllOffsets loads all group offsets from disk into cache.
func (om *OffsetManager) loadAllOffsets() error {
	entries, err := os.ReadDir(om.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No offsets directory yet
		}
		return fmt.Errorf("failed to read offset directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		groupID := entry.Name()
		offsetFile := filepath.Join(om.baseDir, groupID, "offsets.json")

		data, err := os.ReadFile(offsetFile)
		if err != nil {
			if os.IsNotExist(err) {
				continue // Group dir exists but no offsets file
			}
			return fmt.Errorf("failed to read offset file for %s: %w", groupID, err)
		}

		var groupOffsets GroupOffsets
		if err := json.Unmarshal(data, &groupOffsets); err != nil {
			return fmt.Errorf("failed to unmarshal offsets for %s: %w", groupID, err)
		}

		om.cache[groupID] = &groupOffsets
	}

	return nil
}

// copyGroupOffsets creates a deep copy of GroupOffsets.
func (om *OffsetManager) copyGroupOffsets(src *GroupOffsets) *GroupOffsets {
	if src == nil {
		return nil
	}

	dst := &GroupOffsets{
		GroupID:    src.GroupID,
		Topics:     make(map[string]*TopicOffsets, len(src.Topics)),
		Generation: src.Generation,
		UpdatedAt:  src.UpdatedAt,
	}

	for topic, topicOffsets := range src.Topics {
		dstTopic := &TopicOffsets{
			Topic:      topicOffsets.Topic,
			Partitions: make(map[int]*PartitionOffset, len(topicOffsets.Partitions)),
		}
		for partition, po := range topicOffsets.Partitions {
			dstTopic.Partitions[partition] = &PartitionOffset{
				Partition:   po.Partition,
				Offset:      po.Offset,
				CommittedAt: po.CommittedAt,
				Metadata:    po.Metadata,
			}
		}
		dst.Topics[topic] = dstTopic
	}

	return dst
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Close shuts down the offset manager gracefully.
//
// BUG FIX: Protected against double-close panic by tracking closed state.
// Previously, calling Close() twice would panic on close(om.stopCh).
func (om *OffsetManager) Close() error {
	om.mu.Lock()
	if om.closed {
		om.mu.Unlock()
		return nil // Already closed
	}
	om.closed = true
	om.mu.Unlock()

	// Signal auto-commit goroutine to stop
	close(om.stopCh)

	// Wait for final commit
	om.wg.Wait()

	return nil
}
