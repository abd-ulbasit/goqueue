// =============================================================================
// LOG COMPACTION - KEEP ONLY LATEST VALUES PER KEY
// =============================================================================
//
// WHAT IS LOG COMPACTION?
// Log compaction is a cleanup process that removes duplicate records for the
// same key, keeping only the latest value. It's essential for topics that
// represent current state rather than event history.
//
// WHY DO WE NEED LOG COMPACTION?
//
// WITHOUT COMPACTION:
// ┌─────────────────────────────────────────────────────────────────────────┐
// │ __consumer_offsets topic keeps every offset commit forever              │
// │                                                                         │
// │ Offset 0: group1:topic1:0 → offset:100                                  │
// │ Offset 1: group1:topic1:1 → offset:200                                  │
// │ Offset 2: group1:topic1:0 → offset:150  ← Updates offset                │
// │ Offset 3: group1:topic1:1 → offset:250  ← Updates offset                │
// │ Offset 4: group1:topic1:0 → offset:175  ← Updates offset                │
// │ ...                                                                     │
// │ Offset 1,000,000: Still keeping all history                             │
// │                                                                         │
// │ PROBLEMS:                                                               │
// │   - Partition file grows forever (GB → TB)                              │
// │   - Recovery replays all records (slow startup)                         │
// │   - Snapshots include old data (large snapshots)                        │
// │   - Storage waste (95%+ of records are outdated)                        │
// └─────────────────────────────────────────────────────────────────────────┘
//
// WITH COMPACTION:
// ┌─────────────────────────────────────────────────────────────────────────┐
// │ __consumer_offsets keeps only the latest value per key                  │
// │                                                                         │
// │ Before compaction:                                                      │
// │   Offset 0: group1:topic1:0 → offset:100                                │
// │   Offset 2: group1:topic1:0 → offset:150                                │
// │   Offset 4: group1:topic1:0 → offset:175  ← Latest for this key         │
// │   Offset 1: group1:topic1:1 → offset:200                                │
// │   Offset 3: group1:topic1:1 → offset:250  ← Latest for this key         │
// │                                                                         │
// │ After compaction (keeping latest per key):                              │
// │   Offset 0: group1:topic1:0 → offset:175  ← Only latest kept            │
// │   Offset 1: group1:topic1:1 → offset:250  ← Only latest kept            │
// │                                                                         │
// │ BENEFITS:                                                               │
// │   - File size stays bounded (only current state)                        │
// │   - Fast recovery (replay small compacted log)                          │
// │   - Smaller snapshots (only active keys)                                │
// │   - Storage efficiency (discard 95%+ old records)                       │
// └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================
// COMPACTION STRATEGY: COPY-ON-COMPACT
// =============================================================================
//
// WHY COPY-ON-COMPACT?
// We use a copy-on-compact strategy where compaction creates a new log with
// only the latest records, then atomically replaces the old log. This is safer
// and simpler than in-place compaction.
//
// COMPACTION FLOW:
// ┌─────────────────────────────────────────────────────────────────────────┐
// │ STEP 1: Trigger Check                                                   │
// │   ├─ Calculate dirty ratio: (total records - unique keys) / total       │
// │   ├─ If dirty_ratio >= 0.5 (50%+ duplicates) → trigger compaction       │
// │   └─ Or manual trigger via HTTP API                                     │
// │                                                                         │
// │ STEP 2: Build Key Map (Last Value Wins)                                 │
// │   ├─ Scan entire partition using log.ReadFrom()                         │
// │   ├─ For each record: keyMap[record.Key] = record                       │
// │   ├─ Later records overwrite earlier ones (last value wins)             │
// │   └─ Handle tombstones (null value = delete after retention)            │
// │                                                                         │
// │ STEP 3: Write Compacted Log                                             │
// │   ├─ Create new Log in temp directory                                   │
// │   ├─ Write only latest records from keyMap                              │
// │   ├─ Assign new sequential offsets (compact offset space)               │
// │   ├─ Fsync to ensure durability                                         │
// │                                                                         │
// │ STEP 4: Atomic Swap                                                     │
// │   ├─ Close old log                                                      │
// │   ├─ Rename: partition_dir → partition_dir.old                          │
// │   ├─ Rename: temp_dir → partition_dir                                   │
// │   ├─ Delete: partition_dir.old                                          │
// │                                                                         │
// │ STEP 5: Reload Partition                                                │
// │   ├─ Open new log                                                       │
// │   ├─ Rebuild priority index and scheduler                               │
// │   └─ Emit compaction metrics                                            │
// └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================
// TOMBSTONE HANDLING (SOFT DELETES)
// =============================================================================
//
// WHAT ARE TOMBSTONES?
// A tombstone is a record with an empty value, indicating that a key should be
// deleted. However, we can't delete it immediately because:
//   1. Consumers may be lagging and still need to see the deletion
//   2. New consumers need to learn that the key was deleted
//
// TOMBSTONE LIFECYCLE:
//   T=0: Record exists (key → value)
//   T+10: Tombstone written (key → null)
//   T+30: Compaction runs (tombstone kept, old record deleted)
//   T+24h: Tombstone retention expires (safe to delete)
//
// =============================================================================
// COMPARISON WITH OTHER SYSTEMS
// =============================================================================
//
// KAFKA:
//   - Compaction per segment (not entire partition)
//   - Min dirty ratio: 0.5 (50% duplicates triggers compaction)
//   - Copy-on-compact strategy
//   - Tombstone retention: delete.retention.ms (default 24h)
//
// RABBITMQ:
//   - No log compaction (uses transient queues)
//
// GOQUEUE DESIGN:
//   - Follow Kafka's proven approach
//   - Partition-level compaction (simpler than per-segment)
//   - Internal topics only (__consumer_offsets, __transaction_state)
//   - Dirty ratio trigger: 0.5 (50% duplicates)
//   - Tombstone retention: 24 hours
//
// =============================================================================

package broker

import (
	"fmt"
	"os"
	"sync"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// CONSTANTS
// =============================================================================

const (
	// DefaultDirtyRatioThreshold is the minimum dirty ratio to trigger compaction.
	// A dirty ratio of 0.5 means 50% of records are duplicates.
	DefaultDirtyRatioThreshold = 0.5

	// DefaultTombstoneRetention is how long to keep tombstones before deleting.
	// Matches Kafka's default delete.retention.ms (24 hours).
	DefaultTombstoneRetention = 24 * time.Hour

	// DefaultCompactionCheckInterval is how often to check if compaction is needed.
	DefaultCompactionCheckInterval = 10 * time.Minute

	// compactionTempSuffix is the suffix for temporary compaction directories.
	compactionTempSuffix = ".compacting"

	// compactionOldSuffix is the suffix for old directories during swap.
	compactionOldSuffix = ".old"
)

// =============================================================================
// COMPACTOR TYPES
// =============================================================================

// Compactor handles log compaction for partitions.
// It scans all messages, builds a key map, and writes a compacted log.
type Compactor struct {
	dirtyRatioThreshold float64
	tombstoneRetention  time.Duration
	checkInterval       time.Duration
	stopCh              chan struct{}
	wg                  sync.WaitGroup
	mu                  sync.RWMutex
	running             bool
}

// CompactionStats tracks compaction metrics.
type CompactionStats struct {
	TotalRecordsBefore int64
	TotalRecordsAfter  int64
	UniqueKeys         int64
	RecordsRemoved     int64
	TombstonesRemoved  int64
	BytesBefore        int64
	BytesAfter         int64
	DurationMS         int64
	DirtyRatio         float64
	CompactedAt        time.Time
}

// CompactionResult contains the result of a compaction operation.
type CompactionResult struct {
	Success bool
	Stats   CompactionStats
	Error   error
}

// =============================================================================
// COMPACTOR LIFECYCLE
// =============================================================================

// NewCompactor creates a new compactor with default settings.
func NewCompactor() *Compactor {
	return &Compactor{
		dirtyRatioThreshold: DefaultDirtyRatioThreshold,
		tombstoneRetention:  DefaultTombstoneRetention,
		checkInterval:       DefaultCompactionCheckInterval,
		stopCh:              make(chan struct{}),
	}
}

// NewCompactorWithConfig creates a compactor with custom settings.
func NewCompactorWithConfig(dirtyRatio float64, tombstoneRetention, checkInterval time.Duration) *Compactor {
	return &Compactor{
		dirtyRatioThreshold: dirtyRatio,
		tombstoneRetention:  tombstoneRetention,
		checkInterval:       checkInterval,
		stopCh:              make(chan struct{}),
	}
}

// =============================================================================
// DIRTY RATIO CALCULATION
// =============================================================================

// CalculateDirtyRatio computes the dirty ratio for a partition.
// Dirty ratio = (total records - unique keys) / total records
//
// RETURNS:
//   - dirtyRatio: 0.0 to 1.0 (0 = no duplicates, 1 = all duplicates)
//   - totalRecords: Total number of records in partition
//   - uniqueKeys: Number of unique keys
//   - error: If partition cannot be read
func CalculateDirtyRatio(p *Partition) (dirtyRatio float64, totalRecords, uniqueKeys int64, err error) {
	// Get the log from partition
	log := p.Log()
	if log == nil {
		return 0, 0, 0, fmt.Errorf("partition has no log")
	}

	// Read all messages and count unique keys
	keySet := make(map[string]struct{})
	startOffset := log.EarliestOffset()

	// Read in batches to avoid memory pressure
	const batchSize = 10000
	currentOffset := startOffset

	for {
		messages, err := log.ReadFrom(currentOffset, batchSize)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("failed to read messages: %w", err)
		}

		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			totalRecords++
			keySet[string(msg.Key)] = struct{}{}
		}

		// Move to next batch
		currentOffset = messages[len(messages)-1].Offset + 1
	}

	uniqueKeys = int64(len(keySet))

	if totalRecords == 0 {
		return 0.0, 0, 0, nil
	}

	dirtyRatio = float64(totalRecords-uniqueKeys) / float64(totalRecords)
	return dirtyRatio, totalRecords, uniqueKeys, nil
}

// ShouldCompact determines if a partition needs compaction.
func (c *Compactor) ShouldCompact(p *Partition) (bool, float64, error) {
	dirtyRatio, _, _, err := CalculateDirtyRatio(p)
	if err != nil {
		return false, 0, err
	}

	return dirtyRatio >= c.dirtyRatioThreshold, dirtyRatio, nil
}

// =============================================================================
// COMPACTION LOGIC
// =============================================================================

// CompactPartition performs log compaction on a partition.
//
// ALGORITHM:
//  1. Read all messages, build key → latest_message map
//  2. Handle tombstones (keep if fresh, delete if expired)
//  3. Create temporary log with compacted records
//  4. Atomic swap: old → old.bak, temp → current
//  5. Delete old.bak
//  6. Return stats
//
// THREAD SAFETY:
//   - Caller must ensure no concurrent writes during compaction
//   - For production use, acquire exclusive lock on partition first
func CompactPartition(p *Partition, tombstoneRetention time.Duration) (*CompactionResult, error) {
	startTime := time.Now()
	result := &CompactionResult{
		Stats: CompactionStats{},
	}

	// Get log and directory
	log := p.Log()
	if log == nil {
		return nil, fmt.Errorf("partition has no log")
	}

	partitionDir := p.Dir()
	tempDir := partitionDir + compactionTempSuffix
	oldDir := partitionDir + compactionOldSuffix

	// Record initial size
	result.Stats.BytesBefore = log.Size()

	// Step 1: Build key map (last value wins)
	keyMap, totalRecords, tombstonesRemoved, err := buildKeyMap(log, tombstoneRetention)
	if err != nil {
		result.Error = fmt.Errorf("failed to build key map: %w", err)
		return result, result.Error
	}

	result.Stats.TotalRecordsBefore = totalRecords
	result.Stats.UniqueKeys = int64(len(keyMap))
	result.Stats.TombstonesRemoved = tombstonesRemoved

	// Step 2: Create temporary log
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		result.Error = fmt.Errorf("failed to create temp directory: %w", err)
		return result, result.Error
	}

	tempLog, err := storage.NewLog(tempDir)
	if err != nil {
		os.RemoveAll(tempDir)
		result.Error = fmt.Errorf("failed to create temp log: %w", err)
		return result, result.Error
	}

	// Step 3: Write compacted records to temp log
	recordsWritten := int64(0)
	for _, msg := range keyMap {
		// Create new message with fresh offset (assigned by log.Append)
		newMsg := &storage.Message{
			Key:       msg.Key,
			Value:     msg.Value,
			Timestamp: msg.Timestamp,
			Priority:  msg.Priority,
			Headers:   msg.Headers,
		}

		if _, err := tempLog.Append(newMsg); err != nil {
			tempLog.Close()
			os.RemoveAll(tempDir)
			result.Error = fmt.Errorf("failed to write compacted message: %w", err)
			return result, result.Error
		}
		recordsWritten++
	}

	// Sync temp log
	if err := tempLog.Sync(); err != nil {
		tempLog.Close()
		os.RemoveAll(tempDir)
		result.Error = fmt.Errorf("failed to sync temp log: %w", err)
		return result, result.Error
	}

	// Close temp log before swap
	if err := tempLog.Close(); err != nil {
		os.RemoveAll(tempDir)
		result.Error = fmt.Errorf("failed to close temp log: %w", err)
		return result, result.Error
	}

	// Step 4: Atomic swap
	// 4a: Close original log
	if err := log.Close(); err != nil {
		os.RemoveAll(tempDir)
		result.Error = fmt.Errorf("failed to close original log: %w", err)
		return result, result.Error
	}

	// 4b: Rename original to .old
	if err := os.Rename(partitionDir, oldDir); err != nil {
		// Try to reopen original log
		os.RemoveAll(tempDir)
		result.Error = fmt.Errorf("failed to rename original directory: %w", err)
		return result, result.Error
	}

	// 4c: Rename temp to original
	if err := os.Rename(tempDir, partitionDir); err != nil {
		// Rollback: restore original
		os.Rename(oldDir, partitionDir)
		result.Error = fmt.Errorf("failed to rename temp directory: %w", err)
		return result, result.Error
	}

	// Step 5: Delete old directory (best effort)
	os.RemoveAll(oldDir)

	// Calculate final stats
	result.Stats.TotalRecordsAfter = recordsWritten
	result.Stats.RecordsRemoved = totalRecords - recordsWritten
	// Avoid division by zero if partition was empty
	if totalRecords > 0 {
		result.Stats.DirtyRatio = float64(result.Stats.RecordsRemoved) / float64(totalRecords)
	} else {
		result.Stats.DirtyRatio = 0.0
	}
	result.Stats.DurationMS = time.Since(startTime).Milliseconds()
	result.Stats.CompactedAt = time.Now()

	// Get new size (need to reload log temporarily)
	newLog, err := storage.LoadLog(partitionDir)
	if err == nil {
		result.Stats.BytesAfter = newLog.Size()
		newLog.Close()
	}

	result.Success = true
	return result, nil
}

// buildKeyMap scans all records and builds a map of key → latest message.
//
// LAST VALUE WINS:
//   - If key appears multiple times, keep only the latest (highest offset)
//   - Tombstones (empty value) are kept if within retention period
//   - Expired tombstones are discarded
//
// RETURNS:
//   - keyMap: Map of key → latest message
//   - totalRecords: Total records scanned
//   - tombstonesRemoved: Number of expired tombstones removed
func buildKeyMap(log *storage.Log, tombstoneRetention time.Duration) (
	map[string]*storage.Message, int64, int64, error,
) {
	keyMap := make(map[string]*storage.Message)
	totalRecords := int64(0)
	tombstonesRemoved := int64(0)

	startOffset := log.EarliestOffset()
	const batchSize = 10000
	currentOffset := startOffset

	for {
		messages, err := log.ReadFrom(currentOffset, batchSize)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("failed to read messages: %w", err)
		}

		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			totalRecords++
			key := string(msg.Key)

			// Handle tombstones (empty value = deletion marker)
			if len(msg.Value) == 0 {
				// Check if tombstone is expired
				// Note: msg.Timestamp is in nanoseconds (UnixNano)
				tombstoneAge := time.Since(time.Unix(0, msg.Timestamp))
				if tombstoneAge > tombstoneRetention {
					// Expired tombstone: remove from map
					delete(keyMap, key)
					tombstonesRemoved++
					continue
				}
			}

			// Last value wins: store this message
			keyMap[key] = msg
		}

		// Move to next batch
		currentOffset = messages[len(messages)-1].Offset + 1
	}

	return keyMap, totalRecords, tombstonesRemoved, nil
}

// =============================================================================
// PARTITION RELOAD HELPER
// =============================================================================

// ReloadPartition reloads a partition after compaction.
// This should be called after CompactPartition to get the new partition instance.
func ReloadPartition(baseDir, topic string, id int) (*Partition, error) {
	return LoadPartition(baseDir, topic, id)
}
