// =============================================================================
// PRIORITY INDEX - PER-PARTITION PRIORITY TRACKING
// =============================================================================
//
// WHAT IS A PRIORITY INDEX?
// The Priority Index maintains an in-memory view of which messages are ready
// for consumption at each priority level within a partition. It bridges the
// gap between the append-only log (which stores messages sequentially by offset)
// and the priority scheduler (which needs to fetch by priority).
//
// WHY DO WE NEED THIS?
//
// PROBLEM: Messages are stored by offset, not priority
// ┌────────────────────────────────────────────────────────────────────────┐
// │ Log File (on disk):                                                    │
// │ [offset=0, priority=High] [offset=1, priority=Low] [offset=2, High]... │
// └────────────────────────────────────────────────────────────────────────┘
//
// When a consumer asks for "next message by priority", we can't scan the
// entire log to find the highest priority message - that would be O(n).
//
// SOLUTION: Priority Index maps priority → offsets
// ┌────────────────────────────────────────────────────────────────────────┐
// │ Priority Index (in memory):                                            │
// │   Critical:   [offset=5, offset=12, offset=45]                         │
// │   High:       [offset=0, offset=2, offset=8, offset=15]                │
// │   Normal:     [offset=3, offset=9, offset=20]                          │
// │   Low:        [offset=1, offset=7]                                     │
// │   Background: [offset=4, offset=11]                                    │
// └────────────────────────────────────────────────────────────────────────┘
//
// Now finding the next high-priority message is O(log n) per priority level.
//
// INTEGRATION WITH CONSUMER GROUPS:
//
// This index tracks ALL messages in the partition. Consumers use their
// committed offset to skip already-processed messages:
//
//   ready_messages = index[priority].filter(offset > consumer_committed_offset)
//
// MEMORY EFFICIENCY:
//
// We only store offsets (8 bytes each), not full messages.
// For 1M messages: ~8MB per priority × 5 priorities = ~40MB worst case.
// In practice, most messages are Normal priority, so other queues are smaller.
//
// PERSISTENCE:
//
// The index is rebuilt from the log on startup. We don't persist it because:
// - The log is the source of truth
// - Rebuilding is fast (sequential scan, O(n))
// - Avoids index corruption issues
//
// =============================================================================

package broker

import (
	"sync"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// PRIORITY INDEX ENTRY
// =============================================================================

// PriorityIndexEntry represents a message in the priority index.
// We store minimal info to keep memory usage low.
type PriorityIndexEntry struct {
	// Offset is the message position in the partition log
	Offset int64

	// Timestamp is when the message was written (for time-based queries)
	Timestamp int64

	// Size is the message size in bytes (for metrics)
	Size int
}

// =============================================================================
// PRIORITY INDEX
// =============================================================================

// PriorityIndex maintains per-priority message tracking for a partition.
// It enables efficient priority-based message lookup.
//
// THREAD SAFETY:
// All methods are safe for concurrent use.
//
// LIFECYCLE:
//  1. Created when partition is loaded/created
//  2. Rebuilt from log on startup (see RebuildFromLog)
//  3. Updated on each Produce (AddMessage)
//  4. Queried on each Consume (GetNextOffset, GetNextN)
//  5. Cleaned up on message acknowledgment (RemoveOffset)
type PriorityIndex struct {
	// offsets maps priority → sorted slice of entries
	// Using slices instead of heaps because:
	// - We often need to scan from a starting offset (consumer's position)
	// - Insertions are always at the end (append-only log)
	// - Deletions can be lazy (mark as consumed, compact periodically)
	offsets [storage.PriorityCount][]PriorityIndexEntry

	// consumed tracks offsets that have been acknowledged
	// We use a map for O(1) lookup when filtering
	consumed map[int64]bool

	// stats holds per-priority statistics
	stats PriorityIndexStats

	// mu protects all fields
	mu sync.RWMutex

	// lastCompaction is when we last compacted the consumed entries
	lastCompaction time.Time

	// compactionThreshold is how many consumed entries before we compact
	compactionThreshold int
}

// PriorityIndexStats holds statistics for the priority index.
type PriorityIndexStats struct {
	// MessageCount is total messages per priority
	MessageCount [storage.PriorityCount]int64

	// ByteCount is total bytes per priority
	ByteCount [storage.PriorityCount]int64

	// ConsumedCount is acknowledged messages (pending compaction)
	ConsumedCount int64
}

// NewPriorityIndex creates a new empty priority index.
func NewPriorityIndex() *PriorityIndex {
	return &PriorityIndex{
		consumed:            make(map[int64]bool),
		lastCompaction:      time.Now(),
		compactionThreshold: 10000, // Compact after 10k consumed entries
	}
}

// =============================================================================
// ADD MESSAGE
// =============================================================================

// AddMessage adds a message to the priority index.
// Called when a message is produced to the partition.
//
// COMPLEXITY: O(1) amortized (append to slice)
func (pi *PriorityIndex) AddMessage(msg *storage.Message) {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	priority := msg.Priority
	if !priority.IsValid() {
		priority = storage.PriorityNormal
	}

	entry := PriorityIndexEntry{
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
		Size:      msg.Size(),
	}

	pi.offsets[priority] = append(pi.offsets[priority], entry)
	pi.stats.MessageCount[priority]++
	pi.stats.ByteCount[priority] += int64(entry.Size)
}

// AddEntry adds a pre-built entry to the index.
// Used during log replay/rebuild.
func (pi *PriorityIndex) AddEntry(priority storage.Priority, entry PriorityIndexEntry) {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	if !priority.IsValid() {
		priority = storage.PriorityNormal
	}

	pi.offsets[priority] = append(pi.offsets[priority], entry)
	pi.stats.MessageCount[priority]++
	pi.stats.ByteCount[priority] += int64(entry.Size)
}

// =============================================================================
// GET NEXT MESSAGES
// =============================================================================

// GetNextOffset returns the next unconsumed message offset for a priority,
// starting from the given minimum offset.
//
// PARAMETERS:
//   - priority: Which priority queue to check
//   - fromOffset: Starting offset (inclusive) - returns message at this offset if exists
//
// RETURNS:
//   - offset: Next message offset, or -1 if none
//   - found: true if a message was found
//
// COMPLEXITY: O(log n) binary search + O(k) scan for consumed entries
func (pi *PriorityIndex) GetNextOffset(priority storage.Priority, fromOffset int64) (int64, bool) {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	if !priority.IsValid() {
		return -1, false
	}

	entries := pi.offsets[priority]
	if len(entries) == 0 {
		return -1, false
	}

	// Binary search for first entry >= fromOffset
	idx := pi.searchOffsetInclusive(entries, fromOffset)

	// Scan forward, skipping consumed entries
	for idx < len(entries) {
		entry := entries[idx]
		if !pi.consumed[entry.Offset] {
			return entry.Offset, true
		}
		idx++
	}

	return -1, false
}

// GetNextN returns up to n unconsumed message offsets for a priority.
//
// PARAMETERS:
//   - priority: Which priority queue to check
//   - fromOffset: Starting offset (inclusive)
//   - n: Maximum entries to return
//
// RETURNS: Slice of offsets (may be less than n)
func (pi *PriorityIndex) GetNextN(priority storage.Priority, fromOffset int64, n int) []int64 {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	if !priority.IsValid() || n <= 0 {
		return nil
	}

	entries := pi.offsets[priority]
	if len(entries) == 0 {
		return nil
	}

	result := make([]int64, 0, n)
	idx := pi.searchOffsetInclusive(entries, fromOffset)

	for idx < len(entries) && len(result) < n {
		entry := entries[idx]
		if !pi.consumed[entry.Offset] {
			result = append(result, entry.Offset)
		}
		idx++
	}

	return result
}

// GetNextAcrossPriorities returns the next message respecting priority order.
// Uses simple strict priority (Critical first, then High, etc.)
// For WFQ, use PriorityScheduler instead.
//
// PARAMETERS:
//   - fromOffset: Starting offset (inclusive) for ALL priorities
//
// RETURNS:
//   - offset: Next message offset
//   - priority: Priority of the message
//   - found: true if a message was found
func (pi *PriorityIndex) GetNextAcrossPriorities(fromOffset int64) (int64, storage.Priority, bool) {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	// Check each priority from highest (Critical) to lowest (Background)
	for p := storage.PriorityCritical; p <= storage.PriorityBackground; p++ {
		entries := pi.offsets[p]
		if len(entries) == 0 {
			continue
		}

		idx := pi.searchOffsetInclusive(entries, fromOffset)
		for idx < len(entries) {
			entry := entries[idx]
			if !pi.consumed[entry.Offset] {
				return entry.Offset, p, true
			}
			idx++
		}
	}

	return -1, storage.PriorityNormal, false
}

// searchOffsetInclusive performs binary search to find first entry with offset >= target.
// Returns index of first entry >= target, or len(entries) if none.
//
// WHY INCLUSIVE: Matches Kafka-style semantics where fromOffset N means
// "start from message at offset N". This is more intuitive for consumers.
func (pi *PriorityIndex) searchOffsetInclusive(entries []PriorityIndexEntry, target int64) int {
	left, right := 0, len(entries)

	for left < right {
		mid := left + (right-left)/2
		if entries[mid].Offset < target {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}

// =============================================================================
// MARK CONSUMED
// =============================================================================

// MarkConsumed marks an offset as consumed (acknowledged).
// The entry will be filtered out of future queries.
//
// NOTE: This doesn't immediately remove the entry from the slice.
// Periodic compaction removes consumed entries to reclaim memory.
func (pi *PriorityIndex) MarkConsumed(offset int64) {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	pi.consumed[offset] = true
	pi.stats.ConsumedCount++

	// Trigger compaction if threshold exceeded
	if pi.stats.ConsumedCount > int64(pi.compactionThreshold) {
		pi.compactLocked()
	}
}

// MarkConsumedBatch marks multiple offsets as consumed.
// More efficient than calling MarkConsumed repeatedly.
func (pi *PriorityIndex) MarkConsumedBatch(offsets []int64) {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	for _, offset := range offsets {
		pi.consumed[offset] = true
		pi.stats.ConsumedCount++
	}

	if pi.stats.ConsumedCount > int64(pi.compactionThreshold) {
		pi.compactLocked()
	}
}

// =============================================================================
// COMPACTION
// =============================================================================

// Compact removes consumed entries from the index to reclaim memory.
// Called automatically when consumed count exceeds threshold.
// Can also be called manually.
func (pi *PriorityIndex) Compact() {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	pi.compactLocked()
}

// compactLocked performs compaction with lock already held.
func (pi *PriorityIndex) compactLocked() {
	for p := storage.PriorityCritical; p <= storage.PriorityBackground; p++ {
		entries := pi.offsets[p]
		if len(entries) == 0 {
			continue
		}

		// Filter out consumed entries
		newEntries := make([]PriorityIndexEntry, 0, len(entries))
		for _, entry := range entries {
			if !pi.consumed[entry.Offset] {
				newEntries = append(newEntries, entry)
			}
		}

		pi.offsets[p] = newEntries
	}

	// Clear consumed map
	pi.consumed = make(map[int64]bool)
	pi.stats.ConsumedCount = 0
	pi.lastCompaction = time.Now()
}

// =============================================================================
// REBUILD FROM LOG
// =============================================================================

// RebuildFromLog rebuilds the priority index by scanning the partition log.
// Called on startup to restore the index from persistent storage.
//
// PARAMETERS:
//   - reader: Function that reads messages starting from an offset
//   - startOffset: Where to start scanning (typically 0 or last checkpoint)
//
// The reader function should return (message, nextOffset, error).
// Return io.EOF when no more messages.
//
// COMPLEXITY: O(n) where n is number of messages in the log
func (pi *PriorityIndex) RebuildFromLog(
	reader func(offset int64) (*storage.Message, error),
	startOffset int64,
) error {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	// Clear existing data
	for i := range pi.offsets {
		pi.offsets[i] = nil
	}
	pi.consumed = make(map[int64]bool)
	pi.stats = PriorityIndexStats{}

	// Scan log sequentially
	offset := startOffset
	for {
		msg, err := reader(offset)
		if err != nil {
			// End of log or error
			break
		}

		priority := msg.Priority
		if !priority.IsValid() {
			priority = storage.PriorityNormal
		}

		entry := PriorityIndexEntry{
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp,
			Size:      msg.Size(),
		}

		pi.offsets[priority] = append(pi.offsets[priority], entry)
		pi.stats.MessageCount[priority]++
		pi.stats.ByteCount[priority] += int64(entry.Size)

		offset = msg.Offset + 1
	}

	return nil
}

// =============================================================================
// STATISTICS AND INFO
// =============================================================================

// Stats returns a copy of the index statistics.
func (pi *PriorityIndex) Stats() PriorityIndexStats {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	return pi.stats
}

// Len returns total entries in the index (including consumed).
func (pi *PriorityIndex) Len() int {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	total := 0
	for _, entries := range pi.offsets {
		total += len(entries)
	}
	return total
}

// LenByPriority returns entry count for each priority level.
func (pi *PriorityIndex) LenByPriority() [storage.PriorityCount]int {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	var counts [storage.PriorityCount]int
	for i, entries := range pi.offsets {
		counts[i] = len(entries)
	}
	return counts
}

// UnconsumedCount returns the number of unconsumed entries per priority.
func (pi *PriorityIndex) UnconsumedCount() [storage.PriorityCount]int {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	var counts [storage.PriorityCount]int
	for p, entries := range pi.offsets {
		for _, entry := range entries {
			if !pi.consumed[entry.Offset] {
				counts[p]++
			}
		}
	}
	return counts
}

// Clear removes all entries from the index.
func (pi *PriorityIndex) Clear() {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	for i := range pi.offsets {
		pi.offsets[i] = nil
	}
	pi.consumed = make(map[int64]bool)
	pi.stats = PriorityIndexStats{}
}

// =============================================================================
// METRICS SNAPSHOT
// =============================================================================

// PriorityMetricsSnapshot captures a point-in-time view of priority metrics.
type PriorityMetricsSnapshot struct {
	// Priority is which priority this snapshot is for
	Priority storage.Priority

	// TotalMessages is all-time message count
	TotalMessages int64

	// TotalBytes is all-time byte count
	TotalBytes int64

	// PendingMessages is unconsumed messages
	PendingMessages int

	// OldestPendingTimestamp is timestamp of oldest unconsumed message (0 if none)
	OldestPendingTimestamp int64
}

// GetMetricsSnapshot returns detailed metrics for each priority level.
func (pi *PriorityIndex) GetMetricsSnapshot() []PriorityMetricsSnapshot {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	snapshots := make([]PriorityMetricsSnapshot, storage.PriorityCount)

	for p := storage.PriorityCritical; p <= storage.PriorityBackground; p++ {
		snapshot := PriorityMetricsSnapshot{
			Priority:      p,
			TotalMessages: pi.stats.MessageCount[p],
			TotalBytes:    pi.stats.ByteCount[p],
		}

		// Count pending and find oldest
		entries := pi.offsets[p]
		for _, entry := range entries {
			if !pi.consumed[entry.Offset] {
				snapshot.PendingMessages++
				if snapshot.OldestPendingTimestamp == 0 || entry.Timestamp < snapshot.OldestPendingTimestamp {
					snapshot.OldestPendingTimestamp = entry.Timestamp
				}
			}
		}

		snapshots[p] = snapshot
	}

	return snapshots
}
