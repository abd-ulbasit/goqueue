// =============================================================================
// UNCOMMITTED OFFSET TRACKER - LSO (LAST STABLE OFFSET) IMPLEMENTATION
// =============================================================================
//
// WHAT IS THIS?
// This tracker maintains a record of all offsets that belong to uncommitted
// (in-progress) transactions. During consume operations, these offsets are
// filtered out to provide read_committed isolation semantics.
//
// WHY IS THIS NEEDED?
// When a producer publishes messages as part of a transaction, those messages
// are written to the log immediately (for durability), but they should NOT
// be visible to consumers until the transaction commits.
//
// KAFKA COMPARISON:
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │                                                                             │
// │  Kafka uses LSO (Last Stable Offset) concept:                               │
// │    - LSO = offset where all previous offsets are stable (committed/aborted) │
// │    - Consumer with read_committed only reads up to LSO                      │
// │    - More complex: handles multiple concurrent transactions                 │
// │                                                                             │
// │  goqueue simplified approach:                                               │
// │    - Track set of uncommitted offsets per topic-partition                   │
// │    - During consume, skip any offset in uncommitted set                     │
// │    - Simpler but same end result for consumers                              │
// │                                                                             │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// FLOW:
//
//   ┌──────────────────────────────────────────────────────────────────────────┐
//   │                      TRANSACTIONAL PUBLISH FLOW                          │
//   │                                                                          │
//   │  1. Producer calls PublishTransactional(topic, partition, msg)           │
//   │     └── Message written to log at offset N                               │
//   │     └── TrackUncommitted(topic, partition, offset, txnId) called         │
//   │                                                                          │
//   │  2. Uncommitted tracker records: {topic/partition: [offset N] → txnId}   │
//   │                                                                          │
//   │  3. Consumer calls Consume(topic, partition, ...)                        │
//   │     └── Reads messages from log                                          │
//   │     └── Filters out offset N (IsUncommitted returns true)                │
//   │                                                                          │
//   │  4a. Transaction COMMITS:                                                │
//   │     └── ClearTransaction(txnId) removes all tracked offsets              │
//   │     └── Next consume WILL see offset N                                   │
//   │                                                                          │
//   │  4b. Transaction ABORTS:                                                 │
//   │     └── ClearTransaction(txnId) removes all tracked offsets              │
//   │     └── Abort control record in log tells consumer to skip offset N      │
//   │                                                                          │
//   └──────────────────────────────────────────────────────────────────────────┘
//
// DATA STRUCTURE:
//
//   uncommittedByPartition: map[topic]map[partition]map[offset]*txnInfo
//   offsetsByTransaction:   map[txnId][]partitionOffset
//
//   This dual-indexed structure allows:
//     - O(1) lookup: "Is offset X uncommitted?"
//     - O(n) cleanup: "Remove all offsets for txnId Y" (n = offsets in txn)
//
// THREAD SAFETY:
//   All methods are thread-safe (protected by mutex).
//
// =============================================================================

package broker

import (
	"sync"
)

// =============================================================================
// TYPES
// =============================================================================

// partitionKey uniquely identifies a topic-partition combination.
type partitionKey struct {
	Topic     string
	Partition int
}

// txnInfo holds information about the transaction that owns an offset.
type txnInfo struct {
	TransactionId string
	ProducerId    int64
	Epoch         int16
}

// partitionOffset records an offset in a specific partition.
type partitionOffset struct {
	Topic     string
	Partition int
	Offset    int64
}

// =============================================================================
// UNCOMMITTED TRACKER
// =============================================================================

// UncommittedTracker tracks offsets belonging to uncommitted transactions.
//
// USE CASE: read_committed isolation
// Consumers should not see messages from uncommitted transactions.
// This tracker enables efficient filtering during consume operations.
type UncommittedTracker struct {
	// uncommitted maps topic -> partition -> offset -> txnInfo
	// Used for fast lookup: "Is this offset uncommitted?"
	uncommitted map[string]map[int]map[int64]*txnInfo

	// byTransaction maps txnId -> list of (topic, partition, offset)
	// Used for efficient cleanup when transaction commits/aborts
	byTransaction map[string][]partitionOffset

	mu sync.RWMutex
}

// NewUncommittedTracker creates a new tracker for uncommitted offsets.
func NewUncommittedTracker() *UncommittedTracker {
	return &UncommittedTracker{
		uncommitted:   make(map[string]map[int]map[int64]*txnInfo),
		byTransaction: make(map[string][]partitionOffset),
	}
}

// =============================================================================
// TRACKING OPERATIONS
// =============================================================================

// Track records an offset as belonging to an uncommitted transaction.
//
// PARAMETERS:
//   - topic: The topic name
//   - partition: The partition number
//   - offset: The offset that was written
//   - txnId: The transaction ID (for cleanup on commit/abort)
//   - producerId: The producer's ID (for debugging)
//   - epoch: The producer's epoch (for debugging)
//
// WHEN CALLED:
//
//	After PublishTransactional successfully writes a message to the log.
func (ut *UncommittedTracker) Track(topic string, partition int, offset int64, txnId string, producerId int64, epoch int16) {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	// Initialize nested maps if needed
	if ut.uncommitted[topic] == nil {
		ut.uncommitted[topic] = make(map[int]map[int64]*txnInfo)
	}
	if ut.uncommitted[topic][partition] == nil {
		ut.uncommitted[topic][partition] = make(map[int64]*txnInfo)
	}

	// Record the offset as uncommitted
	ut.uncommitted[topic][partition][offset] = &txnInfo{
		TransactionId: txnId,
		ProducerId:    producerId,
		Epoch:         epoch,
	}

	// Also track by transaction for cleanup
	ut.byTransaction[txnId] = append(ut.byTransaction[txnId], partitionOffset{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	})
}

// IsUncommitted checks if an offset belongs to an uncommitted transaction.
//
// PARAMETERS:
//   - topic: The topic name
//   - partition: The partition number
//   - offset: The offset to check
//
// RETURNS:
//
//	true if the offset is part of an uncommitted transaction (should be filtered)
//	false if the offset is committed or not part of any transaction (visible)
//
// WHEN CALLED:
//
//	During consume operations, for each message read from the log.
func (ut *UncommittedTracker) IsUncommitted(topic string, partition int, offset int64) bool {
	ut.mu.RLock()
	defer ut.mu.RUnlock()

	// Check if topic exists in tracker
	partitions, topicExists := ut.uncommitted[topic]
	if !topicExists {
		return false
	}

	// Check if partition exists in tracker
	offsets, partitionExists := partitions[partition]
	if !partitionExists {
		return false
	}

	// Check if this specific offset is tracked
	_, isUncommitted := offsets[offset]
	return isUncommitted
}

// =============================================================================
// CLEANUP OPERATIONS
// =============================================================================

// ClearTransaction removes all tracked offsets for a transaction.
//
// PARAMETERS:
//   - txnId: The transaction ID to clear
//
// RETURNS:
//
//	List of offsets that were cleared (for abort filtering)
//
// WHEN CALLED:
//   - When a transaction commits (offsets become visible)
//   - When a transaction aborts (offsets remain invisible via abort markers)
//
// WHY BOTH CASES CALL THIS:
//   - COMMIT: Offsets are now committed, should be visible → remove from tracker
//   - ABORT: Offsets will be filtered by abortedTracker → move offsets there
//
// The returned offsets can be passed to AbortedTracker.MarkAborted() for abort cases.
func (ut *UncommittedTracker) ClearTransaction(txnId string) []partitionOffset {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	// Get all offsets for this transaction
	offsets, exists := ut.byTransaction[txnId]
	if !exists {
		return nil
	}

	// Make a copy to return (before we delete from byTransaction)
	result := make([]partitionOffset, len(offsets))
	copy(result, offsets)

	// Remove each offset from the uncommitted map
	for _, po := range offsets {
		if partitions, ok := ut.uncommitted[po.Topic]; ok {
			if offsetMap, ok := partitions[po.Partition]; ok {
				delete(offsetMap, po.Offset)

				// Clean up empty maps to prevent memory growth
				if len(offsetMap) == 0 {
					delete(partitions, po.Partition)
				}
			}
			if len(partitions) == 0 {
				delete(ut.uncommitted, po.Topic)
			}
		}
	}

	// Remove the transaction tracking
	delete(ut.byTransaction, txnId)

	return result
}

// =============================================================================
// STATISTICS
// =============================================================================

// Stats returns statistics about the uncommitted tracker.
type UncommittedStats struct {
	// TotalUncommittedOffsets is the total count of uncommitted offsets
	TotalUncommittedOffsets int

	// ActiveTransactions is the number of transactions being tracked
	ActiveTransactions int

	// ByTopic maps topic name to count of uncommitted offsets
	ByTopic map[string]int
}

// Stats returns current tracker statistics.
func (ut *UncommittedTracker) Stats() UncommittedStats {
	ut.mu.RLock()
	defer ut.mu.RUnlock()

	stats := UncommittedStats{
		ActiveTransactions: len(ut.byTransaction),
		ByTopic:            make(map[string]int),
	}

	for topic, partitions := range ut.uncommitted {
		topicCount := 0
		for _, offsets := range partitions {
			topicCount += len(offsets)
		}
		stats.ByTopic[topic] = topicCount
		stats.TotalUncommittedOffsets += topicCount
	}

	return stats
}

// GetTransactionOffsets returns all offsets for a specific transaction.
// Useful for debugging and testing.
func (ut *UncommittedTracker) GetTransactionOffsets(txnId string) []partitionOffset {
	ut.mu.RLock()
	defer ut.mu.RUnlock()

	offsets := ut.byTransaction[txnId]
	if offsets == nil {
		return nil
	}

	// Return a copy to prevent external modification
	result := make([]partitionOffset, len(offsets))
	copy(result, offsets)
	return result
}

// =============================================================================
// ABORTED OFFSET TRACKER
// =============================================================================
//
// WHY A SEPARATE ABORTED TRACKER?
// When a transaction aborts, its messages should remain invisible to consumers
// FOREVER (or until compaction removes them). Unlike uncommitted messages
// (which become visible on commit), aborted messages stay hidden.
//
// DESIGN:
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │                    UNCOMMITTED vs ABORTED TRACKING                          │
// │                                                                             │
// │  UncommittedTracker:                                                        │
// │    - Tracks offsets from IN-PROGRESS transactions                           │
// │    - Cleared on COMMIT (offsets become visible)                             │
// │    - Cleared on ABORT (offsets moved to AbortedTracker)                     │
// │                                                                             │
// │  AbortedTracker:                                                            │
// │    - Tracks offsets from ABORTED transactions                               │
// │    - Never cleared (aborted messages stay invisible)                        │
// │    - Memory-efficient: stores only offset sets per partition                │
// │                                                                             │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// ALTERNATIVE APPROACHES:
//   1. Store abort info IN control records and scan on consume (Kafka's approach)
//      - Pro: No in-memory state
//      - Con: Complex, requires scanning backwards to find abort markers
//
//   2. Store aborted offsets in-memory (our approach)
//      - Pro: Simple, O(1) lookup
//      - Con: Memory usage grows with aborts (but aborts should be rare)
//
// TODO: Persistence: can be enhanced to persist aborted offsets:
//   3. Store aborted offsets in a persistent index
//      - Pro: Survives restarts
//      - Con: Added complexity
//
// We use approach 2 for simplicity. For production, we'd add persistence.
//
// =============================================================================

// AbortedTracker tracks offsets from aborted transactions.
// These offsets should remain invisible to consumers forever.
type AbortedTracker struct {
	// aborted maps topic -> partition -> set of aborted offsets
	aborted map[string]map[int]map[int64]struct{}
	mu      sync.RWMutex
}

// NewAbortedTracker creates a new tracker for aborted offsets.
func NewAbortedTracker() *AbortedTracker {
	return &AbortedTracker{
		aborted: make(map[string]map[int]map[int64]struct{}),
	}
}

// MarkAborted marks a set of offsets as belonging to an aborted transaction.
// These offsets will be filtered out during consume operations.
//
// PARAMETERS:
//   - offsets: List of partition offsets to mark as aborted
//
// WHEN CALLED:
//
//	When a transaction aborts, after clearing from UncommittedTracker.
func (at *AbortedTracker) MarkAborted(offsets []partitionOffset) {
	if len(offsets) == 0 {
		return
	}

	at.mu.Lock()
	defer at.mu.Unlock()

	for _, po := range offsets {
		// Initialize nested maps if needed
		if at.aborted[po.Topic] == nil {
			at.aborted[po.Topic] = make(map[int]map[int64]struct{})
		}
		if at.aborted[po.Topic][po.Partition] == nil {
			at.aborted[po.Topic][po.Partition] = make(map[int64]struct{})
		}

		// Mark offset as aborted
		at.aborted[po.Topic][po.Partition][po.Offset] = struct{}{}
	}
}

// IsAborted checks if an offset belongs to an aborted transaction.
//
// PARAMETERS:
//   - topic: The topic name
//   - partition: The partition number
//   - offset: The offset to check
//
// RETURNS:
//
//	true if the offset is from an aborted transaction (should be filtered)
//	false if the offset is not from an aborted transaction (visible)
func (at *AbortedTracker) IsAborted(topic string, partition int, offset int64) bool {
	at.mu.RLock()
	defer at.mu.RUnlock()

	partitions, topicExists := at.aborted[topic]
	if !topicExists {
		return false
	}

	offsets, partitionExists := partitions[partition]
	if !partitionExists {
		return false
	}

	_, isAborted := offsets[offset]
	return isAborted
}

// AbortedStats returns statistics about the aborted tracker.
type AbortedStats struct {
	// TotalAbortedOffsets is the total count of aborted offsets
	TotalAbortedOffsets int

	// ByTopic maps topic name to count of aborted offsets
	ByTopic map[string]int
}

// Stats returns current tracker statistics.
func (at *AbortedTracker) Stats() AbortedStats {
	at.mu.RLock()
	defer at.mu.RUnlock()

	stats := AbortedStats{
		ByTopic: make(map[string]int),
	}

	for topic, partitions := range at.aborted {
		topicCount := 0
		for _, offsets := range partitions {
			topicCount += len(offsets)
		}
		stats.ByTopic[topic] = topicCount
		stats.TotalAbortedOffsets += topicCount
	}

	return stats
}
