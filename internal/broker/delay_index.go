// =============================================================================
// DELAY INDEX - PERSISTENT STORAGE FOR DELAYED MESSAGES
// =============================================================================
//
// WHAT IS THE DELAY INDEX?
// The delay index tracks which messages have delayed delivery times. When a
// producer publishes a message with a delay, the message is:
//   1. Written to the main partition log (persistence)
//   2. Recorded in the delay index (tracking)
//   3. Added to the timer wheel (scheduling)
//
// WHY A SEPARATE INDEX?
//
//   OPTION A: Store delay in message format
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Pros: Single write path, all data in one place                          │
//   │ Cons: Must scan entire log on startup to find delayed messages          │
//   │       Changes message format (backward compatibility issues)            │
//   └───────────────────────────────────────────────────────────────────── ───┘
//
//   OPTION B: Separate delay index file (CHOSEN)
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Pros: Fast startup (just load index, not entire log)                    │
//   │       No message format changes                                         │
//   │       Can have different retention for index vs messages                │
//   │ Cons: Two write paths (log + index)                                     │
//   │       Index can get out of sync (need recovery logic)                   │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// INDEX FILE FORMAT:
//
//   Header (16 bytes):
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Magic (4B) │ Version (2B) │ Flags (2B) │ Count (8B)                     │
//   │ "GQDL"     │ 1            │ 0          │ Number of entries              │
//   └─────────────────────────────────────────────────────────────────────────┘
//
//   Entry (32 bytes each):
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Offset (8B) │ DeliverAt (8B) │ Partition (4B) │ State (4B) │ Reserved   │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// STATE VALUES:
//   0 = PENDING   - Waiting for delivery time
//   1 = DELIVERED - Has been delivered to timer wheel
//   2 = CANCELLED - User cancelled the delay
//   3 = EXPIRED   - Delivered and consumed
//
// RECOVERY ON STARTUP:
//   1. Load delay index file
//   2. For each PENDING entry:
//      - If DeliverAt is in the past → mark as DELIVERED, make visible
//      - If DeliverAt is in the future → add to timer wheel
//   3. Rebuild timer wheel from pending entries
//
// =============================================================================
// PERFORMANCE OPTIMIZATIONS (M15)
// =============================================================================
//
// TIME-BUCKETED INDEX (P1):
//   Problem: GetReadyEntries() was O(n) scanning all pending entries.
//   Solution: Time-bucketed map with 1-second granularity.
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ timeBuckets: map[int64][]*DelayEntry                                    │
//   │                                                                         │
//   │   bucket[1704067200] → [entry1, entry2, entry3]  (all due at same sec)  │
//   │   bucket[1704067201] → [entry4, entry5]                                 │
//   │   bucket[1704067205] → [entry6]                                         │
//   │                                                                         │
//   │ GetReadyEntries():                                                      │
//   │   1. Get current timestamp in seconds                                   │
//   │   2. Iterate buckets from earliest to current second                    │
//   │   3. Return all entries in those buckets                                │
//   │                                                                         │
//   │ Complexity: O(k) where k = number of due buckets, typically O(1)        │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// REVERSE INDEX FOR O(1) STATE UPDATES (P2):
//   Problem: updateState() was O(n) scanning file to find entry position.
//   Solution: Track file position in DelayEntry during Add().
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ When Add() writes entry to file:                                        │
//   │   1. Calculate file position: header + (entryCount * entrySize)         │
//   │   2. Store position in entry.FilePosition                               │
//   │   3. On updateState(), use entry.FilePosition directly → O(1)           │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// =============================================================================
// CONSTANTS
// =============================================================================

const (
	// Index file header
	delayIndexMagic   = "GQDL" // GoQueue Delay
	delayIndexVersion = 1
	delayIndexHeader  = 16 // bytes

	// Entry size
	delayEntrySize = 32 // bytes per entry

	// Entry states
	delayStatePending   = 0
	delayStateDelivered = 1
	delayStateCancelled = 2
	delayStateExpired   = 3
)

// =============================================================================
// ERRORS
// =============================================================================

var (
	// ErrDelayIndexCorrupt means the index file is corrupted
	ErrDelayIndexCorrupt = errors.New("delay index file is corrupted")

	// ErrDelayEntryNotFound means no entry exists for the given offset
	ErrDelayEntryNotFound = errors.New("delay entry not found")

	// ErrDelayIndexFull means maximum entries reached
	ErrDelayIndexFull = errors.New("delay index full: maximum entries reached")
)

// =============================================================================
// DELAY ENTRY
// =============================================================================

// DelayEntry represents a delayed message in the index.
type DelayEntry struct {
	// Offset is the message offset in the partition log
	Offset int64

	// DeliverAt is when the message should become visible (Unix nano)
	DeliverAt int64

	// Partition is which partition this message belongs to
	Partition int32

	// State tracks the entry lifecycle
	State int32

	// Topic is stored in memory only (derived from index file path)
	Topic string

	// FilePosition is the byte offset in the index file where this entry is stored.
	// Used for O(1) state updates instead of O(n) file scan.
	// This field is memory-only (not persisted, reconstructed on load).
	FilePosition int64
}

// IsReady returns true if the message should be delivered now.
func (e *DelayEntry) IsReady() bool {
	return time.Now().UnixNano() >= e.DeliverAt
}

// TimeUntilDelivery returns duration until message should be delivered.
func (e *DelayEntry) TimeUntilDelivery() time.Duration {
	remaining := e.DeliverAt - time.Now().UnixNano()
	if remaining < 0 {
		return 0
	}
	return time.Duration(remaining)
}

// =============================================================================
// DELAY INDEX
// =============================================================================

// DelayIndex manages persistent storage of delayed message metadata.
//
// THREAD SAFETY: All methods are safe for concurrent use.
//
// FILE LAYOUT:
//
//	data/
//	└── delays/
//	    └── {topic}/
//	        └── delay.index
type DelayIndex struct {
	// topic this index belongs to
	topic string

	// path to the index file
	path string

	// file handle for reading/writing
	file *os.File

	// entries maps offset to entry for fast lookup
	entries map[int64]*DelayEntry

	// timeBuckets maps Unix seconds to entries due at that time.
	// This provides O(1) lookup for GetReadyEntries() instead of O(n) scan.
	// Key: Unix timestamp in seconds (nanoseconds / 1e9)
	// Value: Slice of entries due in that second
	timeBuckets map[int64][]*DelayEntry

	// sortedBucketKeys maintains sorted bucket timestamps for efficient iteration.
	// Updated when buckets are added/removed.
	sortedBucketKeys []int64

	// entryCount tracks total entries (including expired/cancelled)
	entryCount int64

	// pendingCount tracks active pending entries
	pendingCount int64

	// maxEntries limits memory usage
	maxEntries int64

	// mu protects all state
	mu sync.RWMutex

	// dirty tracks if index needs flush
	dirty bool
}

// DelayIndexConfig holds configuration for delay index.
type DelayIndexConfig struct {
	// DataDir is the base directory for delay index files
	DataDir string

	// Topic name
	Topic string

	// MaxEntries limits the number of delayed messages per topic
	// Default: 1,000,000
	MaxEntries int64
}

// DefaultDelayIndexConfig returns default configuration.
func DefaultDelayIndexConfig(dataDir, topic string) DelayIndexConfig {
	return DelayIndexConfig{
		DataDir:    dataDir,
		Topic:      topic,
		MaxEntries: 1_000_000,
	}
}

// NewDelayIndex creates or opens a delay index for a topic.
func NewDelayIndex(config DelayIndexConfig) (*DelayIndex, error) {
	// Create directory structure
	dir := filepath.Join(config.DataDir, "delays", config.Topic)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create delay index directory: %w", err)
	}

	path := filepath.Join(dir, "delay.index")

	idx := &DelayIndex{
		topic:            config.Topic,
		path:             path,
		entries:          make(map[int64]*DelayEntry),
		timeBuckets:      make(map[int64][]*DelayEntry),
		sortedBucketKeys: make([]int64, 0),
		maxEntries:       config.MaxEntries,
	}

	// Open or create the index file
	if err := idx.openOrCreate(); err != nil {
		return nil, err
	}

	return idx, nil
}

// =============================================================================
// FILE OPERATIONS
// =============================================================================

// openOrCreate opens existing index or creates a new one.
func (idx *DelayIndex) openOrCreate() error {
	// Try to open existing file
	f, err := os.OpenFile(idx.path, os.O_RDWR, 0644)
	if err == nil {
		idx.file = f
		return idx.loadExisting()
	}

	if !os.IsNotExist(err) {
		return fmt.Errorf("failed to open delay index: %w", err)
	}

	// Create new file
	f, err = os.OpenFile(idx.path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to create delay index: %w", err)
	}

	idx.file = f
	return idx.writeHeader()
}

// writeHeader writes the index file header.
func (idx *DelayIndex) writeHeader() error {
	header := make([]byte, delayIndexHeader)

	// Magic bytes
	copy(header[0:4], delayIndexMagic)

	// Version
	binary.BigEndian.PutUint16(header[4:6], delayIndexVersion)

	// Flags (reserved)
	binary.BigEndian.PutUint16(header[6:8], 0)

	// Entry count
	binary.BigEndian.PutUint64(header[8:16], uint64(idx.entryCount))

	if _, err := idx.file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	return idx.file.Sync()
}

// loadExisting loads entries from an existing index file.
func (idx *DelayIndex) loadExisting() error {
	// Read header
	header := make([]byte, delayIndexHeader)
	if _, err := idx.file.ReadAt(header, 0); err != nil {
		if err == io.EOF {
			// Empty file, write header
			return idx.writeHeader()
		}
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Validate magic
	if string(header[0:4]) != delayIndexMagic {
		return ErrDelayIndexCorrupt
	}

	// Check version
	version := binary.BigEndian.Uint16(header[4:6])
	if version != delayIndexVersion {
		return fmt.Errorf("unsupported delay index version: %d", version)
	}

	// Read entry count
	count := binary.BigEndian.Uint64(header[8:16])
	idx.entryCount = int64(count)

	// Load all entries
	entryBuf := make([]byte, delayEntrySize)
	for i := int64(0); i < idx.entryCount; i++ {
		filePos := delayIndexHeader + (i * delayEntrySize)
		if _, err := idx.file.ReadAt(entryBuf, filePos); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read entry %d: %w", i, err)
		}

		entry := idx.decodeEntry(entryBuf)
		entry.Topic = idx.topic
		entry.FilePosition = filePos // Store file position for O(1) updates

		// Only track pending entries in memory
		if entry.State == delayStatePending {
			idx.entries[entry.Offset] = entry
			idx.pendingCount++

			// Add to time bucket for O(1) ready lookups
			idx.addToTimeBucketLocked(entry)
		}
	}

	return nil
}

// =============================================================================
// ENTRY ENCODING/DECODING
// =============================================================================

// encodeEntry converts an entry to bytes.
func (idx *DelayIndex) encodeEntry(entry *DelayEntry) []byte {
	buf := make([]byte, delayEntrySize)

	binary.BigEndian.PutUint64(buf[0:8], uint64(entry.Offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(entry.DeliverAt))
	binary.BigEndian.PutUint32(buf[16:20], uint32(entry.Partition))
	binary.BigEndian.PutUint32(buf[20:24], uint32(entry.State))
	// bytes 24-32 reserved

	return buf
}

// decodeEntry converts bytes to an entry.
func (idx *DelayIndex) decodeEntry(buf []byte) *DelayEntry {
	return &DelayEntry{
		Offset:    int64(binary.BigEndian.Uint64(buf[0:8])),
		DeliverAt: int64(binary.BigEndian.Uint64(buf[8:16])),
		Partition: int32(binary.BigEndian.Uint32(buf[16:20])),
		State:     int32(binary.BigEndian.Uint32(buf[20:24])),
	}
}

// =============================================================================
// PUBLIC API
// =============================================================================

// Add records a new delayed message.
//
// PARAMETERS:
//   - offset: Message offset in partition
//   - partition: Partition number
//   - deliverAt: When message should become visible
//
// RETURNS:
//   - Error if index is full or write fails
func (idx *DelayIndex) Add(offset int64, partition int, deliverAt time.Time) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.pendingCount >= idx.maxEntries {
		return ErrDelayIndexFull
	}

	// Calculate file position BEFORE incrementing entryCount
	filePos := delayIndexHeader + (idx.entryCount * delayEntrySize)

	entry := &DelayEntry{
		Offset:       offset,
		DeliverAt:    deliverAt.UnixNano(),
		Partition:    int32(partition),
		State:        delayStatePending,
		Topic:        idx.topic,
		FilePosition: filePos, // Store for O(1) state updates
	}

	// Write entry to file
	if _, err := idx.file.WriteAt(idx.encodeEntry(entry), filePos); err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	// Update header with new count
	idx.entryCount++
	idx.pendingCount++
	idx.entries[offset] = entry
	idx.dirty = true

	// Add to time bucket for O(1) ready lookups
	idx.addToTimeBucketLocked(entry)

	// Update count in header
	countBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(countBuf, uint64(idx.entryCount))
	if _, err := idx.file.WriteAt(countBuf, 8); err != nil {
		return fmt.Errorf("failed to update header: %w", err)
	}

	return nil
}

// Get retrieves a delay entry by offset.
func (idx *DelayIndex) Get(offset int64) (*DelayEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entry, exists := idx.entries[offset]
	if !exists {
		return nil, ErrDelayEntryNotFound
	}

	// Return a copy
	return &DelayEntry{
		Offset:       entry.Offset,
		DeliverAt:    entry.DeliverAt,
		Partition:    entry.Partition,
		State:        entry.State,
		Topic:        entry.Topic,
		FilePosition: entry.FilePosition,
	}, nil
}

// MarkDelivered updates entry state to delivered.
func (idx *DelayIndex) MarkDelivered(offset int64) error {
	return idx.updateState(offset, delayStateDelivered)
}

// MarkCancelled updates entry state to cancelled.
func (idx *DelayIndex) MarkCancelled(offset int64) error {
	return idx.updateState(offset, delayStateCancelled)
}

// MarkExpired updates entry state to expired.
func (idx *DelayIndex) MarkExpired(offset int64) error {
	return idx.updateState(offset, delayStateExpired)
}

// updateState changes an entry's state.
// Uses FilePosition for O(1) file updates instead of O(n) scan.
func (idx *DelayIndex) updateState(offset int64, newState int32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	entry, exists := idx.entries[offset]
	if !exists {
		return ErrDelayEntryNotFound
	}

	// Use the stored file position for O(1) update
	// Read current entry from file
	entryBuf := make([]byte, delayEntrySize)
	if _, err := idx.file.ReadAt(entryBuf, entry.FilePosition); err != nil {
		return fmt.Errorf("failed to read entry: %w", err)
	}

	// Verify we're updating the correct entry (sanity check)
	storedOffset := int64(binary.BigEndian.Uint64(entryBuf[0:8]))
	if storedOffset != offset {
		// Fallback to linear scan if position is stale (shouldn't happen)
		return idx.updateStateLinearScan(offset, newState)
	}

	// Update state in buffer and write back
	binary.BigEndian.PutUint32(entryBuf[20:24], uint32(newState))
	if _, err := idx.file.WriteAt(entryBuf, entry.FilePosition); err != nil {
		return fmt.Errorf("failed to update entry state: %w", err)
	}

	// Update in-memory entry
	oldDeliverAt := entry.DeliverAt
	entry.State = newState

	// Remove from pending tracking and time bucket if no longer pending
	if newState != delayStatePending {
		delete(idx.entries, offset)
		idx.pendingCount--
		idx.removeFromTimeBucketLocked(oldDeliverAt, offset)
	}

	return nil
}

// updateStateLinearScan is a fallback for when FilePosition is invalid.
// This should rarely happen but provides safety.
func (idx *DelayIndex) updateStateLinearScan(offset int64, newState int32) error {
	entryBuf := make([]byte, delayEntrySize)
	for i := int64(0); i < idx.entryCount; i++ {
		pos := delayIndexHeader + (i * delayEntrySize)
		if _, err := idx.file.ReadAt(entryBuf, pos); err != nil {
			continue
		}

		storedOffset := int64(binary.BigEndian.Uint64(entryBuf[0:8]))
		if storedOffset == offset {
			// Update state in buffer and write back
			binary.BigEndian.PutUint32(entryBuf[20:24], uint32(newState))
			if _, err := idx.file.WriteAt(entryBuf, pos); err != nil {
				return fmt.Errorf("failed to update entry state: %w", err)
			}

			// Update in-memory entry
			if entry, exists := idx.entries[offset]; exists {
				oldDeliverAt := entry.DeliverAt
				entry.State = newState
				entry.FilePosition = pos // Update file position

				if newState != delayStatePending {
					delete(idx.entries, offset)
					idx.pendingCount--
					idx.removeFromTimeBucketLocked(oldDeliverAt, offset)
				}
			}
			return nil
		}
	}
	return ErrDelayEntryNotFound
}

// GetPendingEntries returns all entries waiting for delivery.
func (idx *DelayIndex) GetPendingEntries() []*DelayEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entries := make([]*DelayEntry, 0, len(idx.entries))
	for _, entry := range idx.entries {
		entries = append(entries, &DelayEntry{
			Offset:       entry.Offset,
			DeliverAt:    entry.DeliverAt,
			Partition:    entry.Partition,
			State:        entry.State,
			Topic:        entry.Topic,
			FilePosition: entry.FilePosition,
		})
	}

	return entries
}

// GetPendingEntriesPaginated returns a paginated slice of pending entries.
// This avoids loading all entries into memory at once for large indexes.
//
// PARAMETERS:
//   - limit: Maximum number of entries to return (0 = no limit)
//   - skip: Number of entries to skip
//
// RETURNS:
//   - Slice of entries (up to limit), total pending count
func (idx *DelayIndex) GetPendingEntriesPaginated(limit, skip int) ([]*DelayEntry, int64) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	total := idx.pendingCount

	// Collect all entries first (needed for consistent pagination)
	allEntries := make([]*DelayEntry, 0, len(idx.entries))
	for _, entry := range idx.entries {
		allEntries = append(allEntries, entry)
	}

	// Apply skip
	if skip >= len(allEntries) {
		return []*DelayEntry{}, total
	}
	allEntries = allEntries[skip:]

	// Apply limit
	if limit > 0 && limit < len(allEntries) {
		allEntries = allEntries[:limit]
	}

	// Return copies
	result := make([]*DelayEntry, len(allEntries))
	for i, entry := range allEntries {
		result[i] = &DelayEntry{
			Offset:       entry.Offset,
			DeliverAt:    entry.DeliverAt,
			Partition:    entry.Partition,
			State:        entry.State,
			Topic:        entry.Topic,
			FilePosition: entry.FilePosition,
		}
	}

	return result, total
}

// GetReadyEntries returns entries that are ready for delivery.
// Uses time-bucketed index for O(k) lookup where k = number of due buckets.
// This is a significant improvement over the previous O(n) scan.
func (idx *DelayIndex) GetReadyEntries() []*DelayEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	now := time.Now().UnixNano()
	nowSec := now / int64(time.Second) // Current time in seconds

	entries := make([]*DelayEntry, 0)

	// Iterate through sorted bucket keys up to current time
	for _, bucketKey := range idx.sortedBucketKeys {
		if bucketKey > nowSec {
			// All remaining buckets are in the future
			break
		}

		// Get all entries in this bucket
		bucket := idx.timeBuckets[bucketKey]
		for _, entry := range bucket {
			// Double-check the entry is actually ready (handles sub-second precision)
			if entry.DeliverAt <= now && entry.State == delayStatePending {
				entries = append(entries, &DelayEntry{
					Offset:       entry.Offset,
					DeliverAt:    entry.DeliverAt,
					Partition:    entry.Partition,
					State:        entry.State,
					Topic:        entry.Topic,
					FilePosition: entry.FilePosition,
				})
			}
		}
	}

	return entries
}

// =============================================================================
// TIME BUCKET MANAGEMENT
// =============================================================================
//
// TIME BUCKETS PROVIDE O(1) READY LOOKUPS:
//   Instead of scanning all N pending entries to find ready ones,
//   we group entries by their delivery second. GetReadyEntries() only
//   needs to check buckets <= current time.
//
// BUCKET KEY: Unix timestamp in seconds (DeliverAt / 1e9)
//
// EXAMPLE:
//   Entry with DeliverAt = 1704067200_500000000 (ns) → bucket 1704067200
//   Entry with DeliverAt = 1704067200_900000000 (ns) → bucket 1704067200 (same)
//   Entry with DeliverAt = 1704067201_100000000 (ns) → bucket 1704067201
//
// =============================================================================

// addToTimeBucketLocked adds an entry to its time bucket.
// Must be called with idx.mu held.
func (idx *DelayIndex) addToTimeBucketLocked(entry *DelayEntry) {
	bucketKey := entry.DeliverAt / int64(time.Second)

	// Add to bucket
	if idx.timeBuckets[bucketKey] == nil {
		idx.timeBuckets[bucketKey] = make([]*DelayEntry, 0, 8)
		// Add to sorted keys and re-sort
		idx.sortedBucketKeys = append(idx.sortedBucketKeys, bucketKey)
		sort.Slice(idx.sortedBucketKeys, func(i, j int) bool {
			return idx.sortedBucketKeys[i] < idx.sortedBucketKeys[j]
		})
	}
	idx.timeBuckets[bucketKey] = append(idx.timeBuckets[bucketKey], entry)
}

// removeFromTimeBucketLocked removes an entry from its time bucket.
// Must be called with idx.mu held.
func (idx *DelayIndex) removeFromTimeBucketLocked(deliverAt int64, offset int64) {
	bucketKey := deliverAt / int64(time.Second)

	bucket := idx.timeBuckets[bucketKey]
	if bucket == nil {
		return
	}

	// Find and remove the entry
	for i, entry := range bucket {
		if entry.Offset == offset {
			// Remove by swapping with last element
			bucket[i] = bucket[len(bucket)-1]
			bucket = bucket[:len(bucket)-1]
			idx.timeBuckets[bucketKey] = bucket
			break
		}
	}

	// If bucket is empty, remove it from the map and sorted keys
	if len(idx.timeBuckets[bucketKey]) == 0 {
		delete(idx.timeBuckets, bucketKey)
		idx.removeBucketKeyLocked(bucketKey)
	}
}

// removeBucketKeyLocked removes a key from sortedBucketKeys.
// Must be called with idx.mu held.
func (idx *DelayIndex) removeBucketKeyLocked(key int64) {
	for i, k := range idx.sortedBucketKeys {
		if k == key {
			idx.sortedBucketKeys = append(idx.sortedBucketKeys[:i], idx.sortedBucketKeys[i+1:]...)
			return
		}
	}
}

// Stats returns delay index statistics.
type DelayIndexStats struct {
	Topic           string
	TotalEntries    int64
	PendingCount    int64
	FilePath        string
	TimeBucketCount int   // Number of time buckets (for debugging)
	EarliestDueTime int64 // Unix seconds of earliest bucket (0 if none)
}

func (idx *DelayIndex) Stats() DelayIndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var earliestDue int64
	if len(idx.sortedBucketKeys) > 0 {
		earliestDue = idx.sortedBucketKeys[0]
	}

	return DelayIndexStats{
		Topic:           idx.topic,
		TotalEntries:    idx.entryCount,
		PendingCount:    idx.pendingCount,
		FilePath:        idx.path,
		TimeBucketCount: len(idx.timeBuckets),
		EarliestDueTime: earliestDue,
	}
}

// PendingCount returns number of pending delayed messages.
func (idx *DelayIndex) PendingCount() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.pendingCount
}

// Sync flushes pending writes to disk.
func (idx *DelayIndex) Sync() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.dirty {
		return nil
	}

	if err := idx.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync delay index: %w", err)
	}

	idx.dirty = false
	return nil
}

// Close closes the delay index file.
func (idx *DelayIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.file == nil {
		return nil
	}

	if err := idx.file.Sync(); err != nil {
		return err
	}

	return idx.file.Close()
}
