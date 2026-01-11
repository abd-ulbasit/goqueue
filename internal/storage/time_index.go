// =============================================================================
// TIME INDEX - TIMESTAMP TO OFFSET LOOKUP
// =============================================================================
//
// WHAT IS A TIME INDEX?
// The time index maps timestamps to offsets, enabling queries like:
//   - "Give me all messages from yesterday 3pm"
//   - "Reset consumer to start of today"
//   - "Replay messages in time range [start, end]"
//
// WHY DO WE NEED THIS?
// Without a time index, finding messages by timestamp requires:
//   1. Scan from segment 0
//   2. Read every message, check timestamp
//   3. Find first message >= target timestamp
//   4. This could take MINUTES for a large topic!
//
// With a time index:
//   1. Binary search time index entries
//   2. Find entry <= target timestamp → get offset
//   3. Use regular offset index: offset → position
//   4. Instant! O(log n) instead of O(n)
//
// TIME INDEX FILE FORMAT:
//
// ┌────────────────────────────────────────────────────────────────────────────┐
// │                            TIME INDEX FILE                                 │
// ├────────────────────────────────────────────────────────────────────────────┤
// │  Entry 0: [ Timestamp (8B) | Offset (8B) ]  ← 16 bytes per entry           │
// │  Entry 1: [ Timestamp (8B) | Offset (8B) ]                                 │
// │  Entry 2: [ Timestamp (8B) | Offset (8B) ]                                 │
// │  ...                                                                       │
// │  Entry N: [ Timestamp (8B) | Offset (8B) ]                                 │
// └────────────────────────────────────────────────────────────────────────────┘
//
// INDEX GRANULARITY:
// We add a time index entry at the same rate as the offset index (every 4KB).
// This keeps implementation consistent and provides good lookup accuracy.
//
// For a 64MB segment with 4KB granularity:
//   - 64MB ÷ 4KB = 16,384 entries
//   - 16,384 × 16 bytes = 256KB index file
//   - Same overhead as offset index
//
// LOOKUP ALGORITHM:
//   1. Binary search for largest timestamp <= target
//   2. Get the offset from that entry
//   3. Use offset index to find byte position
//   4. Scan forward to find exact message
//
// COMPARISON - How other systems handle time-based queries:
//   - Kafka: .timeindex files with same approach
//   - RabbitMQ: No native time indexing (messages are transient)
//   - SQS: No time-based queries (visibility-based model)
//   - Elasticsearch: Inverted index on timestamp field
//
// USE CASES:
//   - "Replay from 2 hours ago" (debugging)
//   - "Reset consumer to yesterday midnight"
//   - "Get messages in time range [start, end]"
//   - "Audit: what happened between 2pm-3pm?"
//   - "Data recovery: restore from specific point in time"
//
// =============================================================================

package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

// =============================================================================
// CONSTANTS
// =============================================================================

const (
	// TimeIndexEntrySize is the size of one time index entry in bytes.
	// Timestamp (8 bytes) + Offset (8 bytes) = 16 bytes
	TimeIndexEntrySize = 16

	// TimeIndexGranularity matches the offset index granularity.
	// We add a time index entry every 4KB of log data.
	// This provides consistent behavior and good lookup accuracy.
	TimeIndexGranularity = IndexGranularity // 4KB, same as offset index
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrTimestampNotFound means no entry exists for the given timestamp.
	ErrTimestampNotFound = errors.New("timestamp not found in time index")

	// ErrTimeIndexCorrupted means the time index file is damaged.
	ErrTimeIndexCorrupted = errors.New("time index file corrupted")

	// ErrTimestampOutOfRange means the timestamp is outside the indexed range.
	ErrTimestampOutOfRange = errors.New("timestamp out of indexed range")
)

// =============================================================================
// TIME INDEX ENTRY
// =============================================================================

// TimeIndexEntry maps a timestamp to an offset.
//
// EXAMPLE:
//
//	TimeIndexEntry{Timestamp: 1704067200000, Offset: 5000}
//	means "at timestamp 1704067200000ms, offset 5000 was being written"
//
// NOTE: Timestamps are in milliseconds since Unix epoch.
// This matches Kafka's timestamp format and provides millisecond precision.
type TimeIndexEntry struct {
	// Timestamp is the message timestamp in milliseconds since Unix epoch.
	Timestamp int64

	// Offset is the message offset at or around this timestamp.
	Offset int64
}

// =============================================================================
// TIME INDEX STRUCT
// =============================================================================

// TimeIndex provides fast timestamp-to-offset lookup for a segment file.
//
// DESIGN DECISIONS:
//   - Entries kept in memory for fast binary search (same as offset index)
//   - Written to disk for durability
//   - Same granularity as offset index (4KB) for consistency
//
// THREAD SAFETY:
//   - Uses RWMutex: multiple readers, single writer
//   - Reads (lookup) don't block each other
//   - Writes (append) block everything
//
// INVARIANT:
//   - Entries are sorted by timestamp (and offset, since they correlate)
//   - Timestamps should be monotonically increasing within a segment
//   - If out-of-order timestamps occur, we still add entries (consumer's problem)
type TimeIndex struct {
	// entries holds all time index entries, sorted by timestamp.
	entries []TimeIndexEntry

	// file is the backing file for persistence.
	file *os.File

	// mu protects concurrent access.
	mu sync.RWMutex

	// lastLogPosition tracks how much log data we've indexed.
	// New entry is added when (log position - last position) >= TimeIndexGranularity.
	lastLogPosition int64

	// lastTimestamp tracks the last indexed timestamp.
	// Used for detecting out-of-order timestamps.
	lastTimestamp int64

	// baseOffset is the first offset in this segment.
	baseOffset int64
}

// =============================================================================
// TIME INDEX CREATION & LOADING
// =============================================================================

// TimeIndexFileName generates the time index filename for a segment.
// Format: 20-digit zero-padded offset + ".timeindex"
func TimeIndexFileName(baseOffset int64) string {
	return fmt.Sprintf("%020d.timeindex", baseOffset)
}

// NewTimeIndex creates a new empty time index for a segment.
//
// PARAMETERS:
//   - path: Where to store the time index file (e.g., "00000000000000001000.timeindex")
//   - baseOffset: The first offset that will be in this segment
//
// The time index file is created immediately (empty) for durability.
func NewTimeIndex(path string, baseOffset int64) (*TimeIndex, error) {
	// O_RDWR: read and write
	// O_CREATE: create if doesn't exist
	// O_APPEND: all writes go to end
	// 0644: owner can read/write, others can read
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create time index file: %w", err)
	}

	return &TimeIndex{
		entries:         make([]TimeIndexEntry, 0, 1024), // Pre-allocate for ~1024 entries
		file:            file,
		lastLogPosition: 0,
		lastTimestamp:   0,
		baseOffset:      baseOffset,
	}, nil
}

// LoadTimeIndex opens an existing time index file and loads entries into memory.
//
// RECOVERY FLOW:
//  1. Open the time index file
//  2. Read all entries into memory
//  3. Validate entry ordering (should be sorted by timestamp)
//  4. Set lastTimestamp from last entry
//
// WHY LOAD ALL INTO MEMORY?
//   - Time index is small (256KB for 64MB segment)
//   - Binary search in memory is much faster than disk
//   - Simplifies the code (no disk I/O during lookup)
func LoadTimeIndex(path string, baseOffset int64) (*TimeIndex, error) {
	// Open for reading and appending
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open time index file: %w", err)
	}

	// Get file size to know how many entries to expect
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat time index file: %w", err)
	}

	fileSize := stat.Size()

	// Validate file size is multiple of entry size
	if fileSize%TimeIndexEntrySize != 0 {
		file.Close()
		return nil, fmt.Errorf("%w: file size %d is not multiple of %d",
			ErrTimeIndexCorrupted, fileSize, TimeIndexEntrySize)
	}

	// Calculate number of entries
	numEntries := fileSize / TimeIndexEntrySize

	// Pre-allocate entries slice
	entries := make([]TimeIndexEntry, 0, numEntries)

	// Read all entries
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	buf := make([]byte, TimeIndexEntrySize)
	var lastTimestamp int64 = 0
	var lastLogPosition int64 = 0

	for i := int64(0); i < numEntries; i++ {
		_, err := io.ReadFull(file, buf)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to read entry %d: %w", i, err)
		}

		entry := TimeIndexEntry{
			Timestamp: int64(binary.BigEndian.Uint64(buf[0:8])),
			Offset:    int64(binary.BigEndian.Uint64(buf[8:16])),
		}

		entries = append(entries, entry)
		lastTimestamp = entry.Timestamp
	}

	// Estimate lastLogPosition based on number of entries
	if numEntries > 0 {
		lastLogPosition = numEntries * TimeIndexGranularity
	}

	return &TimeIndex{
		entries:         entries,
		file:            file,
		lastLogPosition: lastLogPosition,
		lastTimestamp:   lastTimestamp,
		baseOffset:      baseOffset,
	}, nil
}

// =============================================================================
// TIME INDEX OPERATIONS
// =============================================================================

// MaybeAppend adds a time index entry if enough log data has been written.
//
// WHEN TO ADD AN ENTRY:
// We add an entry every TimeIndexGranularity (4KB) of log data.
// This is checked by comparing currentLogPosition with lastLogPosition.
//
// PARAMETERS:
//   - timestamp: Message timestamp in milliseconds since Unix epoch
//   - offset: Message offset
//   - currentLogPosition: Current byte position in the log file
//
// RETURNS:
//   - true if an entry was added
//   - false if we haven't crossed the granularity threshold
func (ti *TimeIndex) MaybeAppend(timestamp int64, offset int64, currentLogPosition int64) (bool, error) {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	// ============================================================================
	// FIRST-ENTRY GUARANTEE
	// ============================================================================
	//
	// WHY:
	// Segment-level helpers like Segment.GetFirstTimestamp()/GetLastTimestamp()
	// rely on the time index having at least one entry.
	//
	// With pure "every 4KB" granularity, the first message in a segment would
	// never be indexed because its start position is 0 (0 - 0 < 4KB).
	// That makes time-based APIs much less useful on small segments and breaks
	// corruption recovery rebuilds (first timestamp can't be reconstructed).
	//
	// FIX:
	// Always write the first time index entry, then apply the granularity rule.
	// This mirrors our offset index behavior (first message is always indexed).
	// ============================================================================
	if len(ti.entries) > 0 {
		// Check if we've written enough log data since last entry.
		if currentLogPosition-ti.lastLogPosition < TimeIndexGranularity {
			return false, nil
		}
	}

	// Create new entry
	entry := TimeIndexEntry{
		Timestamp: timestamp,
		Offset:    offset,
	}

	// Write to file
	buf := make([]byte, TimeIndexEntrySize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(timestamp))
	binary.BigEndian.PutUint64(buf[8:16], uint64(offset))

	_, err := ti.file.Write(buf)
	if err != nil {
		return false, fmt.Errorf("failed to write time index entry: %w", err)
	}

	// Add to in-memory entries
	ti.entries = append(ti.entries, entry)

	// Update tracking
	ti.lastLogPosition = currentLogPosition
	ti.lastTimestamp = timestamp

	return true, nil
}

// Lookup finds the offset for a given timestamp.
//
// ALGORITHM:
//  1. Binary search for largest timestamp <= target
//  2. Return the corresponding offset
//
// USE CASE:
//
//	offset, err := timeIndex.Lookup(1704067200000) // Find offset for specific time
//	// Then use offset index to find byte position
//
// RETURNS:
//   - offset: The offset at or before the given timestamp
//   - error: ErrTimestampNotFound if no entry exists, or timestamp is before first entry
func (ti *TimeIndex) Lookup(timestamp int64) (int64, error) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	if len(ti.entries) == 0 {
		return 0, ErrTimestampNotFound
	}

	// If timestamp is before our first entry, return first offset
	if timestamp < ti.entries[0].Timestamp {
		// Could be valid - caller might want to start from beginning
		return ti.entries[0].Offset, nil
	}

	// Binary search for largest timestamp <= target
	// sort.Search returns the smallest index i where f(i) is true
	// We want the largest timestamp <= target, so we search for timestamp > target
	// and then take the previous index
	idx := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].Timestamp > timestamp
	})

	// idx is now the first entry > timestamp
	// We want the entry at idx-1 (last entry <= timestamp)
	if idx == 0 {
		// All entries are > timestamp, return first entry
		return ti.entries[0].Offset, nil
	}

	return ti.entries[idx-1].Offset, nil
}

// LookupRange finds offsets for a time range [startTime, endTime].
//
// USE CASE:
//
//	startOffset, endOffset, err := timeIndex.LookupRange(start, end)
//	// Then fetch messages from startOffset to endOffset
//
// RETURNS:
//   - startOffset: Offset at or before startTime
//   - endOffset: Offset at or after endTime (or -1 if beyond indexed range)
//   - error: Any lookup errors
func (ti *TimeIndex) LookupRange(startTime, endTime int64) (startOffset, endOffset int64, err error) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	if len(ti.entries) == 0 {
		return 0, 0, ErrTimestampNotFound
	}

	// Find start offset (largest timestamp <= startTime)
	startIdx := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].Timestamp > startTime
	})
	if startIdx > 0 {
		startIdx--
	}
	startOffset = ti.entries[startIdx].Offset

	// Find end offset (smallest timestamp >= endTime)
	endIdx := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].Timestamp >= endTime
	})

	if endIdx >= len(ti.entries) {
		// endTime is beyond our indexed range
		// Return -1 to indicate "read to end"
		endOffset = -1
	} else {
		endOffset = ti.entries[endIdx].Offset
	}

	return startOffset, endOffset, nil
}

// GetFirstTimestamp returns the timestamp of the first entry.
// Useful for determining the time range covered by this segment.
func (ti *TimeIndex) GetFirstTimestamp() (int64, error) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	if len(ti.entries) == 0 {
		return 0, ErrTimestampNotFound
	}

	return ti.entries[0].Timestamp, nil
}

// GetLastTimestamp returns the timestamp of the last entry.
// Useful for determining the time range covered by this segment.
func (ti *TimeIndex) GetLastTimestamp() (int64, error) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	if len(ti.entries) == 0 {
		return 0, ErrTimestampNotFound
	}

	return ti.entries[len(ti.entries)-1].Timestamp, nil
}

// GetEntryCount returns the number of entries in the time index.
func (ti *TimeIndex) GetEntryCount() int {
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	return len(ti.entries)
}

// Sync flushes the time index to disk.
func (ti *TimeIndex) Sync() error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	return ti.file.Sync()
}

// Close closes the time index file.
func (ti *TimeIndex) Close() error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	if ti.file != nil {
		return ti.file.Close()
	}
	return nil
}

// Truncate removes all entries after the given offset.
// Used during recovery to remove entries for messages that were truncated.
func (ti *TimeIndex) Truncate(afterOffset int64) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	// Find first entry with offset > afterOffset
	truncateIdx := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].Offset > afterOffset
	})

	if truncateIdx >= len(ti.entries) {
		// Nothing to truncate
		return nil
	}

	// Truncate in-memory entries
	ti.entries = ti.entries[:truncateIdx]

	// Truncate file
	newSize := int64(truncateIdx) * TimeIndexEntrySize
	if err := ti.file.Truncate(newSize); err != nil {
		return fmt.Errorf("failed to truncate time index file: %w", err)
	}

	// Seek to end for future appends
	if _, err := ti.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to end after truncate: %w", err)
	}

	// Update lastTimestamp
	if len(ti.entries) > 0 {
		ti.lastTimestamp = ti.entries[len(ti.entries)-1].Timestamp
	} else {
		ti.lastTimestamp = 0
	}

	return nil
}

// =============================================================================
// TIME INDEX REBUILD
// =============================================================================

// RebuildTimeIndex creates a new time index by scanning the log file.
// Used when the time index file is corrupted or missing.
//
// PARAMETERS:
//   - logPath: Path to the log file
//   - indexPath: Path where time index should be created
//   - baseOffset: First offset in the segment
//
// This is an expensive operation (requires full log scan) but ensures correctness.
func RebuildTimeIndex(logPath, indexPath string, baseOffset int64) (*TimeIndex, error) {
	// Delete existing corrupt index if present
	_ = os.Remove(indexPath)

	// Create new index
	ti, err := NewTimeIndex(indexPath, baseOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to create time index: %w", err)
	}

	// Open log file for scanning
	logFile, err := os.Open(logPath)
	if err != nil {
		ti.Close()
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	defer logFile.Close()

	// Scan log file and extract timestamps.
	//
	// IMPORTANT:
	// Our on-disk message header is HeaderSize (34 bytes), not 32.
	// Rebuild must parse the *full* header and include HeadersLen when advancing,
	// otherwise we desync and read garbage offsets/timestamps.
	var position int64 = 0
	headerBuf := make([]byte, HeaderSize)

	for {
		// Seek to the current message start.
		if _, err := logFile.Seek(position, io.SeekStart); err != nil {
			break
		}

		// Read full header.
		_, err := io.ReadFull(logFile, headerBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			ti.Close()
			return nil, fmt.Errorf("failed to read log header: %w", err)
		}

		// Parse header to get offset and timestamp
		// Header format:
		//   Magic(2) + Version(1) + Flags(1) + CRC(4) + Offset(8) + Timestamp(8)
		//   + Priority(1) + Reserved(1) + KeyLen(2) + ValueLen(4) + HeadersLen(2)
		offset := int64(binary.BigEndian.Uint64(headerBuf[8:16]))
		timestamp := int64(binary.BigEndian.Uint64(headerBuf[16:24]))

		keyLen := int(binary.BigEndian.Uint16(headerBuf[26:28]))
		valueLen := int(binary.BigEndian.Uint32(headerBuf[28:32]))
		headersLen := int(binary.BigEndian.Uint16(headerBuf[32:34]))

		// Maybe add time index entry
		_, _ = ti.MaybeAppend(timestamp, offset, position)

		// Advance to next message.
		messageSize := HeaderSize + keyLen + valueLen + headersLen
		position += int64(messageSize)
	}

	return ti, nil
}
