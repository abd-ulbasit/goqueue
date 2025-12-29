// =============================================================================
// INDEX FILE - FAST OFFSET LOOKUP
// =============================================================================
//
// THE PROBLEM:
// When a consumer says "give me messages starting from offset 12345", how do we
// find where offset 12345 is located in a multi-gigabyte log file?
//
// NAIVE APPROACH: Scan from the beginning
//   - Start at byte 0, read message headers, count offsets until we find 12345
//   - Problem: With 1 million messages, this takes forever
//
// BETTER APPROACH: Sparse Index
//   - Keep a small index file mapping offset → byte position
//   - But storing EVERY offset wastes space (8 bytes offset + 8 bytes position = 16 bytes per message)
//   - For 1 billion messages, index alone would be 16GB!
//
// OUR APPROACH: Sparse Index with 4KB Granularity
//   - Add an index entry every 4KB of log data
//   - 64MB segment ÷ 4KB = 16,384 index entries per segment
//   - 16,384 × 16 bytes = 256KB index file per 64MB segment (0.4% overhead)
//   - To find offset X: binary search index, then scan at most 4KB of log data
//
// COMPARISON - How other systems do indexing:
//   - Kafka: Similar sparse index, configurable interval (default 4KB)
//   - RocksDB: Two-level index (index of index blocks)
//   - B-trees: More complex, but O(log n) for any lookup
//   - We chose Kafka's approach: simple, proven, good enough
//
// INDEX ENTRY FORMAT:
// ┌────────────────────────────────────────┐
// │ Offset (8 bytes) │ Position (8 bytes) │
// └────────────────────────────────────────┘
//
// LOOKUP ALGORITHM:
//   1. Binary search index for largest offset ≤ target
//   2. Seek to that position in log file
//   3. Scan forward reading message headers until we find target offset
//   4. Max scan distance = index granularity (4KB)
//
// EXAMPLE:
//   Index entries: [(0, 0), (100, 4096), (200, 8192), (300, 12288)]
//   Find offset 250:
//     1. Binary search → entry (200, 8192)
//     2. Seek to position 8192 in log
//     3. Scan forward until we find offset 250
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
	// IndexEntrySize is the size of one index entry in bytes
	// Offset (8 bytes) + Position (8 bytes) = 16 bytes
	IndexEntrySize = 16

	// IndexGranularity is how often we add an index entry (every 4KB of log data)
	// This is a balance between:
	//   - Smaller value = faster lookups, but larger index file
	//   - Larger value = smaller index file, but slower lookups (more scanning)
	// 4KB means we scan at most 4KB to find a message after binary search
	IndexGranularity = 4 * 1024 // 4KB
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrOffsetNotFound means the requested offset is not in this segment
	ErrOffsetNotFound = errors.New("offset not found in index")

	// ErrIndexCorrupted means the index file is damaged
	ErrIndexCorrupted = errors.New("index file corrupted")
)

// =============================================================================
// INDEX ENTRY
// =============================================================================

// IndexEntry maps an offset to its byte position in the log file.
//
// EXAMPLE:
//
//	IndexEntry{Offset: 1000, Position: 45678}
//	means "offset 1000 starts at byte 45678 in the log file"
type IndexEntry struct {
	Offset   int64 // Logical offset (message sequence number)
	Position int64 // Byte position in the log file
}

// =============================================================================
// INDEX STRUCT
// =============================================================================

// Index provides fast offset-to-position lookup for a segment file.
//
// DESIGN DECISIONS:
//   - We keep all entries in memory for fast binary search
//   - For a 64MB segment with 4KB granularity, this is only 256KB
//   - Entries are written to disk for durability (recoverable after restart)
//
// THREAD SAFETY:
//   - Uses RWMutex: multiple readers, single writer
//   - Reads (lookup) don't block each other
//   - Writes (append) block everything
//
// WHY NOT MEMORY-MAP THE INDEX?
//   - Index is small (256KB per 64MB segment)
//   - Keeping it in memory is simpler
//   - mmap adds complexity (page faults, msync for durability)
type Index struct {
	// entries holds all index entries, sorted by offset
	// Sorted order enables binary search for O(log n) lookup
	entries []IndexEntry

	// file is the backing file for persistence
	file *os.File

	// mu protects concurrent access
	// RWMutex allows multiple concurrent readers
	mu sync.RWMutex

	// lastLogPosition tracks how much log data we've indexed
	// New entry is added when (log position - last position) >= IndexGranularity
	lastLogPosition int64

	// baseOffset is the first offset in this segment
	// Used for segment identification
	baseOffset int64
}

// =============================================================================
// INDEX CREATION & LOADING
// =============================================================================

// NewIndex creates a new empty index for a segment.
//
// PARAMETERS:
//   - path: Where to store the index file (e.g., "00000000000000001000.index")
//   - baseOffset: The first offset that will be in this segment
//
// The index file is created immediately (empty) for durability.
func NewIndex(path string, baseOffset int64) (*Index, error) {
	// O_RDWR: read and write
	// O_CREATE: create if doesn't exist
	// O_APPEND: all writes go to end (important for durability)
	// 0644: owner can read/write, others can read
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create index file: %w", err)
	}

	return &Index{
		entries:         make([]IndexEntry, 0, 1024), // Pre-allocate for ~1024 entries
		file:            file,
		lastLogPosition: 0,
		baseOffset:      baseOffset,
	}, nil
}

// LoadIndex opens an existing index file and loads entries into memory.
//
// RECOVERY FLOW:
//  1. Open the index file
//  2. Read all entries into memory
//  3. Validate entry ordering (should be sorted by offset)
//  4. Set lastLogPosition from last entry
//
// WHY LOAD ALL INTO MEMORY?
//   - Index is small (256KB for 64MB segment)
//   - Binary search in memory is much faster than disk
//   - Simplifies the code (no disk I/O during lookup)
func LoadIndex(path string, baseOffset int64) (*Index, error) {
	// Open for reading and appending
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	// Get file size to know how many entries to expect
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat index file: %w", err)
	}

	// Validate file size is multiple of entry size
	if stat.Size()%IndexEntrySize != 0 {
		file.Close()
		return nil, fmt.Errorf("%w: file size %d is not multiple of entry size %d",
			ErrIndexCorrupted, stat.Size(), IndexEntrySize)
	}

	// Calculate number of entries
	numEntries := int(stat.Size() / IndexEntrySize)
	entries := make([]IndexEntry, 0, numEntries)

	// Read all entries
	// We seek to beginning since file might be opened in append mode
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek index file: %w", err)
	}

	buf := make([]byte, IndexEntrySize)
	var lastOffset int64 = -1
	var lastPosition int64 = 0

	for i := 0; i < numEntries; i++ {
		if _, err := io.ReadFull(file, buf); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to read index entry %d: %w", i, err)
		}

		entry := IndexEntry{
			Offset:   int64(binary.BigEndian.Uint64(buf[0:8])),
			Position: int64(binary.BigEndian.Uint64(buf[8:16])),
		}

		// Validate entries are sorted (should always be true)
		if entry.Offset <= lastOffset {
			file.Close()
			return nil, fmt.Errorf("%w: entries not sorted at index %d", ErrIndexCorrupted, i)
		}

		entries = append(entries, entry)
		lastOffset = entry.Offset
		lastPosition = entry.Position
	}

	// Seek back to end for future appends
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to end: %w", err)
	}

	return &Index{
		entries:         entries,
		file:            file,
		lastLogPosition: lastPosition,
		baseOffset:      baseOffset,
	}, nil
}

// =============================================================================
// INDEX OPERATIONS
// =============================================================================

// MaybeAppend adds an index entry if we've passed the granularity threshold.
//
// This is called after every message write. It checks if we've written
// enough data since the last index entry to warrant a new entry.
//
// PARAMETERS:
//   - offset: The offset of the message just written
//   - position: The byte position where that message starts in the log
//
// RETURNS:
//   - true if an entry was added, false otherwise
//
// EXAMPLE:
//
//	lastLogPosition = 0
//	MaybeAppend(100, 4100)  // position 4100 >= 0 + 4096, so add entry → true
//	lastLogPosition = 4100
//	MaybeAppend(101, 4200)  // position 4200 < 4100 + 4096, skip → false
func (idx *Index) MaybeAppend(offset int64, position int64) (bool, error) {
	// TODO: Maybe first check with RLock for performance?
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Check if we've passed the granularity threshold
	if position-idx.lastLogPosition < IndexGranularity {
		return false, nil
	}

	// Create and store the entry
	entry := IndexEntry{
		Offset:   offset,
		Position: position,
	}
	idx.entries = append(idx.entries, entry)
	idx.lastLogPosition = position

	// Persist to disk
	if err := idx.writeEntry(entry); err != nil {
		// Rollback memory state on disk failure
		idx.entries = idx.entries[:len(idx.entries)-1]
		return false, err
	}

	return true, nil
}

// ForceAppend adds an index entry regardless of granularity.
// Used when starting a new segment to ensure the first message is indexed.
func (idx *Index) ForceAppend(offset int64, position int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	entry := IndexEntry{
		Offset:   offset,
		Position: position,
	}
	idx.entries = append(idx.entries, entry)
	idx.lastLogPosition = position

	return idx.writeEntry(entry)
}

// writeEntry writes a single entry to the index file (must hold lock)
func (idx *Index) writeEntry(entry IndexEntry) error {
	buf := make([]byte, IndexEntrySize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(entry.Offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(entry.Position))

	_, err := idx.file.Write(buf)
	return err
}

// Lookup finds the position to start reading for a given offset.
//
// ALGORITHM:
//  1. Binary search for largest entry with offset ≤ target
//  2. Return that entry's position
//  3. Caller must scan forward from that position to find exact offset
//
// RETURNS:
//   - Entry with the floor offset (largest offset ≤ target)
//   - ErrOffsetNotFound if offset is before this segment's range
//
// EXAMPLE:
//
//	entries: [(0,0), (100,4096), (200,8192)]
//	Lookup(150) → returns (100, 4096)
//	Lookup(200) → returns (200, 8192)
//	Lookup(50)  → returns (0, 0)
func (idx *Index) Lookup(targetOffset int64) (IndexEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return IndexEntry{}, ErrOffsetNotFound
	}

	// Binary search for the largest offset <= targetOffset
	// sort.Search finds the smallest i where f(i) is true
	// We want the largest i where entries[i].Offset <= targetOffset
	// So we search for first i where entries[i].Offset > targetOffset, then go back one
	i := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Offset > targetOffset
	})

	// i is now the index of first entry > targetOffset
	// So i-1 is the entry we want (largest entry <= targetOffset)
	if i == 0 {
		// All entries are > targetOffset
		// But wait - if first entry's offset equals targetOffset, we should return it
		if idx.entries[0].Offset == targetOffset {
			return idx.entries[0], nil
		}
		// Target is before our first indexed offset
		// Return first entry anyway (caller will scan from there)
		return idx.entries[0], nil
	}

	return idx.entries[i-1], nil
}

// LastOffset returns the highest offset in the index.
// Returns -1 if the index is empty.
func (idx *Index) LastOffset() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return -1
	}
	return idx.entries[len(idx.entries)-1].Offset
}

// EntryCount returns the number of entries in the index.
func (idx *Index) EntryCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// Sync forces the index file to be written to disk.
// Call this before closing to ensure durability.
func (idx *Index) Sync() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.file.Sync()
}

// Close syncs and closes the index file.
func (idx *Index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if err := idx.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync index: %w", err)
	}
	return idx.file.Close()
}

// TruncateTo removes all entries after the given offset.
// Used during crash recovery when log is truncated.
func (idx *Index) TruncateTo(offset int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Find first entry > offset
	cutIndex := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Offset > offset
	})

	// Remove entries from cutIndex onwards
	idx.entries = idx.entries[:cutIndex]

	// Update lastLogPosition
	if len(idx.entries) > 0 {
		idx.lastLogPosition = idx.entries[len(idx.entries)-1].Position
	} else {
		idx.lastLogPosition = 0
	}

	// Truncate file
	truncateSize := int64(cutIndex * IndexEntrySize)
	if err := idx.file.Truncate(truncateSize); err != nil {
		return fmt.Errorf("failed to truncate index file: %w", err)
	}

	// Seek to end for future writes
	if _, err := idx.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek after truncate: %w", err)
	}

	return nil
}
