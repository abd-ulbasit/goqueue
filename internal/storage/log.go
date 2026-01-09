// =============================================================================
// APPEND-ONLY LOG - THE HEART OF THE STORAGE ENGINE
// =============================================================================
//
// WHAT IS A LOG?
// A log is an ordered, append-only sequence of messages. Think of it as:
//   - A book where you can only add pages at the end
//   - A bank statement where transactions are only added, never modified
//   - Git commit history (each commit appends, never modifies)
//
// WHY APPEND-ONLY?
//
//   1. SEQUENTIAL WRITES ARE FAST
//      Disk I/O: Random writes ~100 ops/sec, Sequential writes ~100,000+ ops/sec
//      This is true for both SSDs and HDDs (though HDDs benefit more)
//
//      ┌─────────────────────────────────────────────────────────────────────┐
//      │           RANDOM vs SEQUENTIAL DISK ACCESS                          │
//      │                                                                      │
//      │   Random Write (B-tree update):                                      │
//      │     1. Seek to page (10ms on HDD, 0.1ms on SSD)                      │
//      │     2. Read page                                                     │
//      │     3. Modify in memory                                              │
//      │     4. Write page back                                               │
//      │     5. Repeat for index updates                                      │
//      │                                                                      │
//      │   Sequential Write (append-only):                                    │
//      │     1. Write at current position (no seek!)                          │
//      │     2. Increment position                                            │
//      │     3. That's it                                                     │
//      │                                                                      │
//      │   Result: 100-1000x faster for write-heavy workloads                 │
//      └─────────────────────────────────────────────────────────────────────┘
//
//   2. SIMPLER CONCURRENCY
//      - No need to lock pages/rows for updates
//      - Writers only touch the end of the file
//      - Readers can read old data without conflicts
//
//   3. NATURAL REPLICATION
//      - To replicate: just copy bytes from leader to follower
//      - No complex transaction log synchronization
//
//   4. AUDIT TRAIL / TIME TRAVEL
//      - Every message ever written is preserved (until retention expires)
//      - Can replay from any point in time
//      - Perfect for debugging, compliance, rebuilding state
//
// COMPARISON - How other systems use logs:
//   - Kafka: This entire design is based on Kafka's log model
//   - MySQL/PostgreSQL: Use WAL (Write-Ahead Log) for durability
//   - Redis: AOF (Append-Only File) for persistence
//   - Event Sourcing: Same concept at application level
//
// LOG STRUCTURE:
//
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │                         LOG                                         │
//   │                                                                      │
//   │   ┌───────────────┐ ┌───────────────┐ ┌───────────────┐             │
//   │   │  Segment 0    │ │  Segment 1    │ │  Segment 2    │  (active)   │
//   │   │ offsets 0-999 │ │ 1000-1999     │ │ 2000-...      │             │
//   │   │  [sealed]     │ │  [sealed]     │ │  [writable]   │             │
//   │   └───────────────┘ └───────────────┘ └───────────────┘             │
//   │                                                                      │
//   │   Only ONE segment is active (writable) at a time                   │
//   │   When active segment fills up → seal it → create new active        │
//   │                                                                      │
//   └─────────────────────────────────────────────────────────────────────┘
//
// OFFSET SEMANTICS:
//   - Offset is a 64-bit integer, starts at 0
//   - Monotonically increasing (never reused, never goes backwards)
//   - Unique within a partition (globally unique with topic+partition+offset)
//
// =============================================================================

package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrLogClosed means operations attempted on closed log
	ErrLogClosed = errors.New("log is closed")

	// ErrLogEmpty means the log has no messages
	ErrLogEmpty = errors.New("log is empty")
)

// =============================================================================
// LOG STRUCT
// =============================================================================

// Log is an append-only log composed of multiple segments.
//
// DESIGN PRINCIPLES:
//   - Only ONE segment is active (accepting writes) at a time
//   - Old segments are sealed (read-only)
//   - Each segment has a base offset (first offset it contains)
//   - Segments are stored as files named by their base offset
//
// THREAD SAFETY:
//   - Append operations are serialized
//   - Read operations can be concurrent
//   - Segment rollover is atomic from caller's perspective
type Log struct {
	// dir is the directory containing segment files
	// Structure: dir/{baseOffset}.log and {baseOffset}.index
	dir string

	// segments is all segments, sorted by base offset
	// The last segment is the active (writable) one
	segments []*Segment

	// activeSegment is the current segment accepting writes
	// This is always the last element of segments slice
	activeSegment *Segment

	// nextOffset is the next offset to be assigned
	// Equal to activeSegment.NextOffset()
	nextOffset int64

	// mu protects concurrent access
	mu sync.RWMutex

	// closed tracks if log is closed
	closed bool
}

// =============================================================================
// LOG CREATION & LOADING
// =============================================================================

// NewLog creates a new log in the given directory.
// Creates the first segment starting at offset 0.
func NewLog(dir string) (*Log, error) {
	// Create directory if needed
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	// Create first segment
	segment, err := NewSegment(dir, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create initial segment: %w", err)
	}

	return &Log{
		dir:           dir,
		segments:      []*Segment{segment},
		activeSegment: segment,
		nextOffset:    0,
	}, nil
}

// LoadLog opens an existing log from the given directory.
//
// RECOVERY PROCESS:
//  1. List all .log files in directory
//  2. Parse base offsets from filenames
//  3. Load each segment (which validates/repairs data)
//  4. Set active segment to the one with highest base offset
//
// This handles crash recovery - partial writes are truncated,
// corrupted indexes are rebuilt.
func LoadLog(dir string) (*Log, error) {
	// Check directory exists
	stat, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("log directory not found: %w", err)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("path is not a directory: %s", dir)
	}

	// Find all segment files
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read log directory: %w", err)
	}

	// Parse segment base offsets from filenames
	var baseOffsets []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}

		// Parse offset from filename (e.g., "00000000000000001000.log")
		offsetStr := strings.TrimSuffix(name, ".log")
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			// Skip invalid filenames
			continue
		}
		baseOffsets = append(baseOffsets, offset)
	}

	// Handle empty log directory
	if len(baseOffsets) == 0 {
		return NewLog(dir)
	}

	// Sort by offset (ascending)
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// Load all segments
	segments := make([]*Segment, 0, len(baseOffsets))
	for _, baseOffset := range baseOffsets {
		segment, err := LoadSegment(dir, baseOffset)
		if err != nil {
			// Close already loaded segments
			for _, s := range segments {
				s.Close()
			}
			return nil, fmt.Errorf("failed to load segment %d: %w", baseOffset, err)
		}
		segments = append(segments, segment)
	}

	// Seal all segments except the last one
	for i := 0; i < len(segments)-1; i++ {
		if !segments[i].IsReadOnly() {
			if err := segments[i].Seal(); err != nil {
				// Non-fatal, log and continue
				fmt.Printf("Warning: failed to seal segment %d: %v\n", segments[i].BaseOffset(), err)
			}
		}
	}

	activeSegment := segments[len(segments)-1]

	return &Log{
		dir:           dir,
		segments:      segments,
		activeSegment: activeSegment,
		nextOffset:    activeSegment.NextOffset(),
	}, nil
}

// =============================================================================
// WRITE OPERATIONS
// =============================================================================

// Append writes a message to the log and returns the assigned offset.
//
// FLOW:
//  1. Check if active segment is full
//  2. If full → create new segment (rollover)
//  3. Write message to active segment
//  4. Return assigned offset
//
// SEGMENT ROLLOVER:
// When active segment reaches 64MB:
//  1. Seal current segment (make read-only)
//  2. Create new segment with base offset = next offset
//  3. Switch active segment pointer
//  4. Continue writing to new segment
//
// This is transparent to the caller - they just see a continuous offset space.
func (l *Log) Append(msg *Message) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, ErrLogClosed
	}

	// Try to append to active segment
	offset, err := l.activeSegment.Append(msg)
	if err == ErrSegmentFull {
		// Segment is full, need to rollover
		if err := l.rollover(); err != nil {
			return 0, fmt.Errorf("failed to rollover segment: %w", err)
		}
		// Retry append to new segment
		offset, err = l.activeSegment.Append(msg)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to append message: %w", err)
	}

	l.nextOffset = offset + 1
	return offset, nil
}

// =============================================================================
// APPEND AT SPECIFIC OFFSET (M11 REPLICATION)
// =============================================================================
//
// WHY: Followers must append messages at exact offsets to maintain consistency
// with the leader. Regular Append() assigns the next sequential offset, but
// replicated messages must preserve their original offset from the leader.
//
// FLOW:
//   Leader writes:   Offset 100, 101, 102
//   Follower fetches: [Msg@100, Msg@101, Msg@102]
//   Follower appends: AppendAtOffset(msg, 100), AppendAtOffset(msg, 101), ...
//
// CONSTRAINTS:
//   - expectedOffset must match log's next offset (no gaps)
//   - If offset < nextOffset, message already exists (idempotent)
//   - If offset > nextOffset, gap would be created (error)
//
// =============================================================================

// AppendAtOffset appends a message at a specific offset.
// Used by followers when replicating from the leader.
//
// This ensures followers maintain exact offset consistency with the leader.
// Returns the offset written to, or error if:
//   - Log is closed
//   - Expected offset doesn't match next offset
//   - Segment operations fail
func (l *Log) AppendAtOffset(msg *Message, expectedOffset int64) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, ErrLogClosed
	}

	// Idempotency check: if offset already exists, skip.
	// This can happen if follower restarts and re-fetches overlapping range.
	if expectedOffset < l.nextOffset {
		// Already have this offset - idempotent success
		return expectedOffset, nil
	}

	// Gap check: offset must be exactly next offset (no gaps allowed).
	if expectedOffset > l.nextOffset {
		return 0, fmt.Errorf("offset gap: expected %d but next is %d", expectedOffset, l.nextOffset)
	}

	// Normal append at expected offset
	offset, err := l.activeSegment.Append(msg)
	if err == ErrSegmentFull {
		if err := l.rollover(); err != nil {
			return 0, fmt.Errorf("failed to rollover segment: %w", err)
		}
		offset, err = l.activeSegment.Append(msg)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to append message: %w", err)
	}

	// Verify we got the expected offset
	if offset != expectedOffset {
		// This shouldn't happen, but check anyway
		return 0, fmt.Errorf("offset mismatch: got %d, expected %d", offset, expectedOffset)
	}

	l.nextOffset = offset + 1
	return offset, nil
}

// rollover creates a new segment and switches to it.
// Must be called with lock held.
func (l *Log) rollover() error {
	// Seal current segment
	if err := l.activeSegment.Seal(); err != nil {
		return fmt.Errorf("failed to seal segment: %w", err)
	}

	// Create new segment with next offset as base
	newSegment, err := NewSegment(l.dir, l.nextOffset)
	if err != nil {
		return fmt.Errorf("failed to create new segment: %w", err)
	}

	// Add to segments list and switch active
	l.segments = append(l.segments, newSegment)
	l.activeSegment = newSegment

	return nil
}

// Sync ensures all data is written to disk.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrLogClosed
	}

	return l.activeSegment.Sync()
}

// =============================================================================
// READ OPERATIONS
// =============================================================================

// Read returns the message at the given offset.
//
// ALGORITHM:
//  1. Find which segment contains the offset (binary search by base offset)
//  2. Read from that segment
//
// FINDING THE RIGHT SEGMENT:
//
//	Segments: [0-999], [1000-1999], [2000-...]
//	Read offset 1500:
//	  - Binary search for largest base offset ≤ 1500
//	  - Find segment with base 1000
//	  - Read offset 1500 from that segment
func (l *Log) Read(offset int64) (*Message, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrLogClosed
	}

	// Find segment containing offset
	segment := l.findSegment(offset)
	if segment == nil {
		return nil, ErrOffsetNotFound
	}

	return segment.Read(offset)
}

// ReadFrom reads messages starting from the given offset.
//
// PARAMETERS:
//   - startOffset: First offset to read from
//   - maxMessages: Maximum messages to return (0 = no limit, read all available)
//
// RETURNS:
//   - Messages from startOffset to min(startOffset+maxMessages, nextOffset)
//   - May span multiple segments transparently
//
// This is the primary read method for consumers.
func (l *Log) ReadFrom(startOffset int64, maxMessages int) ([]*Message, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrLogClosed
	}

	if startOffset >= l.nextOffset {
		return []*Message{}, nil // No new messages
	}

	// Clamp to earliest available offset
	if startOffset < l.EarliestOffset() {
		startOffset = l.EarliestOffset()
	}

	// Find starting segment
	segmentIdx := l.findSegmentIndex(startOffset)
	if segmentIdx == -1 {
		return nil, ErrOffsetNotFound
	}

	// Read from segments, potentially spanning multiple
	var messages []*Message
	currentOffset := startOffset

	for i := segmentIdx; i < len(l.segments); i++ {
		segment := l.segments[i]

		// Calculate how many messages to read from this segment
		remaining := maxMessages - len(messages)
		if maxMessages == 0 {
			remaining = 0 // 0 means no limit
		}

		// Read from segment
		segmentMessages, err := segment.ReadFrom(currentOffset, remaining)
		if err != nil {
			return nil, fmt.Errorf("failed to read from segment %d: %w", segment.BaseOffset(), err)
		}

		messages = append(messages, segmentMessages...)

		// Check if we've read enough
		if maxMessages > 0 && len(messages) >= maxMessages {
			break
		}

		// Move to next segment
		currentOffset = segment.NextOffset()
	}

	return messages, nil
}

// findSegment returns the segment containing the given offset.
// Returns nil if offset is not in any segment.
func (l *Log) findSegment(offset int64) *Segment {
	idx := l.findSegmentIndex(offset)
	if idx == -1 {
		return nil
	}
	return l.segments[idx]
}

// findSegmentIndex returns the index of the segment containing the offset.
// Uses binary search for O(log n) lookup.
func (l *Log) findSegmentIndex(offset int64) int {
	if len(l.segments) == 0 {
		return -1
	}

	// Binary search for the segment with largest base offset ≤ offset
	// sort.Search finds smallest i where f(i) is true
	// We want largest i where segments[i].BaseOffset() <= offset
	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].BaseOffset() > offset
	})

	// i is now first segment with base > offset
	// So i-1 is the segment we want
	if i == 0 {
		// All segments have base > offset
		return -1
	}

	// Check if offset is actually in range
	segment := l.segments[i-1]
	if offset >= segment.NextOffset() {
		// Offset is past the end of this segment
		return -1
	}

	return i - 1
}

// =============================================================================
// LOG METADATA
// =============================================================================

// NextOffset returns the next offset that will be assigned.
func (l *Log) NextOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOffset
}

// EarliestOffset returns the first available offset in the log.
// This may not be 0 if old segments have been deleted (retention cleanup).
func (l *Log) EarliestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].BaseOffset()
}

// LatestOffset returns the last offset in the log (nextOffset - 1).
// Returns -1 if log is empty.
func (l *Log) LatestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.nextOffset == 0 {
		return -1
	}
	return l.nextOffset - 1
}

// Size returns total size of the log in bytes (across all segments).
func (l *Log) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var total int64
	for _, s := range l.segments {
		total += s.Size()
	}
	return total
}

// SegmentCount returns the number of segments in the log.
func (l *Log) SegmentCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.segments)
}

// =============================================================================
// LOG MANAGEMENT
// =============================================================================

// Close closes the log and all segments.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	var errs []error
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			errs = append(errs, fmt.Errorf("segment %d: %w", segment.BaseOffset(), err))
		}
	}

	l.closed = true

	if len(errs) > 0 {
		return fmt.Errorf("errors closing log: %v", errs)
	}
	return nil
}

// DeleteSegmentsBefore removes segments with all offsets before the given offset.
// Used for retention cleanup.
//
// EXAMPLE:
//
//	Segments: [0-999], [1000-1999], [2000-2999], [3000-...]
//	DeleteSegmentsBefore(2000):
//	  - Delete segments [0-999] and [1000-1999]
//	  - Keep [2000-2999] and [3000-...]
//
// SAFETY: Never deletes the active segment.
func (l *Log) DeleteSegmentsBefore(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrLogClosed
	}

	// Find segments to delete
	var toDelete []*Segment
	var toKeep []*Segment

	for i, segment := range l.segments {
		// Keep active segment always
		if i == len(l.segments)-1 {
			toKeep = append(toKeep, segment)
			continue
		}

		// Delete if all messages are before cutoff
		if segment.NextOffset() <= offset {
			toDelete = append(toDelete, segment)
		} else {
			toKeep = append(toKeep, segment)
		}
	}

	// Delete old segments
	for _, segment := range toDelete {
		if err := segment.Delete(); err != nil {
			return fmt.Errorf("failed to delete segment %d: %w", segment.BaseOffset(), err)
		}
	}

	// Update segments list
	l.segments = toKeep

	return nil
}

// TruncateAfter removes all messages after the given offset.
// Used for crash recovery when leader has less data than follower.
func (l *Log) TruncateAfter(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrLogClosed
	}

	// Find segment containing offset
	idx := l.findSegmentIndex(offset)
	if idx == -1 {
		// Offset not in log - truncate everything?
		// For now, return error
		return fmt.Errorf("offset %d not found in log", offset)
	}

	// Delete all segments after idx
	for i := len(l.segments) - 1; i > idx; i-- {
		if err := l.segments[i].Delete(); err != nil {
			return fmt.Errorf("failed to delete segment: %w", err)
		}
	}
	l.segments = l.segments[:idx+1]

	// Truncate the target segment (handled by segment itself)
	// For now, we'll just update our tracking
	l.activeSegment = l.segments[len(l.segments)-1]
	l.nextOffset = offset + 1

	return nil
}

// Dir returns the log directory path.
func (l *Log) Dir() string {
	return l.dir
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

// ListSegmentFiles returns all segment file base offsets in a directory.
// Useful for external tools inspecting log structure.
func ListSegmentFiles(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var offsets []int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}

		offsetStr := strings.TrimSuffix(entry.Name(), ".log")
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			continue
		}
		offsets = append(offsets, offset)
	}

	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

	return offsets, nil
}

// GetSegmentPaths returns full paths to all segment files.
func GetSegmentPaths(dir string) ([]string, error) {
	offsets, err := ListSegmentFiles(dir)
	if err != nil {
		return nil, err
	}

	paths := make([]string, len(offsets))
	for i, offset := range offsets {
		paths[i] = filepath.Join(dir, SegmentFileName(offset))
	}

	return paths, nil
}
