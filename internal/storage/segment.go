// =============================================================================
// SEGMENT FILE - A CHUNK OF THE LOG
// =============================================================================
//
// WHAT IS A SEGMENT?
// The log is split into multiple segment files instead of one giant file.
// Each segment contains a range of offsets and has a maximum size (64MB).
//
// WHY SEGMENTS INSTEAD OF ONE BIG FILE?
//
//   1. GARBAGE COLLECTION / RETENTION
//      - Retention policy: "keep messages for 7 days"
//      - With one file: Must rewrite entire file to remove old data (expensive!)
//      - With segments: Just delete old segment files (instant!)
//
//   2. FASTER RECOVERY
//      - If a file is corrupted, we only lose one segment (64MB max)
//      - Not the entire log history
//
//   3. PARALLEL I/O
//      - Different segments can be read/written by different goroutines
//      - Enables concurrent consumers reading different parts of the log
//
//   4. FILE SIZE LIMITS
//      - Some filesystems have max file size limits
//      - 64MB segments avoid hitting these limits
//
// SEGMENT NAMING CONVENTION:
//   Files are named by their base offset (the first offset in the segment)
//   Format: {20-digit zero-padded offset}.log
//
//   Examples:
//     00000000000000000000.log  → Contains offsets 0 to ~999
//     00000000000000001000.log  → Contains offsets 1000 to ~1999
//     00000000000000002000.log  → Contains offsets 2000 to ~2999
//
//   WHY 20 DIGITS?
//   - Ensures lexicographic sort = numeric sort
//   - "00000000000000001000" < "00000000000000002000" alphabetically too
//   - Makes listing segments in order trivial: just sort filenames
//
// SEGMENT LIFECYCLE:
//
//   ┌─────────────┐  size >= 64MB  ┌─────────────┐  retention  ┌─────────────┐
//   │   ACTIVE    │ ────────────► │   SEALED    │ ──────────► │   DELETED   │
//   │ (writable)  │               │ (read-only) │  (expired)  │             │
//   └─────────────┘               └─────────────┘             └─────────────┘
//
//   - ACTIVE: Current segment being written to (only one per partition)
//   - SEALED: Segment is full, no more writes, but still readable
//   - DELETED: Past retention period, files deleted
//
// COMPARISON - How other systems segment:
//   - Kafka: Segments with configurable size (default 1GB)
//   - RabbitMQ: Doesn't use segments (uses Mnesia database)
//   - SQS: Internal, not exposed (AWS manages)
//
// =============================================================================

package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// =============================================================================
// CONSTANTS
// =============================================================================

const (
	// MaxSegmentSize is the maximum size of a segment file in bytes (64MB)
	// When segment exceeds this, a new segment is created
	MaxSegmentSize = 64 * 1024 * 1024

	// DefaultSyncInterval is how often we fsync to disk
	// Trade-off: More frequent = safer but slower
	DefaultSyncInterval = 1000 * time.Millisecond

	// ReadBufferSize for buffered reading
	ReadBufferSize = 64 * 1024 // 64KB buffer
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrSegmentFull means the segment has reached max size
	ErrSegmentFull = errors.New("segment is full")

	// ErrSegmentClosed means operations attempted on closed segment
	ErrSegmentClosed = errors.New("segment is closed")

	// ErrSegmentReadOnly means writes attempted on sealed segment
	ErrSegmentReadOnly = errors.New("segment is read-only")
)

// =============================================================================
// SEGMENT STRUCT
// =============================================================================

// Segment represents a single segment file in the log.
//
// STRUCTURE ON DISK:
//
//	data/logs/{topic}/{partition}/
//	  00000000000000000000.log        <- Segment file (messages)
//	  00000000000000000000.index      <- Index file (offset → position)
//	  00000000000000000000.timeindex  <- Time index file (timestamp → offset)
//
// THREAD SAFETY:
//   - Write operations are serialized (single writer)
//   - Read operations can be concurrent
//   - Uses RWMutex for coordination
//
// BUFFERED I/O:
//   - Writes go through a buffer (faster than syscall per write)
//   - Buffer is flushed on sync or when full
type Segment struct {
	// baseOffset is the first offset in this segment
	// Also used for segment identification and filename
	baseOffset int64

	// currentOffset is the next offset to be assigned
	// Incremented after each successful write
	currentOffset int64

	// currentPosition is current byte position in the file
	// Used to know when segment is full
	currentPosition int64

	// file is the underlying log file
	file *os.File

	// writer is a buffered writer for performance
	// Batches small writes into larger ones
	writer *bufio.Writer

	// index provides offset→position lookup
	index *Index

	// timeIndex provides timestamp→offset lookup (Milestone 14)
	// Enables queries like "get messages from yesterday 3pm"
	timeIndex *TimeIndex

	// mu protects concurrent access
	mu sync.RWMutex

	// closed tracks if segment is closed
	closed bool

	// readonly means no more writes allowed (segment is sealed)
	readonly bool

	// path is the directory containing segment files
	path string

	// lastSync tracks when we last fsynced
	lastSync time.Time

	// syncInterval is how often to fsync (0 = every write)
	syncInterval time.Duration
}

// =============================================================================
// SEGMENT CREATION & LOADING
// =============================================================================

// SegmentFileName generates the filename for a segment with given base offset.
// Format: 20-digit zero-padded offset + ".log"
func SegmentFileName(baseOffset int64) string {
	return fmt.Sprintf("%020d.log", baseOffset)
}

// IndexFileName generates the index filename for a segment.
func IndexFileName(baseOffset int64) string {
	return fmt.Sprintf("%020d.index", baseOffset)
}

// NewSegment creates a new segment starting at the given base offset.
//
// PARAMETERS:
//   - dir: Directory to store segment files
//   - baseOffset: First offset in this segment
//
// CREATES:
//   - {dir}/{baseOffset}.log        - The segment data file
//   - {dir}/{baseOffset}.index      - The index file
//   - {dir}/{baseOffset}.timeindex  - The time index file
func NewSegment(dir string, baseOffset int64) (*Segment, error) {
	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create segment directory: %w", err)
	}

	// Create log file
	logPath := filepath.Join(dir, SegmentFileName(baseOffset))
	file, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment file: %w", err)
	}

	// Create index
	indexPath := filepath.Join(dir, IndexFileName(baseOffset))
	index, err := NewIndex(indexPath, baseOffset)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	// Create time index (Milestone 14)
	timeIndexPath := filepath.Join(dir, TimeIndexFileName(baseOffset))
	timeIndex, err := NewTimeIndex(timeIndexPath, baseOffset)
	if err != nil {
		file.Close()
		index.Close()
		return nil, fmt.Errorf("failed to create time index: %w", err)
	}

	return &Segment{
		baseOffset:      baseOffset,
		currentOffset:   baseOffset,
		currentPosition: 0,
		file:            file,
		writer:          bufio.NewWriter(file),
		index:           index,
		timeIndex:       timeIndex,
		path:            dir,
		lastSync:        time.Now(),
		syncInterval:    DefaultSyncInterval,
	}, nil
}

// LoadSegment opens an existing segment for reading and writing.
//
// RECOVERY PROCESS:
//  1. Open log file and index file
//  2. Scan log to find actual end position (may differ from file size if crash)
//  3. Truncate any partial writes at end
//  4. Update index to match actual log content
//
// WHY SCAN THE LOG?
// After a crash, the file might have:
//   - Partial message at the end (incomplete write)
//   - Zeros at the end (allocated but not written)
//
// We must find the last valid message to recover correctly.
func LoadSegment(dir string, baseOffset int64) (*Segment, error) {
	// Open log file
	logPath := filepath.Join(dir, SegmentFileName(baseOffset))
	file, err := os.OpenFile(logPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %w", err)
	}

	// Load index
	indexPath := filepath.Join(dir, IndexFileName(baseOffset))
	index, err := LoadIndex(indexPath, baseOffset)
	if err != nil {
		// Index might be corrupted or missing, rebuild it
		file.Close()
		return rebuildSegment(dir, baseOffset)
	}

	// Load time index (Milestone 14)
	timeIndexPath := filepath.Join(dir, TimeIndexFileName(baseOffset))
	timeIndex, err := LoadTimeIndex(timeIndexPath, baseOffset)
	if err != nil {
		// Time index might be missing or corrupted - try to rebuild
		file.Close()
		index.Close()
		return rebuildSegment(dir, baseOffset)
	}

	// Scan log to find actual position and offset
	currentOffset, currentPosition, err := scanLogToEnd(file, baseOffset)
	if err != nil {
		file.Close()
		index.Close()
		timeIndex.Close()
		// Try rebuild if scan fails
		return rebuildSegment(dir, baseOffset)
	}

	// =========================================================================
	// BUG FIX: Handle file.Stat() error
	// =========================================================================
	// WHY: Previously the error was ignored with `stat, _ := file.Stat()`.
	// If Stat() fails (disk error, file deleted, etc.), stat would be nil
	// and stat.Size() would panic with nil pointer dereference.
	//
	// FIX: Handle the error explicitly. If Stat fails, try to rebuild the segment.
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		index.Close()
		timeIndex.Close()
		// Try rebuild if stat fails
		return rebuildSegment(dir, baseOffset)
	}

	// Truncate any garbage at end
	if currentPosition < stat.Size() {
		if err := file.Truncate(currentPosition); err != nil {
			file.Close()
			index.Close()
			timeIndex.Close()
			return nil, fmt.Errorf("failed to truncate segment: %w", err)
		}
	}

	// Seek to end for appending
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		index.Close()
		timeIndex.Close()
		return nil, fmt.Errorf("failed to seek to end: %w", err)
	}

	return &Segment{
		baseOffset:      baseOffset,
		currentOffset:   currentOffset,
		currentPosition: currentPosition,
		file:            file,
		writer:          bufio.NewWriter(file),
		index:           index,
		timeIndex:       timeIndex,
		path:            dir,
		lastSync:        time.Now(),
		syncInterval:    DefaultSyncInterval,
	}, nil
}

// scanLogToEnd reads through the log to find the last valid message.
// Returns the next offset to use and the byte position after last message.
//
// This is a recovery operation - we read every message header to verify integrity.
func scanLogToEnd(file *os.File, baseOffset int64) (nextOffset int64, position int64, err error) {
	// Start from beginning
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}

	reader := bufio.NewReader(file)
	nextOffset = baseOffset
	position = 0

	for {
		// Try to read the full header
		header := make([]byte, HeaderSize)
		n, err := io.ReadFull(reader, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// End of file or partial header - we're done
			break
		}
		if err != nil {
			return 0, 0, fmt.Errorf("error reading header at position %d: %w", position, err)
		}
		if n < HeaderSize {
			// Partial header at end - truncate
			break
		}

		// Validate magic bytes
		if header[0] != MagicByte1 || header[1] != MagicByte2 {
			// Invalid magic - either corruption or end of valid data
			break
		}

		// Extract lengths to know message size
		// Header layout: KeyLen at [26:28], ValueLen at [28:32], HeadersLen at [32:34]
		keyLen := binary.BigEndian.Uint16(header[26:28])
		valueLen := binary.BigEndian.Uint32(header[28:32])
		headersLen := binary.BigEndian.Uint16(header[32:34])
		bodySize := int64(keyLen) + int64(valueLen) + int64(headersLen)
		totalSize := int64(HeaderSize) + bodySize

		// Skip over key, value, and headers
		if _, err := io.CopyN(io.Discard, reader, bodySize); err != nil {
			// Partial message body - stop here
			break
		}

		// Extract offset from header
		messageOffset := int64(binary.BigEndian.Uint64(header[8:16]))

		// Valid message found
		position += totalSize
		nextOffset = messageOffset + 1
	}

	return nextOffset, position, nil
}

// rebuildSegment creates a fresh segment by scanning an existing log file
// and rebuilding the index. Used when index is corrupted.
func rebuildSegment(dir string, baseOffset int64) (*Segment, error) {
	logPath := filepath.Join(dir, SegmentFileName(baseOffset))
	indexPath := filepath.Join(dir, IndexFileName(baseOffset))
	timeIndexPath := filepath.Join(dir, TimeIndexFileName(baseOffset))

	// Remove corrupted indices
	os.Remove(indexPath)
	os.Remove(timeIndexPath)

	// Open log file
	file, err := os.OpenFile(logPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment for rebuild: %w", err)
	}

	// Create fresh index
	index, err := NewIndex(indexPath, baseOffset)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create index for rebuild: %w", err)
	}

	// Create fresh time index
	timeIndex, err := NewTimeIndex(timeIndexPath, baseOffset)
	if err != nil {
		file.Close()
		index.Close()
		return nil, fmt.Errorf("failed to create time index for rebuild: %w", err)
	}

	// Scan log and rebuild indices
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		index.Close()
		timeIndex.Close()
		return nil, err
	}

	reader := bufio.NewReader(file)
	var position int64 = 0
	nextOffset := baseOffset
	firstMessage := true

	for {
		// Read full header
		header := make([]byte, HeaderSize)
		n, err := io.ReadFull(reader, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF || n < HeaderSize {
			break
		}
		if err != nil {
			break
		}

		if header[0] != MagicByte1 || header[1] != MagicByte2 {
			break
		}

		// Extract fields from 34-byte header
		messageOffset := int64(binary.BigEndian.Uint64(header[8:16]))
		timestamp := int64(binary.BigEndian.Uint64(header[16:24]))
		keyLen := binary.BigEndian.Uint16(header[26:28])
		valueLen := binary.BigEndian.Uint32(header[28:32])
		headersLen := binary.BigEndian.Uint16(header[32:34])
		bodySize := int64(keyLen) + int64(valueLen) + int64(headersLen)
		totalSize := int64(HeaderSize) + bodySize

		// Add to index (force first entry, then respect granularity)
		if firstMessage {
			index.ForceAppend(messageOffset, position)
			firstMessage = false
		} else {
			index.MaybeAppend(messageOffset, position)
		}

		// Add to time index (respects granularity internally)
		timeIndex.MaybeAppend(timestamp, messageOffset, position)

		// Skip body
		if _, err := io.CopyN(io.Discard, reader, bodySize); err != nil {
			break
		}

		position += totalSize
		nextOffset = messageOffset + 1
	}

	// Truncate any garbage
	if err := file.Truncate(position); err != nil {
		file.Close()
		index.Close()
		timeIndex.Close()
		return nil, err
	}

	// Seek to end for appending
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		index.Close()
		timeIndex.Close()
		return nil, err
	}

	return &Segment{
		baseOffset:      baseOffset,
		currentOffset:   nextOffset,
		currentPosition: position,
		file:            file,
		writer:          bufio.NewWriter(file),
		index:           index,
		timeIndex:       timeIndex,
		path:            dir,
		lastSync:        time.Now(),
		syncInterval:    DefaultSyncInterval,
	}, nil
}

// =============================================================================
// WRITE OPERATIONS
// =============================================================================

// Append writes a message to the segment and returns the assigned offset.
//
// FLOW:
//  1. Check segment isn't closed/readonly/full
//  2. Assign offset to message
//  3. Encode message to binary
//  4. Write to buffer
//  5. Update index if needed
//  6. Maybe sync to disk
//
// RETURNS:
//   - Assigned offset
//   - ErrSegmentFull if segment exceeded max size (caller should create new segment)
func (s *Segment) Append(msg *Message) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check state
	if s.closed {
		return 0, ErrSegmentClosed
	}
	if s.readonly {
		return 0, ErrSegmentReadOnly
	}

	// Assign offset
	msg.Offset = s.currentOffset

	// Encode message
	data, err := msg.Encode()
	if err != nil {
		return 0, fmt.Errorf("failed to encode message: %w", err)
	}

	// Check if this write would exceed segment size
	newPosition := s.currentPosition + int64(len(data))
	if newPosition > MaxSegmentSize {
		return 0, ErrSegmentFull
	}

	// Write to buffer
	if _, err := s.writer.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write message: %w", err)
	}

	// Update index
	// First message of segment is always indexed
	if s.currentOffset == s.baseOffset {
		if err := s.index.ForceAppend(msg.Offset, s.currentPosition); err != nil {
			return 0, fmt.Errorf("failed to index first message: %w", err)
		}
	} else {
		// Add index entry if we've passed granularity threshold
		if _, err := s.index.MaybeAppend(msg.Offset, s.currentPosition); err != nil {
			return 0, fmt.Errorf("failed to update index: %w", err)
		}
	}

	// Update time index (Milestone 14)
	// Respects granularity internally - adds entry every 4KB
	if _, err := s.timeIndex.MaybeAppend(msg.Timestamp, msg.Offset, s.currentPosition); err != nil {
		return 0, fmt.Errorf("failed to update time index: %w", err)
	}

	// Update position and offset
	offset := s.currentOffset
	s.currentOffset++
	s.currentPosition = newPosition

	// Maybe sync to disk
	if err := s.maybeSync(); err != nil {
		return 0, fmt.Errorf("failed to sync: %w", err)
	}

	return offset, nil
}

// maybeSync flushes buffer and fsyncs if enough time has passed.
// Called after every write. Actual sync happens based on syncInterval.
func (s *Segment) maybeSync() error {
	// Flush buffer to OS (not necessarily to disk yet)
	if err := s.writer.Flush(); err != nil {
		return err
	}

	// Check if we should fsync
	if s.syncInterval == 0 || time.Since(s.lastSync) >= s.syncInterval {
		if err := s.file.Sync(); err != nil {
			return err
		}
		s.lastSync = time.Now()
	}

	return nil
}

// Sync forces all buffered data to disk.
// Call this before closing or when durability is critical.
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSegmentClosed
	}

	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	if err := s.index.Sync(); err != nil {
		return fmt.Errorf("failed to sync index: %w", err)
	}

	// Sync time index (Milestone 14)
	if err := s.timeIndex.Sync(); err != nil {
		return fmt.Errorf("failed to sync time index: %w", err)
	}

	s.lastSync = time.Now()
	return nil
}

// =============================================================================
// READ OPERATIONS
// =============================================================================

// Read returns the message at the given offset.
//
// ALGORITHM:
//  1. Look up offset in index → get approximate position
//  2. Seek to that position in log file
//  3. Scan forward reading messages until we find the target offset
//  4. Decode and return the message
//
// WHY SCAN FORWARD?
// The index is sparse (one entry per 4KB). The target offset might be
// between index entries, so we must scan from the nearest indexed position.
func (s *Segment) Read(offset int64) (*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	// Check offset is in this segment's range
	if offset < s.baseOffset || offset >= s.currentOffset {
		return nil, ErrOffsetNotFound
	}

	// Look up position in index
	entry, err := s.index.Lookup(offset)
	if err != nil {
		return nil, fmt.Errorf("index lookup failed: %w", err)
	}

	// Open a separate file handle for reading
	// (Don't interfere with the writer's position)
	readFile, err := os.Open(filepath.Join(s.path, SegmentFileName(s.baseOffset)))
	if err != nil {
		return nil, fmt.Errorf("failed to open segment for reading: %w", err)
	}
	defer readFile.Close()

	// Seek to indexed position
	if _, err := readFile.Seek(entry.Position, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to position %d: %w", entry.Position, err)
	}

	// Scan forward to find target offset
	reader := bufio.NewReader(readFile)
	for {
		// Read message
		msg, err := s.readOneMessage(reader)
		if err != nil {
			return nil, err
		}

		if msg.Offset == offset {
			return msg, nil
		}

		if msg.Offset > offset {
			// Passed the target offset - it doesn't exist
			return nil, ErrOffsetNotFound
		}
	}
}

// ReadFrom reads messages starting from the given offset, up to maxMessages.
// This is the primary method for consumers fetching batches of messages.
//
// PARAMETERS:
//   - startOffset: First offset to read
//   - maxMessages: Maximum messages to return (0 = all remaining)
//
// RETURNS:
//   - Slice of messages (may be empty if startOffset >= currentOffset)
//   - Error if something goes wrong
func (s *Segment) ReadFrom(startOffset int64, maxMessages int) ([]*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	// Check offset range
	if startOffset >= s.currentOffset {
		return []*Message{}, nil // No messages to read
	}
	if startOffset < s.baseOffset {
		startOffset = s.baseOffset
	}

	// Look up position
	entry, err := s.index.Lookup(startOffset)
	if err != nil && !errors.Is(err, ErrOffsetNotFound) {
		return nil, fmt.Errorf("index lookup failed: %w", err)
	}

	// Open file for reading
	readFile, err := os.Open(filepath.Join(s.path, SegmentFileName(s.baseOffset)))
	if err != nil {
		return nil, fmt.Errorf("failed to open segment: %w", err)
	}
	defer readFile.Close()

	// Seek to starting position
	seekPos := entry.Position
	if seekPos > 0 {
		if _, err := readFile.Seek(seekPos, io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek: %w", err)
		}
	}

	// Read messages
	var messages []*Message
	reader := bufio.NewReader(readFile)

	for {
		msg, err := s.readOneMessage(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// Skip messages before startOffset (due to sparse index)
		if msg.Offset < startOffset {
			continue
		}

		messages = append(messages, msg)

		// Check limit
		if maxMessages > 0 && len(messages) >= maxMessages {
			break
		}
	}

	return messages, nil
}

// readOneMessage reads and decodes a single message from the reader.
//
// HEADER FORMAT (32 bytes):
//
//	[0:2]   Magic bytes ("GQ")
//	[2]     Version (1)
//	[3]     Flags
//	[4:8]   CRC32
//	[8:16]  Offset
//	[16:24] Timestamp
//	[24]    Priority
//	[25]    Reserved
//	[26:28] Key length
//	[28:32] Value length
//	[32:34] Headers length
func (s *Segment) readOneMessage(reader *bufio.Reader) (*Message, error) {
	// Read the full 34-byte header
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err // EOF or read error
	}

	// Validate magic
	if header[0] != MagicByte1 || header[1] != MagicByte2 {
		return nil, ErrInvalidMagic
	}

	// Get key, value, and headers lengths from fixed positions
	keyLen := binary.BigEndian.Uint16(header[26:28])
	valueLen := binary.BigEndian.Uint32(header[28:32])
	headersLen := binary.BigEndian.Uint16(header[32:34])

	// Read body (key + value + headers)
	body := make([]byte, int(keyLen)+int(valueLen)+int(headersLen))
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, fmt.Errorf("failed to read message body: %w", err)
	}

	// Combine header and body for decoding
	fullMessage := make([]byte, HeaderSize+len(body))
	copy(fullMessage, header)
	copy(fullMessage[HeaderSize:], body)

	// Decode
	return Decode(fullMessage)
}

// =============================================================================
// SEGMENT MANAGEMENT
// =============================================================================

// Seal marks the segment as read-only. No more writes allowed.
// Called when segment reaches max size and a new segment is created.
func (s *Segment) Seal() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSegmentClosed
	}

	// Ensure all data is on disk
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}
	if err := s.index.Sync(); err != nil {
		return fmt.Errorf("failed to sync index: %w", err)
	}
	// Sync time index (Milestone 14)
	if err := s.timeIndex.Sync(); err != nil {
		return fmt.Errorf("failed to sync time index: %w", err)
	}

	s.readonly = true
	return nil
}

// Close closes the segment, releasing all resources.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	var errs []error

	// Flush and close writer
	if err := s.writer.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("flush: %w", err))
	}

	// Close file
	if err := s.file.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close file: %w", err))
	}

	// Close index
	if err := s.index.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close index: %w", err))
	}

	// Close time index (Milestone 14)
	if err := s.timeIndex.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close time index: %w", err))
	}

	s.closed = true

	if len(errs) > 0 {
		return fmt.Errorf("errors closing segment: %v", errs)
	}
	return nil
}

// Delete removes the segment files from disk.
// Segment must be closed first.
func (s *Segment) Delete() error {
	if !s.closed {
		if err := s.Close(); err != nil {
			return err
		}
	}

	logPath := filepath.Join(s.path, SegmentFileName(s.baseOffset))
	indexPath := filepath.Join(s.path, IndexFileName(s.baseOffset))
	timeIndexPath := filepath.Join(s.path, TimeIndexFileName(s.baseOffset))

	if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete log file: %w", err)
	}
	if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete index file: %w", err)
	}
	// Delete time index file (Milestone 14)
	if err := os.Remove(timeIndexPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete time index file: %w", err)
	}

	return nil
}

// =============================================================================
// SEGMENT METADATA
// =============================================================================

// BaseOffset returns the first offset in this segment.
func (s *Segment) BaseOffset() int64 {
	return s.baseOffset
}

// NextOffset returns the next offset that will be assigned.
func (s *Segment) NextOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentOffset
}

// Size returns the current size of the segment in bytes.
func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentPosition
}

// IsFull returns true if the segment has reached max size.
func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentPosition >= MaxSegmentSize
}

// IsReadOnly returns true if the segment is sealed.
func (s *Segment) IsReadOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readonly
}

// MessageCount returns approximate number of messages (nextOffset - baseOffset).
func (s *Segment) MessageCount() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentOffset - s.baseOffset
}

// =============================================================================
// TIME-BASED QUERIES (MILESTONE 14)
// =============================================================================

// ReadFromTimestamp reads messages starting from a given timestamp.
//
// USE CASE:
//   - "Replay from 2 hours ago"
//   - "Get all messages since yesterday midnight"
//
// ALGORITHM:
//  1. Use time index to find offset near timestamp
//  2. Use ReadFrom to read messages from that offset
//  3. Filter messages to only include those >= timestamp (scan forward if needed)
//
// RETURNS:
//   - Messages with timestamp >= startTimestamp
//   - maxMessages limit applies (0 = all)
func (s *Segment) ReadFromTimestamp(startTimestamp int64, maxMessages int) ([]*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	// Use time index to find starting offset
	startOffset, err := s.timeIndex.Lookup(startTimestamp)
	if err != nil {
		// No time index entries or timestamp out of range
		// Start from base offset and filter
		startOffset = s.baseOffset
	}

	// Use regular ReadFrom to get messages
	// Note: We need to unlock before calling ReadFrom (it locks again)
	s.mu.RUnlock()
	messages, err := s.ReadFrom(startOffset, 0) // Get all from start offset
	s.mu.RLock()

	if err != nil {
		return nil, err
	}

	// Filter to only messages >= startTimestamp
	var filtered []*Message
	for _, msg := range messages {
		if msg.Timestamp >= startTimestamp {
			filtered = append(filtered, msg)
			if maxMessages > 0 && len(filtered) >= maxMessages {
				break
			}
		}
	}

	return filtered, nil
}

// ReadTimeRange reads messages in a specific time range [startTime, endTime].
//
// USE CASE:
//   - "Get all messages between 2pm and 3pm"
//   - "Audit: what happened in the last hour?"
//
// RETURNS:
//   - Messages with startTime <= timestamp < endTime
func (s *Segment) ReadTimeRange(startTime, endTime int64, maxMessages int) ([]*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	// Use time index to find offset range
	startOffset, endOffsetHint, err := s.timeIndex.LookupRange(startTime, endTime)
	if err != nil {
		// Fall back to scanning from base
		startOffset = s.baseOffset
		endOffsetHint = -1 // Read to end
	}

	// Determine max offset to read
	maxOffset := s.currentOffset
	if endOffsetHint != -1 && endOffsetHint < maxOffset {
		maxOffset = endOffsetHint
	}

	// Calculate approximate message count
	readCount := 0
	if maxMessages > 0 {
		readCount = maxMessages
	}

	// Read messages in range
	s.mu.RUnlock()
	messages, err := s.ReadFrom(startOffset, readCount)
	s.mu.RLock()

	if err != nil {
		return nil, err
	}

	// Filter to time range
	var filtered []*Message
	for _, msg := range messages {
		if msg.Timestamp >= startTime && msg.Timestamp < endTime {
			filtered = append(filtered, msg)
			if maxMessages > 0 && len(filtered) >= maxMessages {
				break
			}
		}
		// Stop if we've passed endTime
		if msg.Timestamp >= endTime {
			break
		}
	}

	return filtered, nil
}

// GetFirstTimestamp returns the timestamp of the first message in the segment.
// Returns ErrTimestampNotFound if the time index is empty.
func (s *Segment) GetFirstTimestamp() (int64, error) {
	return s.timeIndex.GetFirstTimestamp()
}

// GetLastTimestamp returns the timestamp of the last indexed message.
// Returns ErrTimestampNotFound if the time index is empty.
func (s *Segment) GetLastTimestamp() (int64, error) {
	return s.timeIndex.GetLastTimestamp()
}
