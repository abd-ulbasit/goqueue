// =============================================================================
// WRITE BUFFER - HIGH-THROUGHPUT BATCHING FOR SEGMENT WRITES
// =============================================================================
//
// WHY A WRITE BUFFER?
// Synchronous writes are the #1 performance killer in message queues.
// Every individual write goes through:
//   1. Lock acquisition
//   2. Memory copy
//   3. Buffer flush
//   4. Potential fsync
//
// At 100 bytes/message + overhead, that's maybe 10,000 writes/sec MAX.
// Kafka achieves 1M+ messages/sec by batching.
//
// HOW IT WORKS:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                      WRITE BUFFER ARCHITECTURE                          │
//   │                                                                         │
//   │   Producers                      Buffer                    Disk         │
//   │   ┌─────┐                     ┌─────────────┐           ┌─────────┐     │
//   │   │ P1  │──┐                  │  ┌───────┐  │           │         │     │
//   │   └─────┘  │                  │  │ msg 1 │  │  flush()  │ Segment │     │
//   │   ┌─────┐  │   Enqueue()      │  │ msg 2 │  │ ───────►  │  File   │     │
//   │   │ P2  │──┼───────────────►  │  │ msg 3 │  │  (batch)  │         │     │
//   │   └─────┘  │  (non-blocking)  │  │ ...   │  │           │         │     │
//   │   ┌─────┐  │                  │  └───────┘  │           └─────────┘     │
//   │   │ P3  │──┘                  └─────────────┘                           │
//   │   └─────┘                                                               │
//   │                                                                         │
//   │   FLUSH TRIGGERS:                                                       │
//   │   1. Buffer size reaches threshold (e.g., 1MB)                          │
//   │   2. Message count reaches threshold (e.g., 10,000)                     │
//   │   3. Time since last flush exceeds linger (e.g., 5ms)                   │
//   │   4. Explicit Sync() call for durability                                │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON TO OTHER SYSTEMS:
//
//   | System  | Batching Mechanism           | Default Linger |
//   |---------|------------------------------|----------------|
//   | Kafka   | Producer batching + linger   | 0ms (immediate)|
//   | GoQueue | Write buffer + linger        | 5ms            |
//   | Redis   | AOF rewrite in background    | 1s (everysec)  |
//   | RabbitMQ| Lazy queues, batch publish   | N/A            |
//
// DURABILITY MODES (Like Kafka's acks):
//
//   - ASYNC (acks=0):  Return immediately, flush in background
//                      Risk: Last few messages lost on crash
//                      Throughput: Maximum (millions/sec)
//
//   - LEADER (acks=1): Wait for buffer flush to segment
//                      Risk: Lost if leader crashes before replication
//                      Throughput: High (hundreds of thousands/sec)
//
//   - ALL (acks=all):  Wait for flush + replication quorum
//                      Risk: None (fully durable)
//                      Throughput: Lower (depends on replication)
//
// =============================================================================

package storage

import (
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// CONFIGURATION
// =============================================================================

// WriteBufferConfig controls write batching behavior.
type WriteBufferConfig struct {
	// MaxBufferSize is the maximum bytes before forced flush.
	// Default: 1MB (1048576 bytes)
	// Higher = better throughput, more memory usage
	MaxBufferSize int

	// MaxBatchMessages is the maximum messages before forced flush.
	// Default: 10000 messages
	// Higher = better throughput, higher latency
	MaxBatchMessages int

	// LingerMs is the maximum time to wait before flushing.
	// Default: 5ms
	// Higher = better batching, higher latency
	// Set to 0 for immediate flush (like Kafka linger.ms=0)
	LingerMs int

	// SyncOnFlush controls whether to fsync after each flush.
	// Default: false (rely on OS page cache)
	// Set to true for maximum durability (slower)
	SyncOnFlush bool

	// FlushIntervalMs is how often the background flusher runs.
	// Default: 1ms
	// Only relevant when LingerMs > 0
	FlushIntervalMs int
}

// DefaultWriteBufferConfig returns sensible defaults for high throughput.
func DefaultWriteBufferConfig() WriteBufferConfig {
	return WriteBufferConfig{
		MaxBufferSize:    1024 * 1024, // 1MB
		MaxBatchMessages: 10000,
		LingerMs:         5,     // 5ms linger for batching
		SyncOnFlush:      false, // Rely on OS page cache
		FlushIntervalMs:  1,     // Check every 1ms
	}
}

// HighThroughputConfig optimizes for maximum throughput (some durability risk).
func HighThroughputConfig() WriteBufferConfig {
	return WriteBufferConfig{
		MaxBufferSize:    4 * 1024 * 1024, // 4MB
		MaxBatchMessages: 50000,
		LingerMs:         10, // 10ms linger
		SyncOnFlush:      false,
		FlushIntervalMs:  1,
	}
}

// LowLatencyConfig optimizes for low latency (lower throughput).
func LowLatencyConfig() WriteBufferConfig {
	return WriteBufferConfig{
		MaxBufferSize:    64 * 1024, // 64KB
		MaxBatchMessages: 100,
		LingerMs:         0, // Immediate flush
		SyncOnFlush:      false,
		FlushIntervalMs:  1,
	}
}

// DurableConfig optimizes for durability (fsync on every flush).
func DurableConfig() WriteBufferConfig {
	return WriteBufferConfig{
		MaxBufferSize:    256 * 1024, // 256KB
		MaxBatchMessages: 1000,
		LingerMs:         1,
		SyncOnFlush:      true, // fsync after every flush
		FlushIntervalMs:  1,
	}
}

// =============================================================================
// PENDING WRITE
// =============================================================================

// PendingWrite represents a message waiting to be written.
type PendingWrite struct {
	// Msg is the message to write
	Msg *Message

	// Done is signaled when the write completes (or fails)
	Done chan error

	// Offset will be set after successful write
	Offset int64

	// Size is the encoded size of the message
	Size int
}

// =============================================================================
// WRITE BUFFER
// =============================================================================

// WriteBuffer batches writes for high throughput.
//
// THREAD SAFETY:
//   - Enqueue() is safe to call from multiple goroutines
//   - Flush() is safe to call from multiple goroutines (uses lock)
//   - Internal flusher runs in background
type WriteBuffer struct {
	config WriteBufferConfig

	// pending holds messages waiting to be flushed
	pending []*PendingWrite

	// currentSize tracks bytes in buffer
	currentSize int

	// mu protects pending and currentSize
	mu sync.Mutex

	// segment is the target segment for writes
	segment *Segment

	// lastFlush tracks when we last flushed
	lastFlush time.Time

	// closed indicates buffer is shutting down
	closed atomic.Bool

	// stopCh signals background flusher to stop
	stopCh chan struct{}

	// doneCh signals background flusher has stopped
	doneCh chan struct{}

	// stats for monitoring
	flushCount   atomic.Uint64
	messageCount atomic.Uint64
	byteCount    atomic.Uint64
	lastFlushMs  atomic.Int64
}

// NewWriteBuffer creates a new write buffer for a segment.
func NewWriteBuffer(segment *Segment, config WriteBufferConfig) *WriteBuffer {
	if config.MaxBufferSize == 0 {
		config = DefaultWriteBufferConfig()
	}

	wb := &WriteBuffer{
		config:    config,
		pending:   make([]*PendingWrite, 0, config.MaxBatchMessages),
		segment:   segment,
		lastFlush: time.Now(),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}

	// Start background flusher if linger > 0
	if config.LingerMs > 0 {
		go wb.backgroundFlusher()
	}

	return wb
}

// =============================================================================
// ENQUEUE - ADD MESSAGE TO BUFFER
// =============================================================================

// Enqueue adds a message to the write buffer.
//
// BEHAVIOR:
//   - Message is added to buffer (fast, no I/O)
//   - If buffer is full, triggers flush before adding
//   - Returns offset and error channel
//
// CALLER RESPONSIBILITY:
//   - For acks=0 (async): Ignore returned channel
//   - For acks=1 (leader): Wait on channel for flush completion
//   - For acks=all: Wait on channel, then wait for replication
//
// RETURNS:
//   - PendingWrite with Done channel to wait on
//   - Offset will be set after flush completes
func (wb *WriteBuffer) Enqueue(msg *Message) (*PendingWrite, error) {
	if wb.closed.Load() {
		return nil, ErrSegmentClosed
	}

	// Pre-encode message to know size
	data, err := msg.Encode()
	if err != nil {
		return nil, err
	}
	msgSize := len(data)

	pw := &PendingWrite{
		Msg:  msg,
		Done: make(chan error, 1),
		Size: msgSize,
	}

	wb.mu.Lock()

	// Check if this message would exceed buffer
	// If so, flush first (while holding lock to maintain order)
	if wb.currentSize+msgSize > wb.config.MaxBufferSize ||
		len(wb.pending) >= wb.config.MaxBatchMessages {
		wb.flushLocked()
	}

	// Add to buffer
	wb.pending = append(wb.pending, pw)
	wb.currentSize += msgSize

	// Immediate flush if linger=0
	if wb.config.LingerMs == 0 {
		wb.flushLocked()
	}

	wb.mu.Unlock()

	return pw, nil
}

// =============================================================================
// ENQUEUE BATCH - EFFICIENT MULTI-MESSAGE ADD
// =============================================================================

// EnqueueBatch adds multiple messages to the buffer efficiently.
//
// This is more efficient than calling Enqueue() repeatedly because:
//  1. Single lock acquisition for all messages
//  2. Single flush check/trigger
//  3. Better cache locality
//
// RETURNS: Slice of PendingWrites (same order as input)
func (wb *WriteBuffer) EnqueueBatch(msgs []*Message) ([]*PendingWrite, error) {
	if wb.closed.Load() {
		return nil, ErrSegmentClosed
	}

	if len(msgs) == 0 {
		return nil, nil
	}

	// Pre-encode all messages
	writes := make([]*PendingWrite, len(msgs))
	totalSize := 0

	for i, msg := range msgs {
		data, err := msg.Encode()
		if err != nil {
			return nil, err
		}
		writes[i] = &PendingWrite{
			Msg:  msg,
			Done: make(chan error, 1),
			Size: len(data),
		}
		totalSize += len(data)
	}

	wb.mu.Lock()

	// Flush existing buffer if needed
	if wb.currentSize+totalSize > wb.config.MaxBufferSize {
		wb.flushLocked()
	}

	// Add all messages
	wb.pending = append(wb.pending, writes...)
	wb.currentSize += totalSize

	// For linger=0 or if batch exceeds threshold, flush immediately
	if wb.config.LingerMs == 0 || len(wb.pending) >= wb.config.MaxBatchMessages {
		wb.flushLocked()
	}

	wb.mu.Unlock()

	return writes, nil
}

// =============================================================================
// FLUSH - WRITE BUFFER TO SEGMENT
// =============================================================================

// Flush writes all pending messages to the segment.
// Safe to call from any goroutine.
func (wb *WriteBuffer) Flush() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	return wb.flushLocked()
}

// flushLocked performs the actual flush. Must be called with lock held.
func (wb *WriteBuffer) flushLocked() error {
	if len(wb.pending) == 0 {
		return nil
	}

	// Get all pending writes
	toFlush := wb.pending
	wb.pending = make([]*PendingWrite, 0, wb.config.MaxBatchMessages)
	flushedSize := wb.currentSize
	wb.currentSize = 0

	// Write all messages to segment
	// This is still sequential but now batched
	var writeErr error
	for _, pw := range toFlush {
		if writeErr != nil {
			// Previous write failed, fail remaining
			pw.Done <- writeErr
			close(pw.Done)
			continue
		}

		// Write to segment
		offset, err := wb.segment.Append(pw.Msg)
		if err != nil {
			writeErr = err
			pw.Done <- err
			close(pw.Done)
			continue
		}

		pw.Offset = offset
		pw.Done <- nil
		close(pw.Done)
	}

	// Optional fsync
	if wb.config.SyncOnFlush && writeErr == nil {
		if err := wb.segment.Sync(); err != nil {
			// Already wrote messages, but sync failed
			// Log this but don't fail - data is in OS cache
			writeErr = err
		}
	}

	// Update stats
	wb.flushCount.Add(1)
	wb.messageCount.Add(uint64(len(toFlush)))
	wb.byteCount.Add(uint64(flushedSize))
	wb.lastFlushMs.Store(time.Now().UnixMilli())
	wb.lastFlush = time.Now()

	return writeErr
}

// =============================================================================
// BACKGROUND FLUSHER
// =============================================================================

// backgroundFlusher periodically flushes the buffer based on linger time.
func (wb *WriteBuffer) backgroundFlusher() {
	defer close(wb.doneCh)

	ticker := time.NewTicker(time.Duration(wb.config.FlushIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	lingerDuration := time.Duration(wb.config.LingerMs) * time.Millisecond

	for {
		select {
		case <-wb.stopCh:
			// Final flush before shutdown
			wb.Flush()
			return
		case <-ticker.C:
			wb.mu.Lock()
			if len(wb.pending) > 0 && time.Since(wb.lastFlush) >= lingerDuration {
				wb.flushLocked()
			}
			wb.mu.Unlock()
		}
	}
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Close flushes remaining messages and stops the background flusher.
func (wb *WriteBuffer) Close() error {
	if wb.closed.Swap(true) {
		return nil // Already closed
	}

	// Stop background flusher
	close(wb.stopCh)

	// Wait for flusher to finish (includes final flush)
	if wb.config.LingerMs > 0 {
		<-wb.doneCh
	} else {
		// No background flusher, do final flush here
		wb.Flush()
	}

	return nil
}

// =============================================================================
// STATS
// =============================================================================

// WriteBufferStats contains buffer statistics.
type WriteBufferStats struct {
	FlushCount   uint64
	MessageCount uint64
	ByteCount    uint64
	LastFlushMs  int64
	PendingCount int
	PendingBytes int
}

// Stats returns current buffer statistics.
func (wb *WriteBuffer) Stats() WriteBufferStats {
	wb.mu.Lock()
	pendingCount := len(wb.pending)
	pendingBytes := wb.currentSize
	wb.mu.Unlock()

	return WriteBufferStats{
		FlushCount:   wb.flushCount.Load(),
		MessageCount: wb.messageCount.Load(),
		ByteCount:    wb.byteCount.Load(),
		LastFlushMs:  wb.lastFlushMs.Load(),
		PendingCount: pendingCount,
		PendingBytes: pendingBytes,
	}
}

// =============================================================================
// SEGMENT INTEGRATION
// =============================================================================

// SetSegment updates the target segment (for rollover).
func (wb *WriteBuffer) SetSegment(segment *Segment) {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// Flush to old segment first
	if len(wb.pending) > 0 {
		wb.flushLocked()
	}

	wb.segment = segment
}
