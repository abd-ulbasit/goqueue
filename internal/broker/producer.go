// =============================================================================
// PRODUCER - CLIENT-SIDE MESSAGE BATCHING
// =============================================================================
//
// WHAT IS A PRODUCER?
// A producer is the client-side component that sends messages to the broker.
// It's more than just a simple "send" - it handles:
//   - Batching (grouping messages for efficiency)
//   - Partitioning (routing messages to correct partitions)
//   - Buffering (smoothing out traffic spikes)
//   - Acknowledgments (confirming delivery)
//
// WHY BATCHING?
//
//   Without batching (message-at-a-time):
//   ┌──────────────────────────────────────────────────────────────────────────┐
//   │  Producer                                    Broker                      │
//   │     │                                          │                         │
//   │     │──── msg1 ──────────────────────────────►│  1 network round-trip    │
//   │     │◄─── ack1 ───────────────────────────────│                          │
//   │     │──── msg2 ──────────────────────────────►│  1 network round-trip    │
//   │     │◄─── ack2 ───────────────────────────────│                          │
//   │     │──── msg3 ──────────────────────────────►│  1 network round-trip    │
//   │     │◄─── ack3 ───────────────────────────────│                          │
//   │                                                                          │
//   │  3 messages = 3 round-trips = HIGH LATENCY                               │
//   │  Each write = 1 disk fsync = HIGH I/O                                    │
//   └──────────────────────────────────────────────────────────────────────────┘
//
//   With batching:
//   ┌──────────────────────────────────────────────────────────────────────────┐
//   │  Producer                                    Broker                      │
//   │     │                                          │                         │
//   │     │ (accumulate msg1, msg2, msg3 locally)    │                         │
//   │     │                                          │                         │
//   │     │──── [msg1, msg2, msg3] ─────────────────►│  1 network round-trip   │
//   │     │◄─── [ack1, ack2, ack3] ──────────────────│                         │
//   │                                                                          │
//   │  3 messages = 1 round-trip = LOW LATENCY                                 │
//   │  Single batch write = 1 disk fsync = LOW I/O                             │
//   └──────────────────────────────────────────────────────────────────────────┘
//
// BATCHING TRADEOFFS:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │             LATENCY vs THROUGHPUT SPECTRUM                              │
//   │                                                                         │
//   │   ◄──────────────────────────────────────────────────────────────────►  │
//   │   Low Latency                                          High Throughput  │
//   │   (Small batches)                                      (Large batches)  │
//   │                                                                         │
//   │   BatchSize=1          BatchSize=100           BatchSize=10000          │
//   │   LingerMs=0           LingerMs=5ms            LingerMs=100ms           │
//   │                                                                         │
//   │   - Immediate delivery  - Balanced             - Maximum throughput     │
//   │   - Low throughput      - Good for most        - High latency           │
//   │   - High CPU/IO         - Production default   - Memory pressure        │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// FLUSH TRIGGERS - When does a batch get sent?
//
//   1. BATCH SIZE (count)
//      "Flush when we have N messages"
//      - Ensures batches don't grow unboundedly
//      - Good for steady message streams
//
//   2. LINGER TIME (milliseconds)
//      "Flush after N ms even if batch isn't full"
//      - Provides latency guarantee
//      - Prevents messages sitting forever in low-traffic periods
//
//   3. BATCH BYTES (size)
//      "Flush when batch reaches N bytes"
//      - Prevents memory bloat from large messages
//      - Matches network MTU or broker limits
//
//   Batch flushes when ANY trigger fires (first one wins).
//
// COMPARISON - How other systems batch:
//
//   Kafka Producer:
//     - batch.size (bytes, default 16KB)
//     - linger.ms (time, default 0ms - immediate)
//     - buffer.memory (total memory for all batches)
//     - Sticky partitioner (stay on partition until batch full)
//
//   RabbitMQ:
//     - No client batching (channel.basicPublish is immediate)
//     - Server-side: confirm.select for batch acks
//
//   SQS:
//     - SendMessageBatch API (up to 10 messages, 256KB total)
//     - Client batches manually
//
// ACKNOWLEDGMENT MODES:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                      ACKNOWLEDGMENT MODES                               │
//   │                                                                         │
//   │   AckNone (Fire-and-Forget)                                             │
//   │   ─────────────────────────                                             │
//   │   Producer ───► Broker (no response waited)                             │
//   │   - Fastest (no round-trip)                                             │
//   │   - No delivery guarantee                                               │
//   │   - USE FOR: Metrics, logs, non-critical events                         │
//   │                                                                         │
//   │   AckLeader                                                             │
//   │   ──────────                                                            │
//   │   Producer ───► Broker ───► Leader writes to disk ───► ACK              │
//   │   - Medium latency                                                      │
//   │   - Survives leader restart                                             │
//   │   - USE FOR: Most production workloads                                  │
//   │                                                                         │
//   │   AckAll                                                                │
//   │   ──────                                                                │
//   │   Producer ───► Broker ───► Leader + Followers write ───► ACK           │
//   │   - Highest latency                                                     │
//   │   - Survives any single node failure                                    │
//   │   - USE FOR: Financial transactions, critical data                      │
//   │                                                                         │
//   │   NOTE: Until we implement replication (M11), AckLeader = AckAll        │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// =============================================================================
// ACK MODE ENUM
// =============================================================================

// AckMode specifies the acknowledgment behavior for produced messages.
type AckMode int

const (
	// AckNone means don't wait for any acknowledgment (fire-and-forget).
	// Fastest but no delivery guarantee.
	AckNone AckMode = iota

	// AckLeader means wait for the partition leader to acknowledge.
	// Message is durable on leader's disk.
	AckLeader

	// AckAll means wait for all in-sync replicas to acknowledge.
	// Strongest durability guarantee.
	// NOTE: Until replication is implemented, AckAll = AckLeader
	AckAll
)

func (m AckMode) String() string {
	switch m {
	case AckNone:
		return "none"
	case AckLeader:
		return "leader"
	case AckAll:
		return "all"
	default:
		return "unknown"
	}
}

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrProducerClosed is returned when operating on a closed producer
	ErrProducerClosed = errors.New("producer is closed")

	// ErrBatchFull is returned when the batch accumulator is at capacity
	ErrBatchFull = errors.New("batch accumulator is full")

	// ErrFlushTimeout is returned when flush doesn't complete in time
	ErrFlushTimeout = errors.New("flush timeout")
)

// =============================================================================
// PRODUCER CONFIGURATION
// =============================================================================

// ProducerConfig holds producer configuration options.
type ProducerConfig struct {
	// Topic is the target topic for this producer.
	// A producer is bound to a single topic (like Kafka).
	Topic string

	// BatchSize is the maximum number of messages per batch.
	// When reached, batch is flushed immediately.
	// Default: 100
	BatchSize int

	// LingerMs is the maximum time to wait before flushing a non-empty batch.
	// This provides a latency upper bound.
	// Default: 5ms
	// Set to 0 for immediate flush (no batching).
	LingerMs int

	// BatchBytes is the maximum size of a batch in bytes.
	// Prevents memory bloat from accumulating large messages.
	// Default: 64KB
	BatchBytes int

	// BufferSize is the maximum number of messages that can be queued.
	// If exceeded, Send() will block or return error based on BlockOnFull.
	// Default: 10000
	BufferSize int

	// BlockOnFull determines behavior when buffer is full.
	// true: Send() blocks until space available
	// false: Send() returns ErrBatchFull immediately
	// Default: true
	BlockOnFull bool

	// AckMode determines acknowledgment behavior.
	// Default: AckLeader
	AckMode AckMode

	// Partitioner determines which partition messages go to.
	// Default: HashPartitioner (key-based routing)
	Partitioner Partitioner

	// FlushTimeout is the maximum time to wait for a flush to complete.
	// Default: 30 seconds
	FlushTimeout time.Duration
}

// DefaultProducerConfig returns sensible defaults for most use cases.
//
// These defaults balance latency and throughput:
//   - Small enough batches for reasonable latency (~5ms max)
//   - Large enough to amortize network overhead
func DefaultProducerConfig(topic string) ProducerConfig {
	return ProducerConfig{
		Topic:        topic,
		BatchSize:    100,       // Flush after 100 messages
		LingerMs:     5,         // Flush after 5ms max latency
		BatchBytes:   64 * 1024, // Flush after 64KB
		BufferSize:   10000,     // Queue up to 10K messages
		BlockOnFull:  true,      // Block when buffer full
		AckMode:      AckLeader, // Wait for leader ack
		Partitioner:  nil,       // Will use default (hash)
		FlushTimeout: 30 * time.Second,
	}
}

// =============================================================================
// PRODUCER RECORD
// =============================================================================

// ProducerRecord is a message to be sent by the producer.
type ProducerRecord struct {
	// Key is the routing key (determines partition).
	// nil key = round-robin distribution.
	Key []byte

	// Value is the message payload.
	Value []byte

	// Partition is an optional explicit partition override.
	// If >= 0, bypasses partitioner and routes directly.
	// Default: -1 (use partitioner)
	Partition int

	// resultCh receives the result after the message is sent.
	// This is set internally by the producer.
	resultCh chan ProducerResult
}

// ProducerResult is the result of sending a message.
type ProducerResult struct {
	// Topic is the topic the message was sent to
	Topic string

	// Partition is which partition received the message
	Partition int

	// Offset is the offset assigned to the message
	Offset int64

	// Timestamp is when the message was persisted
	Timestamp time.Time

	// Error is non-nil if the send failed
	Error error
}

// =============================================================================
// PRODUCER STRUCT
// =============================================================================

// Producer sends messages to a topic with batching and acknowledgments.
//
// USAGE:
//
//	producer, err := NewProducer(broker, DefaultProducerConfig("orders"))
//	if err != nil { ... }
//	defer producer.Close()
//
//	// Async send (returns immediately, result via callback)
//	producer.Send(ProducerRecord{Key: []byte("user-123"), Value: payload})
//
//	// Sync send (blocks until acknowledged)
//	result := producer.SendSync(ctx, ProducerRecord{...})
//
//	// Flush any pending messages
//	producer.Flush(ctx)
//
// INTERNAL ARCHITECTURE:
//
//	┌──────────────────────────────────────────────────────────────────────────┐
//	│                            PRODUCER                                      │
//	│                                                                          │
//	│   Send() ──►┌─────────────┐                                              │
//	│             │   Records   │  (buffered channel)                          │
//	│   Send() ──►│   Channel   │                                              │
//	│             └──────┬──────┘                                              │
//	│                    │                                                     │
//	│                    ▼                                                     │
//	│   ┌────────────────────────────────────────────────────────────────┐     │
//	│   │                    ACCUMULATOR GOROUTINE                       │     │
//	│   │                                                                │     │
//	│   │   Receives records, groups by partition:                       │     │
//	│   │                                                                │     │
//	│   │   Partition 0: [msg, msg, msg]  ──┐                            │     │
//	│   │   Partition 1: [msg, msg]       ──┼── Flush triggers:          │     │
//	│   │   Partition 2: [msg]            ──┘   - BatchSize reached      │     │
//	│   │                                       - LingerMs elapsed       │     │
//	│   │                                       - BatchBytes exceeded    │     │
//	│   │                                       - Explicit Flush()       │     │
//	│   └────────────────────────────────────────────────────────────────┘     │
//	│                    │                                                     │
//	│                    ▼                                                     │
//	│   ┌────────────────────────────────────────────────────────────────┐     │
//	│   │                         BROKER                                 │     │
//	│   │   Receives batched writes per partition                        │     │
//	│   └────────────────────────────────────────────────────────────────┘     │
//	│                                                                          │
//	└──────────────────────────────────────────────────────────────────────────┘
type Producer struct {
	// config holds producer configuration
	config ProducerConfig

	// broker is the target broker
	broker *Broker

	// topic is cached for quick access
	topic *Topic

	// partitioner routes messages to partitions
	partitioner Partitioner

	// records is the channel for incoming messages
	records chan *ProducerRecord

	// batches holds per-partition accumulators
	// Key: partition number
	batches map[int]*batch

	// batchMu protects batches map
	batchMu sync.Mutex

	// wg waits for background goroutines
	wg sync.WaitGroup

	// closed signals shutdown
	closed bool

	// closeCh signals goroutines to stop
	closeCh chan struct{}

	// flushCh triggers immediate flush
	flushCh chan chan error

	// mu protects closed flag
	mu sync.RWMutex

	// stats for monitoring
	stats   ProducerStats
	statsMu sync.RWMutex
}

// ProducerStats holds producer metrics.
type ProducerStats struct {
	MessagesSent   int64
	MessagesAcked  int64
	MessagesFailed int64
	BatchesSent    int64
	BytesSent      int64
	AvgBatchSize   float64
	LastFlushTime  time.Time
	LastError      error
	LastErrorTime  time.Time
}

// batch accumulates messages for a single partition.
type batch struct {
	partition int
	records   []*ProducerRecord
	bytes     int
	createdAt time.Time
}

// =============================================================================
// PRODUCER LIFECYCLE
// =============================================================================

// NewProducer creates a new producer for the given broker and configuration.
//
// INITIALIZATION:
//  1. Validate configuration
//  2. Get topic reference from broker
//  3. Set up partitioner
//  4. Start accumulator goroutine
//  5. Start linger timer goroutine
func NewProducer(broker *Broker, config ProducerConfig) (*Producer, error) {
	// Validate configuration
	if config.Topic == "" {
		return nil, errors.New("topic is required")
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.LingerMs < 0 {
		config.LingerMs = 0
	}
	if config.BatchBytes <= 0 {
		config.BatchBytes = 64 * 1024
	}
	if config.BufferSize <= 0 {
		config.BufferSize = 10000
	}
	if config.FlushTimeout <= 0 {
		config.FlushTimeout = 30 * time.Second
	}

	// Get topic from broker
	topic, err := broker.GetTopic(config.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic: %w", err)
	}

	// Set up partitioner
	partitioner := config.Partitioner
	if partitioner == nil {
		partitioner = DefaultPartitioner
	}

	p := &Producer{
		config:      config,
		broker:      broker,
		topic:       topic,
		partitioner: partitioner,
		records:     make(chan *ProducerRecord, config.BufferSize),
		batches:     make(map[int]*batch),
		closeCh:     make(chan struct{}),
		flushCh:     make(chan chan error, 1),
	}

	// Start the accumulator goroutine
	p.wg.Add(1)
	go p.accumulatorLoop()

	return p, nil
}

// =============================================================================
// SEND OPERATIONS
// =============================================================================

// Send queues a message for sending asynchronously.
//
// BEHAVIOR:
//   - Message is queued in the records channel
//   - Returns immediately (non-blocking if buffer has space)
//   - If BlockOnFull=true and buffer full, blocks until space available
//   - If BlockOnFull=false and buffer full, returns ErrBatchFull
//
// RESULT:
//   - Set record.resultCh to receive the result when available
//   - Or use SendSync() for synchronous sends
func (p *Producer) Send(record ProducerRecord) error {
	return p.sendPtr(&record)
}

// SendSync sends a message and waits for acknowledgment.
//
// PARAMETERS:
//   - ctx: Context for cancellation/timeout
//   - record: Message to send
//
// RETURNS:
//   - ProducerResult with partition, offset, and any error
//
// USAGE:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	result := producer.SendSync(ctx, ProducerRecord{Key: key, Value: value})
//	if result.Error != nil { ... }
func (p *Producer) SendSync(ctx context.Context, record ProducerRecord) ProducerResult {
	// Create result channel
	record.resultCh = make(chan ProducerResult, 1)

	// Send the record - we must pass by pointer so the accumulator sees our resultCh
	if err := p.sendPtr(&record); err != nil {
		return ProducerResult{Error: err}
	}

	// Wait for result
	select {
	case result := <-record.resultCh:
		return result
	case <-ctx.Done():
		return ProducerResult{Error: ctx.Err()}
	case <-p.closeCh:
		return ProducerResult{Error: ErrProducerClosed}
	}
}

// sendPtr is the internal send that takes a pointer (for SendSync).
func (p *Producer) sendPtr(record *ProducerRecord) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrProducerClosed
	}
	p.mu.RUnlock()

	// Initialize result channel if caller wants result
	if record.resultCh == nil {
		record.resultCh = make(chan ProducerResult, 1)
	}

	// Default partition to -1 (use partitioner)
	if record.Partition == 0 && record.Key == nil && record.Value != nil {
		record.Partition = -1
	}

	// Send to accumulator
	if p.config.BlockOnFull {
		select {
		case p.records <- record:
			return nil
		case <-p.closeCh:
			return ErrProducerClosed
		}
	} else {
		select {
		case p.records <- record:
			return nil
		default:
			return ErrBatchFull
		}
	}
}

// SendBatch sends multiple messages efficiently.
//
// USAGE:
//
//	records := []ProducerRecord{
//	    {Key: []byte("k1"), Value: []byte("v1")},
//	    {Key: []byte("k2"), Value: []byte("v2")},
//	}
//	errors := producer.SendBatch(records)
func (p *Producer) SendBatch(records []ProducerRecord) []error {
	errs := make([]error, len(records))
	for i := range records {
		errs[i] = p.Send(records[i])
	}
	return errs
}

// =============================================================================
// FLUSH OPERATIONS
// =============================================================================

// Flush forces all pending messages to be sent immediately.
//
// BEHAVIOR:
//  1. Signals accumulator to flush all batches
//  2. Waits for flush to complete (up to FlushTimeout)
//  3. Returns any error from the flush
//
// USE CASES:
//   - Before closing producer (ensure all messages sent)
//   - At transaction boundaries
//   - When low latency is temporarily needed
func (p *Producer) Flush(ctx context.Context) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrProducerClosed
	}
	p.mu.RUnlock()

	// Create response channel
	errCh := make(chan error, 1)

	// Request flush
	select {
	case p.flushCh <- errCh:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closeCh:
		return ErrProducerClosed
	}

	// Wait for response
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closeCh:
		return ErrProducerClosed
	}
}

// =============================================================================
// ACCUMULATOR LOOP
// =============================================================================
//
// The accumulator is the heart of the producer. It:
// 1. Receives messages from the records channel
// 2. Routes them to partition-specific batches
// 3. Flushes batches when triggers fire
//
// TRIGGER EVALUATION:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                     FLUSH TRIGGER EVALUATION                            │
//   │                                                                         │
//   │   For each partition batch:                                             │
//   │                                                                         │
//   │   ┌─────────────────┐                                                   │
//   │   │ len(batch) >=   │──YES──► FLUSH                                     │
//   │   │ BatchSize?      │                                                   │
//   │   └────────┬────────┘                                                   │
//   │            │ NO                                                         │
//   │            ▼                                                            │
//   │   ┌─────────────────┐                                                   │
//   │   │ batch.bytes >=  │──YES──► FLUSH                                     │
//   │   │ BatchBytes?     │                                                   │
//   │   └────────┬────────┘                                                   │
//   │            │ NO                                                         │
//   │            ▼                                                            │
//   │   ┌─────────────────┐                                                   │
//   │   │ time.Since(     │──YES──► FLUSH                                     │
//   │   │ created) >=     │                                                   │
//   │   │ LingerMs?       │                                                   │
//   │   └────────┬────────┘                                                   │
//   │            │ NO                                                         │
//   │            ▼                                                            │
//   │        WAIT FOR MORE                                                    │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//

func (p *Producer) accumulatorLoop() {
	defer p.wg.Done()

	// Linger timer - fires every LingerMs to check for stale batches
	var lingerTimer *time.Timer
	var lingerCh <-chan time.Time

	if p.config.LingerMs > 0 {
		lingerTimer = time.NewTimer(time.Duration(p.config.LingerMs) * time.Millisecond)
		lingerCh = lingerTimer.C
		defer lingerTimer.Stop()
	}

	for {
		// =====================================================================
		// PRIORITY HANDLING
		// =====================================================================
		// We need to ensure records are processed before flush/close operations.
		// Using nested selects: first try to drain records, then handle others.
		//
		// WHY: Go's select is non-deterministic when multiple cases are ready.
		// Without priority, a flush request might be handled before all queued
		// records are processed, leading to data loss or race conditions.
		// =====================================================================

		// First, drain any pending records (high priority)
	drainLoop:
		for {
			select {
			case record := <-p.records:
				p.addToBatch(record)
			default:
				// No more records immediately available
				break drainLoop
			}
		}

		// Now handle flush, close, timer, or wait for more records
		select {
		case <-p.closeCh:
			// Drain any remaining records before shutdown
		drainOnClose:
			for {
				select {
				case record := <-p.records:
					p.addToBatch(record)
				default:
					break drainOnClose
				}
			}
			// Shutdown: flush remaining batches
			_ = p.flushAllBatches()
			return

		case record := <-p.records:
			// New record: add to appropriate batch
			p.addToBatch(record)

		case <-lingerCh:
			// Linger timer fired: flush stale batches
			p.flushStaleBatches()
			// Reset timer
			if lingerTimer != nil {
				lingerTimer.Reset(time.Duration(p.config.LingerMs) * time.Millisecond)
			}

		case errCh := <-p.flushCh:
			// Drain any remaining records before flushing
		drainBeforeFlush:
			for {
				select {
				case record := <-p.records:
					p.addToBatch(record)
				default:
					break drainBeforeFlush
				}
			}
			// Explicit flush requested
			err := p.flushAllBatches()
			errCh <- err
		}
	}
}

// addToBatch adds a record to its partition's batch.
func (p *Producer) addToBatch(record *ProducerRecord) {
	// Determine partition
	partition := record.Partition
	if partition < 0 {
		partition = p.partitioner.Partition(
			record.Key,
			record.Value,
			p.topic.NumPartitions(),
		)
	}

	p.batchMu.Lock()
	defer p.batchMu.Unlock()

	// Get or create batch for this partition
	b, exists := p.batches[partition]
	if !exists {
		b = &batch{
			partition: partition,
			records:   make([]*ProducerRecord, 0, p.config.BatchSize),
			createdAt: time.Now(),
		}
		p.batches[partition] = b
	}

	// Add record to batch
	b.records = append(b.records, record)
	b.bytes += len(record.Key) + len(record.Value)

	// Update stats
	p.statsMu.Lock()
	p.stats.MessagesSent++
	p.stats.BytesSent += int64(len(record.Key) + len(record.Value))
	p.statsMu.Unlock()

	// Check if batch should be flushed
	// LingerMs=0 means "no batching, flush immediately" (Kafka semantics)
	shouldFlush := p.config.LingerMs == 0 ||
		len(b.records) >= p.config.BatchSize ||
		b.bytes >= p.config.BatchBytes

	if shouldFlush {
		_ = p.flushBatchLocked(partition)
	}
}

// flushStaleBatches flushes batches that have exceeded LingerMs.
func (p *Producer) flushStaleBatches() {
	p.batchMu.Lock()
	defer p.batchMu.Unlock()

	lingerDuration := time.Duration(p.config.LingerMs) * time.Millisecond

	for partition, b := range p.batches {
		if len(b.records) > 0 && time.Since(b.createdAt) >= lingerDuration {
			_ = p.flushBatchLocked(partition)
		}
	}
}

// flushAllBatches flushes all non-empty batches.
func (p *Producer) flushAllBatches() error {
	p.batchMu.Lock()
	defer p.batchMu.Unlock()

	var lastErr error
	for partition := range p.batches {
		if err := p.flushBatchLocked(partition); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// flushBatchLocked flushes a single partition's batch.
// MUST be called with batchMu held.
func (p *Producer) flushBatchLocked(partition int) error {
	b, exists := p.batches[partition]
	if !exists || len(b.records) == 0 {
		return nil
	}

	// Extract records and reset batch
	records := b.records
	delete(p.batches, partition)

	// Write batch to broker
	// This is where we'd implement different ack modes
	var batchErr error
	for _, record := range records {
		offset, err := p.topic.PublishToPartition(partition, record.Key, record.Value)

		result := ProducerResult{
			Topic:     p.config.Topic,
			Partition: partition,
			Offset:    offset,
			Timestamp: time.Now(),
			Error:     err,
		}

		if err != nil {
			batchErr = err
			p.statsMu.Lock()
			p.stats.MessagesFailed++
			p.stats.LastError = err
			p.stats.LastErrorTime = time.Now()
			p.statsMu.Unlock()
		} else {
			p.statsMu.Lock()
			p.stats.MessagesAcked++
			p.statsMu.Unlock()
		}

		// Send result to caller if channel exists
		if record.resultCh != nil {
			select {
			case record.resultCh <- result:
			default:
				// Channel full, drop result (caller not listening)
			}
		}
	}

	// Update stats
	p.statsMu.Lock()
	p.stats.BatchesSent++
	p.stats.LastFlushTime = time.Now()
	if p.stats.BatchesSent > 0 {
		p.stats.AvgBatchSize = float64(p.stats.MessagesAcked) / float64(p.stats.BatchesSent)
	}
	p.statsMu.Unlock()

	return batchErr
}

// =============================================================================
// PRODUCER LIFECYCLE
// =============================================================================

// Close shuts down the producer gracefully.
//
// SHUTDOWN PROCESS:
//  1. Stop accepting new messages
//  2. Flush all pending batches
//  3. Wait for background goroutines to finish
//  4. Release resources
func (p *Producer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	// Signal shutdown
	close(p.closeCh)

	// Wait for accumulator to finish
	p.wg.Wait()

	return nil
}

// Stats returns current producer statistics.
func (p *Producer) Stats() ProducerStats {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	return p.stats
}

// Config returns the producer configuration.
func (p *Producer) Config() ProducerConfig {
	return p.config
}
