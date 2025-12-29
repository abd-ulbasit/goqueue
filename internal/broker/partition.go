// =============================================================================
// PARTITION - THE UNIT OF PARALLELISM
// =============================================================================
//
// WHAT IS A PARTITION?
// A partition is a totally ordered, immutable sequence of messages. Each topic
// has one or more partitions. Within a partition:
//   - Messages have sequential offsets (0, 1, 2, 3, ...)
//   - Order is guaranteed (read order = write order)
//   - Each message is written exactly once
//
// WHY PARTITIONS?
//
//   1. PARALLELISM
//      - Different partitions can be read/written by different machines
//      - More partitions = more parallelism = higher throughput
//      - Example: 3 partitions → 3 consumers can work in parallel
//
//   2. ORDERING TRADE-OFF
//      - Within partition: STRICT ordering (message A before B always)
//      - Across partitions: NO ordering (partition 1's offset 5 might be
//        before or after partition 2's offset 3)
//
//   3. SCALABILITY
//      - Partitions can be distributed across brokers (nodes)
//      - Add more partitions → spread load across more machines
//
// PARTITION vs LOG:
//   - Log: Storage abstraction (segments, indexes, bytes on disk)
//   - Partition: Business abstraction (offsets, consumers, replication)
//
// =============================================================================

package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// PARTITION STRUCT
// =============================================================================

// Partition represents a single partition within a topic.
type Partition struct {
	topic     string       // topic is the name of the topic this partition belongs to
	id        int          // id is the partition number within the topic (0, 1, 2, ...)
	log       *storage.Log // log is the underlying storage engine
	dir       string       // dir is the directory containing partition data
	mu        sync.RWMutex // mu protects metadata access
	createdAt time.Time    // createdAt is when the partition was created
	closed    bool         // closed tracks if partition is closed
}

// =============================================================================
// PARTITION CREATION & LOADING
// =============================================================================

// NewPartition creates a new partition.
func NewPartition(baseDir string, topic string, id int) (*Partition, error) {
	// Create partition directory
	dir := filepath.Join(baseDir, topic, fmt.Sprintf("%d", id))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}

	// Create log
	log, err := storage.NewLog(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to create log: %w", err)
	}

	return &Partition{
		topic:     topic,
		id:        id,
		log:       log,
		dir:       dir,
		createdAt: time.Now(),
	}, nil
}

// LoadPartition opens an existing partition.
func LoadPartition(baseDir string, topic string, id int) (*Partition, error) {
	dir := filepath.Join(baseDir, topic, fmt.Sprintf("%d", id))

	// Check directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, fmt.Errorf("partition directory not found: %s", dir)
	}

	// Load log
	log, err := storage.LoadLog(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to load log: %w", err)
	}

	return &Partition{
		topic: topic,
		id:    id,
		log:   log,
		dir:   dir,
		// TODO: Can we load actual creation time from file metadata
		createdAt: time.Now(), //  We don't persist creation time yet
	}, nil
}

// =============================================================================
// PRODUCER OPERATIONS
// =============================================================================

// Produce appends a message to the partition and returns the assigned offset.
func (p *Partition) Produce(key, value []byte) (int64, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return 0, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	// Create message
	msg := storage.NewMessage(key, value)

	// Append to log
	offset, err := p.log.Append(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to append message: %w", err)
	}

	return offset, nil
}

// ProduceMessage appends a pre-built message to the partition.
func (p *Partition) ProduceMessage(msg *storage.Message) (int64, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return 0, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	offset, err := p.log.Append(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to append message: %w", err)
	}

	return offset, nil
}

// =============================================================================
// CONSUMER OPERATIONS
// =============================================================================

// Consume reads messages starting from the given offset.
func (p *Partition) Consume(fromOffset int64, maxMessages int) ([]*storage.Message, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	messages, err := p.log.ReadFrom(fromOffset, maxMessages)
	if err != nil {
		return nil, fmt.Errorf("failed to read messages: %w", err)
	}

	return messages, nil
}

// ReadMessage reads a single message at the given offset.
func (p *Partition) ReadMessage(offset int64) (*storage.Message, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	msg, err := p.log.Read(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message at offset %d: %w", offset, err)
	}

	return msg, nil
}

// =============================================================================
// PARTITION METADATA
// =============================================================================

// Topic returns the topic name.
func (p *Partition) Topic() string {
	return p.topic
}

// ID returns the partition number.
func (p *Partition) ID() int {
	return p.id
}

// Name returns the full partition identifier (topic-partition).
func (p *Partition) Name() string {
	return fmt.Sprintf("%s-%d", p.topic, p.id)
}

// EarliestOffset returns the first available offset.
func (p *Partition) EarliestOffset() int64 {
	return p.log.EarliestOffset()
}

// LatestOffset returns the last written offset.
// Returns -1 if partition is empty.
func (p *Partition) LatestOffset() int64 {
	return p.log.LatestOffset()
}

// NextOffset returns the next offset that will be assigned.
func (p *Partition) NextOffset() int64 {
	return p.log.NextOffset()
}

// MessageCount returns the approximate number of messages.
func (p *Partition) MessageCount() int64 {
	earliest := p.log.EarliestOffset()
	next := p.log.NextOffset()
	if next <= earliest {
		return 0
	}
	return next - earliest
}

// Size returns the total size of partition data in bytes.
func (p *Partition) Size() int64 {
	return p.log.Size()
}

// SegmentCount returns the number of log segments.
func (p *Partition) SegmentCount() int {
	return p.log.SegmentCount()
}

// Dir returns the partition directory path.
func (p *Partition) Dir() string {
	return p.dir
}

// =============================================================================
// PARTITION MANAGEMENT
// =============================================================================

// Sync forces all data to be written to disk.
func (p *Partition) Sync() error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	return p.log.Sync()
}

// Close closes the partition, releasing resources.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	if err := p.log.Close(); err != nil {
		return fmt.Errorf("failed to close log: %w", err)
	}

	p.closed = true
	return nil
}

// Delete closes and removes all partition data.
func (p *Partition) Delete() error {
	if err := p.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(p.dir); err != nil {
		return fmt.Errorf("failed to delete partition directory: %w", err)
	}
	return nil
}

// =============================================================================
// RETENTION & CLEANUP
// =============================================================================

// DeleteMessagesBefore removes messages older than the given offset.
func (p *Partition) DeleteMessagesBefore(offset int64) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	return p.log.DeleteSegmentsBefore(offset)
}
