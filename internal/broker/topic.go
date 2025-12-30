// =============================================================================
// TOPIC - LOGICAL MESSAGE STREAM
// =============================================================================
//
// WHAT IS A TOPIC?
// A topic is a named category or feed to which messages are published. Think:
//   - "orders" topic: All order-related events
//   - "user-signups" topic: All new user registration events
//   - "payment-failures" topic: All failed payment notifications
//
// Producers publish to topics, consumers subscribe to topics.
//
// TOPIC vs PARTITION:
//   - Topic: Logical grouping (by business domain)
//   - Partition: Physical distribution (for parallelism)
//
// A topic has ONE or more partitions:
//
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │                          Topic: "orders"                               │
//   │                                                                        │
//   │   ┌────────────────┐  ┌────────────────┐  ┌────────────────┐          │
//   │   │  Partition 0   │  │  Partition 1   │  │  Partition 2   │          │
//   │   │  orders-0      │  │  orders-1      │  │  orders-2      │          │
//   │   │                │  │                │  │                │          │
//   │   │  [msg][msg]... │  │  [msg][msg]... │  │  [msg][msg]... │          │
//   │   └────────────────┘  └────────────────┘  └────────────────┘          │
//   │                                                                        │
//   │   Producer writes:                                                     │
//   │     - With key "user-123" → always goes to partition 1 (hash mod 3)   │
//   │     - With key "user-456" → always goes to partition 0               │
//   │     - Without key → round-robin across partitions                     │
//   │                                                                        │
//   └────────────────────────────────────────────────────────────────────────┘
//
// WHY TOPICS?
//
//   1. ORGANIZATION
//      - Group related messages logically
//      - Different teams/services subscribe to relevant topics
//
//   2. ACCESS CONTROL
//      - Grant permissions at topic level
//      - "Service A can publish to 'orders', Service B can only read"
//
//   3. RETENTION POLICIES
//      - Configure differently per topic
//      - "Keep orders for 7 days, logs for 24 hours"
//
// MILESTONE 1 SIMPLIFICATION:
// For M1, topics have exactly ONE partition. Multi-partition topics
// come in Milestone 2 (Topics, Partitions & Producers).
//
// =============================================================================

package broker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrTopicClosed means operations attempted on closed topic
	ErrTopicClosed = errors.New("topic is closed")

	// ErrTopicExists means trying to create a topic that already exists
	ErrTopicExists = errors.New("topic already exists")

	// ErrTopicNotFound means the topic doesn't exist
	ErrTopicNotFound = errors.New("topic not found")
)

// =============================================================================
// TOPIC CONFIGURATION
// =============================================================================

// TopicConfig holds configuration for a topic.
// This will be expanded in future milestones.
type TopicConfig struct {
	// Name is the topic identifier
	Name string

	// NumPartitions is how many partitions to create
	// For M1, this is always 1
	NumPartitions int

	// RetentionHours is how long to keep messages (0 = forever)
	RetentionHours int

	// RetentionBytes is max size per partition (0 = unlimited)
	RetentionBytes int64
}

// DefaultTopicConfig returns default configuration.
//
// PARTITION COUNT GUIDANCE:
//   - 1 partition: Simple use cases, strict ordering needed
//   - 3 partitions: Small workloads, good starting point
//   - 6-12 partitions: Medium workloads
//   - 50+ partitions: High-throughput systems
//
// Rule of thumb: partitions >= max expected consumers for parallelism
func DefaultTopicConfig(name string) TopicConfig {
	return TopicConfig{
		Name:           name,
		NumPartitions:  3,   // M2: default to 3 partitions for parallelism
		RetentionHours: 168, // 7 days
		RetentionBytes: 0,   // unlimited
	}
}

// =============================================================================
// TOPIC STRUCT
// =============================================================================

// Topic represents a logical message stream with one or more partitions.
//
// MILESTONE 2: Full multi-partition support with:
//   - Murmur3 hash-based partitioning (same key → same partition)
//   - Round-robin for null keys
//   - Explicit partition selection
type Topic struct {
	// config holds topic configuration
	config TopicConfig

	// partitions holds all partitions (M1: just one)
	// In future: partitions[0], partitions[1], etc.
	partitions []*Partition

	// baseDir is where partition data is stored
	baseDir string

	// mu protects concurrent access
	mu sync.RWMutex

	// createdAt is when the topic was created
	createdAt time.Time

	// closed tracks if topic is closed
	closed bool

	// roundRobinCounter for messages without keys
	roundRobinCounter uint64
}

// =============================================================================
// TOPIC CREATION & LOADING
// =============================================================================

// NewTopic creates a new topic with the given configuration.
//
// PARAMETERS:
//   - baseDir: Base directory for data (e.g., "/var/lib/goqueue/logs")
//   - config: Topic configuration
//
// CREATES:
//   - Directory: baseDir/{topicName}/
//   - Partitions: baseDir/{topicName}/0/, baseDir/{topicName}/1/, etc.
func NewTopic(baseDir string, config TopicConfig) (*Topic, error) {
	// Create topic directory
	topicDir := filepath.Join(baseDir, config.Name)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create topic directory: %w", err)
	}

	// Create partitions
	partitions := make([]*Partition, config.NumPartitions)
	for i := 0; i < config.NumPartitions; i++ {
		partition, err := NewPartition(baseDir, config.Name, i)
		if err != nil {
			// Cleanup already created partitions
			for j := 0; j < i; j++ {
				partitions[j].Delete()
			}
			return nil, fmt.Errorf("failed to create partition %d: %w", i, err)
		}
		partitions[i] = partition
	}

	return &Topic{
		config:     config,
		partitions: partitions,
		baseDir:    baseDir,
		createdAt:  time.Now(),
	}, nil
}

// LoadTopic opens an existing topic.
//
// DISCOVERY:
// We discover partitions by looking for numbered directories:
//   - baseDir/{topicName}/0/
//   - baseDir/{topicName}/1/
//   - etc.

func LoadTopic(baseDir string, name string) (*Topic, error) {
	topicDir := filepath.Join(baseDir, name)

	// Check topic exists
	stat, err := os.Stat(topicDir)
	if os.IsNotExist(err) {
		return nil, ErrTopicNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to stat topic directory: %w", err)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("topic path is not a directory: %s", topicDir)
	}

	// Discover partitions
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read topic directory: %w", err)
	}

	var partitionIDs []int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		var id int
		if _, err := fmt.Sscanf(entry.Name(), "%d", &id); err == nil {
			partitionIDs = append(partitionIDs, id)
		}
	}

	if len(partitionIDs) == 0 {
		return nil, fmt.Errorf("no partitions found for topic %s", name)
	}

	// Load partitions
	partitions := make([]*Partition, len(partitionIDs))
	for i, id := range partitionIDs {
		partition, err := LoadPartition(baseDir, name, id)
		if err != nil {
			// Close already loaded
			for j := 0; j < i; j++ {
				partitions[j].Close()
			}
			return nil, fmt.Errorf("failed to load partition %d: %w", id, err)
		}
		partitions[id] = partition
	}

	return &Topic{
		config: TopicConfig{
			Name:          name,
			NumPartitions: len(partitions),
		},
		partitions: partitions,
		baseDir:    baseDir,
		createdAt:  time.Now(), // Don't have persisted creation time yet
	}, nil
}

// =============================================================================
// PRODUCER OPERATIONS
// =============================================================================

// Publish writes a message to the topic.
//
// PARAMETERS:
//   - key: Routing key (determines partition). nil = round-robin.
//   - value: Message payload
//
// PARTITION ROUTING:
//   - If key is provided: hash(key) mod numPartitions
//   - If key is nil: round-robin across partitions
//
// RETURNS:
//   - Partition number message was written to
//   - Offset within that partition
//   - Error if write fails
//
// MILESTONE 1 NOTE:
// With only 1 partition, routing is trivial - everything goes to partition 0.
// But we implement the full interface for forward compatibility.
func (t *Topic) Publish(key, value []byte) (partition int, offset int64, err error) {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return 0, 0, ErrTopicClosed
	}
	numPartitions := len(t.partitions)
	t.mu.RUnlock()

	// Determine target partition
	if key != nil {
		// Hash-based routing for messages with keys
		partition = t.hashPartition(key, numPartitions)
	} else {
		// Round-robin for messages without keys
		t.mu.Lock()
		partition = int(t.roundRobinCounter % uint64(numPartitions))
		t.roundRobinCounter++
		t.mu.Unlock()
	}

	// Write to partition
	offset, err = t.partitions[partition].Produce(key, value)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to publish to partition %d: %w", partition, err)
	}

	return partition, offset, nil
}

// hashPartition computes target partition from key using murmur3 hashing.
//
// WHY MURMUR3?
// Murmur3 provides excellent distribution properties:
//   - Fast: ~3 bytes/cycle on modern CPUs
//   - Low collision rate across partition counts
//   - Same algorithm as Kafka (compatibility)
//
// IMPORTANT: This must match the Producer's partitioner for consistency!
func (t *Topic) hashPartition(key []byte, numPartitions int) int {
	// Use the same murmur3 implementation as the Partitioner
	return DefaultPartitioner.Partition(key, nil, numPartitions)
}

// PublishToPartition writes directly to a specific partition.
// Use with caution - bypasses routing logic.
func (t *Topic) PublishToPartition(partition int, key, value []byte) (int64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closed {
		return 0, ErrTopicClosed
	}
	if partition < 0 || partition >= len(t.partitions) {
		return 0, fmt.Errorf("invalid partition %d (topic has %d partitions)", partition, len(t.partitions))
	}
	p := t.partitions[partition]

	return p.Produce(key, value)
}

// =============================================================================
// CONSUMER OPERATIONS
// =============================================================================

// Consume reads messages from a specific partition.
//
// PARAMETERS:
//   - partition: Which partition to read from
//   - fromOffset: Starting offset (inclusive)
//   - maxMessages: Maximum messages to return
//
// CONSUMER MODEL:
// Each consumer reads from specific partitions at specific offsets.
// The topic doesn't track consumer state - that's the consumer's job.
func (t *Topic) Consume(partition int, fromOffset int64, maxMessages int) ([]*storage.Message, error) {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil, ErrTopicClosed
	}
	if partition < 0 || partition >= len(t.partitions) {
		t.mu.RUnlock()
		return nil, fmt.Errorf("invalid partition %d", partition)
	}
	p := t.partitions[partition]
	t.mu.RUnlock()

	return p.Consume(fromOffset, maxMessages)
}

// ConsumeAll reads messages from all partitions (for single-partition topics).
// This is a convenience method for M1 where there's only one partition.
func (t *Topic) ConsumeAll(fromOffset int64, maxMessages int) ([]*storage.Message, error) {
	return t.Consume(0, fromOffset, maxMessages)
}

// =============================================================================
// TOPIC METADATA
// =============================================================================

// Name returns the topic name.
func (t *Topic) Name() string {
	return t.config.Name
}

// NumPartitions returns the number of partitions.
func (t *Topic) NumPartitions() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.partitions)
}

// Partition returns a specific partition by ID.
func (t *Topic) Partition(id int) (*Partition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if id < 0 || id >= len(t.partitions) {
		return nil, fmt.Errorf("partition %d not found", id)
	}
	return t.partitions[id], nil
}

// EarliestOffset returns the earliest offset across all partitions.
func (t *Topic) EarliestOffset() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var earliest int64 = -1
	for _, p := range t.partitions {
		e := p.EarliestOffset()
		if earliest == -1 || e < earliest {
			earliest = e
		}
	}
	return earliest
}

// LatestOffsets returns the latest offset for each partition.
func (t *Topic) LatestOffsets() map[int]int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	offsets := make(map[int]int64, len(t.partitions))
	for i, p := range t.partitions {
		offsets[i] = p.LatestOffset()
	}
	return offsets
}

// TotalMessages returns sum of messages across all partitions.
func (t *Topic) TotalMessages() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var total int64
	for _, p := range t.partitions {
		total += p.MessageCount()
	}
	return total
}

// TotalSize returns total size in bytes across all partitions.
func (t *Topic) TotalSize() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var total int64
	for _, p := range t.partitions {
		total += p.Size()
	}
	return total
}

// =============================================================================
// TOPIC MANAGEMENT
// =============================================================================

// Sync forces all partitions to sync data to disk.
func (t *Topic) Sync() error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return ErrTopicClosed
	}

	for i, p := range t.partitions {
		if err := p.Sync(); err != nil {
			return fmt.Errorf("failed to sync partition %d: %w", i, err)
		}
	}
	return nil
}

// Close closes all partitions.
func (t *Topic) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	var errs []error
	for i, p := range t.partitions {
		if err := p.Close(); err != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", i, err))
		}
	}

	t.closed = true

	if len(errs) > 0 {
		return fmt.Errorf("errors closing topic: %v", errs)
	}
	return nil
}

// Delete closes and removes all topic data.
func (t *Topic) Delete() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Close all partitions
	for _, p := range t.partitions {
		p.Close()
	}
	t.closed = true

	// Remove topic directory
	topicDir := filepath.Join(t.baseDir, t.config.Name)
	if err := os.RemoveAll(topicDir); err != nil {
		return fmt.Errorf("failed to delete topic directory: %w", err)
	}

	return nil
}
