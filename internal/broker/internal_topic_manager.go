// =============================================================================
// INTERNAL TOPIC MANAGER - MANAGES SYSTEM TOPICS
// =============================================================================
//
// WHAT: Manages the lifecycle and operations of internal/system topics.
//
// RESPONSIBILITIES:
//   - Create internal topics on cluster bootstrap
//   - Ensure internal topics have correct configuration
//   - Provide read/write APIs for internal records
//   - Handle internal topic replication
//   - Rebuild state from internal topic on startup/failover
//
// ┌──────────────────────────────────────────────────────────────────────────┐
// │                    INTERNAL TOPIC MANAGER                                │
// │                                                                          │
// │   ┌─────────────────────────────────────────────────────────────────┐    │
// │   │                  __consumer_offsets Topic                       │    │
// │   │                                                                 │    │
// │   │  Partition 0 ─────► Coordinator for groups hash=0 mod 50        │    │
// │   │  Partition 1 ─────► Coordinator for groups hash=1 mod 50        │    │
// │   │  ...                                                            │    │
// │   │  Partition 49 ────► Coordinator for groups hash=49 mod 50       │    │
// │   │                                                                 │    │
// │   └─────────────────────────────────────────────────────────────────┘    │
// │                                                                          │
// │   Write Flow:                                                            │
// │   ┌─────────┐   hash(group)   ┌───────────┐   write  ┌───────────┐       │
// │   │ Request │ ───────────────►│ Partition │ ────────►│ Local Log │       │
// │   └─────────┘                 │  Leader   │          └───────────┘       │
// │                               └───────────┘                ↓             │
// │                                                     Replicate to ISR     │
// │                                                                          │
// │   Read Flow (State Rebuild):                                             │
// │   ┌───────────┐  read all   ┌──────────────┐  apply   ┌──────────────┐   │
// │   │ Local Log │ ───────────►│Record Stream │ ────────►│ In-Memory    │   │
// │   └───────────┘             └──────────────┘          │ State        │   │
// │                                                       └──────────────┘   │
// │                                                                          │
// └──────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"goqueue/internal/storage"
)

// =============================================================================
// ERRORS
// =============================================================================

var (
	// ErrInternalTopicNotReady means the internal topic hasn't been initialized.
	ErrInternalTopicNotReady = errors.New("internal topic not ready")

	// ErrNotCoordinator means this node is not the coordinator for the group.
	ErrNotCoordinator = errors.New("not coordinator for this group")

	// ErrCoordinatorNotFound means no coordinator is available for the group.
	ErrCoordinatorNotFound = errors.New("coordinator not found for group")
)

// =============================================================================
// CONFIGURATION
// =============================================================================

// InternalTopicConfig holds configuration for internal topics.
type InternalTopicConfig struct {
	// DataDir is where internal topic data is stored.
	DataDir string

	// OffsetsPartitionCount is the number of partitions for __consumer_offsets.
	// Default: 50 (Kafka default)
	OffsetsPartitionCount int

	// ReplicationFactor is how many copies of each partition.
	// Default: 3
	ReplicationFactor int

	// SegmentBytes is the max segment file size.
	// Default: 64MB
	SegmentBytes int64

	// RetentionBytes is max size per partition.
	// Default: -1 (unlimited, rely on compaction)
	RetentionBytes int64

	// EnableReplication enables replication (requires cluster mode).
	EnableReplication bool
}

// DefaultInternalTopicConfig returns sensible defaults.
func DefaultInternalTopicConfig(dataDir string) InternalTopicConfig {
	return InternalTopicConfig{
		DataDir:               dataDir,
		OffsetsPartitionCount: DefaultOffsetsPartitionCount,
		ReplicationFactor:     DefaultOffsetsReplicationFactor,
		SegmentBytes:          64 * 1024 * 1024, // 64MB
		RetentionBytes:        -1,               // unlimited
		EnableReplication:     false,            // single-node by default
	}
}

// =============================================================================
// INTERNAL TOPIC MANAGER
// =============================================================================

// InternalTopicManager manages system topics like __consumer_offsets.
type InternalTopicManager struct {
	// config holds manager configuration.
	config InternalTopicConfig

	// offsetsTopic is the __consumer_offsets topic.
	offsetsTopic *Topic

	// partitionLogs provides direct access to partition logs.
	// Map: partition number → Log
	partitionLogs map[int]*storage.Log

	// localPartitions are partitions this node is leader for.
	// In single-node mode, all partitions are local.
	localPartitions map[int]bool

	// mu protects state.
	mu sync.RWMutex

	// ready indicates the manager is initialized.
	ready bool

	// logger for operations.
	logger *slog.Logger

	// ctx is the manager's context.
	ctx context.Context

	// cancel cancels background operations.
	cancel context.CancelFunc

	// wg waits for background goroutines.
	wg sync.WaitGroup

	// stateRebuilt indicates state has been rebuilt from log.
	stateRebuilt bool

	// listeners receive internal topic events.
	listeners []func(InternalTopicEvent)
}

// InternalTopicEvent represents an event from internal topics.
type InternalTopicEvent struct {
	// Type is the event type.
	Type InternalTopicEventType

	// Topic is the internal topic name.
	Topic string

	// Partition is the affected partition.
	Partition int

	// Record is the record that triggered the event (for writes).
	Record *InternalRecord

	// Details provides additional context.
	Details string
}

// InternalTopicEventType is the type of internal topic event.
type InternalTopicEventType string

const (
	// InternalTopicEventReady indicates the topic is ready.
	InternalTopicEventReady InternalTopicEventType = "ready"

	// InternalTopicEventRecordWritten indicates a record was written.
	InternalTopicEventRecordWritten InternalTopicEventType = "record_written"

	// InternalTopicEventBecameLeader indicates this node became partition leader.
	InternalTopicEventBecameLeader InternalTopicEventType = "became_leader"

	// InternalTopicEventLostLeadership indicates this node lost partition leadership.
	InternalTopicEventLostLeadership InternalTopicEventType = "lost_leadership"
)

// =============================================================================
// CONSTRUCTOR
// =============================================================================

// NewInternalTopicManager creates a new internal topic manager.
func NewInternalTopicManager(config InternalTopicConfig) (*InternalTopicManager, error) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With("component", "internal-topic-manager")

	ctx, cancel := context.WithCancel(context.Background())

	m := &InternalTopicManager{
		config:          config,
		partitionLogs:   make(map[int]*storage.Log),
		localPartitions: make(map[int]bool),
		logger:          logger,
		ctx:             ctx,
		cancel:          cancel,
		listeners:       make([]func(InternalTopicEvent), 0),
	}

	return m, nil
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Start initializes the internal topic manager.
// This creates the __consumer_offsets topic if it doesn't exist.
func (m *InternalTopicManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ready {
		return nil // Already started
	}

	m.logger.Info("starting internal topic manager",
		"offsets_partitions", m.config.OffsetsPartitionCount,
		"replication_factor", m.config.ReplicationFactor)

	// Create internal topics directory
	internalDir := filepath.Join(m.config.DataDir, "internal")
	if err := os.MkdirAll(internalDir, 0755); err != nil {
		return fmt.Errorf("failed to create internal topics directory: %w", err)
	}

	// Create or load __consumer_offsets topic
	if err := m.createOrLoadOffsetsTopic(internalDir); err != nil {
		return fmt.Errorf("failed to initialize __consumer_offsets: %w", err)
	}

	// In single-node mode, we're leader for all partitions
	if !m.config.EnableReplication {
		for i := 0; i < m.config.OffsetsPartitionCount; i++ {
			m.localPartitions[i] = true
		}
	}

	m.ready = true
	m.logger.Info("internal topic manager started",
		"local_partitions", len(m.localPartitions))

	// Notify listeners
	m.notifyListeners(InternalTopicEvent{
		Type:    InternalTopicEventReady,
		Topic:   ConsumerOffsetsTopicName,
		Details: fmt.Sprintf("%d partitions ready", m.config.OffsetsPartitionCount),
	})

	return nil
}

// Stop shuts down the internal topic manager.
func (m *InternalTopicManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.ready {
		return nil
	}

	m.logger.Info("stopping internal topic manager")

	// Cancel background operations
	m.cancel()

	// Wait for goroutines
	m.wg.Wait()

	// Close topic
	if m.offsetsTopic != nil {
		if err := m.offsetsTopic.Close(); err != nil {
			m.logger.Warn("error closing offsets topic", "error", err)
		}
	}

	m.ready = false
	m.logger.Info("internal topic manager stopped")

	return nil
}

// createOrLoadOffsetsTopic creates or loads the __consumer_offsets topic.
func (m *InternalTopicManager) createOrLoadOffsetsTopic(baseDir string) error {
	topicDir := filepath.Join(baseDir, ConsumerOffsetsTopicName)

	// Check if topic exists
	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		// Create new topic
		m.logger.Info("creating __consumer_offsets topic",
			"partitions", m.config.OffsetsPartitionCount)

		config := TopicConfig{
			Name:           ConsumerOffsetsTopicName,
			NumPartitions:  m.config.OffsetsPartitionCount,
			RetentionHours: 0, // Keep forever (rely on compaction in M14)
			RetentionBytes: m.config.RetentionBytes,
		}

		topic, err := NewTopic(baseDir, config)
		if err != nil {
			return fmt.Errorf("failed to create __consumer_offsets: %w", err)
		}

		m.offsetsTopic = topic
	} else {
		// Load existing topic
		m.logger.Info("loading existing __consumer_offsets topic")

		topic, err := LoadTopic(baseDir, ConsumerOffsetsTopicName)
		if err != nil {
			return fmt.Errorf("failed to load __consumer_offsets: %w", err)
		}

		m.offsetsTopic = topic
	}

	// Store references to partition logs for direct access
	for i := 0; i < m.offsetsTopic.NumPartitions(); i++ {
		partition, err := m.offsetsTopic.Partition(i)
		if err == nil && partition != nil {
			m.partitionLogs[i] = partition.Log()
		}
	}

	return nil
}

// getPartition returns the partition for the given partition number.
// This is a helper method to access partitions from the internal topic.
func (m *InternalTopicManager) getPartition(partitionNum int) *Partition {
	if m.offsetsTopic == nil {
		return nil
	}
	partition, err := m.offsetsTopic.Partition(partitionNum)
	if err != nil {
		return nil
	}
	return partition
}

// =============================================================================
// WRITE OPERATIONS
// =============================================================================

// WriteOffsetCommit writes an offset commit to the internal topic.
// Returns the offset where the record was written.
func (m *InternalTopicManager) WriteOffsetCommit(group, topic string, partition int32, offset int64, metadata string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.ready {
		return 0, ErrInternalTopicNotReady
	}

	// Calculate target partition
	targetPartition := GroupToPartition(group, m.config.OffsetsPartitionCount)

	// Check if we're the leader for this partition
	if !m.localPartitions[targetPartition] {
		return 0, ErrNotCoordinator
	}

	// Create record
	record := NewOffsetCommitRecord(group, topic, partition, offset, metadata)

	// Write to partition
	return m.writeRecord(targetPartition, record)
}

// WriteGroupMetadata writes group metadata to the internal topic.
func (m *InternalTopicManager) WriteGroupMetadata(group string, state GroupState, protocol, leader string, generation int32, members []InternalMemberMetadata) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.ready {
		return 0, ErrInternalTopicNotReady
	}

	// Calculate target partition
	targetPartition := GroupToPartition(group, m.config.OffsetsPartitionCount)

	// Check if we're the leader for this partition
	if !m.localPartitions[targetPartition] {
		return 0, ErrNotCoordinator
	}

	// Create record
	record := NewGroupMetadataRecord(group, state, protocol, leader, generation, members)

	// Write to partition
	return m.writeRecord(targetPartition, record)
}

// writeRecord writes an internal record to a partition.
func (m *InternalTopicManager) writeRecord(partition int, record *InternalRecord) (int64, error) {
	// Encode record
	data := record.Encode()

	// Write to partition
	p := m.getPartition(partition)
	if p == nil {
		return 0, fmt.Errorf("partition %d not found", partition)
	}

	// Use partition's log to append
	log := p.Log()
	if log == nil {
		return 0, fmt.Errorf("log not found for partition %d", partition)
	}

	// Create a storage message wrapping our internal record
	msg := &storage.Message{
		Key:       record.Key,
		Value:     data, // Entire encoded record as value
		Timestamp: record.Timestamp.UnixNano(),
	}

	offset, err := log.Append(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to append record: %w", err)
	}

	// Notify listeners
	m.notifyListeners(InternalTopicEvent{
		Type:      InternalTopicEventRecordWritten,
		Topic:     ConsumerOffsetsTopicName,
		Partition: partition,
		Record:    record,
	})

	return offset, nil
}

// =============================================================================
// READ OPERATIONS
// =============================================================================

// ReadPartition reads all records from a partition.
// Used for state rebuild on startup/failover.
func (m *InternalTopicManager) ReadPartition(partition int) ([]*InternalRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.ready {
		return nil, ErrInternalTopicNotReady
	}

	p := m.getPartition(partition)
	if p == nil {
		return nil, fmt.Errorf("partition %d not found", partition)
	}

	log := p.Log()
	if log == nil {
		return nil, fmt.Errorf("log not found for partition %d", partition)
	}

	// Read all messages from the beginning (0 means no limit)
	messages, err := log.ReadFrom(log.EarliestOffset(), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read partition: %w", err)
	}

	var records []*InternalRecord
	for _, msg := range messages {
		// Decode the internal record from message value
		record, err := DecodeInternalRecord(msg.Value)
		if err != nil {
			m.logger.Warn("failed to decode internal record",
				"partition", partition,
				"offset", msg.Offset,
				"error", err)
			continue // Skip corrupted records
		}

		records = append(records, record)
	}

	return records, nil
}

// ReadPartitionFromOffset reads records from a specific offset.
func (m *InternalTopicManager) ReadPartitionFromOffset(partition int, startOffset int64) ([]*InternalRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.ready {
		return nil, ErrInternalTopicNotReady
	}

	p := m.getPartition(partition)
	if p == nil {
		return nil, fmt.Errorf("partition %d not found", partition)
	}

	log := p.Log()
	if log == nil {
		return nil, fmt.Errorf("log not found for partition %d", partition)
	}

	// Read all messages from startOffset (0 means no limit)
	messages, err := log.ReadFrom(startOffset, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read partition: %w", err)
	}

	var records []*InternalRecord
	for _, msg := range messages {
		record, err := DecodeInternalRecord(msg.Value)
		if err != nil {
			continue
		}

		records = append(records, record)
	}

	return records, nil
}

// =============================================================================
// STATE QUERIES
// =============================================================================

// IsReady returns whether the manager is initialized.
func (m *InternalTopicManager) IsReady() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ready
}

// IsCoordinatorFor returns whether this node is coordinator for the given group.
func (m *InternalTopicManager) IsCoordinatorFor(groupID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.ready {
		return false
	}

	partition := GroupToPartition(groupID, m.config.OffsetsPartitionCount)
	return m.localPartitions[partition]
}

// GetPartitionForGroup returns the partition number for a group.
func (m *InternalTopicManager) GetPartitionForGroup(groupID string) int {
	return GroupToPartition(groupID, m.config.OffsetsPartitionCount)
}

// GetLocalPartitions returns the partitions this node is leader for.
func (m *InternalTopicManager) GetLocalPartitions() []int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	partitions := make([]int, 0, len(m.localPartitions))
	for p := range m.localPartitions {
		partitions = append(partitions, p)
	}
	return partitions
}

// GetPartitionCount returns the number of partitions.
func (m *InternalTopicManager) GetPartitionCount() int {
	return m.config.OffsetsPartitionCount
}

// =============================================================================
// LEADERSHIP MANAGEMENT
// =============================================================================

// BecameLeader is called when this node becomes leader for a partition.
// This triggers state rebuild from the partition log.
func (m *InternalTopicManager) BecameLeader(partition int) error {
	m.mu.Lock()
	m.localPartitions[partition] = true
	m.mu.Unlock()

	m.logger.Info("became leader for internal topic partition",
		"topic", ConsumerOffsetsTopicName,
		"partition", partition)

	// Notify listeners
	m.notifyListeners(InternalTopicEvent{
		Type:      InternalTopicEventBecameLeader,
		Topic:     ConsumerOffsetsTopicName,
		Partition: partition,
	})

	return nil
}

// LostLeadership is called when this node loses leadership for a partition.
func (m *InternalTopicManager) LostLeadership(partition int) {
	m.mu.Lock()
	delete(m.localPartitions, partition)
	m.mu.Unlock()

	m.logger.Info("lost leadership for internal topic partition",
		"topic", ConsumerOffsetsTopicName,
		"partition", partition)

	// Notify listeners
	m.notifyListeners(InternalTopicEvent{
		Type:      InternalTopicEventLostLeadership,
		Topic:     ConsumerOffsetsTopicName,
		Partition: partition,
	})
}

// =============================================================================
// EVENT LISTENERS
// =============================================================================

// AddListener adds a listener for internal topic events.
func (m *InternalTopicManager) AddListener(listener func(InternalTopicEvent)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listeners = append(m.listeners, listener)
}

func (m *InternalTopicManager) notifyListeners(event InternalTopicEvent) {
	for _, listener := range m.listeners {
		go listener(event) // Non-blocking notification
	}
}

// =============================================================================
// STATS
// =============================================================================

// Stats returns statistics about internal topics.
type InternalTopicStats struct {
	Ready           bool
	PartitionCount  int
	LocalPartitions int
	StateRebuilt    bool
	RecordsWritten  int64
}

func (m *InternalTopicManager) Stats() InternalTopicStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return InternalTopicStats{
		Ready:           m.ready,
		PartitionCount:  m.config.OffsetsPartitionCount,
		LocalPartitions: len(m.localPartitions),
		StateRebuilt:    m.stateRebuilt,
	}
}

// =============================================================================
// HELPER: CHECK IF TOPIC IS INTERNAL
// =============================================================================

// IsInternalTopic returns true if the topic name is an internal system topic.
func IsInternalTopic(name string) bool {
	return strings.HasPrefix(name, "__")
}
