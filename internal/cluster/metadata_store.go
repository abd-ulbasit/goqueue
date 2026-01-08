// =============================================================================
// METADATA STORE - CLUSTER-WIDE CONFIGURATION STORAGE
// =============================================================================
//
// WHAT: Stores cluster-wide metadata that must be consistent across all nodes.
//
// TYPES OF METADATA:
//   1. Topic configurations (partitions, replication factor, retention)
//   2. Partition assignments (which node owns which partition)
//   3. Consumer group configs (offsets, assignments)
//   4. Cluster-wide settings
//
// CONSISTENCY MODEL:
//   - Controller is the single writer (avoids conflicts)
//   - Followers read from their local copy
//   - Updates propagate via heartbeat responses
//
// WHY NOT USE THE BROKER'S EXISTING STORAGE?
//   - Broker stores message data (large, append-only logs)
//   - Metadata is small, frequently updated, needs different patterns
//   - Metadata must be replicated for fault tolerance
//   - Kafka uses __cluster_metadata topic, but we start simpler
//
// COMPARISON:
//   - Kafka (old): ZooKeeper for metadata
//   - Kafka (KRaft): Internal __cluster_metadata topic with Raft
//   - Cassandra: System keyspace tables
//   - goqueue: File-based JSON (M10), internal topic replication (M11)
//
// =============================================================================

package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// =============================================================================
// TOPIC METADATA
// =============================================================================
//
// WHY SEPARATE FROM BROKER'S TOPIC?
//   - Broker's Topic struct manages runtime state (partitions, producers)
//   - TopicMeta is the configuration - what SHOULD exist
//   - On startup, broker creates Topic objects from TopicMeta
//
// =============================================================================

// TopicMeta contains the configuration for a topic.
type TopicMeta struct {
	// Name is the topic name.
	Name string `json:"name"`

	// PartitionCount is the number of partitions.
	PartitionCount int `json:"partition_count"`

	// ReplicationFactor is how many copies of each partition.
	// Must be <= number of nodes.
	ReplicationFactor int `json:"replication_factor"`

	// CreatedAt is when the topic was created.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the topic was last modified.
	UpdatedAt time.Time `json:"updated_at"`

	// Config contains topic-specific settings.
	Config TopicConfig `json:"config"`
}

// TopicConfig contains topic-level settings.
type TopicConfig struct {
	// RetentionMs is how long to keep messages (-1 = forever).
	RetentionMs int64 `json:"retention_ms"`

	// RetentionBytes is max size per partition (-1 = unlimited).
	RetentionBytes int64 `json:"retention_bytes"`

	// SegmentBytes is the max segment file size.
	SegmentBytes int64 `json:"segment_bytes"`

	// CleanupPolicy is "delete" or "compact".
	CleanupPolicy string `json:"cleanup_policy"`

	// MinInSyncReplicas is min replicas that must ack writes.
	MinInSyncReplicas int `json:"min_in_sync_replicas"`

	// DelayEnabled allows delayed messages on this topic.
	DelayEnabled bool `json:"delay_enabled"`

	// SchemaValidation enables schema validation.
	SchemaValidation bool `json:"schema_validation"`
}

// DefaultTopicConfig returns sensible defaults.
func DefaultTopicConfig() TopicConfig {
	return TopicConfig{
		RetentionMs:       7 * 24 * 60 * 60 * 1000, // 7 days
		RetentionBytes:    -1,                      // unlimited
		SegmentBytes:      64 * 1024 * 1024,        // 64MB
		CleanupPolicy:     "delete",
		MinInSyncReplicas: 1,
		DelayEnabled:      true,
		SchemaValidation:  false,
	}
}

// =============================================================================
// PARTITION ASSIGNMENT
// =============================================================================
//
// WHAT: Maps partitions to nodes.
// Determines which node is leader and which are replicas for each partition.
//
// EXAMPLE (3 nodes, 3 partitions, replication=2):
//   Partition 0: Leader=Node-A, Replicas=[Node-A, Node-B]
//   Partition 1: Leader=Node-B, Replicas=[Node-B, Node-C]
//   Partition 2: Leader=Node-C, Replicas=[Node-C, Node-A]
//
// =============================================================================

// PartitionAssignment describes where a partition lives.
type PartitionAssignment struct {
	// Topic is the topic name.
	Topic string `json:"topic"`

	// Partition is the partition number.
	Partition int `json:"partition"`

	// Leader is the node that handles read/write for this partition.
	Leader NodeID `json:"leader"`

	// Replicas is the list of nodes that have copies (including leader).
	Replicas []NodeID `json:"replicas"`

	// ISR is the In-Sync Replicas (replicas that are caught up).
	// ISR ⊆ Replicas
	ISR []NodeID `json:"isr"`

	// Version increments on each change.
	Version int64 `json:"version"`
}

// =============================================================================
// CLUSTER METADATA
// =============================================================================

// ClusterMeta contains all cluster-wide metadata.
type ClusterMeta struct {
	// Version is incremented on every change.
	// Used for optimistic concurrency and cache invalidation.
	Version int64 `json:"version"`

	// Topics maps topic name to metadata.
	Topics map[string]*TopicMeta `json:"topics"`

	// Assignments maps "topic-partition" to assignment.
	Assignments map[string]*PartitionAssignment `json:"assignments"`

	// UpdatedAt is when this metadata was last modified.
	UpdatedAt time.Time `json:"updated_at"`
}

// NewClusterMeta creates empty cluster metadata.
func NewClusterMeta() *ClusterMeta {
	return &ClusterMeta{
		Version:     0,
		Topics:      make(map[string]*TopicMeta),
		Assignments: make(map[string]*PartitionAssignment),
		UpdatedAt:   time.Now(),
	}
}

// Clone creates a deep copy of ClusterMeta.
func (m *ClusterMeta) Clone() *ClusterMeta {
	if m == nil {
		return nil
	}

	clone := &ClusterMeta{
		Version:     m.Version,
		Topics:      make(map[string]*TopicMeta, len(m.Topics)),
		Assignments: make(map[string]*PartitionAssignment, len(m.Assignments)),
		UpdatedAt:   m.UpdatedAt,
	}

	for k, v := range m.Topics {
		topicCopy := *v
		clone.Topics[k] = &topicCopy
	}

	for k, v := range m.Assignments {
		assignCopy := *v
		// Deep copy slices
		assignCopy.Replicas = make([]NodeID, len(v.Replicas))
		copy(assignCopy.Replicas, v.Replicas)
		assignCopy.ISR = make([]NodeID, len(v.ISR))
		copy(assignCopy.ISR, v.ISR)
		clone.Assignments[k] = &assignCopy
	}

	return clone
}

// =============================================================================
// METADATA STORE
// =============================================================================

// MetadataStore manages cluster metadata.
type MetadataStore struct {
	// mu protects all mutable state
	mu sync.RWMutex

	// meta is the current cluster metadata
	meta *ClusterMeta

	// dataDir is where metadata is persisted
	dataDir string

	// listeners receive metadata change events
	listeners []MetadataListener

	// logger for metadata operations
	// logger *slog.Logger
}

// MetadataListener is called when metadata changes.
type MetadataListener func(meta *ClusterMeta)

// NewMetadataStore creates a new metadata store.
func NewMetadataStore(dataDir string) *MetadataStore {
	return &MetadataStore{
		meta:      NewClusterMeta(),
		dataDir:   dataDir,
		listeners: make([]MetadataListener, 0),
	}
}

// =============================================================================
// METADATA ACCESS
// =============================================================================

// Meta returns a snapshot of current metadata.
func (ms *MetadataStore) Meta() *ClusterMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.meta.Clone()
}

// Version returns the current metadata version.
func (ms *MetadataStore) Version() int64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.meta.Version
}

// GetTopic returns metadata for a specific topic.
func (ms *MetadataStore) GetTopic(name string) *TopicMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	topic, ok := ms.meta.Topics[name]
	if !ok {
		return nil
	}

	// Return copy
	topicCopy := *topic
	return &topicCopy
}

// GetAllTopics returns all topic metadata.
func (ms *MetadataStore) GetAllTopics() []*TopicMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	topics := make([]*TopicMeta, 0, len(ms.meta.Topics))
	for _, topic := range ms.meta.Topics {
		topicCopy := *topic
		topics = append(topics, &topicCopy)
	}
	return topics
}

// GetAssignment returns the assignment for a specific partition.
func (ms *MetadataStore) GetAssignment(topic string, partition int) *PartitionAssignment {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	assign, ok := ms.meta.Assignments[key]
	if !ok {
		return nil
	}

	// Return copy
	assignCopy := *assign
	assignCopy.Replicas = make([]NodeID, len(assign.Replicas))
	copy(assignCopy.Replicas, assign.Replicas)
	assignCopy.ISR = make([]NodeID, len(assign.ISR))
	copy(assignCopy.ISR, assign.ISR)
	return &assignCopy
}

// GetAssignmentsForTopic returns all assignments for a topic.
func (ms *MetadataStore) GetAssignmentsForTopic(topic string) []*PartitionAssignment {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	var assignments []*PartitionAssignment
	for key, assign := range ms.meta.Assignments {
		if assign.Topic == topic {
			assignCopy := *assign
			assignCopy.Replicas = make([]NodeID, len(assign.Replicas))
			copy(assignCopy.Replicas, assign.Replicas)
			assignCopy.ISR = make([]NodeID, len(assign.ISR))
			copy(assignCopy.ISR, assign.ISR)
			assignments = append(assignments, &assignCopy)
			_ = key // silence unused warning
		}
	}
	return assignments
}

// GetAssignmentsForNode returns all partitions this node is leader of.
func (ms *MetadataStore) GetAssignmentsForNode(nodeID NodeID) []*PartitionAssignment {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	var assignments []*PartitionAssignment
	for _, assign := range ms.meta.Assignments {
		if assign.Leader == nodeID {
			assignCopy := *assign
			assignCopy.Replicas = make([]NodeID, len(assign.Replicas))
			copy(assignCopy.Replicas, assign.Replicas)
			assignCopy.ISR = make([]NodeID, len(assign.ISR))
			copy(assignCopy.ISR, assign.ISR)
			assignments = append(assignments, &assignCopy)
		}
	}
	return assignments
}

// =============================================================================
// METADATA MUTATIONS (CONTROLLER ONLY)
// =============================================================================

// CreateTopic adds a new topic to metadata.
// Should only be called by the controller.
func (ms *MetadataStore) CreateTopic(name string, partitions int, replicationFactor int, config TopicConfig) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.meta.Topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	ms.meta.Topics[name] = &TopicMeta{
		Name:              name,
		PartitionCount:    partitions,
		ReplicationFactor: replicationFactor,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
		Config:            config,
	}

	ms.meta.Version++
	ms.meta.UpdatedAt = time.Now()

	ms.notifyListenersLocked()
	return ms.persistLocked()
}

// DeleteTopic removes a topic from metadata.
// Should only be called by the controller.
func (ms *MetadataStore) DeleteTopic(name string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.meta.Topics[name]; !exists {
		return fmt.Errorf("topic %s not found", name)
	}

	delete(ms.meta.Topics, name)

	// Remove associated assignments
	for key, assign := range ms.meta.Assignments {
		if assign.Topic == name {
			delete(ms.meta.Assignments, key)
		}
	}

	ms.meta.Version++
	ms.meta.UpdatedAt = time.Now()

	ms.notifyListenersLocked()
	return ms.persistLocked()
}

// UpdateTopicConfig updates a topic's configuration.
func (ms *MetadataStore) UpdateTopicConfig(name string, config TopicConfig) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	topic, exists := ms.meta.Topics[name]
	if !exists {
		return fmt.Errorf("topic %s not found", name)
	}

	topic.Config = config
	topic.UpdatedAt = time.Now()

	ms.meta.Version++
	ms.meta.UpdatedAt = time.Now()

	ms.notifyListenersLocked()
	return ms.persistLocked()
}

// SetAssignment sets the partition assignment.
// Should only be called by the controller.
func (ms *MetadataStore) SetAssignment(assign *PartitionAssignment) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := fmt.Sprintf("%s-%d", assign.Topic, assign.Partition)

	// Deep copy
	assignCopy := *assign
	assignCopy.Replicas = make([]NodeID, len(assign.Replicas))
	copy(assignCopy.Replicas, assign.Replicas)
	assignCopy.ISR = make([]NodeID, len(assign.ISR))
	copy(assignCopy.ISR, assign.ISR)

	ms.meta.Assignments[key] = &assignCopy

	ms.meta.Version++
	ms.meta.UpdatedAt = time.Now()

	ms.notifyListenersLocked()
	return ms.persistLocked()
}

// UpdateISR updates the In-Sync Replicas for a partition.
// Called when replicas fall behind or catch up.
func (ms *MetadataStore) UpdateISR(topic string, partition int, isr []NodeID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	assign, exists := ms.meta.Assignments[key]
	if !exists {
		return fmt.Errorf("partition %s-%d not found", topic, partition)
	}

	assign.ISR = make([]NodeID, len(isr))
	copy(assign.ISR, isr)
	assign.Version++

	ms.meta.Version++
	ms.meta.UpdatedAt = time.Now()

	ms.notifyListenersLocked()
	return ms.persistLocked()
}

// UpdateLeader updates the leader for a partition.
// TODO: why do have a leader for partition ? is this cluster leader? or replica leader? like which one is the primary replica?
// Called during leader election (M11).
func (ms *MetadataStore) UpdateLeader(topic string, partition int, leader NodeID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	assign, exists := ms.meta.Assignments[key]
	if !exists {
		return fmt.Errorf("partition %s-%d not found", topic, partition)
	}

	assign.Leader = leader
	assign.Version++

	ms.meta.Version++
	ms.meta.UpdatedAt = time.Now()

	ms.notifyListenersLocked()
	return ms.persistLocked()
}

// =============================================================================
// STATE SYNCHRONIZATION
// =============================================================================

// ApplyMeta replaces local metadata with received metadata.
// Called when syncing from controller.
func (ms *MetadataStore) ApplyMeta(meta *ClusterMeta) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Only apply if version is newer
	if meta.Version <= ms.meta.Version {
		return nil
	}

	ms.meta = meta.Clone()

	ms.notifyListenersLocked()
	return ms.persistLocked()
}

// =============================================================================
// PERSISTENCE
// =============================================================================

// metadataFilePath returns the path to the metadata file.
func (ms *MetadataStore) metadataFilePath() string {
	return filepath.Join(ms.dataDir, "cluster", "metadata.json")
}

// persistLocked saves metadata to disk.
// Must be called with mu held.
func (ms *MetadataStore) persistLocked() error {
	if ms.dataDir == "" {
		return nil // Persistence disabled
	}

	// Ensure directory exists
	dir := filepath.Dir(ms.metadataFilePath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Marshal metadata
	data, err := json.MarshalIndent(ms.meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write atomically
	tempPath := ms.metadataFilePath() + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	if err := os.Rename(tempPath, ms.metadataFilePath()); err != nil {
		return fmt.Errorf("failed to rename metadata file: %w", err)
	}

	return nil
}

// Load loads metadata from disk.
func (ms *MetadataStore) Load() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.dataDir == "" {
		return nil // Persistence disabled
	}

	data, err := os.ReadFile(ms.metadataFilePath())
	if os.IsNotExist(err) {
		return nil // No metadata yet
	}
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta ClusterMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	ms.meta = &meta
	return nil
}

// =============================================================================
// LISTENERS
// =============================================================================

// AddListener registers a callback for metadata changes.
func (ms *MetadataStore) AddListener(listener MetadataListener) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.listeners = append(ms.listeners, listener)
}

// notifyListenersLocked fires the metadata change event.
// Must be called with mu held.
func (ms *MetadataStore) notifyListenersLocked() {
	meta := ms.meta.Clone()
	for _, listener := range ms.listeners {
		go listener(meta)
	}
}

// =============================================================================
// STATISTICS
// =============================================================================

// MetadataStats contains metadata statistics.
type MetadataStats struct {
	Version        int64     `json:"version"`
	TopicCount     int       `json:"topic_count"`
	PartitionCount int       `json:"partition_count"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Stats returns current metadata statistics.
func (ms *MetadataStore) Stats() MetadataStats {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	partitions := 0
	for _, topic := range ms.meta.Topics {
		partitions += topic.PartitionCount
	}

	return MetadataStats{
		Version:        ms.meta.Version,
		TopicCount:     len(ms.meta.Topics),
		PartitionCount: partitions,
		UpdatedAt:      ms.meta.UpdatedAt,
	}
}
