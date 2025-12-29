// =============================================================================
// BROKER - THE CENTRAL COORDINATOR
// =============================================================================
//
// WHAT IS A BROKER?
// A broker is a server that:
//   - Manages topics (create, delete, list)
//   - Handles producer requests (publish messages)
//   - Handles consumer requests (read messages)
//   - Stores data durably on disk
//
// In a distributed setup, multiple brokers form a cluster. For M1, we have
// a single broker (no clustering yet).
//
// BROKER RESPONSIBILITIES:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                           BROKER                                        │
//   │                                                                         │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │                    Topic Management                              │  │
//   │   │   - CreateTopic("orders")                                        │  │
//   │   │   - DeleteTopic("orders")                                        │  │
//   │   │   - ListTopics()                                                 │  │
//   │   │   - GetTopic("orders")                                           │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                              │                                          │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │                    Producer Interface                            │  │
//   │   │   - Publish("orders", key, value) → (partition, offset)          │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                              │                                          │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │                    Consumer Interface                            │  │
//   │   │   - Consume("orders", partition, offset) → []messages            │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                              │                                          │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │                    Storage Layer                                 │  │
//   │   │   - Topics → Partitions → Logs → Segments                        │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON - How other systems structure brokers:
//   - Kafka: Broker manages partitions, ZooKeeper manages cluster metadata
//   - RabbitMQ: Broker manages queues, exchanges, bindings
//   - SQS: Completely managed (no broker concept exposed)
//
// MILESTONE 1 SCOPE:
//   - Single broker (no clustering)
//   - Topic CRUD operations
//   - Simple produce/consume API
//   - File-based storage
//
// =============================================================================

package broker

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrBrokerClosed means the broker has been shut down
	ErrBrokerClosed = errors.New("broker is closed")
)

// =============================================================================
// BROKER CONFIGURATION
// =============================================================================

// BrokerConfig holds broker configuration.
type BrokerConfig struct {
	// DataDir is the root directory for all data storage
	// Structure: DataDir/logs/{topic}/{partition}/
	DataDir string

	// NodeID identifies this broker in a cluster (future use)
	NodeID string

	// LogLevel controls logging verbosity
	LogLevel slog.Level
}

// DefaultBrokerConfig returns sensible defaults.
func DefaultBrokerConfig() BrokerConfig {
	return BrokerConfig{
		DataDir:  "./data",
		NodeID:   "node-1",
		LogLevel: slog.LevelInfo,
	}
}

// =============================================================================
// BROKER STRUCT
// =============================================================================

// Broker is the main server managing topics and handling requests.
type Broker struct {
	// config holds broker configuration
	config BrokerConfig

	// topics maps topic name to Topic instance
	topics map[string]*Topic

	// logsDir is where log files are stored
	logsDir string

	// mu protects topics map
	mu sync.RWMutex

	// logger for broker operations
	logger *slog.Logger

	// startedAt is when broker started
	startedAt time.Time

	// closed tracks if broker is shut down
	closed bool
}

// =============================================================================
// BROKER LIFECYCLE
// =============================================================================

// NewBroker creates and starts a new broker.
//
// STARTUP PROCESS:
//  1. Create data directories if needed
//  2. Discover existing topics
//  3. Load all topics (recovers from crash if needed)
//  4. Ready to accept requests
func NewBroker(config BrokerConfig) (*Broker, error) {
	// Set up logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: config.LogLevel,
	}))

	// Create data directories
	logsDir := filepath.Join(config.DataDir, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %w", err)
	}

	broker := &Broker{
		config:    config,
		topics:    make(map[string]*Topic),
		logsDir:   logsDir,
		logger:    logger,
		startedAt: time.Now(),
	}

	// Discover and load existing topics
	if err := broker.loadExistingTopics(); err != nil {
		return nil, fmt.Errorf("failed to load existing topics: %w", err)
	}

	logger.Info("broker started",
		"nodeID", config.NodeID,
		"dataDir", config.DataDir,
		"topics", len(broker.topics))

	return broker, nil
}

// loadExistingTopics discovers and loads topics from disk.
func (b *Broker) loadExistingTopics() error {
	entries, err := os.ReadDir(b.logsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No topics yet
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		topicName := entry.Name()
		b.logger.Info("loading topic", "topic", topicName)

		topic, err := LoadTopic(b.logsDir, topicName)
		if err != nil {
			b.logger.Error("failed to load topic",
				"topic", topicName,
				"error", err)
			// Continue loading other topics
			continue
		}

		b.topics[topicName] = topic
		b.logger.Info("loaded topic",
			"topic", topicName,
			"partitions", topic.NumPartitions(),
			"messages", topic.TotalMessages())
	}

	return nil
}

// Close shuts down the broker gracefully.
//
// SHUTDOWN PROCESS:
//  1. Stop accepting new requests
//  2. Sync all topics to disk
//  3. Close all topics
//  4. Release resources
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	b.logger.Info("shutting down broker")

	var errs []error
	for name, topic := range b.topics {
		if err := topic.Close(); err != nil {
			errs = append(errs, fmt.Errorf("topic %s: %w", name, err))
		}
	}

	b.closed = true
	b.logger.Info("broker shutdown complete")

	if len(errs) > 0 {
		return fmt.Errorf("errors during shutdown: %v", errs)
	}
	return nil
}

// =============================================================================
// TOPIC MANAGEMENT
// =============================================================================

// CreateTopic creates a new topic with the given configuration.
//
// PARAMETERS:
//   - config: Topic configuration (name, partitions, retention)
//
// RETURNS:
//   - Error if topic already exists or creation fails
//
// NOTE: Topic creation is idempotent in behavior but returns error if exists.
// This matches Kafka's behavior.
func (b *Broker) CreateTopic(config TopicConfig) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBrokerClosed
	}

	// Check if topic already exists
	if _, exists := b.topics[config.Name]; exists {
		return fmt.Errorf("%w: %s", ErrTopicExists, config.Name)
	}

	// Create topic
	topic, err := NewTopic(b.logsDir, config)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	b.topics[config.Name] = topic

	b.logger.Info("created topic",
		"topic", config.Name,
		"partitions", config.NumPartitions)

	return nil
}

// DeleteTopic removes a topic and all its data.
//
// WARNING: This permanently deletes all messages in the topic!
func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBrokerClosed
	}

	topic, exists := b.topics[name]
	if !exists {
		return fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}

	// Delete topic (closes and removes files)
	if err := topic.Delete(); err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	delete(b.topics, name)

	b.logger.Info("deleted topic", "topic", name)

	return nil
}

// GetTopic returns a topic by name.
func (b *Broker) GetTopic(name string) (*Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, ErrBrokerClosed
	}

	topic, exists := b.topics[name]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}

	return topic, nil
}

// ListTopics returns names of all topics.
func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	return names
}

// TopicExists checks if a topic exists.
func (b *Broker) TopicExists(name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	_, exists := b.topics[name]
	return exists
}

// =============================================================================
// PRODUCER INTERFACE
// =============================================================================

// Publish writes a message to a topic.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//
// RETURNS:
//   - Partition the message was written to
//   - Offset within that partition
//   - Error if publish fails
//
// This is the main producer API. It:
//  1. Looks up the topic
//  2. Routes to appropriate partition (by key hash or round-robin)
//  3. Appends message to partition's log
//  4. Returns offset for producer acknowledgment
func (b *Broker) Publish(topic string, key, value []byte) (partition int, offset int64, err error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, 0, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	return t.Publish(key, value)
}

// PublishBatch writes multiple messages to a topic.
// All messages are written to appropriate partitions based on their keys.
//
// RETURNS:
//   - Slice of results (partition, offset) for each message
//   - Error if any publish fails (partial writes may have occurred)
func (b *Broker) PublishBatch(topic string, messages []struct {
	Key   []byte
	Value []byte
}) ([]struct {
	Partition int
	Offset    int64
}, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	results := make([]struct {
		Partition int
		Offset    int64
	}, len(messages))

	for i, msg := range messages {
		partition, offset, err := t.Publish(msg.Key, msg.Value)
		if err != nil {
			return results[:i], fmt.Errorf("failed at message %d: %w", i, err)
		}
		results[i] = struct {
			Partition int
			Offset    int64
		}{partition, offset}
	}

	return results, nil
}

// =============================================================================
// CONSUMER INTERFACE
// =============================================================================

// Consume reads messages from a topic partition.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset (inclusive)
//   - maxMessages: Max messages to return (0 = no limit)
//
// RETURNS:
//   - Slice of messages (may be empty if no new messages)
//   - Error if read fails
//
// This is the main consumer API. Consumers:
//  1. Track their own offset ("I've processed up to offset 100")
//  2. Call Consume to get next batch
//  3. Process messages
//  4. Update their tracked offset
func (b *Broker) Consume(topic string, partition int, fromOffset int64, maxMessages int) ([]Message, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	storageMessages, err := t.Consume(partition, fromOffset, maxMessages)
	if err != nil {
		return nil, err
	}

	// Convert storage.Message to broker.Message for API
	messages := make([]Message, len(storageMessages))
	for i, sm := range storageMessages {
		messages[i] = Message{
			Topic:     topic,
			Partition: partition,
			Offset:    sm.Offset,
			Timestamp: time.Unix(0, sm.Timestamp),
			Key:       sm.Key,
			Value:     sm.Value,
		}
	}

	return messages, nil
}

// GetOffsetBounds returns the earliest and latest offsets for a partition.
// Useful for consumers to know the valid offset range.
func (b *Broker) GetOffsetBounds(topic string, partition int) (earliest, latest int64, err error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, 0, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	p, err := t.Partition(partition)
	if err != nil {
		return 0, 0, err
	}

	return p.EarliestOffset(), p.LatestOffset(), nil
}

// =============================================================================
// MESSAGE TYPE (API representation)
// =============================================================================

// Message is the API representation of a message.
// This is what consumers receive (includes topic/partition context).
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
}

// =============================================================================
// BROKER METADATA
// =============================================================================

// Stats returns broker statistics.
type BrokerStats struct {
	NodeID     string
	Uptime     time.Duration
	TopicCount int
	TotalSize  int64
	TopicStats map[string]TopicStats
}

type TopicStats struct {
	Name          string
	Partitions    int
	TotalMessages int64
	TotalSize     int64
}

func (b *Broker) Stats() BrokerStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := BrokerStats{
		NodeID:     b.config.NodeID,
		Uptime:     time.Since(b.startedAt),
		TopicCount: len(b.topics),
		TopicStats: make(map[string]TopicStats),
	}

	for name, topic := range b.topics {
		ts := TopicStats{
			Name:          name,
			Partitions:    topic.NumPartitions(),
			TotalMessages: topic.TotalMessages(),
			TotalSize:     topic.TotalSize(),
		}
		stats.TopicStats[name] = ts
		stats.TotalSize += ts.TotalSize
	}

	return stats
}

// NodeID returns the broker's node identifier.
func (b *Broker) NodeID() string {
	return b.config.NodeID
}

// DataDir returns the data directory path.
func (b *Broker) DataDir() string {
	return b.config.DataDir
}

// Uptime returns how long the broker has been running.
func (b *Broker) Uptime() time.Duration {
	return time.Since(b.startedAt)
}
