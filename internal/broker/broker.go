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

	// groupCoordinator manages consumer groups and offsets
	// Added in Milestone 3 for consumer group support
	groupCoordinator *GroupCoordinator

	// ackManager handles per-message acknowledgment (M4)
	// Provides ACK/NACK/REJECT semantics on top of offset-based consumption
	ackManager *AckManager

	// reliabilityConfig holds M4 reliability settings
	reliabilityConfig ReliabilityConfig

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

	// Create group coordinator for consumer group management (M3)
	coordinatorConfig := DefaultCoordinatorConfig(config.DataDir)
	groupCoordinator, err := NewGroupCoordinator(coordinatorConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create group coordinator: %w", err)
	}

	// Initialize reliability configuration (M4)
	reliabilityConfig := DefaultReliabilityConfig()

	broker := &Broker{
		config:            config,
		topics:            make(map[string]*Topic),
		logsDir:           logsDir,
		groupCoordinator:  groupCoordinator,
		reliabilityConfig: reliabilityConfig,
		logger:            logger,
		startedAt:         time.Now(),
	}

	// Create ACK manager for per-message acknowledgment (M4)
	// Must be created after broker struct exists (circular dependency)
	broker.ackManager = NewAckManager(broker, reliabilityConfig)

	// Discover and load existing topics
	if err := broker.loadExistingTopics(); err != nil {
		return nil, fmt.Errorf("failed to load existing topics: %w", err)
	}

	logger.Info("broker started",
		"nodeID", config.NodeID,
		"dataDir", config.DataDir,
		"topics", len(broker.topics),
		"visibility_timeout_ms", reliabilityConfig.VisibilityTimeoutMs,
		"max_retries", reliabilityConfig.MaxRetries,
		"dlq_enabled", reliabilityConfig.DLQEnabled)

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

		// Register topic with group coordinator for partition assignment
		b.groupCoordinator.RegisterTopic(topicName, topic.NumPartitions())

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
//  2. Close group coordinator (flushes offsets)
//  3. Sync all topics to disk
//  4. Close all topics
//  5. Release resources
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	b.logger.Info("shutting down broker")

	var errs []error

	// Close ACK manager first (stops visibility tracking, flushes retry queue)
	if b.ackManager != nil {
		if err := b.ackManager.Close(); err != nil {
			errs = append(errs, fmt.Errorf("ack manager: %w", err))
		}
	}

	// Close group coordinator (flushes pending offset commits)
	if b.groupCoordinator != nil {
		if err := b.groupCoordinator.Close(); err != nil {
			errs = append(errs, fmt.Errorf("group coordinator: %w", err))
		}
	}

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

	// Register with group coordinator for consumer group partition assignment
	b.groupCoordinator.RegisterTopic(config.Name, config.NumPartitions)

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

	// Unregister from group coordinator
	b.groupCoordinator.UnregisterTopic(name)

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

// GroupCoordinator returns the broker's consumer group coordinator.
// Used by the API layer for consumer group operations.
func (b *Broker) GroupCoordinator() *GroupCoordinator {
	return b.groupCoordinator
}

// Uptime returns how long the broker has been running.
func (b *Broker) Uptime() time.Duration {
	return time.Since(b.startedAt)
}

// =============================================================================
// MILESTONE 4: RELIABILITY LAYER API
// =============================================================================
//
// These methods provide per-message acknowledgment (ACK/NACK/REJECT) on top
// of the Kafka-style offset-based consumption model.
//
// FLOW COMPARISON:
//
//   KAFKA (offset-only):
//   ┌────────┐  poll  ┌────────┐ process ┌────────┐ commit ┌────────┐
//   │Consumer│───────►│Receives│────────►│Process │───────►│Commit  │
//   │        │        │batch   │         │all     │        │offset  │
//   └────────┘        └────────┘         └────────┘        └────────┘
//
//   GOQUEUE (per-message ACK):
//   ┌────────┐  poll  ┌────────┐ process ┌────────┐ ack    ┌────────┐
//   │Consumer│───────►│Receives│────────►│Process │───────►│ACK each│
//   │        │        │+receipt│         │one msg │        │message │
//   └────────┘        └────────┘         └────────┘        └────────┘
//                                              │                │
//                                              │ fail           │ offset
//                                              ▼                │ advances
//                                         ┌────────┐            │
//                                         │NACK/   │────────────┘
//                                         │Reject  │
//                                         └────────┘
//
// =============================================================================

// AckManager returns the broker's ACK manager for per-message acknowledgment.
// Used by the API layer for ACK/NACK/REJECT operations.
func (b *Broker) AckManager() *AckManager {
	return b.ackManager
}

// ReliabilityConfig returns the current reliability configuration.
func (b *Broker) ReliabilityConfig() ReliabilityConfig {
	return b.reliabilityConfig
}

// ConsumeWithReceipts reads messages and tracks them for per-message ACK.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset
//   - maxMessages: Max messages to return
//   - consumerID, groupID: Consumer identification for tracking
//
// RETURNS:
//   - Messages with receipt handles attached
//   - Error if read fails
//
// IMPORTANT:
// Each returned message has a ReceiptHandle that MUST be used for ACK/NACK/REJECT.
// Messages not ACKed within VisibilityTimeout will be redelivered.
func (b *Broker) ConsumeWithReceipts(
	topic string,
	partition int,
	fromOffset int64,
	maxMessages int,
	consumerID, groupID string,
) ([]MessageWithReceipt, error) {
	// First, get the raw messages using existing Consume method
	messages, err := b.Consume(topic, partition, fromOffset, maxMessages)
	if err != nil {
		return nil, err
	}

	// Track each message for per-message ACK and generate receipt handles
	results := make([]MessageWithReceipt, 0, len(messages))
	visibilityTimeout := time.Duration(b.reliabilityConfig.VisibilityTimeoutMs) * time.Millisecond

	for _, msg := range messages {
		// Track delivery and get receipt handle
		receiptHandle, err := b.ackManager.TrackDelivery(&msg, consumerID, groupID, visibilityTimeout)
		if err != nil {
			// Backpressure or tracking error - stop here
			b.logger.Warn("failed to track delivery",
				"topic", topic,
				"partition", partition,
				"offset", msg.Offset,
				"error", err)
			break
		}

		results = append(results, MessageWithReceipt{
			Message:       msg,
			ReceiptHandle: receiptHandle,
		})
	}

	return results, nil
}

// MessageWithReceipt extends Message with a receipt handle for ACK/NACK/REJECT.
type MessageWithReceipt struct {
	Message
	ReceiptHandle string
}

// Ack acknowledges successful processing of a message.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//
// SEMANTICS:
//   - Message is considered fully processed
//   - Will not be redelivered
//   - Committed offset may advance (if contiguous)
func (b *Broker) Ack(receiptHandle string) (*AckResult, error) {
	return b.ackManager.Ack(receiptHandle)
}

// Nack indicates processing failed and message should be retried.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//   - reason: Why the message failed (for logging/debugging)
//
// SEMANTICS:
//   - Message will be redelivered after exponential backoff
//   - Each NACK increments delivery count
//   - After MaxRetries, message goes to DLQ
func (b *Broker) Nack(receiptHandle, reason string) (*AckResult, error) {
	return b.ackManager.Nack(receiptHandle, reason)
}

// Reject sends a message directly to the dead letter queue.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//   - reason: Why the message was rejected
//
// SEMANTICS:
//   - Message is considered "poison" (can never succeed)
//   - Immediately routed to DLQ (no retry)
//
// USE CASES:
//   - Message format is invalid
//   - Business logic determines message is unprocessable
func (b *Broker) Reject(receiptHandle, reason string) (*AckResult, error) {
	return b.ackManager.Reject(receiptHandle, reason)
}

// ExtendVisibility extends the visibility timeout for a message.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//   - extension: Additional time to add
//
// USE CASE:
//   - Processing takes longer than expected
//   - Prevents timeout while still working on message
//
// EXAMPLE:
//
//	// Processing will take longer than 30s visibility timeout
//	if estimatedTime > 25*time.Second {
//	    broker.ExtendVisibility(receipt, 30*time.Second)
//	}
func (b *Broker) ExtendVisibility(receiptHandle string, extension time.Duration) (time.Time, error) {
	return b.ackManager.ExtendVisibility(receiptHandle, extension)
}

// GetConsumerLag returns lag information for a consumer.
func (b *Broker) GetConsumerLag(consumerID, groupID, topic string, partition int) (*ConsumerLag, error) {
	return b.ackManager.GetConsumerLag(consumerID, groupID, topic, partition)
}

// ReliabilityStats returns combined reliability layer statistics.
type ReliabilityStats struct {
	AckManager AckManagerStats
	Visibility VisibilityStats
	DLQ        DLQStats
}

func (b *Broker) ReliabilityStats() ReliabilityStats {
	return ReliabilityStats{
		AckManager: b.ackManager.Stats(),
		Visibility: b.ackManager.visibilityTracker.Stats(),
		DLQ:        b.ackManager.dlqRouter.Stats(),
	}
}
