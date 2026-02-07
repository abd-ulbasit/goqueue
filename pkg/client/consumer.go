// =============================================================================
// GOQUEUE HIGH-LEVEL CONSUMER
// =============================================================================
//
// This is the high-level consumer that provides a simple API for consuming
// messages with consumer groups. It wraps the low-level client with:
//   - Consumer group membership management
//   - Automatic offset committing
//   - Rebalance handling
//   - Graceful shutdown
//
// ARCHITECTURE:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                          Application                                    │
//   │                                                                         │
//   │   for msg := range consumer.Messages() {                               │
//   │       // process msg                                                    │
//   │       msg.Ack()                                                         │
//   │   }                                                                     │
//   │              │                                                          │
//   │              │                                                          │
//   │     ┌────────▼────────┐                                                │
//   │     │    Consumer     │ ◄── This file                                  │
//   │     │   (high-level)  │                                                │
//   │     └────────┬────────┘                                                │
//   │              │                                                          │
//   │     ┌────────▼────────┐                                                │
//   │     │     Client      │                                                │
//   │     │   (low-level)   │                                                │
//   │     └────────┬────────┘                                                │
//   └──────────────┼──────────────────────────────────────────────────────────┘
//                  │
//          ┌───────▼───────┐
//          │  gRPC Server  │
//          └───────────────┘
//
// CONSUMER GROUP SEMANTICS:
//
//   ┌──────────────────────────────────────────────────────────────────────────┐
//   │                    How Consumer Groups Work                              │
//   │                                                                          │
//   │  1. Multiple consumers join the same group                              │
//   │  2. Partitions are distributed among members                            │
//   │  3. Each partition → exactly one consumer                               │
//   │  4. When members join/leave → rebalance                                 │
//   │                                                                          │
//   │  Before Rebalance:           After Rebalance:                           │
//   │  ┌─────────┬─────────┐       ┌─────────┬─────────┬─────────┐           │
//   │  │Consumer1│Consumer2│       │Consumer1│Consumer2│Consumer3│           │
//   │  │ P0,P1,P2│  P3,P4  │  ──►  │  P0,P1  │  P2,P3  │   P4    │           │
//   │  └─────────┴─────────┘       └─────────┴─────────┴─────────┘           │
//   │                                                                          │
//   └──────────────────────────────────────────────────────────────────────────┘
//
// USAGE:
//
//   // Create consumer
//   consumer, err := client.NewConsumer(client.ConsumerConfig{
//       Address:  "localhost:9000",
//       GroupID:  "order-processors",
//       Topics:   []string{"orders"},
//       AutoCommit: true,
//   })
//   if err != nil {
//       log.Fatal(err)
//   }
//   defer consumer.Close()
//
//   // Process messages
//   for msg := range consumer.Messages() {
//       // Process the message
//       processOrder(msg.Value)
//
//       // Acknowledge (if auto-commit is off)
//       if err := msg.Ack(); err != nil {
//           log.Printf("Failed to ack: %v", err)
//       }
//   }
//
// =============================================================================

package client

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "goqueue/api/proto/gen/go"
)

// =============================================================================
// CONSUMER CONFIGURATION
// =============================================================================

// ConsumerConfig configures the high-level consumer.
//
// AUTO-COMMIT EXPLAINED:
//
//	AutoCommit: Automatically commit offsets after processing
//	- true: Offsets committed automatically (simpler, but may lose messages on crash)
//	- false: You must call msg.Ack() explicitly (safer, but more code)
//
//	AutoCommitInterval: How often to commit when auto-commit is on
//	- More frequent: More overhead, but less data loss on crash
//	- Less frequent: Better performance, but more reprocessing on crash
//	- Default: 5 seconds
//
// COMPARISON:
//   - Kafka: enable.auto.commit + auto.commit.interval.ms
//   - RabbitMQ: autoAck mode (no offset concept)
//   - SQS: Message deleted on receive (visibility timeout controls redelivery)
type ConsumerConfig struct {
	// Connection
	Address string

	// Consumer group
	GroupID string
	Topics  []string

	// Client configuration (optional)
	ClientConfig *Config

	// Offset management
	AutoCommit         bool          // Auto-commit offsets (default: true)
	AutoCommitInterval time.Duration // Commit interval (default: 5s)

	// Starting position for new partitions
	StartPosition pb.ConsumeStartPosition

	// Processing
	BufferSize int // Message buffer size (default: 100)

	// Logger
	Logger *slog.Logger
}

// DefaultConsumerConfig returns a consumer config with sensible defaults.
func DefaultConsumerConfig(address, groupID string, topics []string) ConsumerConfig {
	return ConsumerConfig{
		Address:            address,
		GroupID:            groupID,
		Topics:             topics,
		AutoCommit:         true,
		AutoCommitInterval: 5 * time.Second,
		StartPosition:      pb.ConsumeStartPosition_CONSUME_START_POSITION_EARLIEST,
		BufferSize:         100,
	}
}

// =============================================================================
// CONSUMER STRUCTURE
// =============================================================================

// Consumer is the high-level message consumer.
type Consumer struct {
	client *Client
	config ConsumerConfig
	logger *slog.Logger

	// Subscription stream
	stream *SubscribeStream

	// Message channel
	msgCh chan *ConsumerMessage

	// State
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex
	closed bool

	// Assignment tracking
	assignmentMu sync.RWMutex
	assignment   map[string][]int32 // topic -> partitions
}

// ConsumerMessage wraps a message with consumer context.
type ConsumerMessage struct {
	// Message data
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time

	// Receipt handle for ack/nack
	ReceiptHandle string

	// Consumer reference (for ack operations)
	consumer *Consumer
}

// =============================================================================
// CONSUMER LIFECYCLE
// =============================================================================

// NewConsumer creates a new high-level consumer.
//
// The consumer starts a background goroutine that:
//  1. Subscribes to the consumer group
//  2. Receives messages from assigned partitions
//  3. Handles rebalance events
//  4. Auto-commits offsets (if enabled)
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	// Validate config
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("group_id is required")
	}
	if len(cfg.Topics) == 0 {
		return nil, fmt.Errorf("at least one topic is required")
	}

	// Apply defaults
	if cfg.AutoCommitInterval == 0 {
		cfg.AutoCommitInterval = 5 * time.Second
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 100
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Create underlying client
	clientCfg := DefaultConfig(cfg.Address)
	if cfg.ClientConfig != nil {
		clientCfg = *cfg.ClientConfig
		clientCfg.Address = cfg.Address
	}
	clientCfg.Logger = logger

	client, err := New(clientCfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		client:     client,
		config:     cfg,
		logger:     logger,
		msgCh:      make(chan *ConsumerMessage, cfg.BufferSize),
		ctx:        ctx,
		cancel:     cancel,
		assignment: make(map[string][]int32),
	}

	// Start subscription
	if err := c.subscribe(); err != nil {
		client.Close()
		cancel()
		return nil, err
	}

	// Start message receiver
	c.wg.Add(1)
	go c.receiveLoop()

	// Start auto-commit if enabled
	if cfg.AutoCommit {
		c.wg.Add(1)
		go c.autoCommitLoop()
	}

	return c, nil
}

// subscribe establishes the subscription stream.
func (c *Consumer) subscribe() error {
	stream, err := c.client.Subscribe(c.ctx, c.config.GroupID, c.config.Topics,
		WithStartPosition(c.config.StartPosition),
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	c.stream = stream
	return nil
}

// Close closes the consumer.
// Waits for pending messages to be processed.
func (c *Consumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	// Cancel context to stop receivers
	c.cancel()

	// Wait for goroutines to finish
	c.wg.Wait()

	// Close message channel
	close(c.msgCh)

	return c.client.Close()
}

// =============================================================================
// MESSAGE RECEIVING
// =============================================================================

// Messages returns a channel for receiving messages.
// The channel is closed when the consumer is closed.
//
// USAGE:
//
//	for msg := range consumer.Messages() {
//	    // Process message
//	    processOrder(msg.Value)
//
//	    // Acknowledge
//	    msg.Ack()
//	}
func (c *Consumer) Messages() <-chan *ConsumerMessage {
	return c.msgCh
}

// receiveLoop receives messages from the subscription stream.
func (c *Consumer) receiveLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		resp, err := c.stream.Recv()
		if err != nil {
			c.logger.Error("receive error", "error", err)

			// Check if context canceled
			select {
			case <-c.ctx.Done():
				return
			default:
			}

			// Try to resubscribe
			time.Sleep(time.Second)
			if err := c.subscribe(); err != nil {
				c.logger.Error("resubscribe failed", "error", err)
			}
			continue
		}

		c.handleResponse(resp)
	}
}

// handleResponse processes a response from the stream.
func (c *Consumer) handleResponse(resp *pb.ConsumeResponse) {
	// Check for messages
	if msgs := resp.GetMessages(); msgs != nil {
		for _, msg := range msgs.Messages {
			consumerMsg := &ConsumerMessage{
				Topic:         msg.Topic,
				Partition:     msg.Partition,
				Offset:        msg.Offset,
				Key:           msg.Key,
				Value:         msg.Value,
				Headers:       msg.Headers,
				ReceiptHandle: msg.ReceiptHandle,
				consumer:      c,
			}

			if msg.Timestamp != nil {
				consumerMsg.Timestamp = msg.Timestamp.AsTime()
			}

			select {
			case c.msgCh <- consumerMsg:
			case <-c.ctx.Done():
				return
			}
		}
	}

	// Check for assignment change
	if assignment := resp.GetAssignment(); assignment != nil {
		c.handleAssignment(assignment)
	}

	// Check for rebalance notification
	if rebalance := resp.GetRebalance(); rebalance != nil {
		c.handleRebalance(rebalance)
	}

	// Check for error
	if errResp := resp.GetError(); errResp != nil {
		c.logger.Error("server error", "code", errResp.Code, "message", errResp.Message)
	}
}

// handleAssignment updates the current partition assignment.
func (c *Consumer) handleAssignment(assignment *pb.Assignment) {
	c.assignmentMu.Lock()
	defer c.assignmentMu.Unlock()

	// Clear existing assignment
	c.assignment = make(map[string][]int32)

	// Build new assignment
	for _, tp := range assignment.Partitions {
		c.assignment[tp.Topic] = append(c.assignment[tp.Topic], tp.Partition)
	}

	c.logger.Info("assignment updated",
		"member_id", assignment.MemberId,
		"generation", assignment.Generation,
		"partitions", c.assignment,
	)
}

// handleRebalance handles rebalance notifications.
func (c *Consumer) handleRebalance(rebalance *pb.RebalanceNotification) {
	c.logger.Info("rebalance notification",
		"type", rebalance.Type,
		"partitions", rebalance.Partitions,
	)

	// For revoke, we might want to commit offsets before losing partitions
	if rebalance.Type == pb.RebalanceType_REBALANCE_TYPE_REVOKE {
		// Could commit offsets here if needed
		c.logger.Debug("partitions being revoked")
	}
}

// =============================================================================
// OFFSET MANAGEMENT
// =============================================================================

// autoCommitLoop periodically commits offsets.
func (c *Consumer) autoCommitLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.AutoCommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			// In a real implementation, we'd track processed offsets
			// and commit them here. For now, this is a placeholder.
			c.logger.Debug("auto-commit tick")
		}
	}
}

// =============================================================================
// MESSAGE OPERATIONS
// =============================================================================

// Ack acknowledges successful processing of the message.
// After ack, the message won't be redelivered.
func (m *ConsumerMessage) Ack() error {
	if m.consumer == nil || m.consumer.client == nil {
		return fmt.Errorf("consumer not available")
	}

	return m.consumer.client.Ack(m.consumer.ctx, m.consumer.config.GroupID, m.ReceiptHandle)
}

// Nack negative-acknowledges the message.
// The message will be redelivered after the visibility timeout.
func (m *ConsumerMessage) Nack(visibilityTimeout time.Duration) error {
	if m.consumer == nil || m.consumer.client == nil {
		return fmt.Errorf("consumer not available")
	}

	return m.consumer.client.Nack(m.consumer.ctx, m.consumer.config.GroupID, m.ReceiptHandle, visibilityTimeout)
}

// Reject rejects the message permanently.
// The message will be sent to DLQ if configured.
func (m *ConsumerMessage) Reject(reason string) error {
	if m.consumer == nil || m.consumer.client == nil {
		return fmt.Errorf("consumer not available")
	}

	return m.consumer.client.Reject(m.consumer.ctx, m.consumer.config.GroupID, m.ReceiptHandle, reason)
}

// ExtendVisibility extends the processing time for this message.
func (m *ConsumerMessage) ExtendVisibility(extension time.Duration) error {
	if m.consumer == nil || m.consumer.client == nil {
		return fmt.Errorf("consumer not available")
	}

	return m.consumer.client.ExtendVisibility(m.consumer.ctx, m.consumer.config.GroupID, m.ReceiptHandle, extension)
}

// =============================================================================
// CONSUMER STATE
// =============================================================================

// Assignment returns the current partition assignment.
func (c *Consumer) Assignment() map[string][]int32 {
	c.assignmentMu.RLock()
	defer c.assignmentMu.RUnlock()

	// Return a copy
	result := make(map[string][]int32)
	for topic, partitions := range c.assignment {
		result[topic] = append([]int32{}, partitions...)
	}
	return result
}

// GroupID returns the consumer group ID.
func (c *Consumer) GroupID() string {
	return c.config.GroupID
}

// Topics returns the subscribed topics.
func (c *Consumer) Topics() []string {
	return c.config.Topics
}
