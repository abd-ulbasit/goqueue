// =============================================================================
// SCHEDULER - COORDINATING DELAYED MESSAGE DELIVERY
// =============================================================================
//
// WHAT IS THE SCHEDULER?
// The scheduler is the central coordinator for delayed messages. It ties together:
//   - Timer wheel (in-memory scheduling)
//   - Delay index (persistent storage)
//   - Broker (message delivery)
//
// DELAYED MESSAGE FLOW:
//
//   PUBLISH (with delay):
//   ┌──────────┐   1. Write message   ┌──────────┐
//   │ Producer │ ──────────────────►  │ Partition│
//   │          │                      │   Log    │
//   └──────────┘                      └──────────┘
//        │                                  │
//        │ 2. Record delay                  │ offset returned
//        ▼                                  │
//   ┌──────────┐   3. Add to wheel    ┌──────────┐
//   │  Delay   │ ──────────────────►  │  Timer   │
//   │  Index   │                      │  Wheel   │
//   └──────────┘                      └──────────┘
//
//   DELIVERY (when delay expires):
//   ┌──────────┐   1. Timer fires     ┌──────────┐
//   │  Timer   │ ──────────────────►  │Scheduler │
//   │  Wheel   │                      │ Callback │
//   └──────────┘                      └──────────┘
//        │                                  │
//        │                                  │ 2. Update index
//        │                                  ▼
//        │                            ┌──────────┐
//        │                            │  Delay   │
//        │                            │  Index   │
//        │                            └──────────┘
//        │                                  │
//        │ 3. Message now visible           │
//        ▼                                  ▼
//   ┌──────────┐                      ┌──────────┐
//   │ Consumer │ ◄────────────────────│ Partition│
//   │   Poll   │    4. Normal consume │   Log    │
//   └──────────┘                      └──────────┘
//
// VISIBILITY INTEGRATION:
//
// Delayed messages interact with the visibility system (M4):
//   - Before DeliverAt: Message is invisible (not returned by Consume)
//   - After DeliverAt: Message becomes visible, normal ACK flow applies
//   - If consumer NACKs: Retry uses normal backoff, NOT original delay
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   ┌─────────────┬─────────────────────────────────────────────────────────┐
//   │ System      │ Delay Implementation                                    │
//   ├─────────────┼─────────────────────────────────────────────────────────┤
//   │ SQS         │ DelaySeconds parameter (0-900 seconds max per message)  │
//   │             │ Queue-level delay also supported                        │
//   ├─────────────┼─────────────────────────────────────────────────────────┤
//   │ Kafka       │ No native support. Workarounds:                         │
//   │             │ - Separate delay topics with scheduled consumers        │
//   │             │ - External scheduler (Kafka Streams with punctuators)   │
//   │             │ - Time-bucketed partitions                              │
//   ├─────────────┼─────────────────────────────────────────────────────────┤
//   │ RabbitMQ    │ Plugin-based (rabbitmq-delayed-message-exchange)        │
//   │             │ Or message TTL + dead-letter exchange trick             │
//   ├─────────────┼─────────────────────────────────────────────────────────┤
//   │ GoQueue     │ Native timer wheel with persistent index                │
//   │             │ Supports delays up to 7+ days with 10ms precision       │
//   └─────────────┴─────────────────────────────────────────────────────────┘
//
// =============================================================================

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// ERRORS
// =============================================================================

var (
	// ErrSchedulerClosed means the scheduler has been stopped
	ErrSchedulerClosed = errors.New("scheduler is closed")

	// ErrMessageAlreadyDelayed means a delay already exists for this message
	ErrMessageAlreadyDelayed = errors.New("message already has a delay scheduled")

	// ErrDelayedMessageNotFound means no delay exists for the given message
	ErrDelayedMessageNotFound = errors.New("delayed message not found")

	// ErrTooManyDelayed means the topic or partition has too many delayed messages
	ErrTooManyDelayed = errors.New("too many delayed messages")
)

// =============================================================================
// SCHEDULER CONFIGURATION
// =============================================================================

// SchedulerConfig holds scheduler configuration.
type SchedulerConfig struct {
	// DataDir is the base directory for delay index files
	DataDir string

	// MaxDelayedPerTopic limits delayed messages per topic
	// Default: 1,000,000
	MaxDelayedPerTopic int64

	// MaxDelayedPerPartition limits delayed messages per partition
	// Default: 100,000
	MaxDelayedPerPartition int64

	// MaxDelay is the maximum allowed delay duration
	// Default: 7 days (matches timer wheel capacity)
	MaxDelay time.Duration
}

// DefaultSchedulerConfig returns default configuration.
func DefaultSchedulerConfig(dataDir string) SchedulerConfig {
	return SchedulerConfig{
		DataDir:                dataDir,
		MaxDelayedPerTopic:     1_000_000,
		MaxDelayedPerPartition: 100_000,
		MaxDelay:               MaxDelay,
	}
}

// =============================================================================
// SCHEDULED MESSAGE
// =============================================================================

// ScheduledMessage represents a message scheduled for future delivery.
type ScheduledMessage struct {
	// Topic the message belongs to
	Topic string

	// Partition within the topic
	Partition int

	// Offset in the partition log
	Offset int64

	// DeliverAt is when the message should become visible
	DeliverAt time.Time

	// ScheduledAt is when the delay was created
	ScheduledAt time.Time

	// TimeRemaining until delivery
	TimeRemaining time.Duration

	// State indicates current status
	State string // "pending", "delivered", "cancelled"
}

// =============================================================================
// SCHEDULER
// =============================================================================

// Scheduler coordinates delayed message delivery using timer wheel and
// persistent delay indices.
//
// THREAD SAFETY: All methods are safe for concurrent use.
type Scheduler struct {
	// config holds scheduler settings
	config SchedulerConfig

	// timerWheel handles in-memory scheduling with O(1) operations
	timerWheel *TimerWheel

	// indices maps topic name to delay index
	indices map[string]*DelayIndex

	// broker provides message visibility control
	// Set via SetBroker after construction (breaks circular dependency)
	broker *Broker

	// onDeliver is called when a delayed message should become visible
	// This is set by the broker during initialization
	onDeliver func(topic string, partition int, offset int64) error

	// mu protects indices map
	mu sync.RWMutex

	// logger for scheduler operations
	logger *slog.Logger

	// closed indicates scheduler has been stopped
	closed atomic.Bool

	// stats
	totalScheduled atomic.Uint64
	totalDelivered atomic.Uint64
	totalCancelled atomic.Uint64
	totalExpired   atomic.Uint64
}

// NewScheduler creates a new delay scheduler.
//
// Call SetBroker() after broker creation to complete initialization.
// Call Start() to begin processing delayed messages.
func NewScheduler(config SchedulerConfig) (*Scheduler, error) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	s := &Scheduler{
		config:  config,
		indices: make(map[string]*DelayIndex),
		logger:  logger,
	}

	// Create timer wheel with our callback
	s.timerWheel = NewTimerWheel(TimerWheelConfig{
		Callback: s.handleTimerExpiry,
	})

	logger.Info("scheduler created",
		"max_delay", config.MaxDelay.String(),
		"max_per_topic", config.MaxDelayedPerTopic)

	return s, nil
}

// SetBroker sets the broker reference for message operations.
// Must be called before Start().
func (s *Scheduler) SetBroker(broker *Broker) {
	s.broker = broker
}

// SetDeliveryCallback sets the function called when delayed messages are ready.
func (s *Scheduler) SetDeliveryCallback(cb func(topic string, partition int, offset int64) error) {
	s.onDeliver = cb
}

// OnDeliver sets a simple callback when delayed messages are ready.
// This is a simpler version of SetDeliveryCallback that doesn't return an error.
func (s *Scheduler) OnDeliver(cb func(topic string, partition int, offset int64)) {
	s.onDeliver = func(topic string, partition int, offset int64) error {
		cb(topic, partition, offset)
		return nil
	}
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Start begins processing delayed messages.
// This loads existing delay indices and adds pending entries to the timer wheel.
func (s *Scheduler) Start() error {
	s.logger.Info("starting scheduler")

	// Load existing delay indices would happen here
	// For now, indices are created on-demand when topics have delayed messages

	return nil
}

// Close stops the scheduler and releases resources.
func (s *Scheduler) Close() error {
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	s.logger.Info("stopping scheduler")

	// Stop timer wheel
	if err := s.timerWheel.Close(); err != nil {
		s.logger.Error("error closing timer wheel", "error", err)
	}

	// Close all delay indices
	s.mu.Lock()
	for topic, idx := range s.indices {
		if err := idx.Close(); err != nil {
			s.logger.Error("error closing delay index",
				"topic", topic,
				"error", err)
		}
	}
	s.mu.Unlock()

	s.logger.Info("scheduler stopped",
		"total_scheduled", s.totalScheduled.Load(),
		"total_delivered", s.totalDelivered.Load(),
		"total_cancelled", s.totalCancelled.Load())

	return nil
}

// =============================================================================
// SCHEDULING API
// =============================================================================

// Schedule adds a delayed message to the scheduler.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - offset: Message offset (already written to log)
//   - delay: How long to delay delivery
//
// RETURNS:
//   - Error if delay exceeds maximum or scheduler is full
//
// FLOW:
//  1. Validate delay duration
//  2. Get or create delay index for topic
//  3. Record in delay index (persistence)
//  4. Add to timer wheel (scheduling)
func (s *Scheduler) Schedule(topic string, partition int, offset int64, delay time.Duration) error {
	if s.closed.Load() {
		return ErrSchedulerClosed
	}

	if delay > s.config.MaxDelay {
		return fmt.Errorf("%w: requested %v, max %v", ErrDelayTooLong, delay, s.config.MaxDelay)
	}

	if delay < 0 {
		return ErrDelayNegative
	}

	deliverAt := time.Now().Add(delay)
	return s.scheduleAt(topic, partition, offset, deliverAt)
}

// ScheduleAt adds a message to deliver at a specific time.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - offset: Message offset
//   - deliverAt: Absolute time for delivery
func (s *Scheduler) ScheduleAt(topic string, partition int, offset int64, deliverAt time.Time) error {
	if s.closed.Load() {
		return ErrSchedulerClosed
	}

	delay := time.Until(deliverAt)
	if delay > s.config.MaxDelay {
		return fmt.Errorf("%w: scheduled time is too far in future", ErrDelayTooLong)
	}

	return s.scheduleAt(topic, partition, offset, deliverAt)
}

// scheduleAt is the internal implementation for both Schedule and ScheduleAt.
func (s *Scheduler) scheduleAt(topic string, partition int, offset int64, deliverAt time.Time) error {
	// Get or create delay index for topic
	idx, err := s.getOrCreateIndex(topic)
	if err != nil {
		return fmt.Errorf("failed to get delay index: %w", err)
	}

	// Record in persistent index
	if err := idx.Add(offset, partition, deliverAt); err != nil {
		return fmt.Errorf("failed to record delay: %w", err)
	}

	// Create timer ID: topic:partition:offset
	timerID := fmt.Sprintf("%s:%d:%d", topic, partition, offset)

	// Add to timer wheel
	data := &DelayedMessageData{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	}

	if err := s.timerWheel.ScheduleAt(timerID, deliverAt, data); err != nil {
		// Rollback index entry
		idx.MarkCancelled(offset)
		return fmt.Errorf("failed to schedule timer: %w", err)
	}

	s.totalScheduled.Add(1)
	s.logger.Debug("scheduled delayed message",
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"deliver_at", deliverAt.Format(time.RFC3339))

	return nil
}

// Cancel removes a delayed message from the scheduler.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - offset: Message offset
//
// RETURNS:
//   - Error if message is not found or already delivered
func (s *Scheduler) Cancel(topic string, partition int, offset int64) error {
	if s.closed.Load() {
		return ErrSchedulerClosed
	}

	// Get delay index
	idx, err := s.getIndex(topic)
	if err != nil {
		return ErrDelayedMessageNotFound
	}

	// Check if entry exists
	entry, err := idx.Get(offset)
	if err != nil {
		return ErrDelayedMessageNotFound
	}

	if entry.State != delayStatePending {
		return fmt.Errorf("message already processed (state=%d)", entry.State)
	}

	// Cancel timer
	timerID := fmt.Sprintf("%s:%d:%d", topic, partition, offset)
	s.timerWheel.Cancel(timerID)

	// Update index
	if err := idx.MarkCancelled(offset); err != nil {
		return fmt.Errorf("failed to mark cancelled: %w", err)
	}

	s.totalCancelled.Add(1)
	s.logger.Debug("cancelled delayed message",
		"topic", topic,
		"partition", partition,
		"offset", offset)

	return nil
}

// =============================================================================
// QUERY API
// =============================================================================

// GetDelayedMessage retrieves information about a delayed message.
func (s *Scheduler) GetDelayedMessage(topic string, partition int, offset int64) (*ScheduledMessage, error) {
	idx, err := s.getIndex(topic)
	if err != nil {
		return nil, ErrDelayedMessageNotFound
	}

	entry, err := idx.Get(offset)
	if err != nil {
		return nil, ErrDelayedMessageNotFound
	}

	state := "pending"
	switch entry.State {
	case delayStateDelivered:
		state = "delivered"
	case delayStateCancelled:
		state = "cancelled"
	case delayStateExpired:
		state = "expired"
	}

	deliverAt := time.Unix(0, entry.DeliverAt)
	timeRemaining := time.Until(deliverAt)
	if timeRemaining < 0 {
		timeRemaining = 0
	}

	return &ScheduledMessage{
		Topic:         topic,
		Partition:     int(entry.Partition),
		Offset:        entry.Offset,
		DeliverAt:     deliverAt,
		TimeRemaining: timeRemaining,
		State:         state,
	}, nil
}

// GetDelayedMessages returns all pending delayed messages for a topic.
// Supports pagination with limit and skip parameters.
func (s *Scheduler) GetDelayedMessages(topic string, limit int, skip int) []*ScheduledMessage {
	idx, err := s.getIndex(topic)
	if err != nil {
		return []*ScheduledMessage{}
	}
	// TODO: shouldn't we do this pagination in the index itself to avoid loading all entries into memory ?
	entries := idx.GetPendingEntries()

	// Apply skip
	if skip >= len(entries) {
		return []*ScheduledMessage{}
	}
	entries = entries[skip:]

	// Apply limit
	if limit > 0 && limit < len(entries) {
		entries = entries[:limit]
	}

	result := make([]*ScheduledMessage, 0, len(entries))
	for _, entry := range entries {
		result = append(result, &ScheduledMessage{
			Topic:     topic,
			Partition: int(entry.Partition),
			Offset:    entry.Offset,
			DeliverAt: time.Unix(0, entry.DeliverAt),
			State:     "pending",
		})
	}

	return result
}

// IsDelayed checks if a message has an active delay.
func (s *Scheduler) IsDelayed(topic string, partition int, offset int64) bool {
	idx, err := s.getIndex(topic)
	if err != nil {
		return false
	}

	entry, err := idx.Get(offset)
	if err != nil {
		return false
	}

	return entry.State == delayStatePending && !entry.IsReady()
}

// GetDeliverTime returns when a delayed message will be delivered.
// Returns zero time if message is not delayed.
func (s *Scheduler) GetDeliverTime(topic string, partition int, offset int64) time.Time {
	idx, err := s.getIndex(topic)
	if err != nil {
		return time.Time{}
	}

	entry, err := idx.Get(offset)
	if err != nil {
		return time.Time{}
	}

	return time.Unix(0, entry.DeliverAt)
}

// =============================================================================
// STATISTICS
// =============================================================================

// SchedulerStats holds scheduler statistics.
type SchedulerStats struct {
	TotalScheduled uint64
	TotalDelivered uint64
	TotalCancelled uint64
	TotalPending   int64 // Total pending across all topics
	TimerWheelSize int
	TopicStats     map[string]TopicDelayStats
	ByTopic        map[string]int64 // Shortcut: topic -> pending count
}

// TopicDelayStats holds per-topic delay statistics.
type TopicDelayStats struct {
	Topic        string
	PendingCount int64
	TotalEntries int64
}

// Stats returns scheduler statistics.
func (s *Scheduler) Stats() SchedulerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topicStats := make(map[string]TopicDelayStats)
	byTopic := make(map[string]int64)
	var totalPending int64

	for topic, idx := range s.indices {
		stats := idx.Stats()
		topicStats[topic] = TopicDelayStats{
			Topic:        topic,
			PendingCount: stats.PendingCount,
			TotalEntries: stats.TotalEntries,
		}
		byTopic[topic] = stats.PendingCount
		totalPending += stats.PendingCount
	}

	return SchedulerStats{
		TotalScheduled: s.totalScheduled.Load(),
		TotalDelivered: s.totalDelivered.Load(),
		TotalCancelled: s.totalCancelled.Load(),
		TotalPending:   totalPending,
		TimerWheelSize: s.timerWheel.Size(),
		TopicStats:     topicStats,
		ByTopic:        byTopic,
	}
}

// =============================================================================
// INTERNAL: INDEX MANAGEMENT
// =============================================================================

// getOrCreateIndex gets or creates a delay index for a topic.
func (s *Scheduler) getOrCreateIndex(topic string) (*DelayIndex, error) {
	s.mu.RLock()
	idx, exists := s.indices[topic]
	s.mu.RUnlock()

	if exists {
		return idx, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if idx, exists = s.indices[topic]; exists {
		return idx, nil
	}

	// Create new index
	config := DelayIndexConfig{
		DataDir:    s.config.DataDir,
		Topic:      topic,
		MaxEntries: s.config.MaxDelayedPerTopic,
	}

	idx, err := NewDelayIndex(config)
	if err != nil {
		return nil, err
	}

	s.indices[topic] = idx
	return idx, nil
}

// getIndex gets an existing delay index (doesn't create).
func (s *Scheduler) getIndex(topic string) (*DelayIndex, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	idx, exists := s.indices[topic]
	if !exists {
		return nil, fmt.Errorf("no delay index for topic: %s", topic)
	}

	return idx, nil
}

// =============================================================================
// INTERNAL: TIMER CALLBACK
// =============================================================================

// handleTimerExpiry is called by the timer wheel when a delay expires.
func (s *Scheduler) handleTimerExpiry(entry *TimerEntry) {
	data, ok := entry.Data.(*DelayedMessageData)
	if !ok {
		s.logger.Error("invalid timer entry data")
		return
	}

	s.logger.Debug("delayed message ready",
		"topic", data.Topic,
		"partition", data.Partition,
		"offset", data.Offset)

	// Update delay index
	if idx, err := s.getIndex(data.Topic); err == nil {
		idx.MarkDelivered(data.Offset)
	}

	// Call delivery callback if set
	if s.onDeliver != nil {
		if err := s.onDeliver(data.Topic, data.Partition, data.Offset); err != nil {
			s.logger.Error("delivery callback failed",
				"topic", data.Topic,
				"partition", data.Partition,
				"offset", data.Offset,
				"error", err)
		}
	}

	s.totalDelivered.Add(1)
}

// =============================================================================
// RECOVERY
// =============================================================================

// RecoverTopic loads pending delays from an existing delay index.
// Called during broker startup for each topic.
func (s *Scheduler) RecoverTopic(topic string) error {
	idx, err := s.getOrCreateIndex(topic)
	if err != nil {
		return err
	}

	entries := idx.GetPendingEntries()
	now := time.Now()
	recovered := 0
	immediateDelivery := 0

	for _, entry := range entries {
		timerID := fmt.Sprintf("%s:%d:%d", topic, entry.Partition, entry.Offset)
		deliverAt := time.Unix(0, entry.DeliverAt)

		if deliverAt.Before(now) {
			// Already past delivery time - deliver immediately
			s.handleTimerExpiry(&TimerEntry{
				ID:        timerID,
				DeliverAt: deliverAt,
				Data: &DelayedMessageData{
					Topic:     topic,
					Partition: int(entry.Partition),
					Offset:    entry.Offset,
				},
			})
			immediateDelivery++
		} else {
			// Schedule for future delivery
			data := &DelayedMessageData{
				Topic:     topic,
				Partition: int(entry.Partition),
				Offset:    entry.Offset,
			}
			if err := s.timerWheel.ScheduleAt(timerID, deliverAt, data); err != nil {
				s.logger.Warn("failed to recover timer",
					"topic", topic,
					"offset", entry.Offset,
					"error", err)
				continue
			}
			recovered++
		}
	}

	if recovered > 0 || immediateDelivery > 0 {
		s.logger.Info("recovered delayed messages",
			"topic", topic,
			"scheduled", recovered,
			"immediate_delivery", immediateDelivery)
	}

	return nil
}

// =============================================================================
// CONTEXT-AWARE WAITING
// =============================================================================

// WaitForDeliveries waits until all pending delays are delivered or context expires.
func (s *Scheduler) WaitForDeliveries(ctx context.Context) error {
	return s.timerWheel.WaitForEmpty(ctx)
}
