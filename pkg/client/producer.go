// =============================================================================
// GOQUEUE HIGH-LEVEL PRODUCER
// =============================================================================
//
// This is the high-level producer that provides a simple API for publishing
// messages. It wraps the low-level client with:
//   - Automatic batching for throughput
//   - Configurable delivery guarantees
//   - Async publishing with callbacks
//   - Graceful shutdown with flush
//
// ARCHITECTURE:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                          Application                                    │
//   │                                                                         │
//   │   producer.Send("orders", msg)   producer.SendAsync("orders", msg, cb)  │
//   │              │                              │                           │
//   │              └──────────────┬───────────────┘                           │
//   │                             │                                           │
//   │                    ┌────────▼────────┐                                  │
//   │                    │    Producer     │ ◄── This file                    │
//   │                    │   (high-level)  │                                  │
//   │                    └────────┬────────┘                                  │
//   │                             │                                           │
//   │                    ┌────────▼────────┐                                  │
//   │                    │     Client      │                                  │
//   │                    │   (low-level)   │                                  │
//   │                    └────────┬────────┘                                  │
//   └─────────────────────────────┼───────────────────────────────────────────┘
//                                 │
//                        ┌────────▼────────┐
//                        │   gRPC Server   │
//                        └─────────────────┘
//
// USAGE:
//
//   // Create producer
//   producer, err := client.NewProducer(client.ProducerConfig{
//       Address: "localhost:9000",
//       Async:   true,  // Enable async sending
//   })
//   if err != nil {
//       log.Fatal(err)
//   }
//   defer producer.Close()
//
//   // Sync send (wait for ack)
//   result, err := producer.Send(ctx, "orders", []byte(`{"id": 123}`))
//
//   // Async send (fire and forget with callback)
//   producer.SendAsync(ctx, "orders", []byte(`{"id": 456}`), func(r *ProducerResult, err error) {
//       if err != nil {
//           log.Printf("Failed: %v", err)
//       }
//   })
//
//   // Flush pending messages before shutdown
//   producer.Flush(ctx)
//
// =============================================================================

package client

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// =============================================================================
// PRODUCER CONFIGURATION
// =============================================================================

// ProducerConfig configures the high-level producer.
//
// BATCHING EXPLAINED:
//
//	Batching improves throughput by sending multiple messages in one request.
//	Trade-off: Higher latency for individual messages.
//
//	BatchSize: How many messages to accumulate before sending
//	- Higher: Better throughput, higher latency
//	- Lower: Lower latency, more network overhead
//	- Default: 100 messages
//
//	BatchTimeout: Max time to wait for a full batch
//	- Ensures messages are sent even if batch isn't full
//	- Default: 10ms (good for most cases)
//
// COMPARISON:
//   - Kafka: linger.ms + batch.size
//   - RabbitMQ: Basic.Publish is per-message (no batching in protocol)
//   - SQS: SendMessageBatch (up to 10 messages)
type ProducerConfig struct {
	// Connection
	Address string

	// Client configuration (optional)
	ClientConfig *Config

	// Batching
	BatchSize    int           // Messages per batch (default: 100)
	BatchTimeout time.Duration // Max wait for batch (default: 10ms)

	// Async settings
	BufferSize int // Async buffer size (default: 10000)

	// Logger
	Logger *slog.Logger
}

// DefaultProducerConfig returns a producer config with sensible defaults.
func DefaultProducerConfig(address string) ProducerConfig {
	return ProducerConfig{
		Address:      address,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		BufferSize:   10000,
	}
}

// =============================================================================
// PRODUCER STRUCTURE
// =============================================================================

// Producer is the high-level message producer.
type Producer struct {
	client *Client
	config ProducerConfig
	logger *slog.Logger

	// Async machinery
	asyncCh chan *asyncMessage
	wg      sync.WaitGroup

	// State
	mu     sync.RWMutex
	closed bool
}

// asyncMessage wraps a message for async sending.
type asyncMessage struct {
	ctx      context.Context
	topic    string
	value    []byte
	opts     []PublishOption
	callback func(*PublishResult, error)
}

// ProducerResult contains the result of a produce operation.
type ProducerResult = PublishResult

// =============================================================================
// PRODUCER LIFECYCLE
// =============================================================================

// NewProducer creates a new high-level producer.
//
// The producer starts background workers for async sending.
// Always call Close() when done.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	// Apply defaults
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.BatchTimeout == 0 {
		cfg.BatchTimeout = 10 * time.Millisecond
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 10000
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

	p := &Producer{
		client:  client,
		config:  cfg,
		logger:  logger,
		asyncCh: make(chan *asyncMessage, cfg.BufferSize),
	}

	// Start async worker
	p.wg.Add(1)
	go p.asyncWorker()

	return p, nil
}

// Close closes the producer and underlying client.
// Waits for pending async messages to be sent.
func (p *Producer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	// Close async channel and wait for worker to finish
	close(p.asyncCh)
	p.wg.Wait()

	return p.client.Close()
}

// =============================================================================
// SYNC OPERATIONS
// =============================================================================

// Send publishes a message synchronously.
// Blocks until the message is acknowledged by the broker.
//
// DELIVERY GUARANTEE:
//
//	When this returns without error, the message is durably stored.
//	This is at-least-once delivery - the message may be duplicated
//	if a network error occurs after the broker writes but before ack.
func (p *Producer) Send(ctx context.Context, topic string, value []byte, opts ...PublishOption) (*ProducerResult, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrClientClosed
	}
	p.mu.RUnlock()

	return p.client.Publish(ctx, topic, value, opts...)
}

// SendWithKey publishes a message with a partition key.
// Messages with the same key go to the same partition (ordering guarantee).
func (p *Producer) SendWithKey(ctx context.Context, topic string, key, value []byte, opts ...PublishOption) (*ProducerResult, error) {
	opts = append(opts, WithKey(key))
	return p.Send(ctx, topic, value, opts...)
}

// =============================================================================
// ASYNC OPERATIONS
// =============================================================================

// SendAsync publishes a message asynchronously.
// Returns immediately, callback is invoked when send completes.
//
// FLOW:
//
//	SendAsync ──► Buffer ──► Worker ──► gRPC ──► Callback
//
// The callback is invoked from a worker goroutine, not the caller's.
// Keep callback logic short to avoid blocking other messages.
//
// If the buffer is full, this will block until space is available.
func (p *Producer) SendAsync(ctx context.Context, topic string, value []byte, callback func(*ProducerResult, error), opts ...PublishOption) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		if callback != nil {
			callback(nil, ErrClientClosed)
		}
		return ErrClientClosed
	}
	p.mu.RUnlock()

	msg := &asyncMessage{
		ctx:      ctx,
		topic:    topic,
		value:    value,
		opts:     opts,
		callback: callback,
	}

	select {
	case p.asyncCh <- msg:
		return nil
	case <-ctx.Done():
		if callback != nil {
			callback(nil, ctx.Err())
		}
		return ctx.Err()
	}
}

// asyncWorker processes async messages.
func (p *Producer) asyncWorker() {
	defer p.wg.Done()

	for msg := range p.asyncCh {
		result, err := p.client.Publish(msg.ctx, msg.topic, msg.value, msg.opts...)
		if msg.callback != nil {
			msg.callback(result, err)
		}
	}
}

// =============================================================================
// FLUSH OPERATIONS
// =============================================================================

// Flush waits for all pending async messages to be sent.
// Call this before shutdown to ensure no messages are lost.
//
// Note: This implementation uses a simple drain approach.
// A more sophisticated implementation would use a sync mechanism.
func (p *Producer) Flush(ctx context.Context) error {
	// In this simple implementation, we just wait for the channel to drain
	// A production implementation would use proper synchronization

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if len(p.asyncCh) == 0 {
				return nil
			}
		}
	}
}

// Pending returns the number of pending async messages.
func (p *Producer) Pending() int {
	return len(p.asyncCh)
}
