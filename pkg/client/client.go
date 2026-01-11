// =============================================================================
// GOQUEUE GO CLIENT - LOW-LEVEL GRPC CLIENT
// =============================================================================
//
// This is the low-level client that provides direct access to all gRPC operations.
// It wraps the generated gRPC stubs with:
//   - Connection management (reconnection, keepalive)
//   - Consistent error handling
//   - Context propagation
//   - Resource cleanup
//
// ARCHITECTURE:
//
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │                          Application Layer                             │
//   │                                                                        │
//   │   ┌─────────────────┐    ┌─────────────────┐    ┌──────────────────┐   │
//   │   │  High-Level     │    │  High-Level     │    │  Direct Client   │   │
//   │   │  Producer       │    │  Consumer       │    │  Usage           │   │
//   │   └────────┬────────┘    └────────┬────────┘    └────────┬─────────┘   │
//   │            │                      │                      │             │
//   │            └──────────────────────┼──────────────────────┘             │
//   │                                   │                                    │
//   │                          ┌────────▼────────┐                           │
//   │                          │    Client       │ ◄── This file             │
//   │                          │ (low-level)     │                           │
//   │                          └────────┬────────┘                           │
//   └───────────────────────────────────┼────────────────────────────────────┘
//                                       │
//   ┌───────────────────────────────────┼────────────────────────────────────┐
//   │                          gRPC Layer                                    │
//   │                                   │                                    │
//   │   ┌───────────────────────────────▼───────────────────────────────┐    │
//   │   │                    grpc.ClientConn                            │    │
//   │   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐          │    │
//   │   │  │ Publish  │ │ Consume  │ │   Ack    │ │  Offset  │ ...      │    │
//   │   │  │ Client   │ │ Client   │ │ Client   │ │  Client  │          │    │
//   │   │  └──────────┘ └──────────┘ └──────────┘ └──────────┘          │    │
//   │   └───────────────────────────────────────────────────────────────┘    │
//   │                                                                        │
//   └────────────────────────────────────────────────────────────────────────┘
//
// USAGE:
//
//   // Create client
//   client, err := client.New(client.Config{
//       Address: "localhost:9000",
//   })
//   if err != nil {
//       log.Fatal(err)
//   }
//   defer client.Close()
//
//   // Publish a message
//   resp, err := client.Publish(ctx, "orders", []byte(`{"id": 123}`))
//
//   // Consume messages (streaming)
//   stream, err := client.Consume(ctx, "orders", 0, 0)
//   for {
//       msg, err := stream.Recv()
//       // process msg
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// =============================================================================
// CLIENT CONFIGURATION
// =============================================================================

// Config holds the client configuration.
//
// CONNECTION PARAMETERS EXPLAINED:
//
//	DialTimeout: How long to wait for initial connection
//	- Too short: May fail on slow networks
//	- Too long: Slow failure detection
//	- Default: 10s (matches most network timeouts)
//
//	KeepAliveTime: How often to send keepalive pings
//	- Why needed: Detect dead connections that TCP doesn't
//	- Too frequent: Network overhead
//	- Too infrequent: Slow detection of dead servers
//	- Default: 30s (standard for most systems)
//
//	KeepAliveTimeout: How long to wait for keepalive response
//	- If no response: Connection considered dead
//	- Default: 10s
//
// COMPARISON WITH OTHER CLIENTS:
//   - Kafka Java: Similar keepalive mechanism
//   - RabbitMQ: Heartbeat frames (same concept)
//   - Redis: PING/PONG keepalive
type Config struct {
	// Address is the gRPC server address (host:port)
	Address string

	// DialTimeout is the timeout for establishing connection
	DialTimeout time.Duration

	// KeepAliveTime is the interval between keepalive pings
	KeepAliveTime time.Duration

	// KeepAliveTimeout is how long to wait for keepalive response
	KeepAliveTimeout time.Duration

	// MaxRetries is the maximum number of retries for failed operations
	MaxRetries int

	// RetryBackoff is the initial backoff duration for retries
	RetryBackoff time.Duration

	// Logger is the logger to use (optional)
	Logger *slog.Logger
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig(address string) Config {
	return Config{
		Address:          address,
		DialTimeout:      10 * time.Second,
		KeepAliveTime:    30 * time.Second,
		KeepAliveTimeout: 10 * time.Second,
		MaxRetries:       3,
		RetryBackoff:     100 * time.Millisecond,
	}
}

// =============================================================================
// CLIENT STRUCTURE
// =============================================================================

// Client is the low-level gRPC client for goqueue.
// It provides direct access to all gRPC services.
type Client struct {
	// Connection
	conn *grpc.ClientConn

	// Service clients (generated stubs)
	publish pb.PublishServiceClient
	consume pb.ConsumeServiceClient
	ack     pb.AckServiceClient
	offset  pb.OffsetServiceClient
	health  pb.HealthServiceClient

	// Configuration
	config Config
	logger *slog.Logger

	// State
	mu     sync.RWMutex
	closed bool
}

// =============================================================================
// CLIENT LIFECYCLE
// =============================================================================

// New creates a new goqueue client.
//
// GRPC CONNECTION OPTIONS EXPLAINED:
//
//	WithTransportCredentials(insecure.NewCredentials()):
//	- Disables TLS for development
//	- In production, use real credentials!
//
//	WithKeepaliveParams:
//	- Time: Ping interval when connection is idle
//	- Timeout: How long to wait for pong
//	- PermitWithoutStream: Send pings even without active RPCs
//	  (Important for detecting dead connections in consumers)
//
//	WithBlock:
//	- Wait for connection to be established before returning
//	- Without this, Dial returns immediately and connects in background
//	- We want to know if the server is reachable on startup
//
// ERROR HANDLING:
//
//	Returns error if connection cannot be established within timeout.
//	Caller should check error and possibly retry with backoff.
func New(cfg Config) (*Client, error) {
	// Apply defaults
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.KeepAliveTime == 0 {
		cfg.KeepAliveTime = 30 * time.Second
	}
	if cfg.KeepAliveTimeout == 0 {
		cfg.KeepAliveTimeout = 10 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBackoff == 0 {
		cfg.RetryBackoff = 100 * time.Millisecond
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// =========================================================================
	// GRPC DIAL OPTIONS
	// =========================================================================
	//
	// These options control how the client connects and maintains the connection.
	//
	// KEEPALIVE is crucial for:
	//   1. Detecting dead servers that don't close connections
	//   2. Keeping NAT/firewall mappings alive
	//   3. Ensuring long-polling consumers don't timeout
	//
	// COMPARISON:
	//   - HTTP/1.1: Keep-Alive header (same concept, different protocol)
	//   - WebSocket: Ping/Pong frames
	//   - Kafka: Metadata refresh serves similar purpose
	//
	// =========================================================================
	dialOpts := []grpc.DialOption{
		// Use insecure credentials for development
		// In production, use grpc.WithTransportCredentials(creds)
		grpc.WithTransportCredentials(insecure.NewCredentials()),

		// Keepalive configuration
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                cfg.KeepAliveTime,
			Timeout:             cfg.KeepAliveTimeout,
			PermitWithoutStream: true, // Send pings even when no active streams
		}),
	}

	// Create context with timeout for initial connection
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	// =========================================================================
	// ESTABLISHING CONNECTION
	// =========================================================================
	//
	// grpc.NewClient (Go 1.21+) vs grpc.Dial (deprecated):
	//   - NewClient: Non-blocking, returns immediately
	//   - We need to manually verify connectivity
	//
	// The connection is actually established lazily on first RPC.
	// We do a health check to verify connectivity upfront.
	//
	// =========================================================================
	conn, err := grpc.NewClient(cfg.Address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	// Create service clients from the connection
	// Each client shares the same underlying connection (multiplexing)
	client := &Client{
		conn:    conn,
		publish: pb.NewPublishServiceClient(conn),
		consume: pb.NewConsumeServiceClient(conn),
		ack:     pb.NewAckServiceClient(conn),
		offset:  pb.NewOffsetServiceClient(conn),
		health:  pb.NewHealthServiceClient(conn),
		config:  cfg,
		logger:  logger,
	}

	// Verify connectivity with a health check
	// This ensures the server is reachable before returning
	healthResp, err := client.health.Check(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("health check failed: %w", err)
	}

	if healthResp.Status != pb.ServingStatus_SERVING_STATUS_SERVING {
		conn.Close()
		return nil, fmt.Errorf("server not ready: status=%s", healthResp.Status)
	}

	logger.Info("connected to goqueue",
		"address", cfg.Address,
		"status", healthResp.Status,
	)

	return client, nil
}

// Close closes the client connection.
// Always call Close when done to release resources.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// =============================================================================
// PUBLISH OPERATIONS
// =============================================================================

// PublishResult contains the result of a publish operation.
type PublishResult struct {
	MessageID string
	Offset    int64
	Partition int32
}

// Publish publishes a single message to a topic.
//
// FLOW:
//
//	Client ──Publish──► Server ──Write──► Partition ──ACK──► Client
//
// The message is durably stored before the response is returned.
// If no error is returned, the message is guaranteed to be persisted.
//
// PARAMETERS:
//   - topic: Target topic name
//   - value: Message payload (any bytes)
//   - opts: Optional publish options (key, headers, partition)
func (c *Client) Publish(ctx context.Context, topic string, value []byte, opts ...PublishOption) (*PublishResult, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClientClosed
	}
	c.mu.RUnlock()

	// Build request with options
	// Note: PublishRequest has flat fields (Key, Value, Headers) directly
	// not a nested Message type
	req := &pb.PublishRequest{
		Topic: topic,
		Value: value,
	}

	// Apply options
	for _, opt := range opts {
		opt(req)
	}

	// Execute with retry
	var resp *pb.PublishResponse
	var err error

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		resp, err = c.publish.Publish(ctx, req)
		if err == nil {
			break
		}

		// Check if error is retryable
		if !isRetryable(err) {
			return nil, err
		}

		// Exponential backoff
		if attempt < c.config.MaxRetries {
			backoff := c.config.RetryBackoff * time.Duration(1<<attempt)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	if err != nil {
		return nil, err
	}

	return &PublishResult{
		Offset:    resp.Offset,
		Partition: resp.Partition,
	}, nil
}

// PublishOption configures a publish request.
type PublishOption func(*pb.PublishRequest)

// WithKey sets the message key for partitioning.
// Messages with the same key go to the same partition.
func WithKey(key []byte) PublishOption {
	return func(req *pb.PublishRequest) {
		req.Key = key
	}
}

// WithPartition sets a specific partition.
// Overrides key-based partitioning.
func WithPartition(partition int32) PublishOption {
	return func(req *pb.PublishRequest) {
		req.Partition = partition
	}
}

// WithHeaders adds headers to the message.
func WithHeaders(headers map[string]string) PublishOption {
	return func(req *pb.PublishRequest) {
		req.Headers = headers
	}
}

// WithPriority sets the message priority (0-4, where 0 is highest).
//
// PRIORITY LEVELS:
//   - 0: Critical - System emergencies, circuit breakers
//   - 1: High - Paid users, real-time updates
//   - 2: Normal - Default traffic (default)
//   - 3: Low - Batch jobs, reports
//   - 4: Background - Analytics, cleanup
//
// COMPARISON:
//   - RabbitMQ: priority 0-9 (higher is higher priority)
//   - goqueue: priority 0-4 (lower is higher priority, like Unix nice)
func WithPriority(priority int32) PublishOption {
	return func(req *pb.PublishRequest) {
		if priority < 0 {
			priority = 0
		}
		if priority > 4 {
			priority = 4
		}
		req.Priority = priority
	}
}

// WithDelay schedules the message to be delivered after the specified delay.
//
// HOW IT WORKS:
//  1. Message is written to log immediately (durability)
//  2. Timer wheel tracks when to make it visible
//  3. After delay expires, message appears in consumer stream
//
// COMPARISON:
//   - SQS: DelaySeconds (0-900 seconds max)
//   - RabbitMQ: Delayed Message Plugin (x-delay header)
//   - Kafka: No native support (use Kafka Streams windowing)
//   - goqueue: Native support up to ~7 days via timer wheel
func WithDelay(delay time.Duration) PublishOption {
	return func(req *pb.PublishRequest) {
		req.Delay = durationpb.New(delay)
	}
}

// WithDeliverAt schedules the message to be delivered at a specific time.
//
// USE CASES:
//   - "Send reminder at 9am tomorrow"
//   - "Process order at end of business day"
//   - "Trigger scheduled job at midnight"
//
// If the time is in the past, the message is delivered immediately.
func WithDeliverAt(deliverAt time.Time) PublishOption {
	return func(req *pb.PublishRequest) {
		req.DeliverAt = timestamppb.New(deliverAt)
	}
}

// WithAckMode sets the acknowledgment mode for the publish request.
//
// ACK MODES:
//   - ACK_MODE_LEADER (default): Wait for leader to persist
//   - ACK_MODE_NONE: Fire and forget (fastest, may lose messages)
//   - ACK_MODE_ALL: Wait for all replicas (strongest guarantee)
//
// TRADE-OFFS:
//   - NONE: Lowest latency, highest risk of loss
//   - LEADER: Good balance of speed and safety
//   - ALL: Safest, but higher latency
func WithAckMode(mode pb.AckMode) PublishOption {
	return func(req *pb.PublishRequest) {
		req.AckMode = mode
	}
}

// WithIdempotency enables exactly-once semantics for the producer.
//
// HOW IT WORKS:
//  1. Producer assigns monotonic sequence numbers
//  2. Broker tracks last sequence per producer
//  3. Duplicates are detected and rejected
//
// PARAMETERS:
//   - producerID: Unique ID for this producer (assigned by broker)
//   - epoch: Producer epoch (for zombie fencing)
//   - sequence: Monotonic sequence number
//
// COMPARISON:
//   - Kafka: enable.idempotence=true with transactional producer
//   - SQS: MessageDeduplicationId (content-based or explicit)
func WithIdempotency(producerID int64, epoch int32, sequence int64) PublishOption {
	return func(req *pb.PublishRequest) {
		req.ProducerId = producerID
		req.ProducerEpoch = epoch
		req.Sequence = sequence
	}
}

// =============================================================================
// CONSUME OPERATIONS
// =============================================================================

// ConsumeStream wraps a consume stream for easier iteration.
type ConsumeStream struct {
	stream pb.ConsumeService_ConsumeClient
	client *Client
}

// Consume starts consuming messages from a topic partition.
//
// STREAMING EXPLAINED:
//
//	This returns a stream that continuously delivers messages.
//	The stream stays open until you close it or the context is cancelled.
//
//	┌────────┐           ┌────────┐
//	│ Client │◄──stream──│ Server │
//	│        │           │        │
//	│ Recv() │◄──msg 1───│        │
//	│ Recv() │◄──msg 2───│        │
//	│ Recv() │◄──msg 3───│        │
//	│  ...   │◄── ... ───│        │
//	└────────┘           └────────┘
//
// COMPARISON:
//   - Kafka: poll() loop (client pulls batches)
//   - RabbitMQ: basic.consume (server pushes)
//   - SQS: ReceiveMessage with long polling
//   - goqueue: Server streaming (server pushes continuously)
//
// PARAMETERS:
//   - topic: Topic to consume from
//   - partitions: Partition numbers (empty for all partitions)
//   - startPosition: Where to start consuming from
func (c *Client) Consume(ctx context.Context, topic string, partitions []int32, startPosition pb.ConsumeStartPosition) (*ConsumeStream, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClientClosed
	}
	c.mu.RUnlock()

	stream, err := c.consume.Consume(ctx, &pb.ConsumeRequest{
		Topic:         topic,
		Partitions:    partitions,
		StartPosition: startPosition,
	})
	if err != nil {
		return nil, err
	}

	return &ConsumeStream{
		stream: stream,
		client: c,
	}, nil
}

// Recv receives the next message from the stream.
// Returns io.EOF when the stream ends.
func (s *ConsumeStream) Recv() (*pb.Message, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}

	// The response uses a Payload oneof field
	// Extract messages if this is a messages response
	if msgs := resp.GetMessages(); msgs != nil && len(msgs.Messages) > 0 {
		return msgs.Messages[0], nil
	}

	// Check for error response
	if errResp := resp.GetError(); errResp != nil {
		return nil, fmt.Errorf("consume error: %s", errResp.Message)
	}

	// Heartbeat or other non-message response, try again
	return s.Recv()
}

// Close closes the consume stream.
func (s *ConsumeStream) Close() error {
	// Context cancellation will close the stream
	return nil
}

// =============================================================================
// SUBSCRIBE OPERATIONS (CONSUMER GROUPS)
// =============================================================================

// SubscribeStream wraps a subscribe stream for consumer group consumption.
type SubscribeStream struct {
	stream pb.ConsumeService_SubscribeClient
	client *Client

	// Group state
	groupID    string
	memberID   string
	generation int32
}

// Subscribe subscribes to topics as part of a consumer group.
//
// CONSUMER GROUP SEMANTICS:
//
//	Multiple consumers in a group share the work:
//	- Partitions are distributed among group members
//	- Each partition is consumed by exactly one member
//	- Rebalancing happens when members join/leave
//
//	┌──────────────────────────────────────────────────────────┐
//	│                    Consumer Group "orders-group"         │
//	│                                                          │
//	│  Consumer A          Consumer B          Consumer C      │
//	│  ┌─────────┐         ┌─────────┐         ┌─────────┐    │
//	│  │ P0, P1  │         │ P2, P3  │         │ P4, P5  │    │
//	│  └─────────┘         └─────────┘         └─────────┘    │
//	│       │                   │                   │          │
//	└───────┼───────────────────┼───────────────────┼──────────┘
//	        │                   │                   │
//	        ▼                   ▼                   ▼
//	┌───────────────────────────────────────────────────────────┐
//	│  Topic "orders" [P0][P1][P2][P3][P4][P5]                  │
//	└───────────────────────────────────────────────────────────┘
//
// The stream delivers:
//  1. Messages from assigned partitions
//  2. Rebalance notifications (revoke/assign)
//  3. Heartbeat acknowledgments
func (c *Client) Subscribe(ctx context.Context, groupID string, topics []string, opts ...SubscribeOption) (*SubscribeStream, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClientClosed
	}
	c.mu.RUnlock()

	req := &pb.SubscribeRequest{
		GroupId: groupID,
		Topics:  topics,
	}

	// Apply options
	for _, opt := range opts {
		opt(req)
	}

	stream, err := c.consume.Subscribe(ctx, req)
	if err != nil {
		return nil, err
	}

	return &SubscribeStream{
		stream:  stream,
		client:  c,
		groupID: groupID,
	}, nil
}

// SubscribeOption configures a subscribe request.
type SubscribeOption func(*pb.SubscribeRequest)

// WithStartPosition sets where to start consuming for new partitions.
func WithStartPosition(position pb.ConsumeStartPosition) SubscribeOption {
	return func(req *pb.SubscribeRequest) {
		req.StartPosition = position
	}
}

// Recv receives the next response from the subscribe stream.
// The response can be messages, a rebalance event, or a heartbeat.
func (s *SubscribeStream) Recv() (*pb.ConsumeResponse, error) {
	return s.stream.Recv()
}

// Close closes the subscribe stream.
func (s *SubscribeStream) Close() error {
	return nil
}

// GetGroupID returns the consumer group ID.
func (s *SubscribeStream) GetGroupID() string {
	return s.groupID
}

// GetMemberID returns the member ID assigned by the coordinator.
func (s *SubscribeStream) GetMemberID() string {
	return s.memberID
}

// GetGeneration returns the current group generation.
func (s *SubscribeStream) GetGeneration() int32 {
	return s.generation
}

// =============================================================================
// ACK OPERATIONS
// =============================================================================

// Ack acknowledges successful processing of a message.
//
// ACK SEMANTICS:
//
//	Acking tells the broker "I'm done with this message, don't redeliver".
//	The offset is committed to track consumer progress.
//
//	┌────────┐    Consume    ┌────────┐
//	│ Broker │──────────────►│Consumer│
//	│        │◄──────────────│        │
//	│        │      Ack      │        │
//	│        │               │        │
//	│Offset++│               │        │
//	└────────┘               └────────┘
//
// COMPARISON:
//   - RabbitMQ: basic.ack
//   - SQS: DeleteMessage
//   - Kafka: Offset commit (implicit or explicit)
func (c *Client) Ack(ctx context.Context, groupID string, receiptHandle string) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClientClosed
	}
	c.mu.RUnlock()

	_, err := c.ack.Ack(ctx, &pb.AckRequest{
		GroupId:       groupID,
		ReceiptHandle: receiptHandle,
	})
	return err
}

// Nack negative-acknowledges a message (failed processing).
//
// NACK SEMANTICS:
//
//	Nacking tells the broker "I couldn't process this, please redeliver".
//	The message becomes visible again after a visibility timeout.
//
//	Options:
//	- visibilityTimeout: How long to wait before redelivery
func (c *Client) Nack(ctx context.Context, groupID string, receiptHandle string, visibilityTimeout time.Duration) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClientClosed
	}
	c.mu.RUnlock()

	req := &pb.NackRequest{
		GroupId:       groupID,
		ReceiptHandle: receiptHandle,
	}

	if visibilityTimeout > 0 {
		req.VisibilityTimeout = durationpb.New(visibilityTimeout)
	}

	_, err := c.ack.Nack(ctx, req)
	return err
}

// Reject rejects a message permanently (poison message).
//
// REJECT SEMANTICS:
//
//	Rejecting is permanent - the message is either discarded or sent to DLQ.
//	Use this for messages that can never be processed (bad format, etc).
//
//	┌────────┐    Reject    ┌─────┐
//	│ Queue  │─────────────►│ DLQ │
//	└────────┘              └─────┘
func (c *Client) Reject(ctx context.Context, groupID string, receiptHandle string, reason string) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClientClosed
	}
	c.mu.RUnlock()

	_, err := c.ack.Reject(ctx, &pb.RejectRequest{
		GroupId:       groupID,
		ReceiptHandle: receiptHandle,
		Reason:        reason,
	})
	return err
}

// ExtendVisibility extends the visibility timeout for a message.
//
// WHY EXTEND VISIBILITY:
//
//	If processing takes longer than the visibility timeout, the message
//	would become visible again and another consumer might pick it up.
//	Extending visibility prevents duplicate processing.
//
//	Timeline:
//	├─────receive────┼──────────────────timeout──────────────────┤
//	│                │                                           │
//	│  ◄─── extend visibility here to prevent redelivery ───►    │
//	│                                                            │
//	├────────────────┼────────new timeout────────────────────────┤
func (c *Client) ExtendVisibility(ctx context.Context, groupID string, receiptHandle string, extension time.Duration) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClientClosed
	}
	c.mu.RUnlock()

	_, err := c.ack.ExtendVisibility(ctx, &pb.ExtendVisibilityRequest{
		GroupId:           groupID,
		ReceiptHandle:     receiptHandle,
		VisibilityTimeout: durationpb.New(extension),
	})
	return err
}

// BatchAck acknowledges multiple messages in a single request.
//
// BATCH ACK BENEFITS:
//
//   - Reduced network round trips
//   - Lower latency for high-throughput consumers
//   - Atomic commit (all or nothing)
func (c *Client) BatchAck(ctx context.Context, groupID string, receiptHandles []string) (int32, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return 0, ErrClientClosed
	}
	c.mu.RUnlock()

	resp, err := c.ack.BatchAck(ctx, &pb.BatchAckRequest{
		GroupId:        groupID,
		ReceiptHandles: receiptHandles,
	})
	if err != nil {
		return 0, err
	}

	return resp.SuccessCount, nil
}

// =============================================================================
// OFFSET OPERATIONS
// =============================================================================

// CommitOffsets commits consumer offsets.
//
// OFFSET COMMIT SEMANTICS:
//
//	Committing offsets tells the broker "I've processed up to this point".
//	On restart, the consumer resumes from the committed offset.
//
//	┌──────────────────────────────────────────────────────────┐
//	│  Partition Log                                           │
//	│                                                          │
//	│  [0][1][2][3][4][5][6][7][8][9][10]...                  │
//	│              ▲              ▲                            │
//	│              │              │                            │
//	│        Committed       Current Position                  │
//	│       (safe point)    (processing here)                  │
//	└──────────────────────────────────────────────────────────┘
//
// COMPARISON:
//   - Kafka: commitSync/commitAsync
//   - RabbitMQ: Not needed (ack is the commit)
//   - SQS: Not needed (delete is the commit)
func (c *Client) CommitOffsets(ctx context.Context, groupID string, offsets []OffsetCommit) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClientClosed
	}
	c.mu.RUnlock()

	pbOffsets := make([]*pb.OffsetCommit, len(offsets))
	for i, o := range offsets {
		pbOffsets[i] = &pb.OffsetCommit{
			Topic:     o.Topic,
			Partition: o.Partition,
			Offset:    o.Offset,
			Metadata:  o.Metadata,
		}
	}

	_, err := c.offset.CommitOffsets(ctx, &pb.CommitOffsetsRequest{
		GroupId: groupID,
		Offsets: pbOffsets,
	})
	return err
}

// OffsetCommit represents an offset to commit.
type OffsetCommit struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string // Optional metadata
}

// FetchOffsets retrieves committed offsets for a consumer group.
func (c *Client) FetchOffsets(ctx context.Context, groupID string, partitions []TopicPartition) ([]OffsetInfo, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClientClosed
	}
	c.mu.RUnlock()

	pbPartitions := make([]*pb.TopicPartition, len(partitions))
	for i, tp := range partitions {
		pbPartitions[i] = &pb.TopicPartition{
			Topic:     tp.Topic,
			Partition: tp.Partition,
		}
	}

	resp, err := c.offset.FetchOffsets(ctx, &pb.FetchOffsetsRequest{
		GroupId:    groupID,
		Partitions: pbPartitions,
	})
	if err != nil {
		return nil, err
	}

	result := make([]OffsetInfo, len(resp.Offsets))
	for i, o := range resp.Offsets {
		result[i] = OffsetInfo{
			Topic:     o.Topic,
			Partition: o.Partition,
			Offset:    o.Offset,
			Metadata:  o.Metadata,
		}
	}

	return result, nil
}

// TopicPartition identifies a topic-partition pair.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// OffsetInfo contains offset information for a partition.
type OffsetInfo struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
}

// =============================================================================
// HEALTH OPERATIONS
// =============================================================================

// Health checks the server health.
func (c *Client) Health(ctx context.Context) (pb.ServingStatus, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return pb.ServingStatus_SERVING_STATUS_UNKNOWN, ErrClientClosed
	}
	c.mu.RUnlock()

	resp, err := c.health.Check(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		return pb.ServingStatus_SERVING_STATUS_UNKNOWN, err
	}

	return resp.Status, nil
}

// =============================================================================
// ERROR HANDLING
// =============================================================================

// ErrClientClosed is returned when operations are attempted on a closed client.
var ErrClientClosed = fmt.Errorf("client is closed")

// isRetryable determines if an error is retryable.
// Retryable errors are transient and may succeed on retry.
//
// gRPC STATUS CODE CLASSIFICATION:
//
//	┌────────────────────┬────────────┬─────────────────────────────────────┐
//	│ Status Code        │ Retryable  │ Reason                              │
//	├────────────────────┼────────────┼─────────────────────────────────────┤
//	│ OK                 │ N/A        │ Success                             │
//	│ CANCELLED          │ No         │ Client cancelled                    │
//	│ UNKNOWN            │ Maybe      │ Unknown error (retry once)          │
//	│ INVALID_ARGUMENT   │ No         │ Client error, won't change          │
//	│ DEADLINE_EXCEEDED  │ Yes        │ Timeout, may succeed on retry       │
//	│ NOT_FOUND          │ No         │ Resource doesn't exist              │
//	│ ALREADY_EXISTS     │ No         │ Resource already exists             │
//	│ PERMISSION_DENIED  │ No         │ Auth error, won't change            │
//	│ RESOURCE_EXHAUSTED │ Yes        │ Rate limited, retry with backoff    │
//	│ FAILED_PRECONDITION│ No         │ State issue, won't change           │
//	│ ABORTED            │ Yes        │ Operation aborted, retry            │
//	│ OUT_OF_RANGE       │ No         │ Invalid range, won't change         │
//	│ UNIMPLEMENTED      │ No         │ Not supported                       │
//	│ INTERNAL           │ Yes        │ Server bug, may be transient        │
//	│ UNAVAILABLE        │ Yes        │ Server down, retry with backoff     │
//	│ DATA_LOSS          │ No         │ Unrecoverable                       │
//	│ UNAUTHENTICATED    │ No         │ Auth required                       │
//	└────────────────────┴────────────┴─────────────────────────────────────┘
//
// COMPARISON:
//   - AWS SDK: Automatic retry for 5xx, throttling
//   - Kafka: Retriable exceptions vs non-retriable
//   - gRPC-go: Built-in retry policy (we add application-level)
func isRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Extract gRPC status code
	st, ok := status.FromError(err)
	if !ok {
		// Not a gRPC error - might be network error, retry
		return true
	}

	switch st.Code() {
	case codes.Unavailable:
		// Server is temporarily unavailable (e.g., restarting)
		return true

	case codes.ResourceExhausted:
		// Rate limited or resource quota exceeded
		return true

	case codes.Aborted:
		// Operation was aborted (e.g., transaction conflict)
		return true

	case codes.Internal:
		// Server error - may be transient
		return true

	case codes.DeadlineExceeded:
		// Timeout - may succeed with more time
		return true

	case codes.Unknown:
		// Unknown error - retry once to be safe
		return true

	case codes.NotFound,
		codes.InvalidArgument,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.FailedPrecondition,
		codes.OutOfRange,
		codes.Unimplemented,
		codes.DataLoss,
		codes.Unauthenticated,
		codes.Canceled:
		// These errors are permanent - don't retry
		return false

	default:
		// Unknown code - err on side of caution, don't retry
		return false
	}
}
