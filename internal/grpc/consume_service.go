// =============================================================================
// CONSUME SERVICE - REAL-TIME MESSAGE STREAMING
// =============================================================================
//
// WHAT IS THIS?
// The consume service streams messages to consumers in real-time using
// gRPC server streaming. Unlike HTTP long-polling, the server pushes
// messages as soon as they're available.
//
// WHY SERVER STREAMING?
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                      HTTP LONG-POLLING (Current)                        │
//   │                                                                         │
//   │  Consumer                          Broker                               │
//   │     │ ─── GET /poll?timeout=30s ──► │                                   │
//   │     │         [blocks up to 30s]    │                                   │
//   │     │ ◄── [messages 1-10] ───────── │                                   │
//   │     │ ─── GET /poll?timeout=30s ──► │  ← New request, overhead          │
//   │     │         [blocks up to 30s]    │                                   │
//   │     │ ◄── [messages 11-20] ──────── │                                   │
//   │                                                                         │
//   │  Problems:                                                              │
//   │  - Request/response overhead on each poll                               │
//   │  - Latency: wait for poll interval + HTTP round trip                    │
//   │  - Resource waste: connection setup/teardown                            │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                      gRPC SERVER STREAMING (New)                        │
//   │                                                                         │
//   │  Consumer                          Broker                               │
//   │     │ ═══ Open Stream ═════════════►  │                                 │
//   │     │ ◄── Message 1 ────────────────  │  ← Immediate push               │
//   │     │ ◄── Message 2 ────────────────  │  ← Sub-millisecond              │
//   │     │ ◄── Message 3 ────────────────  │  ← No polling delay             │
//   │     │ ◄── [heartbeat] ──────────────  │  ← Keep-alive if no msgs        │
//   │     │ ◄── Message 4 ────────────────  │                                 │
//   │     │     [Stream stays open]         │                                 │
//   │                                                                         │
//   │  Benefits:                                                              │
//   │  - Zero polling latency                                                 │
//   │  - Single connection for entire session                                 │
//   │  - Built-in flow control (HTTP/2)                                       │
//   │  - Server pushes as fast as consumer can handle                         │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// TWO CONSUME METHODS:
//
//   1. Consume(ConsumeRequest) → stream ConsumeResponse
//      - Direct partition consumption
//      - No consumer group (standalone consumer)
//      - User specifies topic, partitions, start offset
//      - Simple, stateless on server
//
//   2. Subscribe(SubscribeRequest) → stream ConsumeResponse
//      - Consumer group based
//      - Automatic partition assignment
//      - Rebalancing notifications
//      - Offset management integration
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   - Kafka Consumer:
//     - poll() loop (similar to our HTTP)
//     - Consumer groups with coordinator
//     - Subscribe vs assign (like our Subscribe vs Consume)
//
//   - Pulsar Consumer:
//     - receive() blocks for single message
//     - Or MessageListener callback
//     - Shared/exclusive subscriptions
//
//   - NATS JetStream:
//     - Push-based (like our streaming)
//     - Pull-based (like our HTTP)
//
// =============================================================================

package grpc

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"goqueue/internal/broker"
	"goqueue/internal/storage"
)

// =============================================================================
// CONSUME SERVICE SERVER
// =============================================================================

// consumeServiceServer implements the ConsumeService gRPC interface.
type consumeServiceServer struct {
	UnimplementedConsumeServiceServer

	broker *broker.Broker
	logger *slog.Logger
}

// NewConsumeServiceServer creates a new consume service server.
func NewConsumeServiceServer(b *broker.Broker, logger *slog.Logger) ConsumeServiceServer {
	return &consumeServiceServer{
		broker: b,
		logger: logger,
	}
}

// =============================================================================
// CONSUME (DIRECT PARTITION ACCESS)
// =============================================================================
//
// Consume streams messages from specific partitions without consumer group
// coordination. This is useful for:
//   - Reprocessing data from specific offset
//   - Testing and debugging
//   - Simple consumers that don't need group features
//   - Tools that need to read all partitions
//
// FLOW:
//   1. Client sends ConsumeRequest with topic, partitions, start position
//   2. Server validates request
//   3. Server starts goroutines to read from each partition
//   4. Messages are sent to client as they become available
//   5. Heartbeats sent if no messages (keeps connection alive)
//   6. Stream closes when client cancels or error occurs
//
// =============================================================================

func (s *consumeServiceServer) Consume(
	req *ConsumeRequest,
	stream ConsumeService_ConsumeServer,
) error {
	ctx := stream.Context()

	// =========================================================================
	// VALIDATION
	// =========================================================================
	if req.Topic == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
	}

	// Get topic info to validate partitions
	topic, err := s.broker.GetTopic(req.Topic)
	if err != nil {
		return mapBrokerError(err)
	}

	// Determine which partitions to consume
	partitions := req.Partitions
	if len(partitions) == 0 {
		// No partitions specified - consume all
		for i := 0; i < topic.NumPartitions(); i++ {
			partitions = append(partitions, int32(i))
		}
	}

	// Validate partition numbers
	for _, p := range partitions {
		if int(p) >= topic.NumPartitions() || p < 0 {
			return status.Errorf(codes.InvalidArgument,
				"invalid partition %d for topic %s (has %d partitions)",
				p, req.Topic, topic.NumPartitions())
		}
	}

	// =========================================================================
	// DETERMINE START OFFSETS
	// =========================================================================
	//
	// Each partition needs a starting offset based on the start_position:
	//   - EARLIEST: Start from offset 0
	//   - LATEST: Start from end of log (new messages only)
	//   - OFFSET: Start from specific offset in request
	//   - TIMESTAMP: Find offset by timestamp (uses time index, M14)
	//
	startOffsets := make(map[int32]int64)
	for _, p := range partitions {
		offset, err := s.resolveStartOffset(req, topic, int(p))
		if err != nil {
			return mapBrokerError(err)
		}
		startOffsets[p] = offset
	}

	// =========================================================================
	// STREAM CONFIGURATION
	// =========================================================================
	maxMessages := int(req.MaxMessages)
	if maxMessages <= 0 || maxMessages > 1000 {
		maxMessages = 100 // Sensible default
	}

	maxWait := 100 * time.Millisecond // How long to wait for messages
	if req.MaxWait != nil {
		maxWait = req.MaxWait.AsDuration()
	}
	if maxWait > 30*time.Second {
		maxWait = 30 * time.Second // Cap at 30s
	}

	heartbeatInterval := 5 * time.Second // Send heartbeat if no messages

	// =========================================================================
	// MESSAGE STREAMING LOOP
	// =========================================================================
	//
	// ARCHITECTURE:
	//
	//   ┌─────────────────────────────────────────────────────────────────────┐
	//   │  For each partition, we:                                            │
	//   │  1. Read batch of messages from current offset                      │
	//   │  2. Send to client                                                  │
	//   │  3. Advance offset                                                  │
	//   │  4. Repeat                                                          │
	//   │                                                                     │
	//   │  If no messages available:                                          │
	//   │  - Wait for maxWait duration                                        │
	//   │  - Send heartbeat if no messages for heartbeatInterval              │
	//   │                                                                     │
	//   │  Loop continues until:                                              │
	//   │  - Client cancels (ctx.Done())                                      │
	//   │  - Error occurs                                                     │
	//   │  - Server shuts down                                                │
	//   └─────────────────────────────────────────────────────────────────────┘
	//

	s.logger.Debug("consume stream started",
		"topic", req.Topic,
		"partitions", partitions,
		"start_offsets", startOffsets,
	)

	// Current offsets (will advance as we consume)
	currentOffsets := make(map[int32]int64)
	for p, offset := range startOffsets {
		currentOffsets[p] = offset
	}

	lastHeartbeat := time.Now()
	messagesSent := int64(0)

	// Window-based flow control to avoid overwhelming slow consumers. This is
	// intentionally simple and mirrors prefetch in RabbitMQ or max.poll.records
	// in Kafka: after a window of messages we pause briefly to give clients time
	// to read/ack. Transport-level flow control (HTTP/2) still applies.
	flow := newFlowController(maxMessages)

	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			s.logger.Debug("consume stream canceled",
				"topic", req.Topic,
				"messages_sent", messagesSent,
			)
			return ctx.Err()
		default:
		}

		// Try to read from each partition
		gotMessages := false
		for _, p := range partitions {
			offset := currentOffsets[p]

			// Read batch from partition
			messages, err := s.broker.Consume(req.Topic, int(p), offset, maxMessages)
			if err != nil {
				// Check if it's a "no messages" type error - that's okay
				if !isNoMessagesError(err) {
					return mapBrokerError(err)
				}
				continue
			}

			if len(messages) == 0 {
				continue
			}

			gotMessages = true

			// Convert to proto and send
			batch := s.convertMessageBatch(req.Topic, int(p), messages)

			response := &ConsumeResponse{
				Payload: &ConsumeResponse_Messages{
					Messages: batch,
				},
			}

			if err := stream.Send(response); err != nil {
				s.logger.Error("consume stream send error",
					"error", err,
					"topic", req.Topic,
					"partition", p,
				)
				return status.Errorf(codes.Internal, "send error: %v", err)
			}

			messagesSent += int64(len(messages))
			lastHeartbeat = time.Now()

			if err := flow.allow(ctx, len(messages), maxWait); err != nil {
				return err
			}

			// Update offset for next read
			lastMsg := messages[len(messages)-1]
			currentOffsets[p] = lastMsg.Offset + 1
		}

		// If no messages from any partition, wait and maybe send heartbeat
		if !gotMessages {
			// Send heartbeat if it's been a while
			if time.Since(lastHeartbeat) >= heartbeatInterval {
				heartbeat := &ConsumeResponse{
					Payload: &ConsumeResponse_Heartbeat{
						Heartbeat: &Heartbeat{
							Timestamp: timestamppb.Now(),
						},
					},
				}
				if err := stream.Send(heartbeat); err != nil {
					return status.Errorf(codes.Internal, "heartbeat send error: %v", err)
				}
				lastHeartbeat = time.Now()
			}

			// Wait before trying again
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(maxWait):
				// Try again
			}
		}
	}
}

// =============================================================================
// SUBSCRIBE (CONSUMER GROUP BASED)
// =============================================================================
//
// Subscribe streams messages with consumer group coordination. This provides:
//   - Automatic partition assignment
//   - Rebalancing when consumers join/leave
//   - Integration with offset management
//
// FLOW:
//   1. Client sends SubscribeRequest with topics, group_id
//   2. Server joins client to consumer group
//   3. Server receives partition assignment from coordinator
//   4. Server sends Assignment message to client
//   5. Server streams messages from assigned partitions
//   6. On rebalance: send RebalanceNotification, then new Assignment
//   7. Stream closes when client leaves or disconnects
//
// REBALANCING:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │  Consumer A              Coordinator              Consumer B            │
//   │      │                       │                        │                 │
//   │      │ ◄── Assignment ────── │                                          │
//   │      │   (P0, P1)            │                                          │
//   │      │                       │ ◄────── Subscribe ─────│                 │
//   │      │                       │                        │                 │
//   │      │ ◄── RebalanceStart ── │ ─── RebalanceStart ──► │                 │
//   │      │   (type=REVOKE, P1)   │   (new member)         │                 │
//   │      │                       │                        │                 │
//   │      │ ◄── Assignment ────── │ ─── Assignment ──────► │                 │
//   │      │   (P0)                │   (P1)                 │                 │
//   │      │                       │                        │                 │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

func (s *consumeServiceServer) Subscribe(
	req *SubscribeRequest,
	stream ConsumeService_SubscribeServer,
) error {
	ctx := stream.Context()

	// =========================================================================
	// VALIDATION
	// =========================================================================
	if len(req.Topics) == 0 {
		return status.Error(codes.InvalidArgument, "at least one topic is required")
	}
	if req.GroupId == "" {
		return status.Error(codes.InvalidArgument, "group_id is required")
	}

	// Validate all topics exist
	for _, topic := range req.Topics {
		if _, err := s.broker.GetTopic(topic); err != nil {
			return mapBrokerError(err)
		}
	}

	// Generate consumer ID if not provided
	consumerID := req.ConsumerId
	if consumerID == "" {
		consumerID = generateConsumerID()
	}

	// =========================================================================
	// JOIN CONSUMER GROUP
	// =========================================================================
	//
	// The group coordinator handles:
	//   - Tracking members
	//   - Partition assignment
	//   - Rebalancing
	//   - Session timeouts
	//
	// We need to:
	//   1. Join the group
	//   2. Get initial assignment
	//   3. Start heartbeating (to stay alive)
	//   4. Handle rebalance notifications
	//

	s.logger.Info("consumer subscribing",
		"group_id", req.GroupId,
		"consumer_id", consumerID,
		"topics", req.Topics,
	)

	// Get group coordinator
	coordinator := s.broker.GetGroupCoordinator()
	if coordinator == nil {
		return status.Error(codes.Internal, "group coordinator not available")
	}

	// Join the group
	sessionTimeout := 30 * time.Second
	if req.SessionTimeout != nil {
		sessionTimeout = req.SessionTimeout.AsDuration()
	}

	// Create join request
	// Note: This uses the existing HTTP-based group coordinator
	// In a full implementation, we'd have a gRPC-native coordinator
	assignments, generation, err := s.joinGroupAndGetAssignment(
		ctx, coordinator, req.GroupId, consumerID, req.Topics, sessionTimeout,
	)
	if err != nil {
		return mapBrokerError(err)
	}

	// Send initial assignment to client
	assignmentMsg := &ConsumeResponse{
		Payload: &ConsumeResponse_Assignment{
			Assignment: &Assignment{
				Generation: int32(generation),
				MemberId:   consumerID,
				Partitions: s.convertAssignment(assignments),
			},
		},
	}
	if err := stream.Send(assignmentMsg); err != nil {
		return status.Errorf(codes.Internal, "send assignment error: %v", err)
	}

	// =========================================================================
	// CONSUMER STATE
	// =========================================================================
	consumerState := &consumerStreamState{
		groupID:     req.GroupId,
		consumerID:  consumerID,
		generation:  generation,
		assignments: assignments,
		offsets:     make(map[topicPartition]int64),
		maxMessages: int(req.MaxMessages),
		maxWait:     100 * time.Millisecond,
	}

	if req.MaxMessages <= 0 || req.MaxMessages > 1000 {
		consumerState.maxMessages = 100
	}
	if req.MaxWait != nil {
		consumerState.maxWait = req.MaxWait.AsDuration()
	}

	// Initialize offsets based on start position
	for _, tp := range assignments {
		offset, err := s.getCommittedOrDefaultOffset(
			coordinator, req.GroupId, tp.Topic, tp.Partition, req.StartPosition,
		)
		if err != nil {
			s.logger.Warn("failed to get offset, using earliest",
				"topic", tp.Topic,
				"partition", tp.Partition,
				"error", err,
			)
			offset = 0
		}
		consumerState.offsets[tp] = offset
	}

	// =========================================================================
	// START HEARTBEAT GOROUTINE
	// =========================================================================
	heartbeatInterval := 10 * time.Second
	if req.HeartbeatInterval != nil {
		heartbeatInterval = req.HeartbeatInterval.AsDuration()
	}

	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()

	rebalanceCh := make(chan *rebalanceEvent, 1)
	go s.heartbeatLoop(heartbeatCtx, coordinator, consumerState, heartbeatInterval, rebalanceCh)

	// =========================================================================
	// MESSAGE STREAMING LOOP
	// =========================================================================
	return s.streamMessages(ctx, stream, consumerState, rebalanceCh)
}

// =============================================================================
// HELPER TYPES AND FUNCTIONS
// =============================================================================

// topicPartition identifies a topic-partition pair.
type topicPartition struct {
	Topic     string
	Partition int
}

// consumerStreamState tracks state for a Subscribe stream.
type consumerStreamState struct {
	mu          sync.RWMutex
	groupID     string
	consumerID  string
	generation  int
	assignments []topicPartition
	offsets     map[topicPartition]int64
	maxMessages int
	maxWait     time.Duration
}

// rebalanceEvent signals a rebalance from the heartbeat goroutine.
type rebalanceEvent struct {
	Assignments []topicPartition
	Generation  int
}

// flowController provides a lightweight application-level backpressure
// mechanism on top of HTTP/2's transport-level flow control. It pauses after
// a configurable window of messages is sent, giving slow consumers a chance
// to drain. This mirrors RabbitMQ prefetch or Kafka's max.poll.records.
type flowController struct {
	window   int // messages allowed before a pause
	inFlight int // messages sent since last pause
}

func newFlowController(maxBatch int) *flowController {
	window := maxBatch * 5 // 5 batches by default
	if window < 100 {
		window = 100
	}
	if window > 5000 {
		window = 5000
	}
	return &flowController{window: window}
}

// allow records a batch send and pauses if the window is exhausted.
func (f *flowController) allow(ctx context.Context, batchSize int, pause time.Duration) error {
	if batchSize <= 0 {
		return nil
	}

	f.inFlight += batchSize
	if f.inFlight < f.window {
		return nil
	}

	if pause <= 0 {
		pause = 50 * time.Millisecond
	}

	timer := time.NewTimer(pause)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		f.inFlight = 0
		return nil
	}
}

// resolveStartOffset determines the starting offset for a partition.
func (s *consumeServiceServer) resolveStartOffset(
	req *ConsumeRequest,
	topic *broker.Topic,
	partition int,
) (int64, error) {
	switch req.StartPosition {
	case ConsumeStartEarliest:
		return 0, nil

	case ConsumeStartLatest:
		// Get log end offset
		p, err := topic.Partition(partition)
		if err != nil {
			return 0, status.Errorf(codes.NotFound, "partition %d not found", partition)
		}
		return p.NextOffset(), nil

	case ConsumeStartOffset:
		return req.StartOffset, nil

	case ConsumeStartTimestamp:
		if req.StartTimestamp == nil {
			return 0, status.Error(codes.InvalidArgument, "start_timestamp required for TIMESTAMP position")
		}
		// TODO: Use time index to find offset (M14)
		// For now, fall back to earliest since time index isn't implemented yet
		// Time Index is implemented in storage but not yet exposed in broker?
		// use that
		s.logger.Warn("timestamp-based offset lookup not yet implemented, using earliest",
			"topic", topic.Name(),
			"partition", partition,
			"requested_timestamp", req.StartTimestamp.AsTime(),
		)
		return 0, nil

	default:
		return 0, nil
	}
}

// convertMessageBatch converts broker messages to proto format.
func (s *consumeServiceServer) convertMessageBatch(
	topic string,
	partition int,
	messages []broker.Message,
) *MessageBatch {
	batch := &MessageBatch{
		Messages: make([]*Message, len(messages)),
	}

	for i, msg := range messages {
		batch.Messages[i] = &Message{
			Topic:     topic,
			Partition: int32(partition),
			Offset:    msg.Offset,
			Timestamp: timestamppb.New(msg.Timestamp),
			Key:       msg.Key,
			Value:     msg.Value,
			Priority:  int32(msg.Priority),
		}
	}

	// Set high watermark (for now, just the last offset)
	if len(messages) > 0 {
		batch.HighWatermark = messages[len(messages)-1].Offset
	}

	return batch
}

// isNoMessagesError checks if an error means no messages available.
func isNoMessagesError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return contains(msg, "no messages") || contains(msg, "offset out of range")
}

// generateConsumerID creates a unique consumer ID.
func generateConsumerID() string {
	return "grpc-consumer-" + time.Now().Format("20060102150405.000")
}

// joinGroupAndGetAssignment joins the consumer group and gets partition assignment.
func (s *consumeServiceServer) joinGroupAndGetAssignment(
	ctx context.Context,
	coordinator *broker.GroupCoordinator,
	groupID, consumerID string,
	topics []string,
	sessionTimeout time.Duration,
) ([]topicPartition, int, error) {
	// Join the group
	// Note: sessionTimeout is not used by current coordinator implementation
	result, err := coordinator.JoinGroup(groupID, consumerID, topics)
	if err != nil {
		return nil, 0, err
	}

	// Convert partitions to topic-partition pairs
	// The JoinResult.Partitions is just partition numbers for the first topic
	// For now, assume single topic (multi-topic handled in M12)
	tps := make([]topicPartition, 0, len(result.Partitions))
	topic := topics[0] // Use first topic
	for _, p := range result.Partitions {
		tps = append(tps, topicPartition{Topic: topic, Partition: p})
	}

	return tps, result.Generation, nil
}

// convertAssignment converts internal assignment to proto format.
func (s *consumeServiceServer) convertAssignment(assignments []topicPartition) []*TopicPartition {
	result := make([]*TopicPartition, len(assignments))
	for i, tp := range assignments {
		result[i] = &TopicPartition{
			Topic:     tp.Topic,
			Partition: int32(tp.Partition),
		}
	}
	return result
}

// getCommittedOrDefaultOffset gets the committed offset or default based on start position.
func (s *consumeServiceServer) getCommittedOrDefaultOffset(
	coordinator *broker.GroupCoordinator,
	groupID, topic string,
	partition int,
	startPosition ConsumeStartPosition,
) (int64, error) {
	// Try to get committed offset
	offset, err := coordinator.GetOffset(groupID, topic, partition)
	if err == nil && offset >= 0 {
		return offset, nil
	}

	// No committed offset - use start position
	switch startPosition {
	case ConsumeStartLatest:
		t, err := s.broker.GetTopic(topic)
		if err != nil {
			return 0, err
		}
		p, err := t.Partition(partition)
		if err != nil {
			return 0, nil
		}
		return p.NextOffset(), nil
	default:
		return 0, nil
	}
}

// heartbeatLoop sends periodic heartbeats and detects rebalances.
// TODO: this needs to be enhanced to actually detect rebalances
// NOTE: Current goqueue coordinator doesn't return rebalance info from Heartbeat.
// Rebalancing is handled through JoinGroup. For now, we just keep the consumer alive.
// A future enhancement could add rebalance notifications via a separate mechanism.
func (s *consumeServiceServer) heartbeatLoop(
	ctx context.Context,
	coordinator *broker.GroupCoordinator,
	state *consumerStreamState,
	interval time.Duration,
	rebalanceCh chan<- *rebalanceEvent,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Leave group on exit
			_ = coordinator.LeaveGroup(state.groupID, state.consumerID)
			return

		case <-ticker.C:
			// Send heartbeat to keep session alive
			state.mu.RLock()
			gen := state.generation
			state.mu.RUnlock()

			err := coordinator.Heartbeat(state.groupID, state.consumerID, gen)
			if err != nil {
				s.logger.Error("heartbeat error",
					"group_id", state.groupID,
					"error", err,
				)
				// On heartbeat error, the consumer might be expired
				// The stream will eventually fail on consume
				continue
			}

			// NOTE: In a full implementation, we would check for rebalance triggers here
			// For now, rebalancing only happens when consumers join/leave
			// and the coordinator handles it internally
		}
	}
}

// streamMessages is the main message streaming loop for Subscribe.
func (s *consumeServiceServer) streamMessages(
	ctx context.Context,
	stream ConsumeService_SubscribeServer,
	state *consumerStreamState,
	rebalanceCh <-chan *rebalanceEvent,
) error {
	heartbeatInterval := 5 * time.Second
	lastHeartbeat := time.Now()
	messagesSent := int64(0)

	// Apply the same window-based flow control for group subscriptions.
	state.mu.RLock()
	initialWindowMessages := state.maxMessages
	if initialWindowMessages <= 0 {
		initialWindowMessages = 100
	}
	state.mu.RUnlock()
	flow := newFlowController(initialWindowMessages)

	for {
		// Check for cancellation or rebalance
		select {
		case <-ctx.Done():
			s.logger.Debug("subscribe stream canceled",
				"group_id", state.groupID,
				"messages_sent", messagesSent,
			)
			return ctx.Err()

		case rebalance := <-rebalanceCh:
			// Handle rebalance
			if err := s.handleRebalance(stream, state, rebalance); err != nil {
				return err
			}

		default:
			// Continue with message streaming
		}

		// Read from assigned partitions
		state.mu.RLock()
		assignments := state.assignments
		offsets := make(map[topicPartition]int64)
		for tp, offset := range state.offsets {
			offsets[tp] = offset
		}
		maxMessages := state.maxMessages
		maxWait := state.maxWait
		state.mu.RUnlock()

		gotMessages := false
		for _, tp := range assignments {
			offset := offsets[tp]

			messages, err := s.broker.Consume(tp.Topic, tp.Partition, offset, maxMessages)
			if err != nil {
				if !isNoMessagesError(err) {
					s.logger.Error("consume error",
						"topic", tp.Topic,
						"partition", tp.Partition,
						"error", err,
					)
				}
				continue
			}

			if len(messages) == 0 {
				continue
			}

			gotMessages = true

			// Send batch
			batch := s.convertMessageBatch(tp.Topic, tp.Partition, messages)
			response := &ConsumeResponse{
				Payload: &ConsumeResponse_Messages{
					Messages: batch,
				},
			}

			if err := stream.Send(response); err != nil {
				return status.Errorf(codes.Internal, "send error: %v", err)
			}

			messagesSent += int64(len(messages))
			lastHeartbeat = time.Now()

			if err := flow.allow(ctx, len(messages), maxWait); err != nil {
				return err
			}

			// Update offset
			state.mu.Lock()
			state.offsets[tp] = messages[len(messages)-1].Offset + 1
			state.mu.Unlock()
		}

		// If no messages, wait and maybe send heartbeat
		if !gotMessages {
			if time.Since(lastHeartbeat) >= heartbeatInterval {
				heartbeat := &ConsumeResponse{
					Payload: &ConsumeResponse_Heartbeat{
						Heartbeat: &Heartbeat{
							Timestamp: timestamppb.Now(),
						},
					},
				}
				if err := stream.Send(heartbeat); err != nil {
					return status.Errorf(codes.Internal, "heartbeat send error: %v", err)
				}
				lastHeartbeat = time.Now()
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case rebalance := <-rebalanceCh:
				if err := s.handleRebalance(stream, state, rebalance); err != nil {
					return err
				}
			case <-time.After(maxWait):
				// Try again
			}
		}
	}
}

// handleRebalance processes a rebalance event.
func (s *consumeServiceServer) handleRebalance(
	stream ConsumeService_SubscribeServer,
	state *consumerStreamState,
	rebalance *rebalanceEvent,
) error {
	s.logger.Info("handling rebalance",
		"group_id", state.groupID,
		"old_generation", state.generation,
		"new_generation", rebalance.Generation,
		"new_assignments", len(rebalance.Assignments),
	)

	// Determine what's being revoked
	state.mu.RLock()
	oldAssignments := make(map[topicPartition]bool)
	for _, tp := range state.assignments {
		oldAssignments[tp] = true
	}
	state.mu.RUnlock()

	newAssignments := make(map[topicPartition]bool)
	for _, tp := range rebalance.Assignments {
		newAssignments[tp] = true
	}

	// Find revoked partitions
	var revoked []*TopicPartition
	for tp := range oldAssignments {
		if !newAssignments[tp] {
			revoked = append(revoked, &TopicPartition{
				Topic:     tp.Topic,
				Partition: int32(tp.Partition),
			})
		}
	}

	// Send revoke notification if any partitions revoked
	if len(revoked) > 0 {
		revokeMsg := &ConsumeResponse{
			Payload: &ConsumeResponse_Rebalance{
				Rebalance: &RebalanceNotification{
					Type:       RebalanceTypeRevoke,
					Partitions: revoked,
					Generation: int32(rebalance.Generation),
				},
			},
		}
		if err := stream.Send(revokeMsg); err != nil {
			return status.Errorf(codes.Internal, "send revoke error: %v", err)
		}
	}

	// Update state
	state.mu.Lock()
	state.generation = rebalance.Generation
	state.assignments = rebalance.Assignments

	// Initialize offsets for new partitions
	for _, tp := range rebalance.Assignments {
		if _, exists := state.offsets[tp]; !exists {
			state.offsets[tp] = 0 // Will be updated from committed offset
		}
	}

	// Remove offsets for revoked partitions
	for tp := range state.offsets {
		if !newAssignments[tp] {
			delete(state.offsets, tp)
		}
	}
	state.mu.Unlock()

	// Send new assignment
	assignMsg := &ConsumeResponse{
		Payload: &ConsumeResponse_Assignment{
			Assignment: &Assignment{
				Generation: int32(rebalance.Generation),
				MemberId:   state.consumerID,
				Partitions: s.convertAssignment(rebalance.Assignments),
			},
		},
	}

	return stream.Send(assignMsg)
}

// Ensure storage.Priority is accessible
var _ = storage.PriorityNormal
