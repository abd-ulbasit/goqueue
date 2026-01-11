// =============================================================================
// OFFSET SERVICE - CONSUMER POSITION MANAGEMENT
// =============================================================================
//
// WHAT IS THIS?
// The offset service manages consumer positions within partitions:
//   - CommitOffsets: Save consumer progress
//   - FetchOffsets: Get current committed positions
//   - ResetOffsets: Rewind or fast-forward consumer position
//
// WHY OFFSET MANAGEMENT MATTERS:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ CONSUMER PROGRESS TRACKING                                              │
//   │                                                                         │
//   │ Partition Log:  [0][1][2][3][4][5][6][7][8][9][10]...                   │
//   │                                   ▲              ▲                      │
//   │                                   │              │                      │
//   │                        Committed Offset    High Watermark               │
//   │                        (consumer's          (newest                     │
//   │                         progress)            message)                   │
//   │                                                                         │
//   │ LAG = High Watermark - Committed Offset = 10 - 4 = 6 messages behind    │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// COMMITTED OFFSET SEMANTICS:
//
//   The committed offset is the NEXT message to process, not the last
//   processed message. This is Kafka-style semantics:
//
//   - Consumed message at offset 5
//   - Commit offset 6 (next message to read)
//   - On restart: fetch from offset 6
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   - Kafka:
//     - Explicit offset commit via ConsumerGroup
//     - Auto-commit option (risky for at-least-once)
//     - Offsets stored in __consumer_offsets topic
//
//   - SQS:
//     - No offset concept (visibility timeout instead)
//     - DeleteMessage removes from queue
//     - No replay capability
//
//   - RabbitMQ:
//     - basic.ack removes from queue
//     - No offset tracking (not a log)
//     - Consumer "position" not a concept
//
//   - goqueue:
//     - Offset-based like Kafka
//     - Per-consumer-group tracking
//     - Supports reset for replay/reprocessing
//
// =============================================================================

package grpc

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"goqueue/internal/broker"
)

// =============================================================================
// OFFSET SERVICE SERVER
// =============================================================================

// offsetServiceServer implements the OffsetService gRPC interface.
type offsetServiceServer struct {
	UnimplementedOffsetServiceServer

	broker *broker.Broker
	logger *slog.Logger
}

// NewOffsetServiceServer creates a new offset service server.
func NewOffsetServiceServer(b *broker.Broker, logger *slog.Logger) OffsetServiceServer {
	return &offsetServiceServer{
		broker: b,
		logger: logger,
	}
}

// =============================================================================
// COMMIT OFFSETS - SAVE CONSUMER PROGRESS
// =============================================================================
//
// CommitOffsets persists the consumer's position in one or more partitions.
// After a successful commit, the consumer can crash and restart from
// the committed position.
//
// COMMIT STRATEGIES:
//
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │ Strategy           │ Description              │ Trade-off              │
//   ├────────────────────┼──────────────────────────┼────────────────────────┤
//   │ Per-message        │ Commit after each msg    │ Slow, safest           │
//   │ Per-batch          │ Commit after N messages  │ Balanced               │
//   │ Time-based         │ Commit every T seconds   │ Fast, some duplicates  │
//   │ Auto-commit        │ Background commits       │ Risk of loss/dups      │
//   └────────────────────┴──────────────────────────┴────────────────────────┘
//
// ATOMIC COMMITS:
//   When committing multiple topic-partitions, we try to make the operation
//   atomic (all-or-nothing). If any partition fails, we report partial
//   success and the client can retry the failed ones.
//
// =============================================================================

func (s *offsetServiceServer) CommitOffsets(
	ctx context.Context,
	req *CommitOffsetsRequest,
) (*CommitOffsetsResponse, error) {
	// Validation
	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id is required")
	}

	if len(req.Offsets) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one offset is required")
	}

	// Get group coordinator
	coordinator := s.broker.GetGroupCoordinator()
	if coordinator == nil {
		return nil, status.Error(codes.Internal, "group coordinator not available")
	}

	// Build offset map for batch commit
	// map[topic]map[partition]offset
	offsetMap := make(map[string]map[int]int64)
	for _, o := range req.Offsets {
		if offsetMap[o.Topic] == nil {
			offsetMap[o.Topic] = make(map[int]int64)
		}
		offsetMap[o.Topic][int(o.Partition)] = o.Offset
	}

	// Commit offsets
	// Note: memberID is optional, using empty string
	err := coordinator.CommitOffsets(req.GroupId, offsetMap, "")
	if err != nil {
		// Return error result for all partitions
		results := make([]*OffsetCommitResult, 0, len(req.Offsets))
		for _, o := range req.Offsets {
			results = append(results, &OffsetCommitResult{
				Topic:     o.Topic,
				Partition: o.Partition,
				Success:   false,
				Error:     brokerErrorToProto(err),
			})
		}
		return &CommitOffsetsResponse{Results: results}, nil
	}

	// Success - build results
	results := make([]*OffsetCommitResult, 0, len(req.Offsets))
	for _, o := range req.Offsets {
		results = append(results, &OffsetCommitResult{
			Topic:     o.Topic,
			Partition: o.Partition,
			Success:   true,
		})
	}

	s.logger.Debug("offsets committed",
		"group_id", req.GroupId,
		"offsets", req.Offsets,
	)

	return &CommitOffsetsResponse{Results: results}, nil
}

// =============================================================================
// FETCH OFFSETS - GET CURRENT POSITIONS
// =============================================================================
//
// FetchOffsets retrieves the committed offsets for a consumer group.
// This is typically called on consumer startup to determine where to
// resume processing.
//
// FLOW:
//   1. Consumer starts
//   2. Fetches committed offsets for assigned partitions
//   3. Starts consuming from committed offset (or default if none)
//   4. Periodically commits new offsets as processing continues
//
// =============================================================================

func (s *offsetServiceServer) FetchOffsets(
	ctx context.Context,
	req *FetchOffsetsRequest,
) (*FetchOffsetsResponse, error) {
	// Validation
	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id is required")
	}

	// Get group coordinator
	coordinator := s.broker.GetGroupCoordinator()
	if coordinator == nil {
		return nil, status.Error(codes.Internal, "group coordinator not available")
	}

	// If specific partitions requested, fetch those
	if len(req.Partitions) > 0 {
		results := make([]*OffsetFetchResult, 0, len(req.Partitions))

		for _, tp := range req.Partitions {
			offset, err := coordinator.GetOffset(req.GroupId, tp.Topic, int(tp.Partition))
			result := &OffsetFetchResult{
				Topic:     tp.Topic,
				Partition: tp.Partition,
				Offset:    offset,
			}
			if err != nil {
				result.Offset = -1
				result.Error = brokerErrorToProto(err)
			}
			results = append(results, result)
		}

		return &FetchOffsetsResponse{Offsets: results}, nil
	}

	// Fetch all offsets for the group
	groupOffsets, err := coordinator.GetGroupOffsets(req.GroupId)
	if err != nil {
		return nil, mapBrokerError(err)
	}

	// Convert to proto format
	var results []*OffsetFetchResult
	if groupOffsets != nil && groupOffsets.Topics != nil {
		for topic, topicOffsets := range groupOffsets.Topics {
			if topicOffsets.Partitions == nil {
				continue
			}
			for _, partOffset := range topicOffsets.Partitions {
				results = append(results, &OffsetFetchResult{
					Topic:     topic,
					Partition: int32(partOffset.Partition),
					Offset:    partOffset.Offset,
				})
			}
		}
	}

	return &FetchOffsetsResponse{Offsets: results}, nil
}

// =============================================================================
// RESET OFFSETS - CHANGE CONSUMER POSITION
// =============================================================================
//
// ResetOffsets allows changing the consumer's position in the log.
// This is useful for:
//   - Reprocessing: Reset to earlier offset to reprocess messages
//   - Skipping: Jump ahead past problematic messages
//   - Fresh start: Reset to earliest/latest
//
// USE CASES:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Scenario                    │ Reset Strategy                            │
//   ├─────────────────────────────┼───────────────────────────────────────────┤
//   │ Bug in consumer logic       │ Reset to EARLIEST to reprocess all        │
//   │ New consumer deployment     │ Reset to LATEST to skip backlog           │
//   │ Reprocess last hour         │ Reset to TIMESTAMP (now - 1 hour)         │
//   │ Skip poison messages        │ Reset to specific OFFSET past bad msgs    │
//   │ Test with fresh data        │ Reset to LATEST, publish test messages    │
//   └─────────────────────────────┴───────────────────────────────────────────┘
//
// CAUTION:
//   Resetting offsets can cause:
//   - Duplicate processing (if reset backward)
//   - Message loss (if reset forward past unprocessed)
//   Use with care in production!
//
// =============================================================================

func (s *offsetServiceServer) ResetOffsets(
	ctx context.Context,
	req *ResetOffsetsRequest,
) (*ResetOffsetsResponse, error) {
	// Validation
	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id is required")
	}

	if len(req.Partitions) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one partition is required")
	}

	// =========================================================================
	// OFFSET RESET STRATEGIES
	// =========================================================================
	//
	// Different strategies serve different use cases:
	//
	//   EARLIEST: Reprocess everything from the beginning
	//   - Use case: Bug fix requires reprocessing all historical data
	//   - Risk: May take a long time for large topics
	//
	//   LATEST: Skip to end, only process new messages
	//   - Use case: Don't care about history, just want new data
	//   - Risk: Lose all unprocessed messages
	//
	//   TIMESTAMP: Jump to a specific point in time
	//   - Use case: "Process everything from last Monday"
	//   - Requires: Time-indexed messages (M14)
	//
	//   OFFSET: Jump to specific offset per partition
	//   - Use case: Precise replay from known position
	//   - Requires: Knowing the exact offsets
	//
	// COMPARISON:
	//   - Kafka: kafka-consumer-groups.sh --reset-offsets
	//   - SQS: Not supported (messages are deleted after processing)
	//   - RabbitMQ: Not supported (AMQP model doesn't have offsets)
	//
	// =========================================================================

	// Get group coordinator
	coordinator := s.broker.GetGroupCoordinator()
	if coordinator == nil {
		return nil, status.Error(codes.Internal, "group coordinator not available")
	}

	// Build a map of target offsets for OFFSET strategy
	// The proto provides req.Offsets as an array of OffsetCommit for explicit targets
	targetOffsets := make(map[string]map[int32]int64)
	if req.Strategy == OffsetResetOffset {
		for _, oc := range req.Offsets {
			if targetOffsets[oc.Topic] == nil {
				targetOffsets[oc.Topic] = make(map[int32]int64)
			}
			targetOffsets[oc.Topic][oc.Partition] = oc.Offset
		}
	}

	// Calculate new offsets based on strategy
	results := make([]*OffsetResetResult, 0, len(req.Partitions))

	for _, tp := range req.Partitions {
		result := &OffsetResetResult{
			Topic:     tp.Topic,
			Partition: tp.Partition,
		}

		// Get current offset for comparison
		oldOffset, _ := coordinator.GetOffset(req.GroupId, tp.Topic, int(tp.Partition))
		result.OldOffset = oldOffset

		var newOffset int64

		switch req.Strategy {
		case OffsetResetEarliest:
			// Reset to beginning
			newOffset = 0

		case OffsetResetLatest:
			// Reset to end (only new messages)
			topic, terr := s.broker.GetTopic(tp.Topic)
			if terr != nil {
				result.Error = &Error{
					Code:    ErrorCodeTopicNotFound,
					Message: terr.Error(),
				}
				results = append(results, result)
				continue
			}
			p, perr := topic.Partition(int(tp.Partition))
			if perr != nil {
				result.Error = &Error{
					Code:    ErrorCodePartitionNotFound,
					Message: perr.Error(),
				}
				results = append(results, result)
				continue
			}
			newOffset = p.NextOffset()

		case OffsetResetTimestamp:
			// Reset to specific time using broker's GetOffsetByTimestamp
			//
			// DESIGN: Broker handles all the orchestration internally:
			//   ResetOffsets ──► broker.GetOffsetByTimestamp()
			//                      └─► GetTopic()
			//                      └─► Partition()
			//                      └─► partition.GetOffsetByTimestamp()
			//
			// This keeps the gRPC service thin and orchestration logic in the broker.

			// Validate timestamp is provided
			if req.Timestamp == nil || !req.Timestamp.IsValid() {
				// No timestamp provided - fall back to earliest
				s.logger.Warn("timestamp reset requested but no timestamp provided, using earliest",
					"topic", tp.Topic,
					"partition", tp.Partition,
				)
				newOffset = 0
			} else {
				// Convert protobuf Timestamp to Unix nanoseconds (our internal format)
				targetTimestamp := req.Timestamp.AsTime().UnixNano()

				// Single broker call handles everything
				offset, err := s.broker.GetOffsetByTimestamp(tp.Topic, int(tp.Partition), targetTimestamp)
				if err != nil {
					s.logger.Warn("timestamp lookup failed, using earliest",
						"topic", tp.Topic,
						"partition", tp.Partition,
						"error", err,
					)
					newOffset = 0
				} else {
					newOffset = offset
					s.logger.Debug("timestamp lookup successful",
						"topic", tp.Topic,
						"partition", tp.Partition,
						"timestamp", targetTimestamp,
						"offset", newOffset,
					)
				}
			}

		case OffsetResetOffset:
			// Reset to specific offset from the Offsets array
			if offsets, ok := targetOffsets[tp.Topic]; ok {
				if offset, found := offsets[tp.Partition]; found {
					newOffset = offset
				} else {
					result.Error = &Error{
						Code:    ErrorCodePartitionNotFound,
						Message: "no target offset specified for partition",
					}
					results = append(results, result)
					continue
				}
			} else {
				result.Error = &Error{
					Code:    ErrorCodeTopicNotFound,
					Message: "no target offset specified for topic",
				}
				results = append(results, result)
				continue
			}

		default:
			return nil, status.Error(codes.InvalidArgument, "invalid reset strategy")
		}

		// Commit the new offset
		err := coordinator.CommitOffset(req.GroupId, tp.Topic, int(tp.Partition), newOffset, "")
		if err != nil {
			result.Error = &Error{
				Code:    ErrorCodeUnspecified,
				Message: err.Error(),
			}
			results = append(results, result)
			continue
		}

		result.NewOffset = newOffset
		results = append(results, result)
	}

	s.logger.Info("offsets reset",
		"group_id", req.GroupId,
		"strategy", req.Strategy,
		"partition_count", len(results),
	)

	return &ResetOffsetsResponse{
		Results: results,
	}, nil
}
