// =============================================================================
// TRANSACTION SERVICE - EXACTLY-ONCE SEMANTICS VIA gRPC
// =============================================================================
//
// WHAT IS THIS?
// The transaction service exposes the transaction coordinator over gRPC,
// enabling clients to manage transactional producers and their transactions.
//
// TRANSACTION LIFECYCLE (gRPC FLOW):
//
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │                                                                     │
//   │  Client                                   Server                    │
//   │    │                                        │                       │
//   │    │ ─── InitProducer ──────────────────► │ Assign PID + epoch     │
//   │    │ ◄── {pid=1, epoch=0} ───────────────  │                       │
//   │    │                                        │                       │
//   │    │ ─── BeginTransaction ──────────────► │ Create txn metadata    │
//   │    │ ◄── {txn_id="txn-abc"} ─────────────  │                       │
//   │    │                                        │                       │
//   │    │ ─── AddPartition("orders", 0) ─────► │ Register partition     │
//   │    │ ─── Publish(msg, pid, epoch) ───────► │ Write with tracking   │
//   │    │                                        │                       │
//   │    │ ─── CommitTransaction ─────────────► │ Write control records  │
//   │    │ ◄── {success} ──────────────────────  │ Messages visible!     │
//   │    │                                        │                       │
//   └─────────────────────────────────────────────────────────────────────┘
//
// COMPARISON WITH KAFKA'S TRANSACTION API:
//
//   Kafka (Java client):
//     producer.initTransactions()
//     producer.beginTransaction()
//     producer.send(record)
//     producer.commitTransaction()
//
//   goqueue (gRPC):
//     InitProducer(transactional_id, timeout)
//     BeginTransaction(transactional_id, pid, epoch)
//     Publish(topic, key, value, pid, epoch, seq)  // via PublishService
//     CommitTransaction(transactional_id, pid, epoch)
//
// WHY SEPARATE SERVICE (NOT IN PUBLISHSERVICE)?
//   Transaction lifecycle management (init, begin, commit, abort) is
//   conceptually separate from message publishing. Kafka also separates
//   these into different protocol requests. This separation allows:
//   - Independent scaling of transaction coordinator vs publish path
//   - Clearer API boundaries
//   - Easier testing
//
// =============================================================================

package grpc

import (
	"context"
	"log/slog"
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"

	"goqueue/internal/broker"
)

// transactionServiceServer implements the TransactionService gRPC service.
type transactionServiceServer struct {
	UnimplementedTransactionServiceServer

	broker *broker.Broker
	logger *slog.Logger
}

// NewTransactionServiceServer creates a new transaction service server.
func NewTransactionServiceServer(b *broker.Broker, logger *slog.Logger) TransactionServiceServer {
	return &transactionServiceServer{
		broker: b,
		logger: logger,
	}
}

// =============================================================================
// INIT PRODUCER
// =============================================================================
//
// Initializes a transactional producer by assigning a unique producer ID
// and epoch. If the transactional ID was previously used, the epoch is bumped
// and any previous producer with the same ID is fenced (zombie fencing).
//
// THIS IS THE ENTRY POINT for the transaction lifecycle. Every transactional
// producer must call this before starting any transactions.
//
// ZOMBIE FENCING EXPLAINED:
//
//	Time ──►
//	Producer A: InitProducer(txn-1) → epoch=0 ──► crash
//	Producer B: InitProducer(txn-1) → epoch=1 ──► active
//	Producer A: recovers, tries to Commit → FENCED (epoch 0 < 1)
//
//	Without fencing, both could commit the same transaction → duplicates!
//
// COMPARISON:
//   - Kafka: InitProducerIdRequest (KIP-98)
//   - Pulsar: Not needed (uses transaction coordinator client)
//   - goqueue: Follows Kafka model
//
// =============================================================================
func (s *transactionServiceServer) InitProducer(ctx context.Context, req *InitProducerRequest) (*InitProducerResponse, error) {
	if req.TransactionalId == "" {
		return &InitProducerResponse{
			Error: &Error{
				Code:    ErrorCodeInvalidTxnState,
				Message: "transactional_id is required",
			},
		}, nil
	}

	tc := s.broker.GetTransactionCoordinator()
	if tc == nil {
		return nil, mapBrokerError(broker.ErrTransactionsNotEnabled)
	}

	// Default timeout: 60 seconds (matches Kafka's default)
	timeoutMs := req.TransactionTimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 60000
	}

	pidAndEpoch, err := tc.InitProducerId(req.TransactionalId, timeoutMs)
	if err != nil {
		return &InitProducerResponse{
			Error: mapTransactionError(err),
		}, nil
	}

	s.logger.Info("producer initialized via gRPC",
		"transactional_id", req.TransactionalId,
		"producer_id", pidAndEpoch.ProducerId,
		"epoch", pidAndEpoch.Epoch)

	return &InitProducerResponse{
		ProducerId: pidAndEpoch.ProducerId,
		Epoch:      int32(pidAndEpoch.Epoch),
	}, nil
}

// =============================================================================
// BEGIN TRANSACTION
// =============================================================================
//
// Starts a new transaction for the given transactional producer.
// Returns a unique transaction ID that identifies this transaction instance.
//
// PRECONDITIONS:
//   - Producer must be initialized (InitProducer called)
//   - No other transaction active for this transactional ID
//   - Epoch must match (not fenced)
//
// STATE TRANSITION:
//
//	Empty → Ongoing
//
// =============================================================================
func (s *transactionServiceServer) BeginTransaction(ctx context.Context, req *BeginTransactionRequest) (*BeginTransactionResponse, error) {
	if req.TransactionalId == "" {
		return &BeginTransactionResponse{
			Error: &Error{
				Code:    ErrorCodeInvalidTxnState,
				Message: "transactional_id is required",
			},
		}, nil
	}

	tc := s.broker.GetTransactionCoordinator()
	if tc == nil {
		return nil, mapBrokerError(broker.ErrTransactionsNotEnabled)
	}

	pid := broker.ProducerIdAndEpoch{
		ProducerId: req.ProducerId,
		Epoch:      int16(req.Epoch),
	}

	txnId, err := tc.BeginTransaction(req.TransactionalId, pid)
	if err != nil {
		return &BeginTransactionResponse{
			Error: mapTransactionError(err),
		}, nil
	}

	s.logger.Debug("transaction started via gRPC",
		"transactional_id", req.TransactionalId,
		"transaction_id", txnId)

	return &BeginTransactionResponse{
		TransactionId: txnId,
	}, nil
}

// =============================================================================
// ADD PARTITION
// =============================================================================
//
// Registers a topic-partition as part of the active transaction.
// Must be called before publishing to this partition within the transaction.
//
// WHY REGISTER PARTITIONS?
//
//	The coordinator needs to know which partitions are involved to:
//	1. Write commit/abort control records to each partition on commit/abort
//	2. Track which partitions to clean up on timeout
//	3. Enable atomic visibility across multiple partitions
//
// COMPARISON:
//   - Kafka: AddPartitionsToTxnRequest (implicit in send() for Java client)
//   - goqueue: Explicit registration (more transparent, less magic)
//
// =============================================================================
func (s *transactionServiceServer) AddPartition(ctx context.Context, req *AddPartitionRequest) (*AddPartitionResponse, error) {
	if req.TransactionalId == "" || req.Topic == "" {
		return &AddPartitionResponse{
			Error: &Error{
				Code:    ErrorCodeInvalidTxnState,
				Message: "transactional_id and topic are required",
			},
		}, nil
	}

	tc := s.broker.GetTransactionCoordinator()
	if tc == nil {
		return nil, mapBrokerError(broker.ErrTransactionsNotEnabled)
	}

	pid := broker.ProducerIdAndEpoch{
		ProducerId: req.ProducerId,
		Epoch:      int16(req.Epoch),
	}

	err := tc.AddPartitionToTransaction(req.TransactionalId, pid, req.Topic, int(req.Partition))
	if err != nil {
		return &AddPartitionResponse{
			Error: mapTransactionError(err),
		}, nil
	}

	return &AddPartitionResponse{}, nil
}

// =============================================================================
// COMMIT TRANSACTION
// =============================================================================
//
// Atomically commits all messages published within the active transaction.
// After commit, messages become visible to consumers (including read_committed).
//
// 2PC FLOW (what happens internally):
//
//	Phase 1 (Prepare):
//	  1. Validate state = Ongoing → set state = PrepareCommit
//	  2. Write prepare_commit WAL record
//
//	Phase 2 (Complete):
//	  3. Write COMMIT control records to each partition
//	  4. Clear uncommitted tracker (messages now visible)
//	  5. Write complete_commit WAL record
//	  6. Set state = CompleteCommit → Empty
//
// CRASH RECOVERY:
//   - Crash before prepare → transaction times out, gets aborted
//   - Crash after prepare, before control records → re-attempt commit on recovery
//   - Crash after control records → just clean up metadata on recovery
//
// COMPARISON:
//   - Kafka: EndTxnRequest(committed=true) → 2PC internally
//   - Pulsar: txn.commit() → sends to transaction coordinator
//   - goqueue: Same 2PC model as Kafka
//
// =============================================================================
func (s *transactionServiceServer) CommitTransaction(ctx context.Context, req *CommitTransactionRequest) (*CommitTransactionResponse, error) {
	if req.TransactionalId == "" {
		return &CommitTransactionResponse{
			Error: &Error{
				Code:    ErrorCodeInvalidTxnState,
				Message: "transactional_id is required",
			},
		}, nil
	}

	tc := s.broker.GetTransactionCoordinator()
	if tc == nil {
		return nil, mapBrokerError(broker.ErrTransactionsNotEnabled)
	}

	pid := broker.ProducerIdAndEpoch{
		ProducerId: req.ProducerId,
		Epoch:      int16(req.Epoch),
	}

	err := tc.CommitTransaction(req.TransactionalId, pid)
	if err != nil {
		return &CommitTransactionResponse{
			Error: mapTransactionError(err),
		}, nil
	}

	s.logger.Info("transaction committed via gRPC",
		"transactional_id", req.TransactionalId)

	return &CommitTransactionResponse{}, nil
}

// =============================================================================
// ABORT TRANSACTION
// =============================================================================
//
// Discards all messages published within the active transaction.
// Messages are marked as aborted and become permanently invisible to
// read_committed consumers.
//
// ABORT FLOW:
//  1. Set state = PrepareAbort
//  2. Write ABORT control records to each partition
//  3. Move offsets from UncommittedTracker → AbortedTracker
//  4. Set state = CompleteAbort → Empty
//
// WHEN TO ABORT:
//   - Application-level error during processing
//   - Timeout (coordinator aborts automatically)
//   - Schema validation failure
//   - Duplicate detection
//
// =============================================================================
func (s *transactionServiceServer) AbortTransaction(ctx context.Context, req *AbortTransactionRequest) (*AbortTransactionResponse, error) {
	if req.TransactionalId == "" {
		return &AbortTransactionResponse{
			Error: &Error{
				Code:    ErrorCodeInvalidTxnState,
				Message: "transactional_id is required",
			},
		}, nil
	}

	tc := s.broker.GetTransactionCoordinator()
	if tc == nil {
		return nil, mapBrokerError(broker.ErrTransactionsNotEnabled)
	}

	pid := broker.ProducerIdAndEpoch{
		ProducerId: req.ProducerId,
		Epoch:      int16(req.Epoch),
	}

	err := tc.AbortTransaction(req.TransactionalId, pid)
	if err != nil {
		return &AbortTransactionResponse{
			Error: mapTransactionError(err),
		}, nil
	}

	s.logger.Info("transaction aborted via gRPC",
		"transactional_id", req.TransactionalId)

	return &AbortTransactionResponse{}, nil
}

// =============================================================================
// LIST TRANSACTIONS
// =============================================================================
//
// Returns all currently active (in-progress) transactions.
// Useful for monitoring and debugging transaction state.
//
// COMPARISON:
//   - Kafka: ListTransactionsRequest (KIP-664)
//   - goqueue: Similar, returns active transactions with metadata
//
// =============================================================================
func (s *transactionServiceServer) ListTransactions(ctx context.Context, req *ListTransactionsRequest) (*ListTransactionsResponse, error) {
	tc := s.broker.GetTransactionCoordinator()
	if tc == nil {
		return nil, mapBrokerError(broker.ErrTransactionsNotEnabled)
	}

	active := tc.GetActiveTransactions()
	txns := make([]*TransactionInfo, 0, len(active))
	for _, txn := range active {
		txns = append(txns, txnMetadataToProto(txn))
	}

	return &ListTransactionsResponse{
		Transactions: txns,
	}, nil
}

// =============================================================================
// DESCRIBE TRANSACTION
// =============================================================================
//
// Returns detailed information about a specific transaction.
//
// COMPARISON:
//   - Kafka: DescribeTransactionsRequest (KIP-664)
//   - goqueue: Returns full transaction metadata including partitions
//
// =============================================================================
func (s *transactionServiceServer) DescribeTransaction(ctx context.Context, req *DescribeTransactionRequest) (*DescribeTransactionResponse, error) {
	if req.TransactionId == "" {
		return &DescribeTransactionResponse{
			Error: &Error{
				Code:    ErrorCodeTransactionNotFound,
				Message: "transaction_id is required",
			},
		}, nil
	}

	tc := s.broker.GetTransactionCoordinator()
	if tc == nil {
		return nil, mapBrokerError(broker.ErrTransactionsNotEnabled)
	}

	txn := tc.GetTransaction(req.TransactionId)
	if txn == nil {
		return &DescribeTransactionResponse{
			Error: &Error{
				Code:    ErrorCodeTransactionNotFound,
				Message: "transaction not found: " + req.TransactionId,
			},
		}, nil
	}

	return &DescribeTransactionResponse{
		Transaction: txnMetadataToProto(txn),
	}, nil
}

// =============================================================================
// HELPERS
// =============================================================================

// txnMetadataToProto converts internal TransactionMetadata to proto TransactionInfo.
func txnMetadataToProto(txn *broker.TransactionMetadata) *TransactionInfo {
	// Convert partition map to proto format
	partitions := make([]*TransactionPartition, 0)
	for topic, parts := range txn.GetPartitionsList() {
		protoPartitions := make([]int32, len(parts))
		for i, p := range parts {
			protoPartitions[i] = int32(p)
		}
		partitions = append(partitions, &TransactionPartition{
			Topic:      topic,
			Partitions: protoPartitions,
		})
	}

	return &TransactionInfo{
		TransactionId:   txn.TransactionId,
		TransactionalId: txn.TransactionalId,
		ProducerId:      txn.ProducerId,
		Epoch:           int32(txn.Epoch),
		State:           txn.State.String(),
		StartTime:       timestamppb.New(txn.StartTime),
		LastUpdateTime:  timestamppb.New(txn.LastUpdateTime),
		TimeoutMs:       txn.TimeoutMs,
		Partitions:      partitions,
	}
}

// mapTransactionError converts a transaction error to a proto Error.
//
// DESIGN: We return errors in the response body (not gRPC status codes) for
// expected business errors like "producer fenced" or "invalid state". This is
// consistent with Kafka's approach where protocol errors are returned in the
// response, not as connection-level errors.
func mapTransactionError(err error) *Error {
	if err == nil {
		return nil
	}

	msg := err.Error()

	switch {
	case strings.Contains(msg, "fenced"):
		return &Error{
			Code:    ErrorCodeProducerFenced,
			Message: msg,
		}
	case strings.Contains(msg, "not found"):
		return &Error{
			Code:    ErrorCodeTransactionNotFound,
			Message: msg,
		}
	case strings.Contains(msg, "timeout"):
		return &Error{
			Code:    ErrorCodeTransactionTimeout,
			Message: msg,
		}
	case strings.Contains(msg, "invalid") || strings.Contains(msg, "state"):
		return &Error{
			Code:    ErrorCodeInvalidTxnState,
			Message: msg,
		}
	default:
		return &Error{
			Code:    ErrorCodeTransactionNotFound,
			Message: msg,
		}
	}
}
