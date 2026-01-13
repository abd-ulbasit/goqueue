// =============================================================================
// TRANSACTION COORDINATOR - THE BRAIN OF TRANSACTIONAL OPERATIONS
// =============================================================================
//
// WHAT IS A TRANSACTION COORDINATOR?
// The transaction coordinator manages the lifecycle of transactions:
//   - Assigns producer IDs and manages epochs (zombie fencing)
//   - Tracks transaction state (Empty → Ongoing → PrepareCommit → Complete)
//   - Writes transaction markers to partitions (COMMIT/ABORT)
//   - Handles timeouts and producer heartbeats
//   - Ensures atomicity across multiple topics/partitions
//
// WHY DO WE NEED A COORDINATOR?
// Without coordination, multi-partition writes could partially succeed:
//
//   WITHOUT COORDINATOR:
//   ┌──────────┐   write to   ┌────────────┐
//   │ Producer │─────────────►│ Partition 0│  ✓ Success
//   └──────────┘              └────────────┘
//        │
//        │     write to   ┌────────────┐
//        └───────────────►│ Partition 1│  ✗ Failure
//                         └────────────┘
//
//   Result: PARTIAL WRITE (data inconsistency)
//
//   WITH COORDINATOR:
//   ┌──────────┐   begin     ┌─────────────┐
//   │ Producer │────────────►│ Coordinator │
//   └──────────┘             └──────┬──────┘
//        │                          │
//        │ write (in txn)           │ track partitions
//        ▼                          ▼
//   ┌────────────┐           ┌────────────┐
//   │ Partition 0│◄──────────│ Partition 1│
//   │ (buffered) │           │ (buffered) │
//   └────────────┘           └────────────┘
//        │                          │
//        │ commit                   │
//        └──────────┬───────────────┘
//                   ▼
//            ┌──────────────┐
//            │ Write COMMIT │  (atomic marker to ALL partitions)
//            │   markers    │
//            └──────────────┘
//
//   Result: ALL-OR-NOTHING (atomic)
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   ┌─────────────┬────────────────────────────────────────────────────────────┐
//   │ System      │ Transaction Coordination                                   │
//   ├─────────────┼────────────────────────────────────────────────────────────┤
//   │ Kafka       │ Transaction Coordinator (one per broker, partitioned)      │
//   │             │ - Owns specific transactional.id ranges                    │
//   │             │ - Uses __transaction_state topic                           │
//   │             │ - Two-phase commit: Prepare → Commit                       │
//   │             │ - PID assignment with epoch for zombie fencing             │
//   ├─────────────┼────────────────────────────────────────────────────────────┤
//   │ PostgreSQL  │ Transaction Manager                                        │
//   │             │ - MVCC with transaction IDs (xid)                          │
//   │             │ - WAL for durability                                       │
//   │             │ - Snapshot isolation                                       │
//   ├─────────────┼────────────────────────────────────────────────────────────┤
//   │ MySQL       │ InnoDB Transaction Manager                                 │
//   │             │ - Redo log + Undo log                                      │
//   │             │ - Two-phase commit for distributed                         │
//   ├─────────────┼────────────────────────────────────────────────────────────┤
//   │ goqueue     │ Single Transaction Coordinator                             │
//   │             │ - File-based state persistence                             │
//   │             │ - Heartbeat + timeout for liveness                         │
//   │             │ - Control records (COMMIT/ABORT) in data log               │
//   │             │ - LSO (Last Stable Offset) for read_committed              │
//   └─────────────┴────────────────────────────────────────────────────────────┘
//
// TRANSACTION FLOW:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                    TRANSACTIONAL PUBLISH FLOW                           │
//   │                                                                         │
//   │  1. InitProducerId(transactional.id)                                    │
//   │     └─► Coordinator assigns/returns (PID, epoch)                        │
//   │                                                                         │
//   │  2. BeginTransaction()                                                  │
//   │     └─► Coordinator: state = Ongoing, start timeout timer               │
//   │                                                                         │
//   │  3. Publish(topic, key, value)  [repeated]                              │
//   │     └─► Write to partition log (marked as transactional)                │
//   │     └─► Coordinator: track partition as pending                         │
//   │                                                                         │
//   │  4a. CommitTransaction()                                                │
//   │      └─► Coordinator: state = PrepareCommit                             │
//   │      └─► Write COMMIT marker to ALL pending partitions                  │
//   │      └─► Coordinator: state = CompleteCommit                            │
//   │      └─► Clear pending partitions                                       │
//   │                                                                         │
//   │  4b. AbortTransaction() (or timeout)                                    │
//   │      └─► Coordinator: state = PrepareAbort                              │
//   │      └─► Write ABORT marker to ALL pending partitions                   │
//   │      └─► Coordinator: state = CompleteAbort                             │
//   │      └─► Clear pending partitions                                       │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// HEARTBEAT + TIMEOUT:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                    LIVENESS DETECTION                                   │
//   │                                                                         │
//   │  Producer ────heartbeat────► Coordinator                                │
//   │     │          (3s interval)      │                                     │
//   │     │                             │ update LastHeartbeat                │
//   │     │                             │                                     │
//   │     │  ← ← ← ← ← ← ← ← ← ← ← ←    │ check every 1s:                     │
//   │     │        (response)           │ if now - LastHeartbeat > 60s:       │
//   │     │                             │   AND now - TxnStart > timeout      │
//   │     │                             │   → ABORT transaction               │
//   │     │                             │   → Fence producer (epoch++)        │
//   │                                                                         │
//   │  WHY BOTH?                                                              │
//   │  - Heartbeat: Detect dead producers quickly (within seconds)            │
//   │  - Timeout: Hard limit on transaction duration (prevent stuck txns)     │
//   │  - Together: Fast detection AND guaranteed progress                     │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// CONTROL RECORDS:
//
//   Regular messages and control records share the same log:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │  Partition Log:                                                         │
//   │                                                                         │
//   │  offset=0: [DATA]     msg1 (key=A, value=...)                           │
//   │  offset=1: [DATA]     msg2 (key=B, value=...)  ─┐                       │
//   │  offset=2: [DATA]     msg3 (key=C, value=...)   │ Transaction T1        │
//   │  offset=3: [CONTROL]  COMMIT T1 (PID=1)        ─┘                       │
//   │  offset=4: [DATA]     msg4 (key=D, value=...)  ─┐                       │
//   │  offset=5: [DATA]     msg5 (key=E, value=...)   │ Transaction T2        │
//   │  offset=6: [CONTROL]  ABORT T2 (PID=2)         ─┘ (aborted)             │
//   │  offset=7: [DATA]     msg6 (key=F, value=...)  ← non-transactional      │
//   │                                                                         │
//   │  Control Record Format:                                                 │
//   │    Flags byte: bit 2 = IsControlRecord (1)                              │
//   │                bit 3 = IsCommit (1) or IsAbort (0)                      │
//   │    Key: encoded PID + epoch                                             │
//   │    Value: encoded transaction metadata                                  │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrTransactionNotFound means the transaction doesn't exist
	ErrTransactionNotFound = errors.New("transaction not found")

	// ErrTransactionAlreadyExists means a transaction is already in progress
	ErrTransactionAlreadyExists = errors.New("transaction already in progress")

	// ErrTransactionTimeout means the transaction exceeded its timeout
	ErrTransactionTimeout = errors.New("transaction timeout")

	// ErrTransactionAborted means the transaction was aborted
	ErrTransactionAborted = errors.New("transaction aborted")

	// ErrInvalidTransactionState means the operation is invalid in current state
	ErrInvalidTransactionState = errors.New("invalid transaction state for operation")

	// ErrCoordinatorClosed means the coordinator has been shut down
	ErrCoordinatorClosed = errors.New("transaction coordinator is closed")

	// ErrNoTransactionInProgress means no transaction is active
	ErrNoTransactionInProgress = errors.New("no transaction in progress")
)

// =============================================================================
// CONFIGURATION
// =============================================================================

// TransactionCoordinatorConfig holds configuration for the coordinator.
type TransactionCoordinatorConfig struct {
	// DataDir is the base directory for data storage
	DataDir string

	// TransactionTimeoutMs is the default transaction timeout
	// If a transaction doesn't commit/abort within this time, it's aborted
	TransactionTimeoutMs int64

	// HeartbeatIntervalMs is how often producers should send heartbeats
	HeartbeatIntervalMs int64

	// SessionTimeoutMs is how long without heartbeat before producer is considered dead
	SessionTimeoutMs int64

	// CheckIntervalMs is how often to check for timeouts
	CheckIntervalMs int64

	// SnapshotIntervalMs is how often to take state snapshots
	SnapshotIntervalMs int64

	// MaxTransactionsPerProducer limits concurrent transactions per producer
	// Usually 1, but could be higher for pipelining
	MaxTransactionsPerProducer int

	// LogLevel controls logging verbosity
	LogLevel slog.Level
}

// DefaultTransactionCoordinatorConfig returns sensible defaults.
//
// DEFAULTS RATIONALE:
//   - 60s transaction timeout: Matches Kafka, allows for slow consumers
//   - 3s heartbeat: Same as consumer groups, responsive but not chatty
//   - 30s session timeout: 10 heartbeats worth of margin
//   - 1s check interval: Quick detection, low overhead
func DefaultTransactionCoordinatorConfig(dataDir string) TransactionCoordinatorConfig {
	return TransactionCoordinatorConfig{
		DataDir:                    dataDir,
		TransactionTimeoutMs:       60000, // 60 seconds
		HeartbeatIntervalMs:        3000,  // 3 seconds
		SessionTimeoutMs:           30000, // 30 seconds
		CheckIntervalMs:            1000,  // 1 second
		SnapshotIntervalMs:         60000, // 1 minute
		MaxTransactionsPerProducer: 1,
		LogLevel:                   slog.LevelInfo,
	}
}

// =============================================================================
// TRANSACTION METADATA
// =============================================================================

// TransactionMetadata holds metadata for an active transaction.
type TransactionMetadata struct {
	// TransactionId is the unique ID for this transaction
	TransactionId string

	// ProducerId is the producer's ID
	ProducerId int64

	// Epoch is the producer's current epoch
	Epoch int16

	// TransactionalId is the producer's transactional ID
	TransactionalId string

	// State is the current transaction state
	State TransactionState

	// StartTime is when the transaction started
	StartTime time.Time

	// LastUpdateTime is when the transaction was last modified
	LastUpdateTime time.Time

	// TimeoutMs is the timeout for this transaction
	TimeoutMs int64

	// Partitions are the topic-partitions in this transaction
	// Maps topic name to set of partition numbers
	Partitions map[string]map[int]struct{}
}

// NewTransactionMetadata creates a new transaction metadata.
func NewTransactionMetadata(txnId, transactionalId string, pid int64, epoch int16, timeoutMs int64) *TransactionMetadata {
	now := time.Now()
	return &TransactionMetadata{
		TransactionId:   txnId,
		ProducerId:      pid,
		Epoch:           epoch,
		TransactionalId: transactionalId,
		State:           TransactionStateOngoing,
		StartTime:       now,
		LastUpdateTime:  now,
		TimeoutMs:       timeoutMs,
		Partitions:      make(map[string]map[int]struct{}),
	}
}

// AddPartition adds a partition to the transaction.
func (t *TransactionMetadata) AddPartition(topic string, partition int) {
	if t.Partitions[topic] == nil {
		t.Partitions[topic] = make(map[int]struct{})
	}
	t.Partitions[topic][partition] = struct{}{}
	t.LastUpdateTime = time.Now()
}

// GetPartitionsList returns all partitions as a map of topic to partition slice.
func (t *TransactionMetadata) GetPartitionsList() map[string][]int {
	result := make(map[string][]int)
	for topic, partitions := range t.Partitions {
		parts := make([]int, 0, len(partitions))
		for p := range partitions {
			parts = append(parts, p)
		}
		result[topic] = parts
	}
	return result
}

// IsTimedOut returns true if the transaction has exceeded its timeout.
func (t *TransactionMetadata) IsTimedOut() bool {
	return time.Since(t.StartTime) > time.Duration(t.TimeoutMs)*time.Millisecond
}

// =============================================================================
// BROKER INTERFACE
// =============================================================================

// TransactionBroker is the interface the coordinator uses to interact with the broker.
// This allows for easier testing by mocking the broker.
type TransactionBroker interface {
	// WriteControlRecord writes a commit/abort control record to a partition
	WriteControlRecord(topic string, partition int, isCommit bool, pid int64, epoch int16, txnId string) error

	// GetTopic returns a topic by name (for validation)
	GetTopic(name string) (*Topic, error)

	// ClearUncommittedTransaction clears tracked uncommitted offsets for a transaction.
	// Returns the list of offsets that were cleared (for abort filtering).
	// Called when a transaction commits or aborts.
	ClearUncommittedTransaction(txnId string) []partitionOffset

	// MarkTransactionAborted marks offsets from an aborted transaction.
	// These offsets will remain invisible to consumers forever.
	// Called only when a transaction aborts (not on commit).
	MarkTransactionAborted(offsets []partitionOffset)
}

// =============================================================================
// TRANSACTION COORDINATOR
// =============================================================================

// TransactionCoordinator manages the lifecycle of transactions.
//
// THREAD SAFETY:
//
//	All public methods are thread-safe.
//
// RESPONSIBILITIES:
//  1. Producer ID assignment and epoch management
//  2. Transaction lifecycle (begin, commit, abort)
//  3. Timeout and heartbeat monitoring
//  4. Writing control records to partitions
//  5. Persistence via transaction log
type TransactionCoordinator struct {
	// config holds coordinator configuration
	config TransactionCoordinatorConfig

	// producerManager manages producer state (PIDs, epochs, sequences)
	producerManager *IdempotentProducerManager

	// transactionLog provides persistent storage
	transactionLog *TransactionLog

	// transactions maps transaction ID to metadata
	// Only tracks active transactions (not completed)
	transactions map[string]*TransactionMetadata
	txnMu        sync.RWMutex

	// broker is the interface for writing control records
	broker TransactionBroker

	// logger for coordinator operations
	logger *slog.Logger

	// ctx and cancel for background goroutines
	ctx    context.Context
	cancel context.CancelFunc

	// wg for waiting on background goroutines
	wg sync.WaitGroup

	// closed indicates if the coordinator is shut down
	closed  bool
	closeMu sync.RWMutex
}

// NewTransactionCoordinator creates and starts a new transaction coordinator.
func NewTransactionCoordinator(config TransactionCoordinatorConfig, broker TransactionBroker) (*TransactionCoordinator, error) {
	// Create logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: config.LogLevel,
	}))

	// Create producer manager
	producerConfig := DefaultIdempotentProducerManagerConfig()
	producerConfig.DefaultTransactionTimeoutMs = config.TransactionTimeoutMs
	producerManager := NewIdempotentProducerManager(producerConfig)

	// Create transaction log
	logConfig := DefaultTransactionLogConfig(config.DataDir)
	transactionLog, err := NewTransactionLog(logConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction log: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	tc := &TransactionCoordinator{
		config:          config,
		producerManager: producerManager,
		transactionLog:  transactionLog,
		transactions:    make(map[string]*TransactionMetadata),
		broker:          broker,
		logger:          logger,
		ctx:             ctx,
		cancel:          cancel,
	}

	// Recover state from persistent storage
	if err := tc.recover(); err != nil {
		cancel()
		transactionLog.Close()
		return nil, fmt.Errorf("failed to recover transaction state: %w", err)
	}

	// Start background goroutines
	tc.wg.Add(2)
	go tc.timeoutChecker()
	go tc.snapshotTaker()

	logger.Info("transaction coordinator started",
		"transactionTimeout", config.TransactionTimeoutMs,
		"heartbeatInterval", config.HeartbeatIntervalMs,
		"sessionTimeout", config.SessionTimeoutMs)

	return tc, nil
}

// =============================================================================
// PRODUCER LIFECYCLE
// =============================================================================

// InitProducerId initializes or retrieves the producer ID for a transactional ID.
//
// FLOW:
//  1. Look up existing transactional ID or create new
//  2. Increment epoch (fence old producer)
//  3. Abort any pending transaction from old epoch
//  4. Return (PID, epoch) for producer to use
//
// PARAMETERS:
//   - transactionalId: Client-provided stable identifier
//   - transactionTimeoutMs: Timeout for transactions (0 = use default)
//
// RETURNS:
//   - ProducerIdAndEpoch: The identity to use
//   - error: If initialization fails
func (tc *TransactionCoordinator) InitProducerId(transactionalId string, transactionTimeoutMs int64) (ProducerIdAndEpoch, error) {
	tc.closeMu.RLock()
	if tc.closed {
		tc.closeMu.RUnlock()
		return ProducerIdAndEpoch{}, ErrCoordinatorClosed
	}
	tc.closeMu.RUnlock()

	// Use default timeout if not specified
	if transactionTimeoutMs <= 0 {
		transactionTimeoutMs = tc.config.TransactionTimeoutMs
	}

	// Get old state to check for pending transactions
	oldState := tc.producerManager.GetTransactionalState(transactionalId)

	// Initialize producer ID (this increments epoch for existing producers)
	pid, err := tc.producerManager.InitProducerId(transactionalId, transactionTimeoutMs)
	if err != nil {
		return ProducerIdAndEpoch{}, err
	}

	// If there was a pending transaction from old epoch, abort it
	if oldState != nil && oldState.State == TransactionStateOngoing {
		tc.txnMu.Lock()
		if txn, exists := tc.transactions[oldState.CurrentTransactionId]; exists {
			// Mark old transaction for abort (will be processed by timeout checker)
			txn.State = TransactionStatePrepareAbort
			tc.logger.Info("aborting pending transaction from old epoch",
				"transactionalId", transactionalId,
				"oldEpoch", oldState.ProducerIdAndEpoch.Epoch,
				"newEpoch", pid.Epoch,
				"transactionId", oldState.CurrentTransactionId)
		}
		tc.txnMu.Unlock()
	}

	// Write to transaction log
	if err := tc.transactionLog.WriteInitProducer(transactionalId, pid.ProducerId, pid.Epoch, transactionTimeoutMs); err != nil {
		tc.logger.Error("failed to write init_producer to log",
			"error", err,
			"transactionalId", transactionalId)
		// Non-fatal: state is in memory, will be persisted on snapshot
	}

	tc.logger.Info("producer initialized",
		"transactionalId", transactionalId,
		"producerId", pid.ProducerId,
		"epoch", pid.Epoch)

	return pid, nil
}

// Heartbeat updates the heartbeat timestamp for a producer.
//
// Producers should call this periodically (every HeartbeatIntervalMs).
// If no heartbeat is received within SessionTimeoutMs, the producer
// is considered dead and any active transaction is aborted.
func (tc *TransactionCoordinator) Heartbeat(transactionalId string, pid ProducerIdAndEpoch) error {
	tc.closeMu.RLock()
	if tc.closed {
		tc.closeMu.RUnlock()
		return ErrCoordinatorClosed
	}
	tc.closeMu.RUnlock()

	// Validate epoch
	if err := tc.producerManager.ValidateProducerEpoch(transactionalId, pid); err != nil {
		return err
	}

	// Update heartbeat
	if err := tc.producerManager.UpdateHeartbeat(transactionalId, pid); err != nil {
		return err
	}

	// Write to log (optional, useful for debugging)
	// tc.transactionLog.WriteHeartbeat(transactionalId, pid.ProducerId, pid.Epoch)

	return nil
}

// =============================================================================
// TRANSACTION LIFECYCLE
// =============================================================================

// BeginTransaction starts a new transaction.
//
// PARAMETERS:
//   - transactionalId: The producer's transactional ID
//   - pid: The producer's identity
//
// RETURNS:
//   - transactionId: Unique ID for this transaction
//   - error: If transaction can't be started
//
// ERRORS:
//   - ErrProducerFenced: Producer has been fenced by newer epoch
//   - ErrTransactionAlreadyExists: Transaction already in progress
//   - ErrUnknownProducerId: Producer not initialized
func (tc *TransactionCoordinator) BeginTransaction(transactionalId string, pid ProducerIdAndEpoch) (string, error) {
	// METRICS: Track transaction start time for latency measurement
	txnStartTime := InstrumentTransactionStarted()

	tc.closeMu.RLock()
	if tc.closed {
		tc.closeMu.RUnlock()
		return "", ErrCoordinatorClosed
	}
	tc.closeMu.RUnlock()

	// Validate producer
	if err := tc.producerManager.ValidateProducerEpoch(transactionalId, pid); err != nil {
		return "", err
	}

	// Check for existing transaction
	state := tc.producerManager.GetTransactionalState(transactionalId)
	if state == nil {
		return "", ErrUnknownProducerId
	}
	if state.State != TransactionStateEmpty {
		return "", ErrTransactionAlreadyExists
	}

	// Generate transaction ID
	txnId := generateTransactionId()

	// Update producer state
	if err := tc.producerManager.SetTransactionState(transactionalId, TransactionStateOngoing); err != nil {
		return "", err
	}
	if err := tc.producerManager.SetCurrentTransactionId(transactionalId, txnId); err != nil {
		return "", err
	}
	if err := tc.producerManager.SetTransactionStartTime(transactionalId, time.Now()); err != nil {
		return "", err
	}

	// Create transaction metadata
	txnMeta := NewTransactionMetadata(txnId, transactionalId, pid.ProducerId, pid.Epoch, state.TransactionTimeoutMs)

	// Store transaction start time for latency tracking on commit/abort
	txnMeta.StartTime = txnStartTime

	tc.txnMu.Lock()
	tc.transactions[txnId] = txnMeta
	tc.txnMu.Unlock()

	// Write to transaction log
	if err := tc.transactionLog.WriteBeginTxn(transactionalId, txnId, pid.ProducerId, pid.Epoch); err != nil {
		tc.logger.Error("failed to write begin_txn to log",
			"error", err,
			"transactionalId", transactionalId,
			"transactionId", txnId)
	}

	tc.logger.Info("transaction started",
		"transactionalId", transactionalId,
		"transactionId", txnId,
		"producerId", pid.ProducerId,
		"epoch", pid.Epoch)

	return txnId, nil
}

// AddPartitionToTransaction records that a partition has been written to.
//
// This is called when publishing a message as part of a transaction.
// The coordinator tracks all partitions so it knows where to write
// control records on commit/abort.
//
// PARAMETERS:
//   - transactionalId: The producer's transactional ID
//   - pid: The producer's identity
//   - topic: The topic being written to
//   - partition: The partition being written to
func (tc *TransactionCoordinator) AddPartitionToTransaction(transactionalId string, pid ProducerIdAndEpoch, topic string, partition int) error {
	tc.closeMu.RLock()
	if tc.closed {
		tc.closeMu.RUnlock()
		return ErrCoordinatorClosed
	}
	tc.closeMu.RUnlock()

	// Validate producer
	if err := tc.producerManager.ValidateProducerEpoch(transactionalId, pid); err != nil {
		return err
	}

	// Get current state
	state := tc.producerManager.GetTransactionalState(transactionalId)
	if state == nil {
		return ErrUnknownProducerId
	}
	if state.State != TransactionStateOngoing {
		return ErrInvalidTransactionState
	}

	// Add partition to producer manager
	if err := tc.producerManager.AddPendingPartition(transactionalId, topic, partition); err != nil {
		return err
	}

	// Update transaction metadata
	tc.txnMu.Lock()
	if txn, exists := tc.transactions[state.CurrentTransactionId]; exists {
		txn.AddPartition(topic, partition)
	}
	tc.txnMu.Unlock()

	// Write to transaction log
	if err := tc.transactionLog.WriteAddPartition(transactionalId, topic, partition); err != nil {
		tc.logger.Error("failed to write add_partition to log",
			"error", err,
			"transactionalId", transactionalId,
			"topic", topic,
			"partition", partition)
	}

	return nil
}

// CommitTransaction commits the current transaction.
//
// FLOW:
//  1. Validate producer and transaction state
//  2. Transition to PrepareCommit state
//  3. Write COMMIT control record to all partitions
//  4. Transition to CompleteCommit state
//  5. Clean up transaction metadata
//
// ATOMICITY:
//
//	If this method returns nil, the transaction is committed.
//	If it returns an error, the transaction may need to be retried or aborted.
func (tc *TransactionCoordinator) CommitTransaction(transactionalId string, pid ProducerIdAndEpoch) error {
	tc.closeMu.RLock()
	if tc.closed {
		tc.closeMu.RUnlock()
		return ErrCoordinatorClosed
	}
	tc.closeMu.RUnlock()

	// Validate producer
	if err := tc.producerManager.ValidateProducerEpoch(transactionalId, pid); err != nil {
		return err
	}

	// Get current state
	state := tc.producerManager.GetTransactionalState(transactionalId)
	if state == nil {
		return ErrUnknownProducerId
	}
	if state.State != TransactionStateOngoing {
		return fmt.Errorf("%w: expected Ongoing, got %s", ErrInvalidTransactionState, state.State)
	}

	txnId := state.CurrentTransactionId

	// Transition to PrepareCommit
	if err := tc.producerManager.SetTransactionState(transactionalId, TransactionStatePrepareCommit); err != nil {
		return err
	}

	tc.txnMu.Lock()
	txn := tc.transactions[txnId]
	if txn != nil {
		txn.State = TransactionStatePrepareCommit
	}
	tc.txnMu.Unlock()

	// Get partitions to write markers to
	partitions := tc.producerManager.GetPendingPartitions(transactionalId)
	partitionsList := make(map[string][]int)
	for topic, parts := range partitions {
		partitionsList[topic] = make([]int, 0, len(parts))
		for p := range parts {
			partitionsList[topic] = append(partitionsList[topic], p)
		}
	}

	// Write to transaction log (prepare phase)
	if err := tc.transactionLog.WritePrepareCommit(transactionalId, partitionsList); err != nil {
		tc.logger.Error("failed to write prepare_commit to log",
			"error", err,
			"transactionalId", transactionalId)
	}

	tc.logger.Info("preparing transaction commit",
		"transactionalId", transactionalId,
		"transactionId", txnId,
		"partitions", partitionsList)

	// Write COMMIT markers to all partitions
	if err := tc.writeControlRecords(transactionalId, pid, partitions, true); err != nil {
		// Failed to write some markers - transaction is in inconsistent state
		// Mark for abort and trigger immediate abort attempt
		tc.producerManager.SetTransactionState(transactionalId, TransactionStatePrepareAbort)

		// Attempt to abort with retry
		tc.logger.Warn("commit failed, attempting abort",
			"transactionalId", transactionalId,
			"transactionId", txnId,
			"error", err)

		// Try to abort - if this fails, the timeout checker will retry
		if abortErr := tc.abortTransactionWithRetry(transactionalId, txnId, pid); abortErr != nil {
			tc.logger.Error("immediate abort also failed, will be retried by timeout checker",
				"transactionalId", transactionalId,
				"error", abortErr)
		}

		return fmt.Errorf("failed to write commit markers, transaction aborted: %w", err)
	}

	// Transition to CompleteCommit
	if err := tc.producerManager.SetTransactionState(transactionalId, TransactionStateCompleteCommit); err != nil {
		return err
	}

	// Clean up
	tc.completeTransaction(transactionalId, txnId, true)

	// Write to transaction log (complete phase)
	if err := tc.transactionLog.WriteCompleteCommit(transactionalId, true, ""); err != nil {
		tc.logger.Error("failed to write complete_commit to log",
			"error", err,
			"transactionalId", transactionalId)
	}

	// METRICS: Record successful transaction commit with latency
	tc.txnMu.RLock()
	if txn != nil && !txn.StartTime.IsZero() {
		InstrumentTransactionCommitted(txn.StartTime)
	}
	tc.txnMu.RUnlock()

	tc.logger.Info("transaction committed",
		"transactionalId", transactionalId,
		"transactionId", txnId)

	return nil
}

// AbortTransaction aborts the current transaction.
//
// FLOW:
//  1. Validate producer and transaction state
//  2. Transition to PrepareAbort state
//  3. Write ABORT control record to all partitions
//  4. Transition to CompleteAbort state
//  5. Clean up transaction metadata
//
// This can be called explicitly by the producer or automatically on timeout.
func (tc *TransactionCoordinator) AbortTransaction(transactionalId string, pid ProducerIdAndEpoch) error {
	tc.closeMu.RLock()
	if tc.closed {
		tc.closeMu.RUnlock()
		return ErrCoordinatorClosed
	}
	tc.closeMu.RUnlock()

	// Validate producer
	if err := tc.producerManager.ValidateProducerEpoch(transactionalId, pid); err != nil {
		return err
	}

	// Get current state
	state := tc.producerManager.GetTransactionalState(transactionalId)
	if state == nil {
		return ErrUnknownProducerId
	}
	if state.State != TransactionStateOngoing && state.State != TransactionStatePrepareAbort {
		return fmt.Errorf("%w: expected Ongoing or PrepareAbort, got %s", ErrInvalidTransactionState, state.State)
	}

	return tc.abortTransactionInternal(transactionalId, state.CurrentTransactionId, pid)
}

// abortTransactionInternal is the internal abort logic.
func (tc *TransactionCoordinator) abortTransactionInternal(transactionalId, txnId string, pid ProducerIdAndEpoch) error {
	// Transition to PrepareAbort
	if err := tc.producerManager.SetTransactionState(transactionalId, TransactionStatePrepareAbort); err != nil {
		return err
	}

	tc.txnMu.Lock()
	txn := tc.transactions[txnId]
	if txn != nil {
		txn.State = TransactionStatePrepareAbort
	}
	tc.txnMu.Unlock()

	// Get partitions
	partitions := tc.producerManager.GetPendingPartitions(transactionalId)
	partitionsList := make(map[string][]int)
	for topic, parts := range partitions {
		partitionsList[topic] = make([]int, 0, len(parts))
		for p := range parts {
			partitionsList[topic] = append(partitionsList[topic], p)
		}
	}

	// Write to transaction log (prepare phase)
	if err := tc.transactionLog.WritePrepareAbort(transactionalId, partitionsList); err != nil {
		tc.logger.Error("failed to write prepare_abort to log",
			"error", err,
			"transactionalId", transactionalId)
	}

	tc.logger.Info("preparing transaction abort",
		"transactionalId", transactionalId,
		"transactionId", txnId,
		"partitions", partitionsList)

	// Write ABORT markers to all partitions
	if err := tc.writeControlRecords(transactionalId, pid, partitions, false); err != nil {
		tc.logger.Error("failed to write some abort markers",
			"error", err,
			"transactionalId", transactionalId)
		// Continue anyway - consumers will treat unmarked messages as aborted
	}

	// Transition to CompleteAbort
	if err := tc.producerManager.SetTransactionState(transactionalId, TransactionStateCompleteAbort); err != nil {
		return err
	}

	// Clean up
	tc.completeTransaction(transactionalId, txnId, false)

	// Write to transaction log (complete phase)
	if err := tc.transactionLog.WriteCompleteAbort(transactionalId, true, ""); err != nil {
		tc.logger.Error("failed to write complete_abort to log",
			"error", err,
			"transactionalId", transactionalId)
	}

	// METRICS: Record transaction abort
	InstrumentTransactionAborted()

	tc.logger.Info("transaction aborted",
		"transactionalId", transactionalId,
		"transactionId", txnId)

	return nil
}

// =============================================================================
// CONTROL RECORD WRITING
// =============================================================================

// writeControlRecords writes commit/abort markers to all partitions in the transaction.
func (tc *TransactionCoordinator) writeControlRecords(transactionalId string, pid ProducerIdAndEpoch, partitions map[string]map[int]struct{}, isCommit bool) error {
	var errs []error

	for topic, parts := range partitions {
		for partition := range parts {
			if err := tc.broker.WriteControlRecord(topic, partition, isCommit, pid.ProducerId, pid.Epoch, transactionalId); err != nil {
				errs = append(errs, fmt.Errorf("partition %s-%d: %w", topic, partition, err))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to write %d control records: %v", len(errs), errs)
	}
	return nil
}

// abortTransactionWithRetry attempts to abort a transaction with retry logic.
//
// This is called when a commit fails and we need to abort. It attempts the abort
// multiple times before giving up. If all retries fail, the timeout checker
// will eventually process the PrepareAbort state.
//
// RETRY STRATEGY:
//   - 3 attempts with exponential backoff (100ms, 200ms, 400ms)
//   - Logs each failure but doesn't block indefinitely
//   - On total failure, relies on timeout checker for eventual cleanup
//
// GOROUTINE LEAK FIX:
// Uses time.NewTimer instead of time.After to avoid goroutine leaks when
// context is cancelled during backoff wait.
func (tc *TransactionCoordinator) abortTransactionWithRetry(transactionalId, txnId string, pid ProducerIdAndEpoch) error {
	const maxRetries = 3
	baseDelay := 100 * time.Millisecond

	// Create reusable timer to avoid goroutine leaks
	timer := time.NewTimer(0)
	if !timer.Stop() {
		// Drain the channel if it already fired
		select {
		case <-timer.C:
		default:
		}
	}
	defer timer.Stop()

	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			delay := baseDelay * time.Duration(1<<uint(attempt-1))
			timer.Reset(delay)
			select {
			case <-tc.ctx.Done():
				return tc.ctx.Err()
			case <-timer.C:
			}
		}

		lastErr = tc.abortTransactionInternal(transactionalId, txnId, pid)
		if lastErr == nil {
			if attempt > 0 {
				tc.logger.Info("abort succeeded after retry",
					"transactionalId", transactionalId,
					"attempt", attempt+1)
			}
			return nil
		}

		tc.logger.Warn("abort attempt failed",
			"transactionalId", transactionalId,
			"attempt", attempt+1,
			"maxRetries", maxRetries,
			"error", lastErr)
	}

	return fmt.Errorf("abort failed after %d attempts: %w", maxRetries, lastErr)
}

// completeTransaction cleans up after a transaction completes.
func (tc *TransactionCoordinator) completeTransaction(transactionalId, txnId string, committed bool) {
	// Clear pending partitions
	tc.producerManager.ClearPendingPartitions(transactionalId)

	// Reset transaction state to Empty
	tc.producerManager.SetTransactionState(transactionalId, TransactionStateEmpty)
	tc.producerManager.SetCurrentTransactionId(transactionalId, "")

	// Remove from active transactions
	tc.txnMu.Lock()
	delete(tc.transactions, txnId)
	tc.txnMu.Unlock()

	// =========================================================================
	// CLEAR UNCOMMITTED OFFSETS (LSO / read_committed support)
	// =========================================================================
	//
	// Clear the tracked uncommitted offsets for this transaction.
	//
	// COMMIT: Offsets become visible to consumers (return value ignored)
	// ABORT:  Offsets moved to abortedTracker (remain invisible forever)
	//
	// =========================================================================
	clearedOffsets := tc.broker.ClearUncommittedTransaction(txnId)

	// For aborts, mark offsets as permanently invisible
	if !committed && len(clearedOffsets) > 0 {
		tc.broker.MarkTransactionAborted(clearedOffsets)
	}
}

// =============================================================================
// SEQUENCE NUMBER VALIDATION
// =============================================================================

// CheckSequence validates a sequence number for idempotent produce.
//
// This is called when a producer sends a message to validate and track
// the sequence number for deduplication.
//
// PARAMETERS:
//   - pid: Producer identity
//   - topic: Target topic
//   - partition: Target partition
//   - sequence: Sequence number from producer
//   - offset: Offset where message will be written
//
// RETURNS:
//   - existingOffset: If duplicate, the original offset
//   - isDuplicate: True if this is a duplicate
//   - error: If sequence is invalid
func (tc *TransactionCoordinator) CheckSequence(pid ProducerIdAndEpoch, topic string, partition int, sequence int32, offset int64) (int64, bool, error) {
	tc.closeMu.RLock()
	if tc.closed {
		tc.closeMu.RUnlock()
		return 0, false, ErrCoordinatorClosed
	}
	tc.closeMu.RUnlock()

	return tc.producerManager.CheckAndUpdateSequence(pid, topic, partition, sequence, offset)
}

// =============================================================================
// BACKGROUND TASKS
// =============================================================================

// timeoutChecker periodically checks for timed-out transactions and dead producers.
func (tc *TransactionCoordinator) timeoutChecker() {
	defer tc.wg.Done()

	ticker := time.NewTicker(time.Duration(tc.config.CheckIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-tc.ctx.Done():
			return
		case <-ticker.C:
			tc.checkTimeouts()
		}
	}
}

// checkTimeouts checks for timed-out transactions and sessions.
func (tc *TransactionCoordinator) checkTimeouts() {
	now := time.Now()
	sessionTimeout := time.Duration(tc.config.SessionTimeoutMs) * time.Millisecond

	tc.txnMu.Lock()
	var toAbort []*TransactionMetadata

	for _, txn := range tc.transactions {
		// Check transaction timeout
		if txn.State == TransactionStateOngoing && txn.IsTimedOut() {
			tc.logger.Warn("transaction timed out",
				"transactionId", txn.TransactionId,
				"transactionalId", txn.TransactionalId,
				"duration", time.Since(txn.StartTime))
			txn.State = TransactionStatePrepareAbort
			toAbort = append(toAbort, txn)
		}
	}
	tc.txnMu.Unlock()

	// Abort timed-out transactions
	for _, txn := range toAbort {
		pid := ProducerIdAndEpoch{
			ProducerId: txn.ProducerId,
			Epoch:      txn.Epoch,
		}
		if err := tc.abortTransactionInternal(txn.TransactionalId, txn.TransactionId, pid); err != nil {
			tc.logger.Error("failed to abort timed-out transaction",
				"error", err,
				"transactionId", txn.TransactionId)
		}
	}

	// Check for dead producers (no heartbeat within session timeout)
	// and abort their transactions
	tc.producerManager.txnMu.Lock()
	for txnId, state := range tc.producerManager.transactionalIds {
		if now.Sub(state.LastHeartbeat) > sessionTimeout {
			if state.State == TransactionStateOngoing {
				tc.logger.Warn("producer session expired, aborting transaction",
					"transactionalId", txnId,
					"lastHeartbeat", state.LastHeartbeat)

				// Find and abort the transaction
				tc.txnMu.Lock()
				if txn, exists := tc.transactions[state.CurrentTransactionId]; exists {
					txn.State = TransactionStatePrepareAbort
				}
				tc.txnMu.Unlock()
			}
		}
	}
	tc.producerManager.txnMu.Unlock()

	// Expire old sequence states (older than 5 minutes)
	tc.producerManager.ExpireOldSequenceStates(5 * time.Minute)
}

// snapshotTaker periodically takes state snapshots.
func (tc *TransactionCoordinator) snapshotTaker() {
	defer tc.wg.Done()

	ticker := time.NewTicker(time.Duration(tc.config.SnapshotIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-tc.ctx.Done():
			return
		case <-ticker.C:
			tc.takeSnapshot()
		}
	}
}

// takeSnapshot creates a snapshot of current state.
func (tc *TransactionCoordinator) takeSnapshot() {
	snapshot := tc.producerManager.TakeSnapshot()

	if err := tc.transactionLog.WriteSnapshot(snapshot); err != nil {
		tc.logger.Error("failed to write snapshot",
			"error", err)
	} else {
		tc.logger.Debug("snapshot written",
			"producers", len(snapshot.TransactionalIds),
			"sequenceStates", len(snapshot.SequenceStates))
	}
}

// =============================================================================
// RECOVERY
// =============================================================================

// recover loads state from persistent storage.
func (tc *TransactionCoordinator) recover() error {
	// Load snapshot
	snapshot, err := tc.transactionLog.LoadSnapshot()
	if err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	if snapshot != nil {
		tc.logger.Info("loading snapshot",
			"timestamp", snapshot.Timestamp,
			"producers", len(snapshot.TransactionalIds))

		if err := tc.producerManager.RestoreFromSnapshot(*snapshot); err != nil {
			return fmt.Errorf("failed to restore snapshot: %w", err)
		}
	}

	// Replay WAL
	count, err := tc.transactionLog.ReplayWAL(tc.replayRecord)
	if err != nil {
		return fmt.Errorf("failed to replay WAL: %w", err)
	}

	tc.logger.Info("WAL replayed",
		"records", count)

	// Recover any transactions that were in-progress
	tc.recoverInProgressTransactions()

	return nil
}

// replayRecord processes a single WAL record during recovery.
func (tc *TransactionCoordinator) replayRecord(record WALRecord) error {
	switch record.Type {
	case WALRecordInitProducer:
		data, err := ParseInitProducerData(record.Data)
		if err != nil {
			return err
		}
		// Re-initialize producer (this is idempotent)
		_, _ = tc.producerManager.InitProducerId(data.TransactionalId, data.TransactionTimeoutMs)

	case WALRecordBeginTxn:
		data, err := ParseBeginTxnData(record.Data)
		if err != nil {
			return err
		}
		// Recreate transaction metadata
		state := tc.producerManager.GetTransactionalState(data.TransactionalId)
		if state != nil {
			tc.producerManager.SetTransactionState(data.TransactionalId, TransactionStateOngoing)
			tc.producerManager.SetCurrentTransactionId(data.TransactionalId, data.TransactionId)

			txn := NewTransactionMetadata(data.TransactionId, data.TransactionalId, data.ProducerId, data.Epoch, state.TransactionTimeoutMs)
			tc.txnMu.Lock()
			tc.transactions[data.TransactionId] = txn
			tc.txnMu.Unlock()
		}

	case WALRecordAddPartition:
		data, err := ParseAddPartitionData(record.Data)
		if err != nil {
			return err
		}
		tc.producerManager.AddPendingPartition(data.TransactionalId, data.Topic, data.Partition)

		state := tc.producerManager.GetTransactionalState(data.TransactionalId)
		if state != nil {
			tc.txnMu.Lock()
			if txn, exists := tc.transactions[state.CurrentTransactionId]; exists {
				txn.AddPartition(data.Topic, data.Partition)
			}
			tc.txnMu.Unlock()
		}

	case WALRecordCompleteCommit, WALRecordCompleteAbort:
		data, err := ParseCompleteCommitData(record.Data)
		if err != nil {
			return err
		}
		// Transaction completed - clean up
		state := tc.producerManager.GetTransactionalState(data.TransactionalId)
		if state != nil {
			tc.producerManager.ClearPendingPartitions(data.TransactionalId)
			tc.producerManager.SetTransactionState(data.TransactionalId, TransactionStateEmpty)
			tc.producerManager.SetCurrentTransactionId(data.TransactionalId, "")

			tc.txnMu.Lock()
			delete(tc.transactions, state.CurrentTransactionId)
			tc.txnMu.Unlock()
		}

		// Other record types can be ignored during recovery
	}

	return nil
}

// recoverInProgressTransactions handles transactions that were in-progress during crash.
func (tc *TransactionCoordinator) recoverInProgressTransactions() {
	tc.txnMu.Lock()
	defer tc.txnMu.Unlock()

	for txnId, txn := range tc.transactions {
		switch txn.State {
		case TransactionStateOngoing:
			// Check if timed out
			if txn.IsTimedOut() {
				tc.logger.Warn("aborting recovered transaction (timed out)",
					"transactionId", txnId)
				txn.State = TransactionStatePrepareAbort
			}

		case TransactionStatePrepareCommit:
			// Try to complete the commit
			tc.logger.Info("completing recovered transaction (commit)",
				"transactionId", txnId)
			// Will be handled by normal timeout checker

		case TransactionStatePrepareAbort:
			// Try to complete the abort
			tc.logger.Info("completing recovered transaction (abort)",
				"transactionId", txnId)
			// Will be handled by normal timeout checker
		}
	}
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Close shuts down the transaction coordinator.
func (tc *TransactionCoordinator) Close() error {
	tc.closeMu.Lock()
	if tc.closed {
		tc.closeMu.Unlock()
		return nil
	}
	tc.closed = true
	tc.closeMu.Unlock()

	// Stop background goroutines
	tc.cancel()
	tc.wg.Wait()

	// Take final snapshot
	tc.takeSnapshot()

	// Close transaction log
	if err := tc.transactionLog.Close(); err != nil {
		return err
	}

	tc.logger.Info("transaction coordinator stopped")
	return nil
}

// =============================================================================
// STATISTICS AND QUERIES
// =============================================================================

// TransactionCoordinatorStats holds statistics about the coordinator.
type TransactionCoordinatorStats struct {
	// ActiveTransactions is the number of ongoing transactions
	ActiveTransactions int

	// TransactionsByState counts transactions by state
	TransactionsByState map[TransactionState]int

	// ProducerStats is producer-related statistics
	ProducerStats IdempotentProducerStats

	// LogStats is transaction log statistics
	LogStats TransactionLogStats
}

// Stats returns current statistics.
func (tc *TransactionCoordinator) Stats() TransactionCoordinatorStats {
	tc.txnMu.RLock()
	byState := make(map[TransactionState]int)
	for _, txn := range tc.transactions {
		byState[txn.State]++
	}
	activeCount := len(tc.transactions)
	tc.txnMu.RUnlock()

	return TransactionCoordinatorStats{
		ActiveTransactions:  activeCount,
		TransactionsByState: byState,
		ProducerStats:       tc.producerManager.Stats(),
		LogStats:            tc.transactionLog.Stats(),
	}
}

// GetTransaction returns metadata for a specific transaction.
func (tc *TransactionCoordinator) GetTransaction(txnId string) *TransactionMetadata {
	tc.txnMu.RLock()
	defer tc.txnMu.RUnlock()

	txn, exists := tc.transactions[txnId]
	if !exists {
		return nil
	}

	// Return a copy
	return &TransactionMetadata{
		TransactionId:   txn.TransactionId,
		ProducerId:      txn.ProducerId,
		Epoch:           txn.Epoch,
		TransactionalId: txn.TransactionalId,
		State:           txn.State,
		StartTime:       txn.StartTime,
		LastUpdateTime:  txn.LastUpdateTime,
		TimeoutMs:       txn.TimeoutMs,
		Partitions:      copyPartitions(txn.Partitions),
	}
}

// GetActiveTransactions returns all active transactions.
func (tc *TransactionCoordinator) GetActiveTransactions() []*TransactionMetadata {
	tc.txnMu.RLock()
	defer tc.txnMu.RUnlock()

	result := make([]*TransactionMetadata, 0, len(tc.transactions))
	for _, txn := range tc.transactions {
		result = append(result, &TransactionMetadata{
			TransactionId:   txn.TransactionId,
			ProducerId:      txn.ProducerId,
			Epoch:           txn.Epoch,
			TransactionalId: txn.TransactionalId,
			State:           txn.State,
			StartTime:       txn.StartTime,
			LastUpdateTime:  txn.LastUpdateTime,
			TimeoutMs:       txn.TimeoutMs,
			Partitions:      copyPartitions(txn.Partitions),
		})
	}
	return result
}

// copyPartitions creates a deep copy of the partitions map.
func copyPartitions(m map[string]map[int]struct{}) map[string]map[int]struct{} {
	result := make(map[string]map[int]struct{}, len(m))
	for topic, parts := range m {
		result[topic] = make(map[int]struct{}, len(parts))
		for p := range parts {
			result[topic][p] = struct{}{}
		}
	}
	return result
}

// =============================================================================
// HELPERS
// =============================================================================

// generateTransactionId generates a unique transaction ID.
func generateTransactionId() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return fmt.Sprintf("txn-%d-%s", time.Now().UnixNano(), hex.EncodeToString(bytes))
}

// GetProducerState returns the state for a transactional ID (for debugging).
func (tc *TransactionCoordinator) GetProducerState(transactionalId string) *TransactionalIdState {
	return tc.producerManager.GetTransactionalState(transactionalId)
}

// GetProducerStateByProducerId looks up transactional state by producer ID and epoch.
// Used by PublishTransactional to find the current transaction ID for LSO tracking.
func (tc *TransactionCoordinator) GetProducerStateByProducerId(producerId int64, epoch int16) *TransactionalIdState {
	return tc.producerManager.GetTransactionalStateByProducerId(producerId, epoch)
}
