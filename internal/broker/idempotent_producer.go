// =============================================================================
// IDEMPOTENT PRODUCER - EXACTLY-ONCE SEMANTICS FOR PRODUCERS
// =============================================================================
//
// WHAT IS IDEMPOTENCY?
// Idempotency means that performing an operation multiple times has the same
// effect as performing it once. For message queues, this means:
//   - Retrying a failed publish doesn't create duplicates
//   - Network timeouts don't cause duplicate messages
//   - Producer crashes and restarts don't cause duplicates
//
// WHY DO WE NEED THIS?
//
//   WITHOUT IDEMPOTENCY (at-least-once):
//   ┌──────────┐   send msg   ┌────────┐   write   ┌─────────┐
//   │ Producer │─────────────►│ Broker │──────────►│   Log   │
//   └──────────┘              └────────┘           └─────────┘
//        │                         │
//        │     ACK lost!           │  (msg written successfully)
//        │◄────────────X───────────│
//        │
//        │     retry (timeout)
//        │─────────────────────────►  DUPLICATE!
//
//   WITH IDEMPOTENCY (exactly-once):
//   ┌──────────┐   send msg   ┌────────┐   check   ┌─────────────────┐
//   │ Producer │─────────────►│ Broker │──────────►│ Sequence Cache  │
//   │ PID=1    │              └────────┘           │ PID=1, seq=42   │
//   │ seq=42   │                   │               └─────────────────┘
//   └──────────┘                   │                      │
//        │                         │  seq=42 already seen │
//        │                         │◄─────────────────────│
//        │     ACK (idempotent)    │
//        │◄────────────────────────│  No duplicate written!
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   ┌─────────────┬───────────────────────────────────────────────────────────┐
//   │ System      │ Idempotency Approach                                      │
//   ├─────────────┼───────────────────────────────────────────────────────────┤
//   │ Kafka       │ ProducerID + Epoch + per-partition sequence numbers       │
//   │             │ Broker tracks last 5 sequences per (PID, partition)       │
//   │             │ enable.idempotence=true config                            │
//   ├─────────────┼───────────────────────────────────────────────────────────┤
//   │ RabbitMQ    │ Publisher Confirms + client-side dedup                    │
//   │             │ No native idempotency, client manages message IDs         │
//   ├─────────────┼───────────────────────────────────────────────────────────┤
//   │ SQS         │ MessageDeduplicationId (FIFO queues only)                 │
//   │             │ 5-minute deduplication window                             │
//   │             │ Content-based deduplication optional                      │
//   ├─────────────┼───────────────────────────────────────────────────────────┤
//   │ Pulsar      │ Producer name + sequence ID                               │
//   │             │ Broker tracks per-producer sequence                       │
//   ├─────────────┼───────────────────────────────────────────────────────────┤
//   │ goqueue     │ PID + Epoch + per-partition sequences (Kafka model)       │
//   │             │ File-based state persistence                              │
//   │             │ Configurable dedup window                                 │
//   └─────────────┴───────────────────────────────────────────────────────────┘
//
// KEY CONCEPTS:
//
// 1. PRODUCER ID (PID)
//    A unique 64-bit identifier assigned to each producer instance.
//    - Assigned by broker on InitProducerId call
//    - Stable across producer lifetime (same PID for all sends)
//    - Different producers have different PIDs
//
// 2. EPOCH
//    A 16-bit counter that increments each time a producer re-initializes.
//    - Used for "zombie fencing" - old producer instances are rejected
//    - Prevents split-brain scenarios where old producer thinks it's still active
//    - Similar to "generation ID" in consumer groups but for producers
//
//    ZOMBIE FENCING SCENARIO:
//    ┌──────────────────────────────────────────────────────────────────────┐
//    │ Time →                                                               │
//    │                                                                      │
//    │ Producer A (epoch=0)                                                 │
//    │     │                                                                │
//    │     │  network partition / GC pause                                  │
//    │     │  ────────────────────────────────                              │
//    │     │                              │                                 │
//    │     │                              ▼                                 │
//    │     │                    Producer A' (epoch=1)                       │
//    │     │                         │                                      │
//    │     │                         │ InitProducerId                       │
//    │     │                         │ (broker increments epoch)            │
//    │     │                         ▼                                      │
//    │     │ recovers!          Starts sending                              │
//    │     ▼                                                                │
//    │  Tries to send ──► REJECTED! (epoch=0 < current epoch=1)             │
//    │  (stale epoch)                                                       │
//    │                                                                      │
//    └──────────────────────────────────────────────────────────────────────┘
//
// 3. SEQUENCE NUMBER
//    A per-partition monotonically increasing counter.
//    - Producer increments before each send to a partition
//    - Broker validates: new_seq = last_seq + 1
//    - Out-of-order or duplicate sequences are rejected/ignored
//
//    SEQUENCE VALIDATION:
//    ┌─────────────────────────────────────────────────────────────────────┐
//    │                                                                     │
//    │   Expected: seq = last_seq + 1                                      │
//    │                                                                     │
//    │   Case 1: seq = last_seq + 1  → ACCEPT (normal)                     │
//    │   Case 2: seq ≤ last_seq      → DUPLICATE (already processed)       │
//    │   Case 3: seq > last_seq + 1  → OUT_OF_ORDER (gap detected)         │
//    │                                                                     │
//    │   Example: Broker has last_seq=42 for (PID=1, partition=0)          │
//    │                                                                     │
//    │   Incoming seq=43 → Accept, update last_seq=43                      │
//    │   Incoming seq=42 → Duplicate! Return success but don't write       │
//    │   Incoming seq=45 → OutOfOrder! Producer must have missed seq=44    │
//    │                                                                     │
//    └─────────────────────────────────────────────────────────────────────┘
//
// 4. TRANSACTIONAL ID
//    A user-provided string that identifies a "logical producer".
//    - Maps to a specific (PID, epoch) pair
//    - Allows producer restarts to resume with same identity
//    - Required for transactional semantics
//
//    TRANSACTIONAL ID FLOW:
//    ┌─────────────────────────────────────────────────────────────────────┐
//    │                                                                     │
//    │   InitProducerId("order-service-1")                                 │
//    │        │                                                            │
//    │        ▼                                                            │
//    │   ┌─────────────────────────────────────────────────────────────┐   │
//    │   │ TransactionalID → PID mapping                               │   │
//    │   │                                                             │   │
//    │   │ "order-service-1" → (PID=1001, epoch=0)  [first init]       │   │
//    │   │ "order-service-1" → (PID=1001, epoch=1)  [after restart]    │   │
//    │   │ "payment-service" → (PID=1002, epoch=0)                     │   │
//    │   │                                                             │   │
//    │   └─────────────────────────────────────────────────────────────┘   │
//    │                                                                     │
//    │   Same transactionalID always gets same PID, but epoch increments   │
//    │                                                                     │
//    └─────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package broker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrProducerFenced means this producer has been fenced by a newer epoch
	ErrProducerFenced = errors.New("producer fenced by newer instance")

	// ErrInvalidProducerEpoch means the epoch doesn't match current
	ErrInvalidProducerEpoch = errors.New("invalid producer epoch")

	// ErrDuplicateSequence means this sequence was already processed
	ErrDuplicateSequence = errors.New("duplicate sequence number")

	// ErrOutOfOrderSequence means sequence numbers arrived out of order
	ErrOutOfOrderSequence = errors.New("out of order sequence number")

	// ErrUnknownProducerId means the PID is not registered
	ErrUnknownProducerId = errors.New("unknown producer ID")

	// ErrTransactionalIdRequired means transactional operations need a transactional ID
	ErrTransactionalIdRequired = errors.New("transactional ID required")

	// ErrProducerIdExhausted means no more producer IDs available (unlikely)
	ErrProducerIdExhausted = errors.New("producer IDs exhausted")
)

// =============================================================================
// CONSTANTS
// =============================================================================

const (
	// DefaultDedupWindowSize is how many sequences to track per partition
	// Kafka uses 5, but we use a larger window for better duplicate detection
	DefaultDedupWindowSize = 50

	// MaxProducerId is the maximum producer ID (2^63 - 1)
	// Using int64 for simplicity (Kafka uses unsigned)
	MaxProducerId int64 = 1<<63 - 1

	// MaxEpoch is the maximum epoch value (2^16 - 1)
	MaxEpoch int16 = 1<<15 - 1

	// NoProducerId represents an uninitialized producer
	NoProducerId int64 = -1

	// NoEpoch represents an uninitialized epoch
	NoEpoch int16 = -1
)

// =============================================================================
// PRODUCER STATE
// =============================================================================

// ProducerIdAndEpoch holds the identity of an idempotent/transactional producer.
//
// WHY SEPARATE ID AND EPOCH?
// - PID identifies the "logical" producer (e.g., order-service)
// - Epoch identifies the "incarnation" (e.g., 3rd restart today)
// - Together they uniquely identify a producer instance
// - Old epochs are rejected (zombie fencing)
type ProducerIdAndEpoch struct {
	// ProducerId is a unique 64-bit identifier assigned by the broker.
	// It remains stable across a producer's lifetime but changes if
	// the transactional ID is different.
	ProducerId int64

	// Epoch is incremented each time a producer with the same
	// transactional ID initializes. Used for zombie fencing.
	// Range: 0 to 32767 (int16)
	Epoch int16
}

// IsValid returns true if this represents a valid producer identity.
func (p ProducerIdAndEpoch) IsValid() bool {
	return p.ProducerId >= 0 && p.Epoch >= 0
}

// String returns a human-readable representation.
func (p ProducerIdAndEpoch) String() string {
	return fmt.Sprintf("PID=%d,epoch=%d", p.ProducerId, p.Epoch)
}

// =============================================================================
// SEQUENCE STATE
// =============================================================================

// PartitionSequenceKey uniquely identifies a sequence tracking context.
// Sequences are tracked per (ProducerId, Topic, Partition) combination.
type PartitionSequenceKey struct {
	ProducerId int64
	Topic      string
	Partition  int
}

// SequenceState tracks sequence numbers for a producer-partition pair.
//
// WHY TRACK MULTIPLE SEQUENCES?
// Network issues can cause retries. If we only track the last sequence,
// we can't detect duplicates from older batches. By tracking a window
// of recent sequences, we can properly deduplicate.
//
// EXAMPLE:
//
//	Producer sends: seq=40, seq=41, seq=42
//	Broker receives: seq=40 (ACK lost), seq=41, seq=42, seq=40 (retry)
//
//	With window tracking, seq=40 retry is detected as duplicate.
type SequenceState struct {
	// LastSequence is the highest sequence number seen
	LastSequence int32

	// FirstSequence is the lowest sequence in the current window
	// Used to detect very old duplicates
	FirstSequence int32

	// SequenceWindow tracks recent sequences for deduplication
	// Maps sequence number to offset where it was written
	// Size limited by DedupWindowSize
	SequenceWindow map[int32]int64

	// LastUpdateTime is when this state was last modified
	LastUpdateTime time.Time
}

// NewSequenceState creates a new sequence state starting at -1 (no sequences yet).
func NewSequenceState() *SequenceState {
	return &SequenceState{
		LastSequence:   -1,
		FirstSequence:  -1,
		SequenceWindow: make(map[int32]int64),
		LastUpdateTime: time.Now(),
	}
}

// =============================================================================
// TRANSACTIONAL ID STATE
// =============================================================================

// TransactionalIdState holds the mapping from transactional ID to producer state.
//
// LIFECYCLE:
//  1. Producer calls InitProducerId("my-txn-id")
//  2. Broker looks up or creates PID for "my-txn-id"
//  3. Broker increments epoch (or starts at 0 for new)
//  4. Broker returns (PID, epoch) to producer
//  5. Producer uses this identity for all subsequent operations
type TransactionalIdState struct {
	// TransactionalId is the client-provided identifier
	TransactionalId string

	// ProducerIdAndEpoch is the current identity
	ProducerIdAndEpoch ProducerIdAndEpoch

	// LastHeartbeat is when the producer last sent a heartbeat
	// Used for timeout detection
	LastHeartbeat time.Time

	// TransactionTimeoutMs is the max time a transaction can remain open
	TransactionTimeoutMs int64

	// State is the current transaction state (if in a transaction)
	State TransactionState

	// CurrentTransactionId is the ID of the current transaction (if any)
	CurrentTransactionId string

	// PendingPartitions are partitions touched in the current transaction
	// Maps topic -> set of partition numbers
	PendingPartitions map[string]map[int]struct{}

	// TransactionStartTime is when the current transaction began
	TransactionStartTime time.Time
}

// NewTransactionalIdState creates a new transactional ID state.
func NewTransactionalIdState(txnId string, pid ProducerIdAndEpoch, timeoutMs int64) *TransactionalIdState {
	return &TransactionalIdState{
		TransactionalId:      txnId,
		ProducerIdAndEpoch:   pid,
		LastHeartbeat:        time.Now(),
		TransactionTimeoutMs: timeoutMs,
		State:                TransactionStateEmpty,
		PendingPartitions:    make(map[string]map[int]struct{}),
	}
}

// =============================================================================
// TRANSACTION STATE MACHINE
// =============================================================================

// TransactionState represents the state of a transaction.
//
// STATE MACHINE:
// ┌─────────────────────────────────────────────────────────────────────────┐
// │                                                                         │
// │                        ┌───────────────┐                                │
// │            ┌───────────│     Empty     │◄───────────────┐               │
// │            │           └───────┬───────┘                │               │
// │            │                   │                        │               │
// │            │           BeginTransaction                 │               │
// │            │                   │                        │               │
// │            │                   ▼                        │               │
// │            │           ┌───────────────┐                │               │
// │            │           │    Ongoing    │────────────────┤               │
// │            │           └───────┬───────┘                │               │
// │            │                   │                        │               │
// │            │      ┌────────────┴────────────┐           │               │
// │            │      │                         │           │               │
// │            │  CommitTransaction      AbortTransaction   │               │
// │            │      │                         │           │               │
// │            │      ▼                         ▼           │               │
// │            │ ┌─────────────┐         ┌─────────────┐    │               │
// │   Timeout  │ │PrepareCommit│         │PrepareAbort │    │               │
// │   Abort    │ └──────┬──────┘         └──────┬──────┘    │               │
// │            │        │                       │           │               │
// │            │        │ Write markers         │           │               │
// │            │        │                       │           │               │
// │            │        ▼                       ▼           │               │
// │            │ ┌──────────────┐       ┌──────────────┐    │               │
// │            └►│CompleteAbort │       │CompleteCommit│────┘               │
// │              └──────────────┘       └──────────────┘                    │
// │                                                                         │
// └─────────────────────────────────────────────────────────────────────────┘
type TransactionState int8

const (
	// TransactionStateEmpty means no active transaction
	TransactionStateEmpty TransactionState = iota

	// TransactionStateOngoing means transaction is active, can add partitions
	TransactionStateOngoing

	// TransactionStatePrepareCommit means preparing to commit (writing markers)
	TransactionStatePrepareCommit

	// TransactionStatePrepareAbort means preparing to abort (writing markers)
	TransactionStatePrepareAbort

	// TransactionStateCompleteCommit means commit markers written, cleanup pending
	TransactionStateCompleteCommit

	// TransactionStateCompleteAbort means abort markers written, cleanup pending
	TransactionStateCompleteAbort

	// TransactionStateDead means the producer is fenced, no more operations allowed
	TransactionStateDead
)

// String returns a human-readable state name.
func (s TransactionState) String() string {
	switch s {
	case TransactionStateEmpty:
		return "Empty"
	case TransactionStateOngoing:
		return "Ongoing"
	case TransactionStatePrepareCommit:
		return "PrepareCommit"
	case TransactionStatePrepareAbort:
		return "PrepareAbort"
	case TransactionStateCompleteCommit:
		return "CompleteCommit"
	case TransactionStateCompleteAbort:
		return "CompleteAbort"
	case TransactionStateDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// IsCompleted returns true if the transaction is in a terminal state.
func (s TransactionState) IsCompleted() bool {
	return s == TransactionStateCompleteCommit || s == TransactionStateCompleteAbort
}

// IsPreparing returns true if the transaction is being prepared for commit/abort.
func (s TransactionState) IsPreparing() bool {
	return s == TransactionStatePrepareCommit || s == TransactionStatePrepareAbort
}

// =============================================================================
// IDEMPOTENT PRODUCER MANAGER
// =============================================================================

// IdempotentProducerManagerConfig holds configuration for the manager.
type IdempotentProducerManagerConfig struct {
	// DedupWindowSize is how many sequences to track per partition
	DedupWindowSize int

	// ProducerSessionTimeoutMs is how long before an inactive producer expires
	// After this, the producer must re-initialize
	ProducerSessionTimeoutMs int64

	// DefaultTransactionTimeoutMs is the default transaction timeout
	DefaultTransactionTimeoutMs int64

	// MaxTransactionTimeoutMs is the maximum allowed transaction timeout
	MaxTransactionTimeoutMs int64
}

// DefaultIdempotentProducerManagerConfig returns sensible defaults.
func DefaultIdempotentProducerManagerConfig() IdempotentProducerManagerConfig {
	return IdempotentProducerManagerConfig{
		DedupWindowSize:             DefaultDedupWindowSize,
		ProducerSessionTimeoutMs:    300000, // 5 minutes
		DefaultTransactionTimeoutMs: 60000,  // 60 seconds (Kafka default)
		MaxTransactionTimeoutMs:     900000, // 15 minutes
	}
}

// IdempotentProducerManager manages all idempotent/transactional producer state.
//
// RESPONSIBILITIES:
//  1. Assign producer IDs to new producers
//  2. Track transactional ID to PID mappings
//  3. Validate and track sequence numbers
//  4. Detect and handle duplicates
//  5. Fence zombie producers (epoch validation)
//
// THREAD SAFETY:
//
//	All methods are thread-safe. Uses fine-grained locking for performance.
type IdempotentProducerManager struct {
	// config holds manager configuration
	config IdempotentProducerManagerConfig

	// nextProducerId is the next PID to assign
	// Atomically incremented for each new producer
	nextProducerId int64

	// transactionalIds maps transactional ID to state
	// Protected by txnMu
	transactionalIds map[string]*TransactionalIdState
	txnMu            sync.RWMutex

	// sequenceStates maps (PID, topic, partition) to sequence state
	// Protected by seqMu
	sequenceStates map[PartitionSequenceKey]*SequenceState
	seqMu          sync.RWMutex

	// pidMu protects nextProducerId
	pidMu sync.Mutex
}

// NewIdempotentProducerManager creates a new manager.
func NewIdempotentProducerManager(config IdempotentProducerManagerConfig) *IdempotentProducerManager {
	return &IdempotentProducerManager{
		config:           config,
		nextProducerId:   1, // Start at 1 (0 reserved for future use)
		transactionalIds: make(map[string]*TransactionalIdState),
		sequenceStates:   make(map[PartitionSequenceKey]*SequenceState),
	}
}

// =============================================================================
// PRODUCER ID OPERATIONS
// =============================================================================

// InitProducerId initializes or retrieves the producer ID for a transactional ID.
//
// BEHAVIOR:
//   - New transactional ID: Assign new PID with epoch=0
//   - Existing transactional ID: Same PID with epoch+1 (fence old producer)
//   - Empty transactional ID: Assign new PID with epoch=0 (non-transactional)
//
// RETURNS:
//   - ProducerIdAndEpoch: The identity to use for subsequent operations
//   - error: If ID exhausted or other error
//
// KAFKA EQUIVALENT: InitProducerIdRequest / InitProducerIdResponse
func (m *IdempotentProducerManager) InitProducerId(transactionalId string, transactionTimeoutMs int64) (ProducerIdAndEpoch, error) {
	// Validate timeout
	if transactionTimeoutMs <= 0 {
		transactionTimeoutMs = m.config.DefaultTransactionTimeoutMs
	}
	if transactionTimeoutMs > m.config.MaxTransactionTimeoutMs {
		transactionTimeoutMs = m.config.MaxTransactionTimeoutMs
	}

	// Non-transactional producer: assign new PID each time
	if transactionalId == "" {
		return m.allocateNewProducerId()
	}

	// Transactional producer: look up or create mapping
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	existing, exists := m.transactionalIds[transactionalId]
	if exists {
		// Existing producer: increment epoch (fence old instance)
		newEpoch := existing.ProducerIdAndEpoch.Epoch + 1
		if newEpoch > MaxEpoch {
			// Epoch overflow: assign new PID
			pid, err := m.allocateNewProducerIdLocked()
			if err != nil {
				return ProducerIdAndEpoch{}, err
			}
			newEpoch = 0
			existing.ProducerIdAndEpoch = ProducerIdAndEpoch{
				ProducerId: pid,
				Epoch:      newEpoch,
			}
		} else {
			existing.ProducerIdAndEpoch.Epoch = newEpoch
		}

		// Reset transaction state
		existing.State = TransactionStateEmpty
		existing.CurrentTransactionId = ""
		existing.PendingPartitions = make(map[string]map[int]struct{})
		existing.LastHeartbeat = time.Now()
		existing.TransactionTimeoutMs = transactionTimeoutMs

		return existing.ProducerIdAndEpoch, nil
	}

	// New transactional ID: allocate new PID
	pid, err := m.allocateNewProducerIdLocked()
	if err != nil {
		return ProducerIdAndEpoch{}, err
	}

	identity := ProducerIdAndEpoch{
		ProducerId: pid,
		Epoch:      0,
	}

	m.transactionalIds[transactionalId] = NewTransactionalIdState(
		transactionalId,
		identity,
		transactionTimeoutMs,
	)

	return identity, nil
}

// allocateNewProducerId allocates a new PID (thread-safe).
func (m *IdempotentProducerManager) allocateNewProducerId() (ProducerIdAndEpoch, error) {
	m.pidMu.Lock()
	defer m.pidMu.Unlock()

	pid, err := m.allocateNewProducerIdLocked()
	if err != nil {
		return ProducerIdAndEpoch{}, err
	}

	return ProducerIdAndEpoch{
		ProducerId: pid,
		Epoch:      0, // New producer starts at epoch 0
	}, nil
}

// allocateNewProducerIdLocked allocates a new PID (caller must hold pidMu).
func (m *IdempotentProducerManager) allocateNewProducerIdLocked() (int64, error) {
	if m.nextProducerId >= MaxProducerId {
		return 0, ErrProducerIdExhausted
	}
	pid := m.nextProducerId
	m.nextProducerId++
	return pid, nil
}

// =============================================================================
// SEQUENCE VALIDATION
// =============================================================================

// CheckAndUpdateSequence validates a sequence number and updates state.
//
// PARAMETERS:
//   - pid: Producer ID and epoch
//   - topic: Target topic
//   - partition: Target partition
//   - sequence: Sequence number from producer
//
// RETURNS:
//   - existingOffset: If duplicate, the offset where it was previously written
//   - isDuplicate: True if this is a duplicate sequence
//   - error: If sequence is invalid (out of order, fenced producer, etc.)
//
// BEHAVIOR:
//   - First message (sequence=0): Accept if no prior state
//   - Normal message (sequence=last+1): Accept and update state
//   - Duplicate (sequence≤last): Return existing offset, don't write
//   - Gap (sequence>last+1): Reject with ErrOutOfOrderSequence
func (m *IdempotentProducerManager) CheckAndUpdateSequence(
	pid ProducerIdAndEpoch,
	topic string,
	partition int,
	sequence int32,
	offset int64,
) (existingOffset int64, isDuplicate bool, err error) {

	// Validate producer identity first
	if !pid.IsValid() {
		return 0, false, ErrUnknownProducerId
	}

	key := PartitionSequenceKey{
		ProducerId: pid.ProducerId,
		Topic:      topic,
		Partition:  partition,
	}

	m.seqMu.Lock()
	defer m.seqMu.Unlock()

	state, exists := m.sequenceStates[key]
	if !exists {
		// First message from this producer to this partition
		if sequence != 0 {
			// First sequence must be 0
			return 0, false, fmt.Errorf("%w: expected 0, got %d", ErrOutOfOrderSequence, sequence)
		}

		// Create new state
		state = NewSequenceState()
		state.LastSequence = 0
		state.FirstSequence = 0
		state.SequenceWindow[0] = offset
		state.LastUpdateTime = time.Now()
		m.sequenceStates[key] = state

		return 0, false, nil
	}

	// Check for duplicate
	if existingOff, found := state.SequenceWindow[sequence]; found {
		// Duplicate detected - return existing offset
		return existingOff, true, nil
	}

	// Check sequence order
	expectedSeq := state.LastSequence + 1
	if sequence < expectedSeq {
		// Very old duplicate (not in window) - reject or accept?
		// Kafka rejects, but we'll accept for simplicity (idempotent behavior)
		// This means the message was already processed sometime ago
		return 0, true, nil
	}
	if sequence > expectedSeq {
		// Gap detected - producer skipped sequence numbers
		return 0, false, fmt.Errorf("%w: expected %d, got %d", ErrOutOfOrderSequence, expectedSeq, sequence)
	}

	// Normal case: sequence = expectedSeq
	state.LastSequence = sequence
	state.SequenceWindow[sequence] = offset
	state.LastUpdateTime = time.Now()

	// Trim window if too large
	if len(state.SequenceWindow) > m.config.DedupWindowSize {
		// Remove oldest entry
		minSeq := state.FirstSequence
		delete(state.SequenceWindow, minSeq)
		state.FirstSequence = minSeq + 1
	}

	return 0, false, nil
}

// GetSequenceState returns the current sequence state for a producer-partition.
// Returns nil if no state exists.
func (m *IdempotentProducerManager) GetSequenceState(pid int64, topic string, partition int) *SequenceState {
	key := PartitionSequenceKey{
		ProducerId: pid,
		Topic:      topic,
		Partition:  partition,
	}

	m.seqMu.RLock()
	defer m.seqMu.RUnlock()

	if state, exists := m.sequenceStates[key]; exists {
		// Return a copy to prevent external modification
		return &SequenceState{
			LastSequence:   state.LastSequence,
			FirstSequence:  state.FirstSequence,
			SequenceWindow: copySequenceWindow(state.SequenceWindow),
			LastUpdateTime: state.LastUpdateTime,
		}
	}
	return nil
}

// copySequenceWindow creates a copy of the sequence window map.
func copySequenceWindow(m map[int32]int64) map[int32]int64 {
	copy := make(map[int32]int64, len(m))
	for k, v := range m {
		copy[k] = v
	}
	return copy
}

// =============================================================================
// EPOCH VALIDATION
// =============================================================================

// ValidateProducerEpoch checks if the producer's epoch is current.
//
// RETURNS:
//   - nil: Epoch is valid
//   - ErrProducerFenced: A newer epoch exists (zombie detected)
//   - ErrUnknownProducerId: No such transactional ID registered
func (m *IdempotentProducerManager) ValidateProducerEpoch(transactionalId string, pid ProducerIdAndEpoch) error {
	if transactionalId == "" {
		// Non-transactional producers: can't be fenced
		return nil
	}

	m.txnMu.RLock()
	defer m.txnMu.RUnlock()

	state, exists := m.transactionalIds[transactionalId]
	if !exists {
		return ErrUnknownProducerId
	}

	if state.ProducerIdAndEpoch.ProducerId != pid.ProducerId {
		return ErrUnknownProducerId
	}

	if state.ProducerIdAndEpoch.Epoch > pid.Epoch {
		return ErrProducerFenced
	}

	if state.ProducerIdAndEpoch.Epoch < pid.Epoch {
		// This shouldn't happen - producer has higher epoch than recorded
		return ErrInvalidProducerEpoch
	}

	return nil
}

// =============================================================================
// TRANSACTIONAL ID OPERATIONS
// =============================================================================

// GetTransactionalState returns the state for a transactional ID.
// Returns nil if not found.
func (m *IdempotentProducerManager) GetTransactionalState(transactionalId string) *TransactionalIdState {
	m.txnMu.RLock()
	defer m.txnMu.RUnlock()

	state, exists := m.transactionalIds[transactionalId]
	if !exists {
		return nil
	}

	// Return a copy to prevent external modification
	return &TransactionalIdState{
		TransactionalId:      state.TransactionalId,
		ProducerIdAndEpoch:   state.ProducerIdAndEpoch,
		LastHeartbeat:        state.LastHeartbeat,
		TransactionTimeoutMs: state.TransactionTimeoutMs,
		State:                state.State,
		CurrentTransactionId: state.CurrentTransactionId,
		PendingPartitions:    copyPendingPartitions(state.PendingPartitions),
		TransactionStartTime: state.TransactionStartTime,
	}
}

// UpdateHeartbeat updates the last heartbeat time for a transactional ID.
func (m *IdempotentProducerManager) UpdateHeartbeat(transactionalId string, pid ProducerIdAndEpoch) error {
	if transactionalId == "" {
		return ErrTransactionalIdRequired
	}

	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	state, exists := m.transactionalIds[transactionalId]
	if !exists {
		return ErrUnknownProducerId
	}

	// Validate epoch
	if state.ProducerIdAndEpoch.ProducerId != pid.ProducerId ||
		state.ProducerIdAndEpoch.Epoch != pid.Epoch {
		return ErrProducerFenced
	}

	state.LastHeartbeat = time.Now()
	return nil
}

// copyPendingPartitions creates a deep copy of pending partitions map.
func copyPendingPartitions(m map[string]map[int]struct{}) map[string]map[int]struct{} {
	result := make(map[string]map[int]struct{}, len(m))
	for topic, partitions := range m {
		result[topic] = make(map[int]struct{}, len(partitions))
		for p := range partitions {
			result[topic][p] = struct{}{}
		}
	}
	return result
}

// =============================================================================
// STATE MANAGEMENT
// =============================================================================

// SetTransactionState updates the transaction state for a transactional ID.
func (m *IdempotentProducerManager) SetTransactionState(transactionalId string, state TransactionState) error {
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	txnState, exists := m.transactionalIds[transactionalId]
	if !exists {
		return ErrUnknownProducerId
	}

	txnState.State = state
	return nil
}

// AddPendingPartition records that a partition has been touched in the current transaction.
func (m *IdempotentProducerManager) AddPendingPartition(transactionalId, topic string, partition int) error {
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	state, exists := m.transactionalIds[transactionalId]
	if !exists {
		return ErrUnknownProducerId
	}

	if state.PendingPartitions[topic] == nil {
		state.PendingPartitions[topic] = make(map[int]struct{})
	}
	state.PendingPartitions[topic][partition] = struct{}{}

	return nil
}

// GetPendingPartitions returns all partitions touched in the current transaction.
func (m *IdempotentProducerManager) GetPendingPartitions(transactionalId string) map[string]map[int]struct{} {
	m.txnMu.RLock()
	defer m.txnMu.RUnlock()

	state, exists := m.transactionalIds[transactionalId]
	if !exists {
		return nil
	}

	return copyPendingPartitions(state.PendingPartitions)
}

// ClearPendingPartitions clears the pending partitions for a transactional ID.
func (m *IdempotentProducerManager) ClearPendingPartitions(transactionalId string) error {
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	state, exists := m.transactionalIds[transactionalId]
	if !exists {
		return ErrUnknownProducerId
	}

	state.PendingPartitions = make(map[string]map[int]struct{})
	return nil
}

// SetTransactionStartTime sets the start time for the current transaction.
func (m *IdempotentProducerManager) SetTransactionStartTime(transactionalId string, t time.Time) error {
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	state, exists := m.transactionalIds[transactionalId]
	if !exists {
		return ErrUnknownProducerId
	}

	state.TransactionStartTime = t
	return nil
}

// SetCurrentTransactionId sets the current transaction ID.
func (m *IdempotentProducerManager) SetCurrentTransactionId(transactionalId, txnId string) error {
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	state, exists := m.transactionalIds[transactionalId]
	if !exists {
		return ErrUnknownProducerId
	}

	state.CurrentTransactionId = txnId
	return nil
}

// =============================================================================
// CLEANUP
// =============================================================================

// ExpireInactiveProducers removes producers that haven't been active recently.
// Returns the number of expired producers.
func (m *IdempotentProducerManager) ExpireInactiveProducers() int {
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	now := time.Now()
	timeout := time.Duration(m.config.ProducerSessionTimeoutMs) * time.Millisecond
	expired := 0

	for txnId, state := range m.transactionalIds {
		// Don't expire producers with active transactions
		if state.State != TransactionStateEmpty {
			continue
		}

		if now.Sub(state.LastHeartbeat) > timeout {
			delete(m.transactionalIds, txnId)
			expired++
		}
	}

	return expired
}

// ExpireOldSequenceStates removes sequence states that haven't been updated recently.
// Returns the number of expired states.
func (m *IdempotentProducerManager) ExpireOldSequenceStates(maxAge time.Duration) int {
	m.seqMu.Lock()
	defer m.seqMu.Unlock()

	now := time.Now()
	expired := 0

	for key, state := range m.sequenceStates {
		if now.Sub(state.LastUpdateTime) > maxAge {
			delete(m.sequenceStates, key)
			expired++
		}
	}

	return expired
}

// =============================================================================
// STATISTICS
// =============================================================================

// IdempotentProducerStats holds statistics about producer state.
type IdempotentProducerStats struct {
	// ActiveProducers is the number of registered transactional IDs
	ActiveProducers int

	// SequenceStates is the number of tracked sequence states
	SequenceStates int

	// NextProducerId is the next PID to be assigned
	NextProducerId int64

	// ProducersByState counts producers in each transaction state
	ProducersByState map[TransactionState]int
}

// Stats returns current statistics.
func (m *IdempotentProducerManager) Stats() IdempotentProducerStats {
	m.txnMu.RLock()
	m.seqMu.RLock()
	m.pidMu.Lock()
	defer m.pidMu.Unlock()
	defer m.seqMu.RUnlock()
	defer m.txnMu.RUnlock()

	stats := IdempotentProducerStats{
		ActiveProducers:  len(m.transactionalIds),
		SequenceStates:   len(m.sequenceStates),
		NextProducerId:   m.nextProducerId,
		ProducersByState: make(map[TransactionState]int),
	}

	for _, state := range m.transactionalIds {
		stats.ProducersByState[state.State]++
	}

	return stats
}

// =============================================================================
// SERIALIZATION FOR PERSISTENCE
// =============================================================================

// ProducerStateSnapshot captures state for persistence.
type ProducerStateSnapshot struct {
	// NextProducerId is the next PID to assign
	NextProducerId int64 `json:"nextProducerId"`

	// TransactionalIds maps transactional ID to its serialized state
	TransactionalIds map[string]TransactionalIdStateSnapshot `json:"transactionalIds"`

	// SequenceStates maps key string to sequence state
	// Key format: "pid:topic:partition"
	SequenceStates map[string]SequenceStateSnapshot `json:"sequenceStates"`

	// Timestamp is when this snapshot was taken
	Timestamp time.Time `json:"timestamp"`
}

// TransactionalIdStateSnapshot is the serializable form of TransactionalIdState.
type TransactionalIdStateSnapshot struct {
	TransactionalId      string           `json:"transactionalId"`
	ProducerId           int64            `json:"producerId"`
	Epoch                int16            `json:"epoch"`
	LastHeartbeat        time.Time        `json:"lastHeartbeat"`
	TransactionTimeoutMs int64            `json:"transactionTimeoutMs"`
	State                TransactionState `json:"state"`
	CurrentTransactionId string           `json:"currentTransactionId,omitempty"`
	PendingPartitions    map[string][]int `json:"pendingPartitions,omitempty"`
	TransactionStartTime time.Time        `json:"transactionStartTime,omitempty"`
}

// SequenceStateSnapshot is the serializable form of SequenceState.
type SequenceStateSnapshot struct {
	LastSequence   int32           `json:"lastSequence"`
	FirstSequence  int32           `json:"firstSequence"`
	SequenceWindow map[int32]int64 `json:"sequenceWindow"`
	LastUpdateTime time.Time       `json:"lastUpdateTime"`
}

// TakeSnapshot creates a snapshot of the current state for persistence.
func (m *IdempotentProducerManager) TakeSnapshot() ProducerStateSnapshot {
	m.txnMu.RLock()
	m.seqMu.RLock()
	m.pidMu.Lock()
	defer m.pidMu.Unlock()
	defer m.seqMu.RUnlock()
	defer m.txnMu.RUnlock()

	snapshot := ProducerStateSnapshot{
		NextProducerId:   m.nextProducerId,
		TransactionalIds: make(map[string]TransactionalIdStateSnapshot),
		SequenceStates:   make(map[string]SequenceStateSnapshot),
		Timestamp:        time.Now(),
	}

	// Serialize transactional IDs
	for txnId, state := range m.transactionalIds {
		pendingPartitions := make(map[string][]int)
		for topic, partitions := range state.PendingPartitions {
			parts := make([]int, 0, len(partitions))
			for p := range partitions {
				parts = append(parts, p)
			}
			pendingPartitions[topic] = parts
		}

		snapshot.TransactionalIds[txnId] = TransactionalIdStateSnapshot{
			TransactionalId:      state.TransactionalId,
			ProducerId:           state.ProducerIdAndEpoch.ProducerId,
			Epoch:                state.ProducerIdAndEpoch.Epoch,
			LastHeartbeat:        state.LastHeartbeat,
			TransactionTimeoutMs: state.TransactionTimeoutMs,
			State:                state.State,
			CurrentTransactionId: state.CurrentTransactionId,
			PendingPartitions:    pendingPartitions,
			TransactionStartTime: state.TransactionStartTime,
		}
	}

	// Serialize sequence states
	for key, state := range m.sequenceStates {
		keyStr := fmt.Sprintf("%d:%s:%d", key.ProducerId, key.Topic, key.Partition)
		snapshot.SequenceStates[keyStr] = SequenceStateSnapshot{
			LastSequence:   state.LastSequence,
			FirstSequence:  state.FirstSequence,
			SequenceWindow: copySequenceWindow(state.SequenceWindow),
			LastUpdateTime: state.LastUpdateTime,
		}
	}

	return snapshot
}

// RestoreFromSnapshot restores state from a snapshot.
func (m *IdempotentProducerManager) RestoreFromSnapshot(snapshot ProducerStateSnapshot) error {
	m.txnMu.Lock()
	m.seqMu.Lock()
	m.pidMu.Lock()
	defer m.pidMu.Unlock()
	defer m.seqMu.Unlock()
	defer m.txnMu.Unlock()

	// Restore next PID
	m.nextProducerId = snapshot.NextProducerId

	// Restore transactional IDs
	m.transactionalIds = make(map[string]*TransactionalIdState)
	for txnId, snap := range snapshot.TransactionalIds {
		pendingPartitions := make(map[string]map[int]struct{})
		for topic, parts := range snap.PendingPartitions {
			pendingPartitions[topic] = make(map[int]struct{})
			for _, p := range parts {
				pendingPartitions[topic][p] = struct{}{}
			}
		}

		m.transactionalIds[txnId] = &TransactionalIdState{
			TransactionalId: snap.TransactionalId,
			ProducerIdAndEpoch: ProducerIdAndEpoch{
				ProducerId: snap.ProducerId,
				Epoch:      snap.Epoch,
			},
			LastHeartbeat:        snap.LastHeartbeat,
			TransactionTimeoutMs: snap.TransactionTimeoutMs,
			State:                snap.State,
			CurrentTransactionId: snap.CurrentTransactionId,
			PendingPartitions:    pendingPartitions,
			TransactionStartTime: snap.TransactionStartTime,
		}
	}

	// Restore sequence states
	m.sequenceStates = make(map[PartitionSequenceKey]*SequenceState)
	for keyStr, snap := range snapshot.SequenceStates {
		// Parse key: "pid:topic:partition"
		var pid int64
		var topic string
		var partition int
		_, err := fmt.Sscanf(keyStr, "%d:%s:%d", &pid, &topic, &partition)
		if err != nil {
			// Try alternative parsing for topics with colons
			parts := splitKeyString(keyStr)
			if len(parts) >= 3 {
				fmt.Sscanf(parts[0], "%d", &pid)
				fmt.Sscanf(parts[len(parts)-1], "%d", &partition)
				topic = joinKeyParts(parts[1 : len(parts)-1])
			} else {
				continue // Skip malformed key
			}
		}

		key := PartitionSequenceKey{
			ProducerId: pid,
			Topic:      topic,
			Partition:  partition,
		}

		m.sequenceStates[key] = &SequenceState{
			LastSequence:   snap.LastSequence,
			FirstSequence:  snap.FirstSequence,
			SequenceWindow: copySequenceWindow(snap.SequenceWindow),
			LastUpdateTime: snap.LastUpdateTime,
		}
	}

	return nil
}

// splitKeyString splits a key string by colons, handling potential colons in topic names.
func splitKeyString(s string) []string {
	var parts []string
	var current []byte
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			parts = append(parts, string(current))
			current = nil
		} else {
			current = append(current, s[i])
		}
	}
	if len(current) > 0 {
		parts = append(parts, string(current))
	}
	return parts
}

// joinKeyParts joins key parts with colons.
func joinKeyParts(parts []string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += ":"
		}
		result += p
	}
	return result
}
