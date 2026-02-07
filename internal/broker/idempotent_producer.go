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
//    - Assigned by broker on InitProducerID call
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
//    │     │                         │ InitProducerID                       │
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
//    │   InitProducerID("order-service-1")                                 │
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

	// ErrUnknownProducerID means the PID is not registered
	ErrUnknownProducerID = errors.New("unknown producer ID")

	// ErrTransactionalIDRequired means transactional operations need a transactional ID
	ErrTransactionalIDRequired = errors.New("transactional ID required")

	// ErrProducerIDExhausted means no more producer IDs available (unlikely)
	ErrProducerIDExhausted = errors.New("producer IDs exhausted")
)

// =============================================================================
// CONSTANTS
// =============================================================================

const (
	// DefaultDedupWindowSize is how many sequences to track per partition
	// Kafka uses 5, but we use a larger window for better duplicate detection
	DefaultDedupWindowSize = 50

	// MaxProducerID is the maximum producer ID (2^63 - 1)
	// Using int64 for simplicity (Kafka uses unsigned)
	MaxProducerID int64 = 1<<63 - 1

	// MaxEpoch is the maximum epoch value (2^16 - 1)
	MaxEpoch int16 = 1<<15 - 1

	// NoProducerID represents an uninitialized producer
	NoProducerID int64 = -1

	// NoEpoch represents an uninitialized epoch
	NoEpoch int16 = -1
)

// =============================================================================
// PRODUCER STATE
// =============================================================================

// ProducerIDAndEpoch holds the identity of an idempotent/transactional producer.
//
// WHY SEPARATE ID AND EPOCH?
// - PID identifies the "logical" producer (e.g., order-service)
// - Epoch identifies the "incarnation" (e.g., 3rd restart today)
// - Together they uniquely identify a producer instance
// - Old epochs are rejected (zombie fencing)
type ProducerIDAndEpoch struct {
	// ProducerID is a unique 64-bit identifier assigned by the broker.
	// It remains stable across a producer's lifetime but changes if
	// the transactional ID is different.
	ProducerID int64

	// Epoch is incremented each time a producer with the same
	// transactional ID initializes. Used for zombie fencing.
	// Range: 0 to 32767 (int16)
	Epoch int16
}

// IsValid returns true if this represents a valid producer identity.
func (p ProducerIDAndEpoch) IsValid() bool {
	return p.ProducerID >= 0 && p.Epoch >= 0
}

// String returns a human-readable representation.
func (p ProducerIDAndEpoch) String() string {
	return fmt.Sprintf("PID=%d,epoch=%d", p.ProducerID, p.Epoch)
}

// =============================================================================
// SEQUENCE STATE
// =============================================================================

// PartitionSequenceKey uniquely identifies a sequence tracking context.
// Sequences are tracked per (ProducerID, Topic, Partition) combination.
type PartitionSequenceKey struct {
	ProducerID int64
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

// TransactionalIDState holds the mapping from transactional ID to producer state.
//
// LIFECYCLE:
//  1. Producer calls InitProducerID("my-txn-id")
//  2. Broker looks up or creates PID for "my-txn-id"
//  3. Broker increments epoch (or starts at 0 for new)
//  4. Broker returns (PID, epoch) to producer
//  5. Producer uses this identity for all subsequent operations
type TransactionalIDState struct {
	// TransactionalID is the client-provided identifier
	TransactionalID string

	// ProducerIDAndEpoch is the current identity
	ProducerIDAndEpoch ProducerIDAndEpoch

	// LastHeartbeat is when the producer last sent a heartbeat
	// Used for timeout detection
	LastHeartbeat time.Time

	// TransactionTimeoutMs is the max time a transaction can remain open
	TransactionTimeoutMs int64

	// State is the current transaction state (if in a transaction)
	State TransactionState

	// CurrentTransactionID is the ID of the current transaction (if any)
	CurrentTransactionID string

	// PendingPartitions are partitions touched in the current transaction
	// Maps topic -> set of partition numbers
	PendingPartitions map[string]map[int]struct{}

	// TransactionStartTime is when the current transaction began
	TransactionStartTime time.Time
}

// NewTransactionalIDState creates a new transactional ID state.
func NewTransactionalIDState(txnID string, pid ProducerIDAndEpoch, timeoutMs int64) *TransactionalIDState {
	return &TransactionalIDState{
		TransactionalID:      txnID,
		ProducerIDAndEpoch:   pid,
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

	// nextProducerID is the next PID to assign
	// Atomically incremented for each new producer
	nextProducerID int64

	// transactionalIDs maps transactional ID to state
	// Protected by txnMu
	transactionalIDs map[string]*TransactionalIDState
	txnMu            sync.RWMutex

	// sequenceStates maps (PID, topic, partition) to sequence state
	// Protected by seqMu
	sequenceStates map[PartitionSequenceKey]*SequenceState
	seqMu          sync.RWMutex

	// pidMu protects nextProducerID
	pidMu sync.Mutex
}

// NewIdempotentProducerManager creates a new manager.
func NewIdempotentProducerManager(config IdempotentProducerManagerConfig) *IdempotentProducerManager {
	return &IdempotentProducerManager{
		config:           config,
		nextProducerID:   1, // Start at 1 (0 reserved for future use)
		transactionalIDs: make(map[string]*TransactionalIDState),
		sequenceStates:   make(map[PartitionSequenceKey]*SequenceState),
	}
}

// =============================================================================
// PRODUCER ID OPERATIONS
// =============================================================================

// InitProducerID initializes or retrieves the producer ID for a transactional ID.
//
// BEHAVIOR:
//   - New transactional ID: Assign new PID with epoch=0
//   - Existing transactional ID: Same PID with epoch+1 (fence old producer)
//   - Empty transactional ID: Assign new PID with epoch=0 (non-transactional)
//
// RETURNS:
//   - ProducerIDAndEpoch: The identity to use for subsequent operations
//   - error: If ID exhausted or other error
//
// KAFKA EQUIVALENT: InitProducerIDRequest / InitProducerIDResponse
func (m *IdempotentProducerManager) InitProducerID(transactionalID string, transactionTimeoutMs int64) (ProducerIDAndEpoch, error) {
	// Validate timeout
	if transactionTimeoutMs <= 0 {
		transactionTimeoutMs = m.config.DefaultTransactionTimeoutMs
	}
	if transactionTimeoutMs > m.config.MaxTransactionTimeoutMs {
		transactionTimeoutMs = m.config.MaxTransactionTimeoutMs
	}

	// Non-transactional producer: assign new PID each time
	if transactionalID == "" {
		return m.allocateNewProducerID()
	}

	// Transactional producer: look up or create mapping
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	existing, exists := m.transactionalIDs[transactionalID]
	if exists {
		// Existing producer: increment epoch (fence old instance)
		newEpoch := existing.ProducerIDAndEpoch.Epoch + 1
		if newEpoch > MaxEpoch {
			// Epoch overflow: assign new PID
			pid, err := m.allocateNewProducerIDLocked()
			if err != nil {
				return ProducerIDAndEpoch{}, err
			}
			newEpoch = 0
			existing.ProducerIDAndEpoch = ProducerIDAndEpoch{
				ProducerID: pid,
				Epoch:      newEpoch,
			}
		} else {
			existing.ProducerIDAndEpoch.Epoch = newEpoch
		}

		// Reset transaction state
		existing.State = TransactionStateEmpty
		existing.CurrentTransactionID = ""
		existing.PendingPartitions = make(map[string]map[int]struct{})
		existing.LastHeartbeat = time.Now()
		existing.TransactionTimeoutMs = transactionTimeoutMs

		return existing.ProducerIDAndEpoch, nil
	}

	// New transactional ID: allocate new PID
	pid, err := m.allocateNewProducerIDLocked()
	if err != nil {
		return ProducerIDAndEpoch{}, err
	}

	identity := ProducerIDAndEpoch{
		ProducerID: pid,
		Epoch:      0,
	}

	m.transactionalIDs[transactionalID] = NewTransactionalIDState(
		transactionalID,
		identity,
		transactionTimeoutMs,
	)

	return identity, nil
}

// allocateNewProducerID allocates a new PID (thread-safe).
func (m *IdempotentProducerManager) allocateNewProducerID() (ProducerIDAndEpoch, error) {
	m.pidMu.Lock()
	defer m.pidMu.Unlock()

	pid, err := m.allocateNewProducerIDLocked()
	if err != nil {
		return ProducerIDAndEpoch{}, err
	}

	return ProducerIDAndEpoch{
		ProducerID: pid,
		Epoch:      0, // New producer starts at epoch 0
	}, nil
}

// allocateNewProducerIDLocked allocates a new PID (caller must hold pidMu).
func (m *IdempotentProducerManager) allocateNewProducerIDLocked() (int64, error) {
	if m.nextProducerID >= MaxProducerID {
		return 0, ErrProducerIDExhausted
	}
	pid := m.nextProducerID
	m.nextProducerID++
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
	pid ProducerIDAndEpoch,
	topic string,
	partition int,
	sequence int32,
	offset int64,
) (existingOffset int64, isDuplicate bool, err error) {

	// Validate producer identity first
	if !pid.IsValid() {
		return 0, false, ErrUnknownProducerID
	}

	key := PartitionSequenceKey{
		ProducerID: pid.ProducerID,
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
		ProducerID: pid,
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
	clonedMap := make(map[int32]int64, len(m))
	for k, v := range m {
		clonedMap[k] = v
	}
	return clonedMap
}

// =============================================================================
// EPOCH VALIDATION
// =============================================================================

// ValidateProducerEpoch checks if the producer's epoch is current.
//
// RETURNS:
//   - nil: Epoch is valid
//   - ErrProducerFenced: A newer epoch exists (zombie detected)
//   - ErrUnknownProducerID: No such transactional ID registered
func (m *IdempotentProducerManager) ValidateProducerEpoch(transactionalID string, pid ProducerIDAndEpoch) error {
	if transactionalID == "" {
		// Non-transactional producers: can't be fenced
		return nil
	}

	m.txnMu.RLock()
	defer m.txnMu.RUnlock()

	state, exists := m.transactionalIDs[transactionalID]
	if !exists {
		return ErrUnknownProducerID
	}

	if state.ProducerIDAndEpoch.ProducerID != pid.ProducerID {
		return ErrUnknownProducerID
	}

	if state.ProducerIDAndEpoch.Epoch > pid.Epoch {
		return ErrProducerFenced
	}

	if state.ProducerIDAndEpoch.Epoch < pid.Epoch {
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
func (m *IdempotentProducerManager) GetTransactionalState(transactionalID string) *TransactionalIDState {
	m.txnMu.RLock()
	defer m.txnMu.RUnlock()

	state, exists := m.transactionalIDs[transactionalID]
	if !exists {
		return nil
	}

	// Return a copy to prevent external modification
	return &TransactionalIDState{
		TransactionalID:      state.TransactionalID,
		ProducerIDAndEpoch:   state.ProducerIDAndEpoch,
		LastHeartbeat:        state.LastHeartbeat,
		TransactionTimeoutMs: state.TransactionTimeoutMs,
		State:                state.State,
		CurrentTransactionID: state.CurrentTransactionID,
		PendingPartitions:    copyPendingPartitions(state.PendingPartitions),
		TransactionStartTime: state.TransactionStartTime,
	}
}

// GetTransactionalStateByProducerID looks up transactional state by producer ID.
// This is a reverse lookup (iterates over all transactional IDs).
//
// RETURNS:
//   - The state if found and producer ID + epoch match
//   - nil if not found
//
// USE CASE:
//
//	PublishTransactional has producerID/epoch but not transactionalID.
//	This method allows looking up the current transaction ID for tracking.
func (m *IdempotentProducerManager) GetTransactionalStateByProducerID(producerID int64, epoch int16) *TransactionalIDState {
	m.txnMu.RLock()
	defer m.txnMu.RUnlock()

	// Linear scan - not optimal but transactional IDs are typically few
	for _, state := range m.transactionalIDs {
		if state.ProducerIDAndEpoch.ProducerID == producerID &&
			state.ProducerIDAndEpoch.Epoch == epoch {
			// Return a copy
			return &TransactionalIDState{
				TransactionalID:      state.TransactionalID,
				ProducerIDAndEpoch:   state.ProducerIDAndEpoch,
				LastHeartbeat:        state.LastHeartbeat,
				TransactionTimeoutMs: state.TransactionTimeoutMs,
				State:                state.State,
				CurrentTransactionID: state.CurrentTransactionID,
				PendingPartitions:    copyPendingPartitions(state.PendingPartitions),
				TransactionStartTime: state.TransactionStartTime,
			}
		}
	}
	return nil
}

// UpdateHeartbeat updates the last heartbeat time for a transactional ID.
func (m *IdempotentProducerManager) UpdateHeartbeat(transactionalID string, pid ProducerIDAndEpoch) error {
	if transactionalID == "" {
		return ErrTransactionalIDRequired
	}

	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	state, exists := m.transactionalIDs[transactionalID]
	if !exists {
		return ErrUnknownProducerID
	}

	// Validate epoch
	if state.ProducerIDAndEpoch.ProducerID != pid.ProducerID ||
		state.ProducerIDAndEpoch.Epoch != pid.Epoch {
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
func (m *IdempotentProducerManager) SetTransactionState(transactionalID string, state TransactionState) error {
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	txnState, exists := m.transactionalIDs[transactionalID]
	if !exists {
		return ErrUnknownProducerID
	}

	txnState.State = state
	return nil
}

// AddPendingPartition records that a partition has been touched in the current transaction.
func (m *IdempotentProducerManager) AddPendingPartition(transactionalID, topic string, partition int) error {
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	state, exists := m.transactionalIDs[transactionalID]
	if !exists {
		return ErrUnknownProducerID
	}

	if state.PendingPartitions[topic] == nil {
		state.PendingPartitions[topic] = make(map[int]struct{})
	}
	state.PendingPartitions[topic][partition] = struct{}{}

	return nil
}

// GetPendingPartitions returns all partitions touched in the current transaction.
func (m *IdempotentProducerManager) GetPendingPartitions(transactionalID string) map[string]map[int]struct{} {
	m.txnMu.RLock()
	defer m.txnMu.RUnlock()

	state, exists := m.transactionalIDs[transactionalID]
	if !exists {
		return nil
	}

	return copyPendingPartitions(state.PendingPartitions)
}

// ClearPendingPartitions clears the pending partitions for a transactional ID.
func (m *IdempotentProducerManager) ClearPendingPartitions(transactionalID string) error {
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	state, exists := m.transactionalIDs[transactionalID]
	if !exists {
		return ErrUnknownProducerID
	}

	state.PendingPartitions = make(map[string]map[int]struct{})
	return nil
}

// SetTransactionStartTime sets the start time for the current transaction.
func (m *IdempotentProducerManager) SetTransactionStartTime(transactionalID string, t time.Time) error {
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	state, exists := m.transactionalIDs[transactionalID]
	if !exists {
		return ErrUnknownProducerID
	}

	state.TransactionStartTime = t
	return nil
}

// SetCurrentTransactionID sets the current transaction ID.
func (m *IdempotentProducerManager) SetCurrentTransactionID(transactionalID, txnID string) error {
	m.txnMu.Lock()
	defer m.txnMu.Unlock()

	state, exists := m.transactionalIDs[transactionalID]
	if !exists {
		return ErrUnknownProducerID
	}

	state.CurrentTransactionID = txnID
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

	for txnID, state := range m.transactionalIDs {
		// Don't expire producers with active transactions
		if state.State != TransactionStateEmpty {
			continue
		}

		if now.Sub(state.LastHeartbeat) > timeout {
			delete(m.transactionalIDs, txnID)
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

	// NextProducerID is the next PID to be assigned
	NextProducerID int64

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
		ActiveProducers:  len(m.transactionalIDs),
		SequenceStates:   len(m.sequenceStates),
		NextProducerID:   m.nextProducerID,
		ProducersByState: make(map[TransactionState]int),
	}

	for _, state := range m.transactionalIDs {
		stats.ProducersByState[state.State]++
	}

	return stats
}

// =============================================================================
// SERIALIZATION FOR PERSISTENCE
// =============================================================================

// ProducerStateSnapshot captures state for persistence.
type ProducerStateSnapshot struct {
	// NextProducerID is the next PID to assign
	NextProducerID int64 `json:"nextProducerId"`

	// TransactionalIDs maps transactional ID to its serialized state
	TransactionalIDs map[string]TransactionalIDStateSnapshot `json:"transactionalIds"`

	// SequenceStates maps key string to sequence state
	// Key format: "pid:topic:partition"
	SequenceStates map[string]SequenceStateSnapshot `json:"sequenceStates"`

	// Timestamp is when this snapshot was taken
	Timestamp time.Time `json:"timestamp"`
}

// TransactionalIDStateSnapshot is the serializable form of TransactionalIDState.
type TransactionalIDStateSnapshot struct {
	TransactionalID      string           `json:"transactionalId"`
	ProducerID           int64            `json:"producerId"`
	Epoch                int16            `json:"epoch"`
	LastHeartbeat        time.Time        `json:"lastHeartbeat"`
	TransactionTimeoutMs int64            `json:"transactionTimeoutMs"`
	State                TransactionState `json:"state"`
	CurrentTransactionID string           `json:"currentTransactionId,omitempty"`
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
		NextProducerID:   m.nextProducerID,
		TransactionalIDs: make(map[string]TransactionalIDStateSnapshot),
		SequenceStates:   make(map[string]SequenceStateSnapshot),
		Timestamp:        time.Now(),
	}

	// Serialize transactional IDs
	for txnID, state := range m.transactionalIDs {
		pendingPartitions := make(map[string][]int)
		for topic, partitions := range state.PendingPartitions {
			parts := make([]int, 0, len(partitions))
			for p := range partitions {
				parts = append(parts, p)
			}
			pendingPartitions[topic] = parts
		}

		snapshot.TransactionalIDs[txnID] = TransactionalIDStateSnapshot{
			TransactionalID:      state.TransactionalID,
			ProducerID:           state.ProducerIDAndEpoch.ProducerID,
			Epoch:                state.ProducerIDAndEpoch.Epoch,
			LastHeartbeat:        state.LastHeartbeat,
			TransactionTimeoutMs: state.TransactionTimeoutMs,
			State:                state.State,
			CurrentTransactionID: state.CurrentTransactionID,
			PendingPartitions:    pendingPartitions,
			TransactionStartTime: state.TransactionStartTime,
		}
	}

	// Serialize sequence states
	for key, state := range m.sequenceStates {
		keyStr := fmt.Sprintf("%d:%s:%d", key.ProducerID, key.Topic, key.Partition)
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
	m.nextProducerID = snapshot.NextProducerID

	// Restore transactional IDs
	m.transactionalIDs = make(map[string]*TransactionalIDState)
	for txnID, snap := range snapshot.TransactionalIDs {
		pendingPartitions := make(map[string]map[int]struct{})
		for topic, parts := range snap.PendingPartitions {
			pendingPartitions[topic] = make(map[int]struct{})
			for _, p := range parts {
				pendingPartitions[topic][p] = struct{}{}
			}
		}

		m.transactionalIDs[txnID] = &TransactionalIDState{
			TransactionalID: snap.TransactionalID,
			ProducerIDAndEpoch: ProducerIDAndEpoch{
				ProducerID: snap.ProducerID,
				Epoch:      snap.Epoch,
			},
			LastHeartbeat:        snap.LastHeartbeat,
			TransactionTimeoutMs: snap.TransactionTimeoutMs,
			State:                snap.State,
			CurrentTransactionID: snap.CurrentTransactionID,
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
				_, _ = fmt.Sscanf(parts[0], "%d", &pid)
				_, _ = fmt.Sscanf(parts[len(parts)-1], "%d", &partition)
				topic = joinKeyParts(parts[1 : len(parts)-1])
			} else {
				continue // Skip malformed key
			}
		}

		key := PartitionSequenceKey{
			ProducerID: pid,
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
