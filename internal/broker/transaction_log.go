// =============================================================================
// TRANSACTION LOG - PERSISTENT TRANSACTION STATE STORAGE
// =============================================================================
//
// WHAT IS A TRANSACTION LOG?
// The transaction log stores all transaction-related state persistently:
//   - Producer ID to transactional ID mappings
//   - Transaction states (Empty, Ongoing, PrepareCommit, etc.)
//   - Pending partitions in each transaction
//   - Sequence number state for deduplication
//
// WHY DO WE NEED THIS?
// Without persistent storage, a broker restart would lose:
//   - Which transactions are in-progress (they'd be stuck forever)
//   - Producer ID mappings (duplicates could occur)
//   - Sequence numbers (deduplication would fail)
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   ┌─────────────┬────────────────────────────────────────────────────────────┐
//   │ System      │ Transaction State Storage                                  │
//   ├─────────────┼────────────────────────────────────────────────────────────┤
//   │ Kafka       │ __transaction_state internal topic                         │
//   │             │ - Compacted topic (keeps latest per key)                   │
//   │             │ - Partitioned by transactional.id hash                     │
//   │             │ - Transaction Coordinator owns partitions                  │
//   ├─────────────┼────────────────────────────────────────────────────────────┤
//   │ PostgreSQL  │ pg_xact (CLOG) for transaction status                      │
//   │             │ - Bit-mapped storage (2 bits per transaction)              │
//   │             │ - In-progress, committed, aborted, sub-committed           │
//   ├─────────────┼────────────────────────────────────────────────────────────┤
//   │ MySQL       │ undo log + redo log (InnoDB)                               │
//   │             │ - Redo log for crash recovery                              │
//   │             │ - Undo log for rollback and MVCC                           │
//   ├─────────────┼────────────────────────────────────────────────────────────┤
//   │ goqueue     │ File-based JSON log                                        │
//   │             │ - data/transactions/producer_state.json (snapshot)         │
//   │             │ - data/transactions/transactions.log (append-only WAL)     │
//   │             │ - Periodic snapshots + WAL for fast recovery               │
//   └─────────────┴────────────────────────────────────────────────────────────┘
//
// STORAGE STRATEGY:
//
// We use a combination of snapshots and write-ahead log (WAL) for durability:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                    STORAGE ARCHITECTURE                                 │
//   │                                                                         │
//   │  ┌──────────────────────┐    ┌──────────────────────┐                   │
//   │  │   Snapshot File      │    │   Transaction WAL    │                   │
//   │  │                      │    │                      │                   │
//   │  │  producer_state.json │    │  transactions.log    │                   │
//   │  │                      │    │                      │                   │
//   │  │  - Full state dump   │    │  - Append-only       │                   │
//   │  │  - Periodic (1min)   │    │  - Every operation   │                   │
//   │  │  - Fast recovery     │    │  - Replay to current │                   │
//   │  │                      │    │                      │                   │
//   │  └──────────────────────┘    └──────────────────────┘                   │
//   │            │                           │                                │
//   │            └─────────┬─────────────────┘                                │
//   │                      │                                                  │
//   │                      ▼                                                  │
//   │              ┌──────────────────┐                                       │
//   │              │    Recovery      │                                       │
//   │              │                  │                                       │
//   │              │  1. Load snapshot│                                       │
//   │              │  2. Replay WAL   │                                       │
//   │              │  3. Resume ops   │                                       │
//   │              │                  │                                       │
//   │              └──────────────────┘                                       │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// WAL RECORD FORMAT:
//
//   Each WAL record is a JSON object on a single line (JSONL format):
//
//   {"type":"init_producer","timestamp":"...","data":{...}}
//   {"type":"begin_txn","timestamp":"...","data":{...}}
//   {"type":"add_partition","timestamp":"...","data":{...}}
//   {"type":"prepare_commit","timestamp":"...","data":{...}}
//   {"type":"complete_commit","timestamp":"...","data":{...}}
//
// RECORD TYPES:
//
//   - init_producer: New producer ID assignment or epoch bump
//   - begin_txn: Transaction started
//   - add_partition: Partition added to transaction
//   - prepare_commit: Beginning commit phase
//   - prepare_abort: Beginning abort phase
//   - complete_commit: Commit finished
//   - complete_abort: Abort finished
//   - heartbeat: Producer heartbeat (optional, for debugging)
//   - expire_producer: Producer session expired
//
// =============================================================================

package broker

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrTransactionLogClosed means the log has been closed
	ErrTransactionLogClosed = errors.New("transaction log is closed")

	// ErrTransactionLogCorrupted means the log file is corrupted
	ErrTransactionLogCorrupted = errors.New("transaction log corrupted")

	// ErrSnapshotCorrupted means the snapshot file is corrupted
	ErrSnapshotCorrupted = errors.New("snapshot file corrupted")
)

// =============================================================================
// WAL RECORD TYPES
// =============================================================================

// WALRecordType identifies the type of transaction log record.
type WALRecordType string

const (
	// WALRecordInitProducer records producer ID initialization
	WALRecordInitProducer WALRecordType = "init_producer"

	// WALRecordBeginTxn records transaction start
	WALRecordBeginTxn WALRecordType = "begin_txn"

	// WALRecordAddPartition records partition added to transaction
	WALRecordAddPartition WALRecordType = "add_partition"

	// WALRecordPrepareCommit records commit preparation start
	WALRecordPrepareCommit WALRecordType = "prepare_commit"

	// WALRecordPrepareAbort records abort preparation start
	WALRecordPrepareAbort WALRecordType = "prepare_abort"

	// WALRecordCompleteCommit records commit completion
	WALRecordCompleteCommit WALRecordType = "complete_commit"

	// WALRecordCompleteAbort records abort completion
	WALRecordCompleteAbort WALRecordType = "complete_abort"

	// WALRecordHeartbeat records producer heartbeat
	WALRecordHeartbeat WALRecordType = "heartbeat"

	// WALRecordExpireProducer records producer expiration
	WALRecordExpireProducer WALRecordType = "expire_producer"

	// WALRecordUpdateSequence records sequence number update
	WALRecordUpdateSequence WALRecordType = "update_sequence"

	// WALRecordTxnPublish records a transactional publish with offset info.
	//
	// WHY THIS RECORD?
	// During recovery, we need to rebuild the UncommittedTracker to know
	// which offsets belong to in-progress transactions. Without this record,
	// the WAL only tracks transaction lifecycle (begin/commit/abort) but not
	// the specific message offsets written as part of each transaction.
	//
	// WHEN THIS IS WRITTEN:
	//   After a successful transactional publish (PublishTransactional)
	//
	// DURING RECOVERY:
	//   - If the transaction is still ongoing → add offset to UncommittedTracker
	//   - If the transaction was committed → ignore (offsets are visible)
	//   - If the transaction was aborted → add offset to AbortedTracker
	//
	// COMPARISON:
	//   - Kafka: Doesn't need this because it uses LSO (Last Stable Offset)
	//     and scans control records in the log segments themselves
	//   - goqueue: We track offsets in-memory, so we need WAL records to
	//     rebuild that state after restart
	WALRecordTxnPublish WALRecordType = "txn_publish"
)

// =============================================================================
// WAL RECORD STRUCTURE
// =============================================================================

// WALRecord is a single record in the transaction log.
type WALRecord struct {
	// Type identifies what kind of record this is
	Type WALRecordType `json:"type"`

	// Timestamp is when this record was written
	Timestamp time.Time `json:"timestamp"`

	// Data contains the record-specific payload
	Data json.RawMessage `json:"data"`
}

// InitProducerData is the payload for WALRecordInitProducer.
type InitProducerData struct {
	TransactionalID      string `json:"transactionalID"`
	ProducerID           int64  `json:"producerId"`
	Epoch                int16  `json:"epoch"`
	TransactionTimeoutMs int64  `json:"transactionTimeoutMs"`
}

// BeginTxnData is the payload for WALRecordBeginTxn.
type BeginTxnData struct {
	TransactionalID string `json:"transactionalID"`
	TransactionID   string `json:"transactionId"`
	ProducerID      int64  `json:"producerId"`
	Epoch           int16  `json:"epoch"`
}

// AddPartitionData is the payload for WALRecordAddPartition.
type AddPartitionData struct {
	TransactionalID string `json:"transactionalID"`
	Topic           string `json:"topic"`
	Partition       int    `json:"partition"`
}

// PrepareCommitData is the payload for WALRecordPrepareCommit/Abort.
type PrepareCommitData struct {
	TransactionalID string           `json:"transactionalID"`
	Partitions      map[string][]int `json:"partitions"` // topic -> partitions
}

// CompleteCommitData is the payload for WALRecordCompleteCommit/Abort.
type CompleteCommitData struct {
	TransactionalID string `json:"transactionalID"`
	Success         bool   `json:"success"`
	Error           string `json:"error,omitempty"`
}

// HeartbeatData is the payload for WALRecordHeartbeat.
type HeartbeatData struct {
	TransactionalID string `json:"transactionalID"`
	ProducerID      int64  `json:"producerId"`
	Epoch           int16  `json:"epoch"`
}

// ExpireProducerData is the payload for WALRecordExpireProducer.
type ExpireProducerData struct {
	TransactionalID string `json:"transactionalID"`
	ProducerID      int64  `json:"producerId"`
	Epoch           int16  `json:"epoch"`
	Reason          string `json:"reason"`
}

// UpdateSequenceData is the payload for WALRecordUpdateSequence.
type UpdateSequenceData struct {
	ProducerID int64  `json:"producerId"`
	Topic      string `json:"topic"`
	Partition  int    `json:"partition"`
	Sequence   int32  `json:"sequence"`
	Offset     int64  `json:"offset"`
}

// TxnPublishData is the payload for WALRecordTxnPublish.
//
// Records the specific offset written during a transactional publish.
// This data is essential for rebuilding the UncommittedTracker during recovery.
//
// FIELDS:
//   - TransactionalID: Identifies the producer (for looking up transaction state)
//   - TransactionID: The specific transaction this publish belongs to
//   - Topic/Partition/Offset: Where the message was written
//   - ProducerID/Epoch: Producer identity for the UncommittedTracker
type TxnPublishData struct {
	TransactionalID string `json:"transactionalID"`
	TransactionID   string `json:"transactionId"`
	Topic           string `json:"topic"`
	Partition       int    `json:"partition"`
	Offset          int64  `json:"offset"`
	ProducerID      int64  `json:"producerId"`
	Epoch           int16  `json:"epoch"`
}

// =============================================================================
// TRANSACTION LOG CONFIGURATION
// =============================================================================

// TransactionLogConfig holds configuration for the transaction log.
type TransactionLogConfig struct {
	// DataDir is the directory for transaction log files
	DataDir string

	// SnapshotIntervalMs is how often to take snapshots
	SnapshotIntervalMs int64

	// WALSyncIntervalMs is how often to sync the WAL to disk
	// 0 means sync after every write (safest but slowest)
	WALSyncIntervalMs int64

	// MaxWALSizeBytes is the max size before truncating WAL after snapshot
	MaxWALSizeBytes int64

	// EnableWAL controls whether WAL is enabled
	// If disabled, only snapshots are used (faster but less durable)
	EnableWAL bool
}

// DefaultTransactionLogConfig returns sensible defaults.
func DefaultTransactionLogConfig(dataDir string) TransactionLogConfig {
	return TransactionLogConfig{
		DataDir:            filepath.Join(dataDir, "transactions"),
		SnapshotIntervalMs: 60000,    // 1 minute
		WALSyncIntervalMs:  1000,     // 1 second
		MaxWALSizeBytes:    10 << 20, // 10 MB
		EnableWAL:          true,
	}
}

// =============================================================================
// TRANSACTION LOG
// =============================================================================

// TransactionLog manages persistent transaction state storage.
//
// THREAD SAFETY:
//
//	All methods are thread-safe. Uses mutex for synchronization.
//
// DURABILITY:
//   - WAL records are fsync'd according to WALSyncIntervalMs
//   - Snapshots are fsync'd immediately
//   - Recovery replays WAL after loading snapshot
type TransactionLog struct {
	// config holds log configuration
	config TransactionLogConfig

	// walFile is the write-ahead log file
	walFile *os.File

	// walWriter is a buffered writer for the WAL
	walWriter *bufio.Writer

	// walSize tracks current WAL file size
	walSize int64

	// lastSync is when we last synced the WAL
	lastSync time.Time

	// snapshotPath is the path to the snapshot file
	snapshotPath string

	// walPath is the path to the WAL file
	walPath string

	// mu protects all fields
	mu sync.Mutex

	// closed indicates if the log is closed
	closed bool

	// recordCount tracks records written since last snapshot
	recordCount int64
}

// NewTransactionLog creates and opens a new transaction log.
func NewTransactionLog(config TransactionLogConfig) (*TransactionLog, error) {
	// Create data directory
	if err := os.MkdirAll(config.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create transaction log directory: %w", err)
	}

	log := &TransactionLog{
		config:       config,
		snapshotPath: filepath.Join(config.DataDir, "producer_state.json"),
		walPath:      filepath.Join(config.DataDir, "transactions.log"),
		lastSync:     time.Now(),
	}

	// Open WAL file if enabled
	if config.EnableWAL {
		walFile, err := os.OpenFile(log.walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, fmt.Errorf("failed to open WAL file: %w", err)
		}
		log.walFile = walFile
		log.walWriter = bufio.NewWriter(walFile)

		// Get current WAL size
		info, err := walFile.Stat()
		if err == nil {
			log.walSize = info.Size()
		}
	}

	return log, nil
}

// =============================================================================
// WRITE OPERATIONS
// =============================================================================

// WriteRecord appends a record to the WAL.
func (l *TransactionLog) WriteRecord(recordType WALRecordType, data interface{}) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrTransactionLogClosed
	}

	if !l.config.EnableWAL {
		return nil // WAL disabled
	}

	// Serialize data
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal record data: %w", err)
	}

	// Create record
	record := WALRecord{
		Type:      recordType,
		Timestamp: time.Now(),
		Data:      dataBytes,
	}

	// Serialize record
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	// Write to WAL (with newline)
	recordBytes = append(recordBytes, '\n')
	n, err := l.walWriter.Write(recordBytes)
	if err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}
	l.walSize += int64(n)
	l.recordCount++

	// Sync if needed
	if l.config.WALSyncIntervalMs == 0 ||
		time.Since(l.lastSync) > time.Duration(l.config.WALSyncIntervalMs)*time.Millisecond {
		if err := l.syncLocked(); err != nil {
			return err
		}
	}

	return nil
}

// syncLocked syncs the WAL to disk (caller must hold mutex).
func (l *TransactionLog) syncLocked() error {
	if l.walWriter != nil {
		if err := l.walWriter.Flush(); err != nil {
			return fmt.Errorf("failed to flush WAL: %w", err)
		}
	}
	if l.walFile != nil {
		if err := l.walFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL: %w", err)
		}
	}
	l.lastSync = time.Now()
	return nil
}

// Sync forces a sync of the WAL to disk.
func (l *TransactionLog) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.syncLocked()
}

// =============================================================================
// SNAPSHOT OPERATIONS
// =============================================================================

// WriteSnapshot writes a full snapshot of producer state.
func (l *TransactionLog) WriteSnapshot(snapshot ProducerStateSnapshot) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrTransactionLogClosed
	}

	// Serialize snapshot
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	// Write to temp file first (atomic write)
	tempPath := l.snapshotPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o600); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	// Rename to final path (atomic on most filesystems)
	if err := os.Rename(tempPath, l.snapshotPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename snapshot: %w", err)
	}

	// Truncate WAL since snapshot contains all state
	if l.config.EnableWAL {
		if err := l.truncateWALLocked(); err != nil {
			// Non-fatal: WAL will just be larger than needed.
			// The snapshot is already persisted, so a larger WAL
			// only wastes disk space until the next successful truncation.
			_ = err
		}
	}

	l.recordCount = 0
	return nil
}

// truncateWALLocked truncates the WAL file (caller must hold mutex).
func (l *TransactionLog) truncateWALLocked() error {
	if l.walFile != nil {
		// Flush and close current WAL
		l.walWriter.Flush()
		l.walFile.Close()

		// Remove old WAL
		os.Remove(l.walPath)

		// Reopen WAL
		walFile, err := os.OpenFile(l.walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		l.walFile = walFile
		l.walWriter = bufio.NewWriter(walFile)
		l.walSize = 0
	}
	return nil
}

// =============================================================================
// READ/RECOVERY OPERATIONS
// =============================================================================

// LoadSnapshot loads the latest snapshot from disk.
// Returns nil snapshot if no snapshot exists.
func (l *TransactionLog) LoadSnapshot() (*ProducerStateSnapshot, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	data, err := os.ReadFile(l.snapshotPath)
	if os.IsNotExist(err) {
		return nil, nil // No snapshot yet
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot: %w", err)
	}

	var snapshot ProducerStateSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSnapshotCorrupted, err)
	}

	return &snapshot, nil
}

// ReplayWAL replays all WAL records, calling the handler for each.
// The handler should apply the record to in-memory state.
func (l *TransactionLog) ReplayWAL(handler func(WALRecord) error) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Open WAL for reading
	file, err := os.Open(l.walPath)
	if os.IsNotExist(err) {
		return 0, nil // No WAL to replay
	}
	if err != nil {
		return 0, fmt.Errorf("failed to open WAL for replay: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Increase buffer size for potentially large records
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	count := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue // Skip empty lines
		}

		var record WALRecord
		if err := json.Unmarshal(line, &record); err != nil {
			// Log warning but continue (best effort recovery)
			continue
		}

		if err := handler(record); err != nil {
			return count, fmt.Errorf("handler error at record %d: %w", count, err)
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("error reading WAL: %w", err)
	}

	return count, nil
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Close closes the transaction log.
func (l *TransactionLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true

	var errs []error

	if l.walWriter != nil {
		if err := l.walWriter.Flush(); err != nil {
			errs = append(errs, err)
		}
	}

	if l.walFile != nil {
		if err := l.walFile.Sync(); err != nil {
			errs = append(errs, err)
		}
		if err := l.walFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing transaction log: %v", errs)
	}
	return nil
}

// =============================================================================
// STATISTICS
// =============================================================================

// TransactionLogStats holds statistics about the transaction log.
type TransactionLogStats struct {
	// WALSizeBytes is the current WAL file size
	WALSizeBytes int64

	// RecordsSinceSnapshot is records written since last snapshot
	RecordsSinceSnapshot int64

	// LastSyncTime is when the WAL was last synced
	LastSyncTime time.Time

	// SnapshotExists indicates if a snapshot file exists
	SnapshotExists bool

	// WALEnabled indicates if WAL is enabled
	WALEnabled bool
}

// Stats returns current statistics.
func (l *TransactionLog) Stats() TransactionLogStats {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, snapshotErr := os.Stat(l.snapshotPath)

	return TransactionLogStats{
		WALSizeBytes:         l.walSize,
		RecordsSinceSnapshot: l.recordCount,
		LastSyncTime:         l.lastSync,
		SnapshotExists:       snapshotErr == nil,
		WALEnabled:           l.config.EnableWAL,
	}
}

// =============================================================================
// HELPER METHODS FOR SPECIFIC RECORD TYPES
// =============================================================================

// WriteInitProducer writes an init_producer record.
func (l *TransactionLog) WriteInitProducer(txnID string, pid int64, epoch int16, timeoutMs int64) error {
	return l.WriteRecord(WALRecordInitProducer, InitProducerData{
		TransactionalID:      txnID,
		ProducerID:           pid,
		Epoch:                epoch,
		TransactionTimeoutMs: timeoutMs,
	})
}

// WriteBeginTxn writes a begin_txn record.
func (l *TransactionLog) WriteBeginTxn(txnID, transactionID string, pid int64, epoch int16) error {
	return l.WriteRecord(WALRecordBeginTxn, BeginTxnData{
		TransactionalID: txnID,
		TransactionID:   transactionID,
		ProducerID:      pid,
		Epoch:           epoch,
	})
}

// WriteAddPartition writes an add_partition record.
func (l *TransactionLog) WriteAddPartition(txnID, topic string, partition int) error {
	return l.WriteRecord(WALRecordAddPartition, AddPartitionData{
		TransactionalID: txnID,
		Topic:           topic,
		Partition:       partition,
	})
}

// WritePrepareCommit writes a prepare_commit record.
func (l *TransactionLog) WritePrepareCommit(txnID string, partitions map[string][]int) error {
	return l.WriteRecord(WALRecordPrepareCommit, PrepareCommitData{
		TransactionalID: txnID,
		Partitions:      partitions,
	})
}

// WritePrepareAbort writes a prepare_abort record.
func (l *TransactionLog) WritePrepareAbort(txnID string, partitions map[string][]int) error {
	return l.WriteRecord(WALRecordPrepareAbort, PrepareCommitData{
		TransactionalID: txnID,
		Partitions:      partitions,
	})
}

// WriteCompleteCommit writes a complete_commit record.
func (l *TransactionLog) WriteCompleteCommit(txnID string, success bool, errMsg string) error {
	return l.WriteRecord(WALRecordCompleteCommit, CompleteCommitData{
		TransactionalID: txnID,
		Success:         success,
		Error:           errMsg,
	})
}

// WriteCompleteAbort writes a complete_abort record.
func (l *TransactionLog) WriteCompleteAbort(txnID string, success bool, errMsg string) error {
	return l.WriteRecord(WALRecordCompleteAbort, CompleteCommitData{
		TransactionalID: txnID,
		Success:         success,
		Error:           errMsg,
	})
}

// WriteHeartbeat writes a heartbeat record.
func (l *TransactionLog) WriteHeartbeat(txnID string, pid int64, epoch int16) error {
	return l.WriteRecord(WALRecordHeartbeat, HeartbeatData{
		TransactionalID: txnID,
		ProducerID:      pid,
		Epoch:           epoch,
	})
}

// WriteExpireProducer writes an expire_producer record.
func (l *TransactionLog) WriteExpireProducer(txnID string, pid int64, epoch int16, reason string) error {
	return l.WriteRecord(WALRecordExpireProducer, ExpireProducerData{
		TransactionalID: txnID,
		ProducerID:      pid,
		Epoch:           epoch,
		Reason:          reason,
	})
}

// WriteUpdateSequence writes an update_sequence record.
func (l *TransactionLog) WriteUpdateSequence(pid int64, topic string, partition int, sequence int32, offset int64) error {
	return l.WriteRecord(WALRecordUpdateSequence, UpdateSequenceData{
		ProducerID: pid,
		Topic:      topic,
		Partition:  partition,
		Sequence:   sequence,
		Offset:     offset,
	})
}

// WriteTxnPublish records a transactional publish with offset information.
//
// WHEN CALLED:
//
//	After a successful PublishTransactional in the broker.
//
// PURPOSE:
//
//	Enables recovery to rebuild the UncommittedTracker by knowing exactly
//	which offsets belong to which transaction.
func (l *TransactionLog) WriteTxnPublish(transactionalID, transactionID, topic string, partition int, offset, producerID int64, epoch int16) error {
	return l.WriteRecord(WALRecordTxnPublish, TxnPublishData{
		TransactionalID: transactionalID,
		TransactionID:   transactionID,
		Topic:           topic,
		Partition:       partition,
		Offset:          offset,
		ProducerID:      producerID,
		Epoch:           epoch,
	})
}

// =============================================================================
// WAL RECORD PARSING HELPERS
// =============================================================================

// ParseInitProducerData parses init_producer record data.
func ParseInitProducerData(raw json.RawMessage) (InitProducerData, error) {
	var data InitProducerData
	err := json.Unmarshal(raw, &data)
	return data, err
}

// ParseBeginTxnData parses begin_txn record data.
func ParseBeginTxnData(raw json.RawMessage) (BeginTxnData, error) {
	var data BeginTxnData
	err := json.Unmarshal(raw, &data)
	return data, err
}

// ParseAddPartitionData parses add_partition record data.
func ParseAddPartitionData(raw json.RawMessage) (AddPartitionData, error) {
	var data AddPartitionData
	err := json.Unmarshal(raw, &data)
	return data, err
}

// ParsePrepareCommitData parses prepare_commit/abort record data.
func ParsePrepareCommitData(raw json.RawMessage) (PrepareCommitData, error) {
	var data PrepareCommitData
	err := json.Unmarshal(raw, &data)
	return data, err
}

// ParseCompleteCommitData parses complete_commit/abort record data.
func ParseCompleteCommitData(raw json.RawMessage) (CompleteCommitData, error) {
	var data CompleteCommitData
	err := json.Unmarshal(raw, &data)
	return data, err
}

// ParseHeartbeatData parses heartbeat record data.
func ParseHeartbeatData(raw json.RawMessage) (HeartbeatData, error) {
	var data HeartbeatData
	err := json.Unmarshal(raw, &data)
	return data, err
}

// ParseExpireProducerData parses expire_producer record data.
func ParseExpireProducerData(raw json.RawMessage) (ExpireProducerData, error) {
	var data ExpireProducerData
	err := json.Unmarshal(raw, &data)
	return data, err
}

// ParseUpdateSequenceData parses update_sequence record data.
func ParseUpdateSequenceData(raw json.RawMessage) (UpdateSequenceData, error) {
	var data UpdateSequenceData
	err := json.Unmarshal(raw, &data)
	return data, err
}

// ParseTxnPublishData parses txn_publish record data.
func ParseTxnPublishData(raw json.RawMessage) (TxnPublishData, error) {
	var data TxnPublishData
	err := json.Unmarshal(raw, &data)
	return data, err
}

// =============================================================================
// FILE CLEANUP
// =============================================================================

// NeedsSnapshot returns true if a new snapshot should be taken.
func (l *TransactionLog) NeedsSnapshot() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Snapshot if WAL is too large
	if l.walSize > l.config.MaxWALSizeBytes {
		return true
	}

	// Could also add: time since last snapshot, record count threshold
	return false
}

// RemoveAll removes all transaction log files (for testing).
func (l *TransactionLog) RemoveAll() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.closed {
		return errors.New("cannot remove files while log is open")
	}

	os.Remove(l.snapshotPath)
	os.Remove(l.walPath)
	return nil
}

// WALReader provides read access to WAL records.
type WALReader struct {
	file    *os.File
	scanner *bufio.Scanner
}

// NewWALReader creates a new WAL reader.
func NewWALReader(walPath string) (*WALReader, error) {
	file, err := os.Open(walPath)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	return &WALReader{
		file:    file,
		scanner: scanner,
	}, nil
}

// Next reads the next record from the WAL.
// Returns io.EOF when no more records.
func (r *WALReader) Next() (*WALRecord, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	line := r.scanner.Bytes()
	if len(line) == 0 {
		return r.Next() // Skip empty lines
	}

	var record WALRecord
	if err := json.Unmarshal(line, &record); err != nil {
		return nil, fmt.Errorf("failed to parse WAL record: %w", err)
	}

	return &record, nil
}

// Close closes the WAL reader.
func (r *WALReader) Close() error {
	return r.file.Close()
}
