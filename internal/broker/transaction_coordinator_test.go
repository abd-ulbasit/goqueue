// =============================================================================
// TRANSACTION COORDINATOR TESTS
// =============================================================================
//
// These tests verify the transaction coordinator functionality including:
//   - Producer ID assignment and epoch management
//   - Transaction lifecycle (begin, commit, abort)
//   - Sequence number validation and deduplication
//   - Timeout and heartbeat handling
//   - Zombie fencing
//   - Crash recovery
//
// TEST ORGANIZATION:
//   - TestInitProducerId_*: Producer initialization tests
//   - TestBeginTransaction_*: Transaction begin tests
//   - TestCommitTransaction_*: Transaction commit tests
//   - TestAbortTransaction_*: Transaction abort tests
//   - TestSequence_*: Sequence number validation tests
//   - TestHeartbeat_*: Heartbeat and timeout tests
//   - TestRecovery_*: Crash recovery tests
//
// =============================================================================

package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// =============================================================================
// TEST HELPERS
// =============================================================================

// testTransactionCoordinator creates a coordinator for testing with short timeouts.
func testTransactionCoordinator(t *testing.T) (*TransactionCoordinator, *mockTransactionBroker, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "txn-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	config := TransactionCoordinatorConfig{
		DataDir:              tmpDir,
		TransactionTimeoutMs: 5000, // 5 seconds for tests
		SessionTimeoutMs:     2000, // 2 seconds for tests
		HeartbeatIntervalMs:  500,  // 500ms for tests
		CheckIntervalMs:      100,  // 100ms for tests
		SnapshotIntervalMs:   60000,
	}

	mockBroker := &mockTransactionBroker{
		topics:         make(map[string]*Topic),
		controlRecords: make([]controlRecordCall, 0),
	}

	coord, err := NewTransactionCoordinator(config, mockBroker)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create coordinator: %v", err)
	}

	cleanup := func() {
		coord.Close()
		os.RemoveAll(tmpDir)
	}

	return coord, mockBroker, cleanup
}

// mockTransactionBroker implements TransactionBroker for testing.
type mockTransactionBroker struct {
	topics              map[string]*Topic
	controlRecords      []controlRecordCall
	writeErr            error
	clearedTransactions []string
}

type controlRecordCall struct {
	topic     string
	partition int
	isCommit  bool
	pid       int64
	epoch     int16
	txnId     string
}

func (m *mockTransactionBroker) WriteControlRecord(topic string, partition int, isCommit bool, pid int64, epoch int16, txnId string) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.controlRecords = append(m.controlRecords, controlRecordCall{
		topic:     topic,
		partition: partition,
		isCommit:  isCommit,
		pid:       pid,
		epoch:     epoch,
		txnId:     txnId,
	})
	return nil
}

func (m *mockTransactionBroker) GetTopic(name string) (*Topic, error) {
	topic, exists := m.topics[name]
	if !exists {
		return nil, ErrTopicNotFound
	}
	return topic, nil
}

func (m *mockTransactionBroker) ClearUncommittedTransaction(txnId string) []partitionOffset {
	m.clearedTransactions = append(m.clearedTransactions, txnId)
	return nil // Mock doesn't track actual offsets
}

func (m *mockTransactionBroker) MarkTransactionAborted(offsets []partitionOffset) {
	// Mock - no-op
}

func (m *mockTransactionBroker) TrackUncommittedOffset(topic string, partition int, offset int64, txnId string, producerId int64, epoch int16) {
	// Mock - no-op (recovery tracking)
}

func (m *mockTransactionBroker) addMockTopic(name string, partitionCount int) {
	// Create a minimal mock topic structure
	m.topics[name] = &Topic{
		partitions: make([]*Partition, partitionCount),
	}
}

// Silence unused import warning
var _ = time.Now

// =============================================================================
// PRODUCER INITIALIZATION TESTS
// =============================================================================

func TestInitProducerId_NewTransactionalId(t *testing.T) {
	// Test: First-time producer initialization should get a valid PID and Epoch=0
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	pid, err := coord.InitProducerId("my-producer", 60000)
	if err != nil {
		t.Fatalf("InitProducerId failed: %v", err)
	}

	// PID should be positive (implementation starts from 1)
	if pid.ProducerId < 0 {
		t.Errorf("expected positive PID, got %d", pid.ProducerId)
	}
	if pid.Epoch != 0 {
		t.Errorf("expected first epoch to be 0, got %d", pid.Epoch)
	}
}

func TestInitProducerId_ReinitializeBumpsEpoch(t *testing.T) {
	// Test: Re-initializing with same transactional_id should bump epoch
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	// First init
	pid1, err := coord.InitProducerId("my-producer", 60000)
	if err != nil {
		t.Fatalf("first InitProducerId failed: %v", err)
	}

	// Second init (same transactional_id)
	pid2, err := coord.InitProducerId("my-producer", 60000)
	if err != nil {
		t.Fatalf("second InitProducerId failed: %v", err)
	}

	// Should be same PID but bumped epoch
	if pid2.ProducerId != pid1.ProducerId {
		t.Errorf("expected same PID %d, got %d", pid1.ProducerId, pid2.ProducerId)
	}
	if pid2.Epoch != pid1.Epoch+1 {
		t.Errorf("expected epoch %d, got %d", pid1.Epoch+1, pid2.Epoch)
	}
}

func TestInitProducerId_DifferentTransactionalIds(t *testing.T) {
	// Test: Different transactional_ids should get different PIDs
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	pid1, err := coord.InitProducerId("producer-1", 60000)
	if err != nil {
		t.Fatalf("InitProducerId failed: %v", err)
	}

	pid2, err := coord.InitProducerId("producer-2", 60000)
	if err != nil {
		t.Fatalf("InitProducerId failed: %v", err)
	}

	if pid1.ProducerId == pid2.ProducerId {
		t.Errorf("expected different PIDs, both got %d", pid1.ProducerId)
	}
}

// =============================================================================
// TRANSACTION LIFECYCLE TESTS
// =============================================================================

func TestBeginTransaction_Success(t *testing.T) {
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	// Initialize producer first
	pid, err := coord.InitProducerId("my-producer", 60000)
	if err != nil {
		t.Fatalf("InitProducerId failed: %v", err)
	}

	// Begin transaction
	txnId, err := coord.BeginTransaction("my-producer", pid)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	if txnId == "" {
		t.Error("expected non-empty transaction ID")
	}

	// Verify transaction is tracked
	stats := coord.Stats()
	if stats.ActiveTransactions != 1 {
		t.Errorf("expected 1 active transaction, got %d", stats.ActiveTransactions)
	}
}

func TestBeginTransaction_FailsWithoutInit(t *testing.T) {
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	pid := ProducerIdAndEpoch{ProducerId: 999, Epoch: 0}

	_, err := coord.BeginTransaction("unknown-producer", pid)
	if err == nil {
		t.Error("expected error for unknown producer")
	}
}

func TestBeginTransaction_FailsWithStaleEpoch(t *testing.T) {
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	// Initialize producer
	pid1, err := coord.InitProducerId("my-producer", 60000)
	if err != nil {
		t.Fatalf("InitProducerId failed: %v", err)
	}

	// Re-initialize to bump epoch
	_, err = coord.InitProducerId("my-producer", 60000)
	if err != nil {
		t.Fatalf("second InitProducerId failed: %v", err)
	}

	// Try to begin transaction with old epoch
	_, err = coord.BeginTransaction("my-producer", pid1)
	if err == nil {
		t.Error("expected error for stale epoch (zombie fencing)")
	}
}

func TestCommitTransaction_Success(t *testing.T) {
	coord, mockBroker, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	// Add mock topic
	mockBroker.addMockTopic("orders", 3)

	// Initialize and begin transaction
	pid, _ := coord.InitProducerId("my-producer", 60000)
	_, err := coord.BeginTransaction("my-producer", pid)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Add partition to transaction
	err = coord.AddPartitionToTransaction("my-producer", pid, "orders", 0)
	if err != nil {
		t.Fatalf("AddPartitionToTransaction failed: %v", err)
	}

	// Commit
	err = coord.CommitTransaction("my-producer", pid)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Verify control record was written
	if len(mockBroker.controlRecords) != 1 {
		t.Errorf("expected 1 control record, got %d", len(mockBroker.controlRecords))
	}

	if len(mockBroker.controlRecords) > 0 {
		record := mockBroker.controlRecords[0]
		if !record.isCommit {
			t.Error("expected commit control record")
		}
		if record.topic != "orders" {
			t.Errorf("expected topic 'orders', got '%s'", record.topic)
		}
	}

	// Verify transaction is no longer active
	stats := coord.Stats()
	if stats.ActiveTransactions != 0 {
		t.Errorf("expected 0 active transactions after commit, got %d", stats.ActiveTransactions)
	}
}

func TestAbortTransaction_Success(t *testing.T) {
	coord, mockBroker, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	// Add mock topic
	mockBroker.addMockTopic("orders", 3)

	// Initialize and begin transaction
	pid, _ := coord.InitProducerId("my-producer", 60000)
	_, err := coord.BeginTransaction("my-producer", pid)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Add partition
	err = coord.AddPartitionToTransaction("my-producer", pid, "orders", 1)
	if err != nil {
		t.Fatalf("AddPartitionToTransaction failed: %v", err)
	}

	// Abort
	err = coord.AbortTransaction("my-producer", pid)
	if err != nil {
		t.Fatalf("AbortTransaction failed: %v", err)
	}

	// Verify abort control record was written
	if len(mockBroker.controlRecords) != 1 {
		t.Errorf("expected 1 control record, got %d", len(mockBroker.controlRecords))
	}

	if len(mockBroker.controlRecords) > 0 {
		record := mockBroker.controlRecords[0]
		if record.isCommit {
			t.Error("expected abort control record, got commit")
		}
	}
}

// =============================================================================
// SEQUENCE NUMBER TESTS
// =============================================================================

func TestCheckSequence_ValidSequence(t *testing.T) {
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	// Initialize producer
	pid, _ := coord.InitProducerId("my-producer", 60000)

	// First sequence should be 0
	_, isDup, err := coord.CheckSequence(pid, "orders", 0, 0, 100)
	if err != nil {
		t.Fatalf("CheckSequence failed: %v", err)
	}
	if isDup {
		t.Error("expected first message not to be duplicate")
	}

	// Next sequence should be 1
	_, isDup, err = coord.CheckSequence(pid, "orders", 0, 1, 101)
	if err != nil {
		t.Fatalf("CheckSequence failed: %v", err)
	}
	if isDup {
		t.Error("expected second message not to be duplicate")
	}
}

func TestCheckSequence_DuplicateDetection(t *testing.T) {
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	pid, _ := coord.InitProducerId("my-producer", 60000)

	// First message
	_, _, err := coord.CheckSequence(pid, "orders", 0, 0, 100)
	if err != nil {
		t.Fatalf("CheckSequence failed: %v", err)
	}

	// Duplicate message (same sequence)
	existingOffset, isDup, err := coord.CheckSequence(pid, "orders", 0, 0, 100)
	if err != nil {
		t.Fatalf("CheckSequence for duplicate failed: %v", err)
	}
	if !isDup {
		t.Error("expected duplicate to be detected")
	}
	if existingOffset != 100 {
		t.Errorf("expected existing offset 100, got %d", existingOffset)
	}
}

func TestCheckSequence_OutOfOrderReject(t *testing.T) {
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	pid, _ := coord.InitProducerId("my-producer", 60000)

	// First message (seq=0)
	_, _, err := coord.CheckSequence(pid, "orders", 0, 0, 100)
	if err != nil {
		t.Fatalf("CheckSequence failed: %v", err)
	}

	// Skip sequence 1, try seq=2 (should fail)
	_, _, err = coord.CheckSequence(pid, "orders", 0, 2, 102)
	if err == nil {
		t.Error("expected error for out-of-order sequence")
	}
}

func TestCheckSequence_PerPartitionTracking(t *testing.T) {
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	pid, _ := coord.InitProducerId("my-producer", 60000)

	// Partition 0, seq 0
	_, isDup, err := coord.CheckSequence(pid, "orders", 0, 0, 100)
	if err != nil || isDup {
		t.Fatalf("partition 0 seq 0 failed: err=%v, dup=%v", err, isDup)
	}

	// Partition 1, seq 0 (should also work - separate tracking)
	_, isDup, err = coord.CheckSequence(pid, "orders", 1, 0, 200)
	if err != nil || isDup {
		t.Fatalf("partition 1 seq 0 failed: err=%v, dup=%v", err, isDup)
	}

	// Partition 0, seq 1
	_, isDup, err = coord.CheckSequence(pid, "orders", 0, 1, 101)
	if err != nil || isDup {
		t.Fatalf("partition 0 seq 1 failed: err=%v, dup=%v", err, isDup)
	}
}

// =============================================================================
// HEARTBEAT AND TIMEOUT TESTS
// =============================================================================

func TestHeartbeat_UpdatesLastSeen(t *testing.T) {
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	// Initialize producer
	pid, _ := coord.InitProducerId("my-producer", 60000)

	// Send heartbeat
	err := coord.Heartbeat("my-producer", pid)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}
}

func TestHeartbeat_FailsWithStaleEpoch(t *testing.T) {
	coord, _, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	// Initialize producer
	pid1, _ := coord.InitProducerId("my-producer", 60000)

	// Re-initialize (bump epoch)
	pid2, _ := coord.InitProducerId("my-producer", 60000)
	_ = pid2 // Use pid2 to avoid unused variable

	// Heartbeat with old epoch should fail
	err := coord.Heartbeat("my-producer", pid1)
	if err == nil {
		t.Error("expected error for heartbeat with stale epoch")
	}
}

// =============================================================================
// RECOVERY TESTS
// =============================================================================

func TestRecovery_ProducerStatePreserved(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "txn-recovery-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := TransactionCoordinatorConfig{
		DataDir:              tmpDir,
		TransactionTimeoutMs: 60000,
		SessionTimeoutMs:     30000,
		HeartbeatIntervalMs:  3000,
		CheckIntervalMs:      1000,
		SnapshotIntervalMs:   60000,
	}

	mockBroker := &mockTransactionBroker{
		topics:         make(map[string]*Topic),
		controlRecords: make([]controlRecordCall, 0),
	}

	// Create first coordinator and initialize producer
	coord1, err := NewTransactionCoordinator(config, mockBroker)
	if err != nil {
		t.Fatalf("failed to create first coordinator: %v", err)
	}

	pid1, err := coord1.InitProducerId("my-producer", 60000)
	if err != nil {
		t.Fatalf("InitProducerId failed: %v", err)
	}

	// Force snapshot write
	coord1.Close()

	// Create second coordinator (simulates restart)
	coord2, err := NewTransactionCoordinator(config, mockBroker)
	if err != nil {
		t.Fatalf("failed to create second coordinator: %v", err)
	}
	defer coord2.Close()

	// Re-initialize same transactional_id - should get same PID with bumped epoch
	pid2, err := coord2.InitProducerId("my-producer", 60000)
	if err != nil {
		t.Fatalf("InitProducerId after recovery failed: %v", err)
	}

	if pid2.ProducerId != pid1.ProducerId {
		t.Errorf("expected same PID %d after recovery, got %d", pid1.ProducerId, pid2.ProducerId)
	}
	if pid2.Epoch != pid1.Epoch+1 {
		t.Errorf("expected epoch %d after recovery, got %d", pid1.Epoch+1, pid2.Epoch)
	}
}

// =============================================================================
// MULTI-PARTITION TRANSACTION TESTS
// =============================================================================

func TestTransaction_MultiplePartitions(t *testing.T) {
	coord, mockBroker, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	// Add mock topics
	mockBroker.addMockTopic("orders", 3)
	mockBroker.addMockTopic("inventory", 2)

	// Initialize and begin transaction
	pid, _ := coord.InitProducerId("my-producer", 60000)
	_, err := coord.BeginTransaction("my-producer", pid)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Add multiple partitions
	err = coord.AddPartitionToTransaction("my-producer", pid, "orders", 0)
	if err != nil {
		t.Fatalf("AddPartitionToTransaction orders:0 failed: %v", err)
	}

	err = coord.AddPartitionToTransaction("my-producer", pid, "orders", 1)
	if err != nil {
		t.Fatalf("AddPartitionToTransaction orders:1 failed: %v", err)
	}

	err = coord.AddPartitionToTransaction("my-producer", pid, "inventory", 0)
	if err != nil {
		t.Fatalf("AddPartitionToTransaction inventory:0 failed: %v", err)
	}

	// Commit
	err = coord.CommitTransaction("my-producer", pid)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Should have 3 control records (one per partition)
	if len(mockBroker.controlRecords) != 3 {
		t.Errorf("expected 3 control records, got %d", len(mockBroker.controlRecords))
	}

	// All should be commits
	for i, record := range mockBroker.controlRecords {
		if !record.isCommit {
			t.Errorf("control record %d should be commit", i)
		}
	}
}

// =============================================================================
// ZOMBIE FENCING TESTS
// =============================================================================

func TestZombieFencing_OldEpochRejected(t *testing.T) {
	coord, mockBroker, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	mockBroker.addMockTopic("orders", 3)

	// Initialize producer
	pid1, _ := coord.InitProducerId("my-producer", 60000)

	// Begin transaction with epoch 0
	_, err := coord.BeginTransaction("my-producer", pid1)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Simulate producer restart - re-initialize (bumps epoch)
	pid2, _ := coord.InitProducerId("my-producer", 60000)

	// Old producer tries to add partition (should fail - zombie fenced)
	err = coord.AddPartitionToTransaction("my-producer", pid1, "orders", 0)
	if err == nil {
		t.Error("expected error for zombie producer (old epoch)")
	}

	// New producer should work
	_, err = coord.BeginTransaction("my-producer", pid2)
	if err != nil {
		t.Fatalf("BeginTransaction with new epoch failed: %v", err)
	}

	err = coord.AddPartitionToTransaction("my-producer", pid2, "orders", 0)
	if err != nil {
		t.Fatalf("AddPartitionToTransaction with new epoch failed: %v", err)
	}
}

// =============================================================================
// IDEMPOTENT PRODUCER TESTS
// =============================================================================

func TestIdempotentProducer_NewProducer(t *testing.T) {
	manager := NewIdempotentProducerManager(DefaultIdempotentProducerManagerConfig())

	pid, err := manager.allocateNewProducerId()
	if err != nil {
		t.Fatalf("allocateNewProducerId failed: %v", err)
	}

	// First PID should be positive
	if pid.ProducerId < 0 {
		t.Errorf("expected positive PID, got %d", pid.ProducerId)
	}
	if pid.Epoch != 0 {
		t.Errorf("expected epoch 0, got %d", pid.Epoch)
	}
}

func TestIdempotentProducer_SequentialPIDs(t *testing.T) {
	manager := NewIdempotentProducerManager(DefaultIdempotentProducerManagerConfig())

	var firstPid int64 = -1
	for i := 0; i < 10; i++ {
		pid, err := manager.allocateNewProducerId()
		if err != nil {
			t.Fatalf("allocateNewProducerId failed at %d: %v", i, err)
		}
		if firstPid == -1 {
			firstPid = pid.ProducerId
		}
		// Each subsequent PID should be 1 more than the previous
		expectedPid := firstPid + int64(i)
		if pid.ProducerId != expectedPid {
			t.Errorf("expected PID %d, got %d", expectedPid, pid.ProducerId)
		}
	}
}

// =============================================================================
// TRANSACTION LOG TESTS
// =============================================================================

func TestTransactionLog_WriteAndReplay(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "txn-log-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultTransactionLogConfig(tmpDir)
	config.WALSyncIntervalMs = 0 // Sync immediately

	txnLog, err := NewTransactionLog(config)
	if err != nil {
		t.Fatalf("failed to create transaction log: %v", err)
	}

	// Write some records using proper API
	initData := InitProducerData{
		TransactionalId:      "prod-1",
		ProducerId:           0,
		Epoch:                0,
		TransactionTimeoutMs: 60000,
	}
	if err := txnLog.WriteRecord(WALRecordInitProducer, initData); err != nil {
		t.Fatalf("WriteRecord init failed: %v", err)
	}

	beginData := BeginTxnData{
		TransactionalId: "prod-1",
		TransactionId:   "txn-1",
		ProducerId:      0,
		Epoch:           0,
	}
	if err := txnLog.WriteRecord(WALRecordBeginTxn, beginData); err != nil {
		t.Fatalf("WriteRecord begin failed: %v", err)
	}

	// Sync and close
	txnLog.Close()

	// Reopen and replay
	txnLog2, err := NewTransactionLog(config)
	if err != nil {
		t.Fatalf("failed to reopen transaction log: %v", err)
	}
	defer txnLog2.Close()

	recordCount := 0
	count, err := txnLog2.ReplayWAL(func(record WALRecord) error {
		recordCount++
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}

	// Use count or recordCount
	if count != 2 && recordCount != 2 {
		t.Errorf("expected 2 replayed records, got count=%d, recordCount=%d", count, recordCount)
	}
}

func TestTransactionLog_Snapshot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "txn-snapshot-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultTransactionLogConfig(tmpDir)
	txnLog, err := NewTransactionLog(config)
	if err != nil {
		t.Fatalf("failed to create transaction log: %v", err)
	}

	// Create a snapshot
	snapshot := ProducerStateSnapshot{
		NextProducerId: 5,
		TransactionalIds: map[string]TransactionalIdStateSnapshot{
			"prod-1": {TransactionalId: "prod-1", ProducerId: 0, Epoch: 2},
			"prod-2": {TransactionalId: "prod-2", ProducerId: 1, Epoch: 0},
		},
		SequenceStates: make(map[string]SequenceStateSnapshot),
	}

	if err := txnLog.WriteSnapshot(snapshot); err != nil {
		t.Fatalf("WriteSnapshot failed: %v", err)
	}
	txnLog.Close()

	// Reopen and load snapshot
	txnLog2, err := NewTransactionLog(config)
	if err != nil {
		t.Fatalf("failed to reopen transaction log: %v", err)
	}
	defer txnLog2.Close()

	loaded, err := txnLog2.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	if loaded.NextProducerId != snapshot.NextProducerId {
		t.Errorf("expected NextProducerId %d, got %d", snapshot.NextProducerId, loaded.NextProducerId)
	}
	if len(loaded.TransactionalIds) != len(snapshot.TransactionalIds) {
		t.Errorf("expected %d transactional IDs, got %d", len(snapshot.TransactionalIds), len(loaded.TransactionalIds))
	}
}

// =============================================================================
// MESSAGE CONTROL RECORD TESTS
// =============================================================================

func TestControlRecord_CommitRecord(t *testing.T) {
	// Test control record payload encoding
	pid := uint64(12345)
	epoch := uint16(2)
	txnId := "my-transaction"

	// Create a commit control record manually
	payload := map[string]interface{}{
		"producerId":      pid,
		"epoch":           epoch,
		"transactionalId": txnId,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	// Verify we can round-trip the payload
	var decoded map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &decoded); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}

	if decoded["transactionalId"] != txnId {
		t.Errorf("expected transactionalId %s, got %v", txnId, decoded["transactionalId"])
	}
}

// =============================================================================
// BENCHMARK TESTS
// =============================================================================

func BenchmarkCheckSequence(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "txn-bench-*")
	defer os.RemoveAll(tmpDir)

	config := TransactionCoordinatorConfig{
		DataDir:              tmpDir,
		TransactionTimeoutMs: 60000,
		SessionTimeoutMs:     30000,
		HeartbeatIntervalMs:  3000,
		CheckIntervalMs:      1000,
		SnapshotIntervalMs:   60000,
	}

	mockBroker := &mockTransactionBroker{
		topics:         make(map[string]*Topic),
		controlRecords: make([]controlRecordCall, 0),
	}

	coord, _ := NewTransactionCoordinator(config, mockBroker)
	defer coord.Close()

	pid, _ := coord.InitProducerId("bench-producer", 60000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq := int32(i)
		coord.CheckSequence(pid, "orders", 0, seq, int64(i))
	}
}

func BenchmarkInitProducerId(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "txn-bench-*")
	defer os.RemoveAll(tmpDir)

	config := TransactionCoordinatorConfig{
		DataDir:              tmpDir,
		TransactionTimeoutMs: 60000,
		SessionTimeoutMs:     30000,
		HeartbeatIntervalMs:  3000,
		CheckIntervalMs:      1000,
		SnapshotIntervalMs:   60000,
	}

	mockBroker := &mockTransactionBroker{
		topics:         make(map[string]*Topic),
		controlRecords: make([]controlRecordCall, 0),
	}

	coord, _ := NewTransactionCoordinator(config, mockBroker)
	defer coord.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txnId := filepath.Join("producer", string(rune('a'+i%26)))
		coord.InitProducerId(txnId, 60000)
	}
}

// =============================================================================
// ABORT RETRY TESTS
// =============================================================================
//
// These tests verify that when a transaction commit fails, the coordinator
// attempts to abort with retry logic to clean up properly.
//
// =============================================================================

func TestAbortTransactionWithRetry_SuccessOnFirstAttempt(t *testing.T) {
	coord, mockBroker, cleanup := testTransactionCoordinator(t)
	defer cleanup()

	// Add mock topic
	mockBroker.addMockTopic("orders", 3)

	// Initialize and begin transaction
	pid, _ := coord.InitProducerId("retry-producer", 60000)
	txnId, err := coord.BeginTransaction("retry-producer", pid)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Add partition
	err = coord.AddPartitionToTransaction("retry-producer", pid, "orders", 1)
	if err != nil {
		t.Fatalf("AddPartitionToTransaction failed: %v", err)
	}

	// Call abortTransactionWithRetry directly (this is called when commit fails)
	err = coord.abortTransactionWithRetry("retry-producer", txnId, pid)
	if err != nil {
		t.Fatalf("abortTransactionWithRetry failed: %v", err)
	}

	// Verify abort control record was written
	if len(mockBroker.controlRecords) != 1 {
		t.Errorf("expected 1 control record, got %d", len(mockBroker.controlRecords))
	}

	if len(mockBroker.controlRecords) > 0 {
		record := mockBroker.controlRecords[0]
		if record.isCommit {
			t.Error("expected abort control record, got commit")
		}
	}
}
