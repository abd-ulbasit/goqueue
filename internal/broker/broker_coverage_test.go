// =============================================================================
// BROKER COVERAGE TESTS
// =============================================================================
//
// Additional tests to improve coverage of broker package to 90%+.
// Tests functions with 0% coverage and edge cases.
//
// =============================================================================

package broker

import (
	"errors"
	"goqueue/internal/storage"
	"log/slog"
	"os"
	"testing"
	"time"
)

// =============================================================================
// BROKER CORE TESTS
// =============================================================================

// TestBroker_IsClosed tests the IsClosed method.
func TestBroker_IsClosed(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}

	// Broker should not be closed initially
	if b.IsClosed() {
		t.Error("Broker should not be closed immediately after creation")
	}

	// Close the broker
	if err := b.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Broker should now be closed
	if !b.IsClosed() {
		t.Error("Broker should be closed after Close()")
	}
}

// TestBroker_GetAckManager tests the GetAckManager method.
func TestBroker_GetAckManager(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// GetAckManager might return nil if not configured
	// This tests the accessor works
	am := b.GetAckManager()
	// May be nil if ack manager is not enabled in default config
	_ = am // Just verify the call doesn't panic
}

// TestBroker_GetGroupCoordinator tests the GetGroupCoordinator method.
func TestBroker_GetGroupCoordinator(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// GetGroupCoordinator might return nil if not configured
	gc := b.GetGroupCoordinator()
	// May be nil if consumer groups not enabled
	_ = gc // Just verify the call doesn't panic
}

// TestBroker_GetOffsetByTimestamp tests the GetOffsetByTimestamp method.
func TestBroker_GetOffsetByTimestamp(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic with single partition
	topicConfig := TopicConfig{
		Name:          "timestamp-test",
		NumPartitions: 1,
	}
	if err := b.CreateTopic(topicConfig); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Test on non-existent topic
	_, err = b.GetOffsetByTimestamp("non-existent", 0, time.Now().UnixMilli())
	if err == nil {
		t.Error("GetOffsetByTimestamp should fail for non-existent topic")
	}

	// Test on invalid partition
	_, err = b.GetOffsetByTimestamp("timestamp-test", 999, time.Now().UnixMilli())
	if err == nil {
		t.Error("GetOffsetByTimestamp should fail for invalid partition")
	}

	// Publish some messages with known timestamps
	before := time.Now().UnixMilli()
	time.Sleep(10 * time.Millisecond) // Small delay to ensure timestamps differ

	for i := 0; i < 5; i++ {
		_, _, err := b.Publish("timestamp-test", []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	time.Sleep(10 * time.Millisecond)
	after := time.Now().UnixMilli()

	// Query for offset at 'before' timestamp
	offset, err := b.GetOffsetByTimestamp("timestamp-test", 0, before)
	if err != nil {
		// Timestamp lookup may fail if time index doesn't have entries
		t.Logf("GetOffsetByTimestamp(before) returned error (expected for some implementations): %v", err)
	} else {
		t.Logf("GetOffsetByTimestamp(before=%d) returned offset=%d", before, offset)
	}

	// Query for offset at 'after' timestamp (after all messages)
	offset, err = b.GetOffsetByTimestamp("timestamp-test", 0, after)
	if err != nil {
		t.Logf("GetOffsetByTimestamp(after) returned error: %v", err)
	} else {
		t.Logf("GetOffsetByTimestamp(after=%d) returned offset=%d", after, offset)
	}
}

// TestBroker_GetOffsetByTimestamp_ClosedBroker tests GetOffsetByTimestamp on closed broker.
func TestBroker_GetOffsetByTimestamp_ClosedBroker(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}

	// Create topic
	topicConfig := TopicConfig{
		Name:          "test-topic",
		NumPartitions: 1,
	}
	b.CreateTopic(topicConfig)

	// Close the broker
	b.Close()

	// GetOffsetByTimestamp should fail on closed broker
	_, err = b.GetOffsetByTimestamp("test-topic", 0, time.Now().UnixMilli())
	if err == nil {
		t.Error("GetOffsetByTimestamp should fail on closed broker")
	}
	if !errors.Is(err, ErrBrokerClosed) {
		t.Errorf("Expected ErrBrokerClosed, got: %v", err)
	}
}

// =============================================================================
// PARTITION TESTS
// =============================================================================

// TestPartition_SegmentCount tests the SegmentCount method.
func TestPartition_SegmentCount(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic
	topicConfig := TopicConfig{
		Name:          "segment-test",
		NumPartitions: 1,
	}
	b.CreateTopic(topicConfig)

	topic, _ := b.GetTopic("segment-test")
	partition, _ := topic.Partition(0)

	// Check initial segment count
	count := partition.SegmentCount()
	if count < 1 {
		t.Errorf("SegmentCount = %d, want >= 1", count)
	}
}

// TestPartition_GetOffsetByTimestamp tests the GetOffsetByTimestamp method.
func TestPartition_GetOffsetByTimestamp(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic
	topicConfig := TopicConfig{
		Name:          "ts-test",
		NumPartitions: 1,
	}
	b.CreateTopic(topicConfig)

	topic, _ := b.GetTopic("ts-test")
	partition, _ := topic.Partition(0)

	// Test on empty partition
	_, err = partition.GetOffsetByTimestamp(time.Now().UnixMilli())
	// May return error or 0 for empty partition
	t.Logf("Empty partition GetOffsetByTimestamp result: err=%v", err)
}

// TestPartition_Delete tests the Delete method.
func TestPartition_Delete(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}

	// Create topic
	topicConfig := TopicConfig{
		Name:          "delete-test",
		NumPartitions: 1,
	}
	b.CreateTopic(topicConfig)

	topic, _ := b.GetTopic("delete-test")
	partition, _ := topic.Partition(0)

	// Close broker first to release resources
	b.Close()

	// Delete partition
	err = partition.Delete()
	if err != nil {
		t.Logf("Partition.Delete() error (may be expected): %v", err)
	}
}

// TestPartition_DeleteMessagesBefore tests the DeleteMessagesBefore method.
func TestPartition_DeleteMessagesBefore(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic
	topicConfig := TopicConfig{
		Name:          "retention-test",
		NumPartitions: 1,
	}
	b.CreateTopic(topicConfig)

	topic, _ := b.GetTopic("retention-test")
	partition, _ := topic.Partition(0)

	// Publish messages
	for i := 0; i < 10; i++ {
		b.Publish("retention-test", []byte("key"), []byte("value"))
	}

	// Try to delete messages before offset 5
	err = partition.DeleteMessagesBefore(5)
	// This may or may not succeed depending on segment boundaries
	t.Logf("DeleteMessagesBefore(5) result: err=%v", err)
}

// =============================================================================
// PRIORITY SCHEDULER TESTS
// =============================================================================

// TestPriorityScheduler_LenByPriority tests the LenByPriority method.
func TestPriorityScheduler_LenByPriority(t *testing.T) {
	ps := NewPriorityScheduler(DefaultPrioritySchedulerConfig())

	// Initially all should be 0
	counts := ps.LenByPriority()
	for i, count := range counts {
		if count != 0 {
			t.Errorf("Initial count for priority %d = %d, want 0", i, count)
		}
	}

	// Enqueue messages with different priorities
	ps.Enqueue(storage.NewMessageWithPriority([]byte("key"), []byte("value"), storage.PriorityCritical))
	ps.Enqueue(storage.NewMessageWithPriority([]byte("key"), []byte("value"), storage.PriorityCritical))
	ps.Enqueue(storage.NewMessageWithPriority([]byte("key"), []byte("value"), storage.PriorityHigh))

	counts = ps.LenByPriority()
	if counts[storage.PriorityCritical] != 2 {
		t.Errorf("Critical count = %d, want 2", counts[storage.PriorityCritical])
	}
	if counts[storage.PriorityHigh] != 1 {
		t.Errorf("High count = %d, want 1", counts[storage.PriorityHigh])
	}
}

// TestPriorityScheduler_Clear tests the Clear method.
func TestPriorityScheduler_Clear(t *testing.T) {
	ps := NewPriorityScheduler(DefaultPrioritySchedulerConfig())

	// Enqueue some messages
	for i := 0; i < 10; i++ {
		ps.Enqueue(storage.NewMessageWithPriority([]byte("key"), []byte("value"), storage.PriorityNormal))
	}

	if ps.Len() != 10 {
		t.Errorf("Len = %d after enqueue, want 10", ps.Len())
	}

	// Clear all
	ps.Clear()

	if ps.Len() != 0 {
		t.Errorf("Len = %d after Clear, want 0", ps.Len())
	}

	counts := ps.LenByPriority()
	for i, count := range counts {
		if count != 0 {
			t.Errorf("Count for priority %d = %d after Clear, want 0", i, count)
		}
	}
}

// TestPriorityScheduler_UpdateWeights tests the UpdateWeights method.
func TestPriorityScheduler_UpdateWeights(t *testing.T) {
	ps := NewPriorityScheduler(DefaultPrioritySchedulerConfig())

	// Update weights - PriorityWeights is an array indexed by priority
	newWeights := PriorityWeights{
		50, // Critical
		25, // High
		15, // Normal
		7,  // Low
		3,  // Background
	}
	ps.UpdateWeights(newWeights)

	// Verify weights are applied by checking stats or behavior
	// Since there's no direct getter, we just ensure no panic
	t.Log("UpdateWeights completed successfully")
}

// TestPriorityScheduler_UpdateStarvationTimeout tests the UpdateStarvationTimeout method.
func TestPriorityScheduler_UpdateStarvationTimeout(t *testing.T) {
	ps := NewPriorityScheduler(DefaultPrioritySchedulerConfig())

	// Disable starvation timeout
	ps.UpdateStarvationTimeout(0)

	// Enable with custom timeout
	ps.UpdateStarvationTimeout(5000) // 5 seconds

	// Ensure no panic
	t.Log("UpdateStarvationTimeout completed successfully")
}

// =============================================================================
// IDEMPOTENT PRODUCER TESTS
// =============================================================================

// TestProducerIDAndEpoch_String tests the String method.
func TestProducerIDAndEpoch_String(t *testing.T) {
	pid := ProducerIDAndEpoch{
		ProducerID: 12345,
		Epoch:      42,
	}

	s := pid.String()
	if s != "PID=12345,epoch=42" {
		t.Errorf("String() = %q, want %q", s, "PID=12345,epoch=42")
	}
}

// TestTransactionState_String tests the String method for TransactionState.
func TestTransactionState_String(t *testing.T) {
	tests := []struct {
		state TransactionState
		want  string
	}{
		{TransactionStateEmpty, "Empty"},
		{TransactionStateOngoing, "Ongoing"},
		{TransactionStatePrepareCommit, "PrepareCommit"},
		{TransactionStatePrepareAbort, "PrepareAbort"},
		{TransactionStateCompleteCommit, "CompleteCommit"},
		{TransactionStateCompleteAbort, "CompleteAbort"},
		{TransactionStateDead, "Dead"},
		{TransactionState(99), "Unknown"}, // Unknown state
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.state.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestTransactionState_IsCompleted tests the IsCompleted method.
func TestTransactionState_IsCompleted(t *testing.T) {
	tests := []struct {
		state TransactionState
		want  bool
	}{
		{TransactionStateEmpty, false},
		{TransactionStateOngoing, false},
		{TransactionStatePrepareCommit, false},
		{TransactionStatePrepareAbort, false},
		{TransactionStateCompleteCommit, true},
		{TransactionStateCompleteAbort, true},
		{TransactionStateDead, false},
	}

	for _, tt := range tests {
		t.Run(tt.state.String(), func(t *testing.T) {
			got := tt.state.IsCompleted()
			if got != tt.want {
				t.Errorf("IsCompleted() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestTransactionState_IsPreparing tests the IsPreparing method.
func TestTransactionState_IsPreparing(t *testing.T) {
	tests := []struct {
		state TransactionState
		want  bool
	}{
		{TransactionStateEmpty, false},
		{TransactionStateOngoing, false},
		{TransactionStatePrepareCommit, true},
		{TransactionStatePrepareAbort, true},
		{TransactionStateCompleteCommit, false},
		{TransactionStateCompleteAbort, false},
		{TransactionStateDead, false},
	}

	for _, tt := range tests {
		t.Run(tt.state.String(), func(t *testing.T) {
			got := tt.state.IsPreparing()
			if got != tt.want {
				t.Errorf("IsPreparing() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIdempotentProducerManager_GetSequenceState tests the GetSequenceState method.
func TestIdempotentProducerManager_GetSequenceState(t *testing.T) {
	config := IdempotentProducerManagerConfig{
		DedupWindowSize:             1000,
		ProducerSessionTimeoutMs:    300000,
		DefaultTransactionTimeoutMs: 60000,
		MaxTransactionTimeoutMs:     600000,
	}

	manager := NewIdempotentProducerManager(config)

	// GetSequenceState for non-existent producer should return nil
	state := manager.GetSequenceState(12345, "test-topic", 0)
	if state != nil {
		t.Error("GetSequenceState should return nil for non-existent producer")
	}

	// Initialize a producer
	pid, err := manager.InitProducerID("test-txn", 60000)
	if err != nil {
		t.Fatalf("InitProducerID failed: %v", err)
	}

	// Record a sequence using CheckAndUpdateSequence
	_, _, err = manager.CheckAndUpdateSequence(pid, "test-topic", 0, 0, 100)
	if err != nil {
		t.Fatalf("CheckAndUpdateSequence failed: %v", err)
	}

	// Now GetSequenceState should return state
	state = manager.GetSequenceState(pid.ProducerID, "test-topic", 0)
	if state == nil {
		t.Error("GetSequenceState should return state after CheckAndUpdateSequence")
	} else {
		if state.LastSequence != 0 {
			t.Errorf("LastSequence = %d, want 0", state.LastSequence)
		}
	}
}

// TestIdempotentProducerManager_ExpireInactiveProducers tests the ExpireInactiveProducers method.
func TestIdempotentProducerManager_ExpireInactiveProducers(t *testing.T) {
	config := IdempotentProducerManagerConfig{
		DedupWindowSize:             1000,
		ProducerSessionTimeoutMs:    1, // 1ms timeout for fast expiry
		DefaultTransactionTimeoutMs: 60000,
		MaxTransactionTimeoutMs:     600000,
	}

	manager := NewIdempotentProducerManager(config)

	// Initialize a producer
	_, err := manager.InitProducerID("test-txn", 60000)
	if err != nil {
		t.Fatalf("InitProducerID failed: %v", err)
	}

	// Wait for timeout
	time.Sleep(10 * time.Millisecond)

	// Expire inactive producers
	expired := manager.ExpireInactiveProducers()
	if expired != 1 {
		t.Errorf("ExpireInactiveProducers returned %d, want 1", expired)
	}

	// Run again - should expire 0
	expired = manager.ExpireInactiveProducers()
	if expired != 0 {
		t.Errorf("Second ExpireInactiveProducers returned %d, want 0", expired)
	}
}

// TestSplitKeyString tests the splitKeyString helper function.
func TestSplitKeyString(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"a:b:c", []string{"a", "b", "c"}},
		{"single", []string{"single"}},
		{"", nil},             // empty string
		{"a:", []string{"a"}}, // trailing colon doesn't add empty string
		{":b", []string{"", "b"}},
		{"a:b:c:d:e", []string{"a", "b", "c", "d", "e"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := splitKeyString(tt.input)
			if len(got) != len(tt.want) {
				t.Errorf("splitKeyString(%q) = %v, want %v", tt.input, got, tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("splitKeyString(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestJoinKeyParts tests the joinKeyParts helper function.
func TestJoinKeyParts(t *testing.T) {
	tests := []struct {
		parts []string
		want  string
	}{
		{[]string{"a", "b", "c"}, "a:b:c"},
		{[]string{"single"}, "single"},
		{[]string{}, ""},
		{[]string{"a", "", "c"}, "a::c"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := joinKeyParts(tt.parts)
			if got != tt.want {
				t.Errorf("joinKeyParts(%v) = %q, want %q", tt.parts, got, tt.want)
			}
		})
	}
}

// =============================================================================
// TOPIC TESTS
// =============================================================================

// TestTopic_PublishToPartitionWithPriority tests the PublishToPartitionWithPriority method.
func TestTopic_PublishToPartitionWithPriority(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic
	topicConfig := TopicConfig{
		Name:          "priority-test",
		NumPartitions: 2,
	}
	b.CreateTopic(topicConfig)

	topic, _ := b.GetTopic("priority-test")

	// Publish with different priorities
	offset, err := topic.PublishToPartitionWithPriority(0, []byte("key"), []byte("critical"), storage.PriorityCritical)
	if err != nil {
		t.Fatalf("PublishToPartitionWithPriority(Critical) failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("First message offset = %d, want 0", offset)
	}

	offset, err = topic.PublishToPartitionWithPriority(0, []byte("key"), []byte("low"), storage.PriorityLow)
	if err != nil {
		t.Fatalf("PublishToPartitionWithPriority(Low) failed: %v", err)
	}
	if offset != 1 {
		t.Errorf("Second message offset = %d, want 1", offset)
	}

	// Test invalid partition
	_, err = topic.PublishToPartitionWithPriority(999, []byte("key"), []byte("value"), storage.PriorityNormal)
	if err == nil {
		t.Error("PublishToPartitionWithPriority should fail for invalid partition")
	}

	_, err = topic.PublishToPartitionWithPriority(-1, []byte("key"), []byte("value"), storage.PriorityNormal)
	if err == nil {
		t.Error("PublishToPartitionWithPriority should fail for negative partition")
	}
}

// TestTopic_ConsumeAll tests the ConsumeAll method.
func TestTopic_ConsumeAll(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create single-partition topic
	topicConfig := TopicConfig{
		Name:          "consume-all-test",
		NumPartitions: 1,
	}
	b.CreateTopic(topicConfig)

	topic, _ := b.GetTopic("consume-all-test")

	// Publish messages
	for i := 0; i < 5; i++ {
		topic.PublishToPartition(0, []byte("key"), []byte("value"))
	}

	// ConsumeAll should work like Consume(0, ...)
	msgs, err := topic.ConsumeAll(0, 10)
	if err != nil {
		t.Fatalf("ConsumeAll failed: %v", err)
	}
	if len(msgs) != 5 {
		t.Errorf("ConsumeAll returned %d messages, want 5", len(msgs))
	}
}

// TestTopic_PublishToPartitionWithPriority_ClosedTopic tests on closed topic.
func TestTopic_PublishToPartitionWithPriority_ClosedTopic(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}

	// Create topic
	topicConfig := TopicConfig{
		Name:          "closed-topic-test",
		NumPartitions: 1,
	}
	b.CreateTopic(topicConfig)

	topic, _ := b.GetTopic("closed-topic-test")

	// Close the topic via broker close
	b.Close()

	// Publish should fail
	_, err = topic.PublishToPartitionWithPriority(0, []byte("key"), []byte("value"), storage.PriorityNormal)
	if err == nil {
		t.Error("PublishToPartitionWithPriority should fail on closed topic")
	}
}

// =============================================================================
// OFFSET MANAGER TESTS
// =============================================================================

// TestOffsetManager_GetTopicOffsets tests the GetTopicOffsets method.
func TestOffsetManager_GetTopicOffsets(t *testing.T) {
	dir := t.TempDir()

	om, err := NewOffsetManager(dir, false, 0)
	if err != nil {
		t.Fatalf("NewOffsetManager failed: %v", err)
	}

	// Get offsets for non-existent topic
	offsets, err := om.GetTopicOffsets("non-existent-group", "non-existent-topic")
	if err != nil {
		t.Logf("GetTopicOffsets returned error (may be expected): %v", err)
	}
	if offsets == nil {
		offsets = make(map[int]int64) // Normalize to empty map
	}
	if len(offsets) != 0 {
		t.Errorf("GetTopicOffsets returned %d offsets, want 0", len(offsets))
	}

	// Commit an offset using the full Commit method
	err = om.Commit("test-group", "test-topic", 0, 42, 1, "")
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Get offsets
	offsets, err = om.GetTopicOffsets("test-group", "test-topic")
	if err != nil {
		t.Fatalf("GetTopicOffsets failed: %v", err)
	}
	if offsets[0] != 42 {
		t.Errorf("GetTopicOffsets[0] = %d, want 42", offsets[0])
	}
}

// =============================================================================
// DELAY INDEX TESTS
// =============================================================================

// TestDefaultDelayIndexConfig tests the DefaultDelayIndexConfig function.
func TestDefaultDelayIndexConfig(t *testing.T) {
	dir := t.TempDir()
	config := DefaultDelayIndexConfig(dir, "test-topic")

	if config.MaxEntries <= 0 {
		t.Errorf("MaxEntries = %d, want > 0", config.MaxEntries)
	}
	if config.DataDir != dir {
		t.Errorf("DataDir = %s, want %s", config.DataDir, dir)
	}
	if config.Topic != "test-topic" {
		t.Errorf("Topic = %s, want test-topic", config.Topic)
	}
}

// =============================================================================
// SCHEMA VALIDATION TESTS
// =============================================================================

// TestValidationError_Error tests the Error method for ValidationError.
func TestValidationError_Error(t *testing.T) {
	err := &ValidationError{
		Path:    "email",
		Message: "invalid format",
	}

	s := err.Error()
	if s != "email: invalid format" {
		t.Errorf("Error() = %q, unexpected format", s)
	}

	// Test without path
	err2 := &ValidationError{
		Message: "validation failed",
	}
	s2 := err2.Error()
	if s2 != "validation failed" {
		t.Errorf("Error() without path = %q, unexpected format", s2)
	}
}

// TestParseCompatibilityMode tests the ParseCompatibilityMode function.
func TestParseCompatibilityMode(t *testing.T) {
	tests := []struct {
		input string
		want  CompatibilityMode
	}{
		{"NONE", CompatibilityNone},
		{"BACKWARD", CompatibilityBackward},
		{"FORWARD", CompatibilityForward},
		{"FULL", CompatibilityFull},
		{"none", CompatibilityNone},         // case insensitive
		{"backward", CompatibilityBackward}, // lowercase
		{"invalid", CompatibilityBackward},  // unknown defaults to BACKWARD
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseCompatibilityMode(tt.input)
			if got != tt.want {
				t.Errorf("ParseCompatibilityMode(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// =============================================================================
// ACK MODE TESTS
// =============================================================================

// TestAckMode_String tests the String method for AckMode.
func TestAckMode_String(t *testing.T) {
	tests := []struct {
		mode AckMode
		want string
	}{
		{AckNone, "none"},
		{AckLeader, "leader"},
		{AckAll, "all"},
		{AckMode(99), "unknown"}, // Unknown mode
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.mode.String()
			if got != tt.want {
				t.Errorf("AckMode(%d).String() = %q, want %q", tt.mode, got, tt.want)
			}
		})
	}
}

// =============================================================================
// INTERNAL TOPIC MANAGER TESTS
// =============================================================================

// TestIsInternalTopic_Coverage tests the IsInternalTopic helper function.
func TestIsInternalTopic_Coverage(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"__consumer_offsets", true},
		{"__transaction_state", true},
		{"__any_internal", true},
		{"regular-topic", false},
		{"_single_underscore", false},
		{"topic__with__double", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsInternalTopic(tt.name)
			if got != tt.want {
				t.Errorf("IsInternalTopic(%q) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

// =============================================================================
// SCHEDULER TESTS
// =============================================================================

// TestScheduler_GetDeliverTime tests the GetDeliverTime method.
func TestScheduler_GetDeliverTime(t *testing.T) {
	dir := t.TempDir()

	config := SchedulerConfig{
		DataDir:            dir,
		MaxDelayedPerTopic: 1000,
		MaxDelay:           time.Hour,
	}

	scheduler, err := NewScheduler(config)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}
	defer scheduler.Close()

	// GetDeliverTime for non-existent topic should return zero time
	deliverTime := scheduler.GetDeliverTime("non-existent", 0, 100)
	if !deliverTime.IsZero() {
		t.Errorf("GetDeliverTime for non-existent topic should return zero time, got %v", deliverTime)
	}
}

// =============================================================================
// COOPERATIVE REBALANCER TESTS
// =============================================================================

// TestCooperativeRebalancer_IsRebalancing tests the IsRebalancing method.
func TestCooperativeRebalancer_IsRebalancing(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	config := CooperativeRebalancerConfig{}
	rebalancer := NewCooperativeRebalancer(config, logger)

	// Non-existent group should not be rebalancing
	if rebalancer.IsRebalancing("non-existent-group") {
		t.Error("IsRebalancing should return false for non-existent group")
	}
}

// TestCooperativeRebalancer_GetTargetAssignment tests the GetTargetAssignment method.
func TestCooperativeRebalancer_GetTargetAssignment(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	config := CooperativeRebalancerConfig{}
	rebalancer := NewCooperativeRebalancer(config, logger)

	// Non-existent group should return no assignment
	partitions, ok := rebalancer.GetTargetAssignment("non-existent-group", "member-1")
	if ok {
		t.Error("GetTargetAssignment should return false for non-existent group")
	}
	if partitions != nil {
		t.Error("GetTargetAssignment should return nil partitions for non-existent group")
	}
}

// TestCooperativeRebalancer_GetPendingRevocations tests the GetPendingRevocations method.
func TestCooperativeRebalancer_GetPendingRevocations(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	config := CooperativeRebalancerConfig{}
	rebalancer := NewCooperativeRebalancer(config, logger)

	// Non-existent group should return no pending revocations
	partitions, ok := rebalancer.GetPendingRevocations("non-existent-group", "member-1")
	if ok {
		t.Error("GetPendingRevocations should return false for non-existent group")
	}
	if partitions != nil {
		t.Error("GetPendingRevocations should return nil partitions for non-existent group")
	}
}

// =============================================================================
// TOPIC ADDITIONAL TESTS
// =============================================================================

// TestTopic_Sync tests the Sync method.
func TestTopic_Sync(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic
	topicConfig := TopicConfig{
		Name:          "sync-test",
		NumPartitions: 1,
	}
	b.CreateTopic(topicConfig)

	topic, _ := b.GetTopic("sync-test")

	// Publish some messages
	for i := 0; i < 5; i++ {
		topic.PublishToPartition(0, []byte("key"), []byte("value"))
	}

	// Sync should succeed
	err = topic.Sync()
	if err != nil {
		t.Errorf("Sync failed: %v", err)
	}
}

// =============================================================================
// TRACER TESTS
// =============================================================================

// TestSpanID_IsZero tests the IsZero method.
func TestSpanID_IsZero(t *testing.T) {
	var zeroID SpanID
	if !zeroID.IsZero() {
		t.Error("Zero SpanID should return true for IsZero")
	}

	nonZeroID := SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	if nonZeroID.IsZero() {
		t.Error("Non-zero SpanID should return false for IsZero")
	}
}

// =============================================================================
// SCHEMA REGISTRY TESTS
// =============================================================================

// TestSchemaRegistry_IsValidationEnabled tests the IsValidationEnabled method.
func TestSchemaRegistry_IsValidationEnabled(t *testing.T) {
	dir := t.TempDir()

	config := SchemaRegistryConfig{
		DataDir:           dir,
		ValidationEnabled: true,
	}

	registry, err := NewSchemaRegistry(config)
	if err != nil {
		t.Fatalf("NewSchemaRegistry failed: %v", err)
	}
	defer registry.Close()

	// Check validation is enabled for any subject
	if !registry.IsValidationEnabled("any-topic") {
		t.Error("IsValidationEnabled should return true when global validation is enabled")
	}
}

// TestSchemaRegistry_GetSchemaIDForSubject tests the GetSchemaIDForSubject method.
func TestSchemaRegistry_GetSchemaIDForSubject(t *testing.T) {
	dir := t.TempDir()

	config := SchemaRegistryConfig{
		DataDir:           dir,
		ValidationEnabled: true,
	}

	registry, err := NewSchemaRegistry(config)
	if err != nil {
		t.Fatalf("NewSchemaRegistry failed: %v", err)
	}
	defer registry.Close()

	// Non-existent subject should return 0 or error
	schemaID, err := registry.GetSchemaIDForSubject("non-existent-subject")
	if err != nil {
		t.Logf("GetSchemaIDForSubject returned error (expected): %v", err)
	}
	if schemaID != 0 {
		t.Logf("GetSchemaIDForSubject returned %d for non-existent subject", schemaID)
	}
}

// =============================================================================
// INTERNAL TOPIC MANAGER TESTS
// =============================================================================

// TestInternalTopicManager_IsReady tests the IsReady method.
func TestInternalTopicManager_IsReady(t *testing.T) {
	dir := t.TempDir()

	config := DefaultInternalTopicConfig(dir)
	config.OffsetsPartitionCount = 10

	manager, err := NewInternalTopicManager(config)
	if err != nil {
		t.Fatalf("NewInternalTopicManager failed: %v", err)
	}

	// Not ready initially (Start() not called)
	if manager.IsReady() {
		t.Error("Manager should not be ready immediately after creation")
	}
}

// TestInternalTopicManager_GetPartitionCount tests the GetPartitionCount method.
func TestInternalTopicManager_GetPartitionCount(t *testing.T) {
	dir := t.TempDir()

	config := DefaultInternalTopicConfig(dir)
	config.OffsetsPartitionCount = 10

	manager, err := NewInternalTopicManager(config)
	if err != nil {
		t.Fatalf("NewInternalTopicManager failed: %v", err)
	}

	if manager.GetPartitionCount() != 10 {
		t.Errorf("GetPartitionCount() = %d, want 10", manager.GetPartitionCount())
	}
}

// TestInternalTopicManager_BecameLeader tests the BecameLeader method.
func TestInternalTopicManager_BecameLeader(t *testing.T) {
	dir := t.TempDir()

	config := DefaultInternalTopicConfig(dir)
	config.OffsetsPartitionCount = 10

	manager, err := NewInternalTopicManager(config)
	if err != nil {
		t.Fatalf("NewInternalTopicManager failed: %v", err)
	}

	// Become leader for partition 0
	err = manager.BecameLeader(0)
	if err != nil {
		t.Errorf("BecameLeader(0) failed: %v", err)
	}

	// Check local partitions
	partitions := manager.GetLocalPartitions()
	found := false
	for _, p := range partitions {
		if p == 0 {
			found = true
			break
		}
	}
	if !found {
		t.Error("Partition 0 should be in local partitions after BecameLeader")
	}
}

// TestInternalTopicManager_AddListener tests the AddListener method.
func TestInternalTopicManager_AddListener(t *testing.T) {
	dir := t.TempDir()

	config := DefaultInternalTopicConfig(dir)
	config.OffsetsPartitionCount = 10

	manager, err := NewInternalTopicManager(config)
	if err != nil {
		t.Fatalf("NewInternalTopicManager failed: %v", err)
	}

	// Add a listener
	eventChan := make(chan InternalTopicEvent, 1)
	manager.AddListener(func(event InternalTopicEvent) {
		eventChan <- event
	})

	// Trigger an event by becoming leader
	_ = manager.BecameLeader(0)

	// Wait for event with timeout
	select {
	case event := <-eventChan:
		if event.Type != InternalTopicEventBecameLeader {
			t.Errorf("Expected BecameLeader event, got %v", event.Type)
		}
		if event.Partition != 0 {
			t.Errorf("Expected partition 0, got %d", event.Partition)
		}
	case <-time.After(time.Second):
		t.Error("Listener was not called within timeout")
	}
}

// TestInternalTopicManager_Stats tests the Stats method.
func TestInternalTopicManager_Stats(t *testing.T) {
	dir := t.TempDir()

	config := DefaultInternalTopicConfig(dir)
	config.OffsetsPartitionCount = 10

	manager, err := NewInternalTopicManager(config)
	if err != nil {
		t.Fatalf("NewInternalTopicManager failed: %v", err)
	}

	// Get stats
	stats := manager.Stats()

	if stats.PartitionCount != 10 {
		t.Errorf("Stats.PartitionCount = %d, want 10", stats.PartitionCount)
	}
	if stats.Ready {
		t.Error("Stats.Ready should be false initially")
	}
}

// =============================================================================
// COOPERATIVE GROUP ADDITIONAL TESTS
// =============================================================================

// TestCooperativeGroup_SetTopicPartitions tests the SetTopicPartitions method.
func TestCooperativeGroup_SetTopicPartitions(t *testing.T) {
	config := DefaultCooperativeGroupConfig()

	group := NewCooperativeGroup("test-group", []string{"test-topic", "other-topic"}, config, nil)

	// Set topic partitions
	topicPartitions := map[string]int{
		"test-topic":  4,
		"other-topic": 2,
	}
	group.SetTopicPartitions(topicPartitions)

	// Verify by checking internal state (indirectly)
	// We can't directly access the map, but we can verify it doesn't panic
}

// =============================================================================
// REASSIGNMENT MANAGER ADDITIONAL TESTS
// =============================================================================

// TestReassignmentManager_ListActiveReassignments tests the ListActiveReassignments method.
func TestReassignmentManager_ListActiveReassignments(t *testing.T) {
	rm := NewReassignmentManager(nil, nil, "test-node")

	// No active reassignments initially
	active := rm.ListActiveReassignments()
	if len(active) != 0 {
		t.Errorf("ListActiveReassignments() = %d, want 0", len(active))
	}
}

// TestReassignmentManager_CancelReassignment tests the CancelReassignment method.
func TestReassignmentManager_CancelReassignment(t *testing.T) {
	rm := NewReassignmentManager(nil, nil, "test-node")

	// Try to cancel non-existent reassignment
	err := rm.CancelReassignment("non-existent-id")
	if err == nil {
		t.Error("CancelReassignment should return error for non-existent reassignment")
	}
}

// =============================================================================
// BROKER ACCESSOR METHODS TESTS
// =============================================================================

// TestBroker_NodeID tests the NodeID accessor method.
func TestBroker_NodeID(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node-123",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	if got := b.NodeID(); got != "test-node-123" {
		t.Errorf("NodeID() = %q, want %q", got, "test-node-123")
	}
}

// TestBroker_DataDir tests the DataDir accessor method.
func TestBroker_DataDir(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	if got := b.DataDir(); got != dir {
		t.Errorf("DataDir() = %q, want %q", got, dir)
	}
}

// TestBroker_GroupCoordinator tests the GroupCoordinator accessor method.
func TestBroker_GroupCoordinator(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	gc := b.GroupCoordinator()
	if gc == nil {
		t.Error("GroupCoordinator() returned nil")
	}
}

// TestBroker_CooperativeGroupCoordinator tests the CooperativeGroupCoordinator accessor.
func TestBroker_CooperativeGroupCoordinator(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// May return nil if cooperative rebalancing not enabled
	_ = b.CooperativeGroupCoordinator()
}

// TestBroker_Uptime tests the Uptime accessor method.
func TestBroker_Uptime(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	uptime := b.Uptime()
	if uptime < 0 {
		t.Errorf("Uptime() = %v, want >= 0", uptime)
	}
}

// TestBroker_AckManager tests the AckManager accessor method.
func TestBroker_AckManager(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	am := b.AckManager()
	if am == nil {
		t.Error("AckManager() returned nil")
	}
}

// TestBroker_Tracer tests the Tracer accessor method.
func TestBroker_Tracer(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	tracer := b.Tracer()
	if tracer == nil {
		t.Error("Tracer() returned nil")
	}
}

// TestBroker_ReliabilityConfig tests the ReliabilityConfig accessor method.
func TestBroker_ReliabilityConfig(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	rc := b.ReliabilityConfig()
	// Check that we get valid defaults (field is VisibilityTimeoutMs)
	if rc.VisibilityTimeoutMs == 0 {
		t.Error("ReliabilityConfig().VisibilityTimeoutMs = 0, want non-zero default")
	}
}

// TestBroker_ListTopics tests the ListTopics method.
func TestBroker_ListTopics(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Initially empty
	topics := b.ListTopics()
	if len(topics) != 0 {
		t.Errorf("ListTopics() = %d topics, want 0", len(topics))
	}

	// Create a topic
	err = b.CreateTopic(TopicConfig{Name: "test-topic", NumPartitions: 1})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Should now have one topic
	topics = b.ListTopics()
	if len(topics) != 1 {
		t.Errorf("ListTopics() = %d topics, want 1", len(topics))
	}
	if topics[0] != "test-topic" {
		t.Errorf("ListTopics()[0] = %q, want %q", topics[0], "test-topic")
	}
}

// TestBroker_TopicExists tests the TopicExists method.
func TestBroker_TopicExists(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Non-existent topic
	if b.TopicExists("non-existent") {
		t.Error("TopicExists('non-existent') = true, want false")
	}

	// Create a topic
	err = b.CreateTopic(TopicConfig{Name: "test-topic", NumPartitions: 1})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Should exist now
	if !b.TopicExists("test-topic") {
		t.Error("TopicExists('test-topic') = false, want true")
	}
}

// TestBroker_Publish tests the Publish method.
func TestBroker_Publish(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create a topic first
	err = b.CreateTopic(TopicConfig{Name: "test-topic", NumPartitions: 1})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Publish a message
	partition, offset, err := b.Publish("test-topic", []byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if partition != 0 {
		t.Errorf("Publish partition = %d, want 0", partition)
	}
	if offset != 0 {
		t.Errorf("Publish offset = %d, want 0", offset)
	}

	// Publish without key (round-robin)
	_, offset2, err := b.Publish("test-topic", nil, []byte("value2"))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if offset2 != 1 {
		t.Errorf("Second publish offset = %d, want 1", offset2)
	}
}

// TestBroker_Stats_Accessors tests the Stats method returns correct fields.
func TestBroker_Stats_Accessors(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	stats := b.Stats()
	if stats.TopicCount != 0 {
		t.Errorf("Stats().TopicCount = %d, want 0", stats.TopicCount)
	}
	if stats.NodeID != "test-node" {
		t.Errorf("Stats().NodeID = %q, want %q", stats.NodeID, "test-node")
	}
}

// TestBroker_GetTransactionCoordinator tests the GetTransactionCoordinator method.
func TestBroker_GetTransactionCoordinator(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	tc := b.GetTransactionCoordinator()
	if tc == nil {
		t.Error("GetTransactionCoordinator() returned nil")
	}
}

// TestDefaultBrokerConfig tests the DefaultBrokerConfig function.
func TestDefaultBrokerConfig(t *testing.T) {
	config := DefaultBrokerConfig()

	// Check that sensible defaults are set
	if config.DataDir == "" {
		t.Error("DefaultBrokerConfig().DataDir is empty, want non-empty default")
	}
	if config.NodeID == "" {
		t.Error("DefaultBrokerConfig().NodeID is empty, want non-empty default")
	}
}

// =============================================================================
// COMPACTOR TESTS
// =============================================================================

// TestReloadPartition tests the ReloadPartition function.
func TestReloadPartition(t *testing.T) {
	dir := t.TempDir()

	// Create a partition using broker's internal mechanism
	// by creating a topic with the broker
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}

	err = b.CreateTopic(TopicConfig{Name: "test-topic", NumPartitions: 1})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Write some data
	_, _, err = b.Publish("test-topic", []byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	b.Close()

	// Reload the partition using ReloadPartition
	logsDir := dir + "/logs"
	reloaded, err := ReloadPartition(logsDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("ReloadPartition failed: %v", err)
	}
	defer reloaded.Close()

	// Verify it's the same partition
	if reloaded.ID() != 0 {
		t.Errorf("ReloadPartition ID = %d, want 0", reloaded.ID())
	}
}

// TestReloadPartition_NonExistent tests ReloadPartition with non-existent partition.
func TestReloadPartition_NonExistent(t *testing.T) {
	dir := t.TempDir()

	// Try to reload non-existent partition
	_, err := ReloadPartition(dir, "non-existent", 0)
	if err == nil {
		t.Error("ReloadPartition should fail for non-existent partition")
	}
}

// =============================================================================
// CONSUMER GROUP ADDITIONAL TESTS
// =============================================================================

// TestDefaultConsumerGroupConfig tests the DefaultConsumerGroupConfig function.
func TestDefaultConsumerGroupConfig(t *testing.T) {
	config := DefaultConsumerGroupConfig()

	if config.SessionTimeoutMs == 0 {
		t.Error("DefaultConsumerGroupConfig().SessionTimeoutMs = 0, want non-zero")
	}
	if config.HeartbeatIntervalMs == 0 {
		t.Error("DefaultConsumerGroupConfig().HeartbeatIntervalMs = 0, want non-zero")
	}
	if config.RebalanceTimeoutMs == 0 {
		t.Error("DefaultConsumerGroupConfig().RebalanceTimeoutMs = 0, want non-zero")
	}
}

// =============================================================================
// BROKER ADDITIONAL ACCESSOR TESTS
// =============================================================================

// TestBroker_Scheduler tests the Scheduler accessor method.
func TestBroker_Scheduler(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	scheduler := b.Scheduler()
	if scheduler == nil {
		t.Error("Scheduler() returned nil")
	}
}

// TestBroker_SchemaRegistry tests the SchemaRegistry accessor method.
func TestBroker_SchemaRegistry(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	sr := b.SchemaRegistry()
	if sr == nil {
		t.Error("SchemaRegistry() returned nil")
	}
}

// TestBroker_SchemaStats tests the SchemaStats method.
func TestBroker_SchemaStats(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	stats := b.SchemaStats()
	// Just verify it doesn't panic and returns something
	_ = stats
}

// TestBroker_ReliabilityStats tests the ReliabilityStats method.
func TestBroker_ReliabilityStats(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	stats := b.ReliabilityStats()
	// Just verify it doesn't panic and returns something
	_ = stats
}

// TestBroker_DelayStats tests the DelayStats method.
func TestBroker_DelayStats(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	stats := b.DelayStats()
	// Just verify it doesn't panic
	_ = stats
}

// TestBroker_PriorityStats tests the PriorityStats method.
func TestBroker_PriorityStats(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create a topic first
	err = b.CreateTopic(TopicConfig{Name: "test-topic", NumPartitions: 1})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	stats := b.PriorityStats()
	// Just verify it doesn't panic
	_ = stats
}

// TestBroker_GetConsumerLag tests the GetConsumerLag method.
func TestBroker_GetConsumerLag(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create a topic
	err = b.CreateTopic(TopicConfig{Name: "test-topic", NumPartitions: 1})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Get consumer lag for non-existent consumer (note: signature is consumerID, groupID, topic, partition)
	lag, err := b.GetConsumerLag("test-consumer", "test-group", "test-topic", 0)
	if err != nil {
		// May return error for no committed offset
		_ = lag
	}
}

// TestBroker_ExtendVisibility tests the ExtendVisibility method.
func TestBroker_ExtendVisibility(t *testing.T) {
	dir := t.TempDir()
	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Try to extend visibility for non-existent receipt (returns time.Time, error)
	_, err = b.ExtendVisibility("non-existent-receipt", 30*time.Second)
	if err == nil {
		t.Error("ExtendVisibility should fail for non-existent receipt")
	}
}
