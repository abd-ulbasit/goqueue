// =============================================================================
// VISIBILITY TRACKER TESTS
// =============================================================================
//
// These tests verify the visibility timeout mechanism that prevents message
// loss when consumers crash mid-processing.
//
// TEST CATEGORIES:
//   1. Basic operations: Track, Untrack, ExtendVisibility
//   2. Heap ordering: Messages expire in correct order
//   3. Timeout behavior: Callback fires on expiration
//   4. Concurrent access: Thread safety under load
//   5. Edge cases: Empty heap, double track, etc.
//
// =============================================================================

package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// TEST HELPERS
// =============================================================================

// testReliabilityConfig returns a ReliabilityConfig suitable for testing.
// Uses short timeouts to make tests fast.
func testReliabilityConfig() ReliabilityConfig {
	return ReliabilityConfig{
		VisibilityTimeoutMs:       100, // 100ms for fast tests
		MaxRetries:                3,
		DLQSuffix:                 ".dlq",
		DLQEnabled:                true,
		BackoffBaseMs:             10, // 10ms base backoff
		BackoffMultiplier:         2.0,
		VisibilityCheckIntervalMs: 10, // Check every 10ms
		MaxInFlightPerConsumer:    100,
	}
}

// createTestInFlightMessage creates a test InFlightMessage.
func createTestInFlightMessage(topic string, partition int, offset int64, visibilityTimeout time.Duration) *InFlightMessage {
	receiptHandle := GenerateReceiptHandle(topic, partition, offset, 1)
	return &InFlightMessage{
		ReceiptHandle:      receiptHandle,
		MessageID:          MessageID(topic, partition, offset),
		Topic:              topic,
		Partition:          partition,
		Offset:             offset,
		ConsumerID:         "test-consumer",
		GroupID:            "test-group",
		DeliveryCount:      1,
		MaxRetries:         3,
		State:              StateInFlight,
		OriginalKey:        []byte("test-key"),
		OriginalValue:      []byte("test-value"),
		FirstDeliveryTime:  time.Now(),
		LastDeliveryTime:   time.Now(),
		VisibilityDeadline: time.Now().Add(visibilityTimeout),
	}
}

// =============================================================================
// BASIC OPERATIONS TESTS
// =============================================================================

func TestVisibilityTracker_TrackAndUntrack(t *testing.T) {
	// SCENARIO:
	// Track a message, verify it's tracked, then untrack it.
	// Verifies basic CRUD operations work correctly.

	expiredCount := int64(0)
	onExpired := func(msg *InFlightMessage) {
		atomic.AddInt64(&expiredCount, 1)
	}

	vt := NewVisibilityTracker(testReliabilityConfig(), onExpired)
	defer vt.Close()

	msg := createTestInFlightMessage("test-topic", 0, 42, 100*time.Millisecond)

	// Track the message
	err := vt.Track(msg)
	if err != nil {
		t.Fatalf("Track() failed: %v", err)
	}

	// Verify stats
	stats := vt.Stats()
	if stats.TotalTracked != 1 {
		t.Errorf("TotalTracked = %d, want 1", stats.TotalTracked)
	}
	if stats.CurrentInFlight != 1 {
		t.Errorf("CurrentInFlight = %d, want 1", stats.CurrentInFlight)
	}

	// Untrack the message
	retrieved, err := vt.Untrack(msg.ReceiptHandle)
	if err != nil {
		t.Fatalf("Untrack() failed: %v", err)
	}

	if retrieved.Offset != msg.Offset {
		t.Errorf("retrieved offset = %d, want %d", retrieved.Offset, msg.Offset)
	}

	// Verify stats after untrack
	stats = vt.Stats()
	if stats.CurrentInFlight != 0 {
		t.Errorf("CurrentInFlight after untrack = %d, want 0", stats.CurrentInFlight)
	}
}

func TestVisibilityTracker_TrackDuplicate(t *testing.T) {
	// SCENARIO:
	// Attempt to track the same message twice.
	// Should return an error (no duplicate tracking allowed).

	onExpired := func(msg *InFlightMessage) {}
	vt := NewVisibilityTracker(testReliabilityConfig(), onExpired)
	defer vt.Close()

	msg := createTestInFlightMessage("test-topic", 0, 42, 100*time.Millisecond)

	// First track should succeed
	err := vt.Track(msg)
	if err != nil {
		t.Fatalf("first Track() failed: %v", err)
	}

	// Second track should fail
	err = vt.Track(msg)
	if err == nil {
		t.Error("second Track() should have failed for duplicate message")
	}
}

func TestVisibilityTracker_UntrackNotFound(t *testing.T) {
	// SCENARIO:
	// Attempt to untrack a message that was never tracked.
	// Should return an error.

	onExpired := func(msg *InFlightMessage) {}
	vt := NewVisibilityTracker(testReliabilityConfig(), onExpired)
	defer vt.Close()

	_, err := vt.Untrack("nonexistent:0:999:1:xyz")
	if err == nil {
		t.Error("Untrack() should have failed for nonexistent message")
	}
}

// =============================================================================
// VISIBILITY TIMEOUT TESTS
// =============================================================================

func TestVisibilityTracker_TimeoutExpiration(t *testing.T) {
	// SCENARIO:
	// Track a message with short timeout, wait for expiration.
	// Verify the onExpired callback is called.

	expired := make(chan *InFlightMessage, 1)
	onExpired := func(msg *InFlightMessage) {
		expired <- msg
	}

	config := testReliabilityConfig()
	config.VisibilityCheckIntervalMs = 5 // Check every 5ms for faster tests

	vt := NewVisibilityTracker(config, onExpired)
	defer vt.Close()

	msg := createTestInFlightMessage("test-topic", 0, 42, 20*time.Millisecond)

	err := vt.Track(msg)
	if err != nil {
		t.Fatalf("Track() failed: %v", err)
	}

	// Wait for expiration
	select {
	case expiredMsg := <-expired:
		if expiredMsg.Offset != msg.Offset {
			t.Errorf("expired message offset = %d, want %d", expiredMsg.Offset, msg.Offset)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for message expiration")
	}

	// Verify stats
	stats := vt.Stats()
	if stats.TotalExpired != 1 {
		t.Errorf("TotalExpired = %d, want 1", stats.TotalExpired)
	}
}

func TestVisibilityTracker_NoExpirationIfUntracked(t *testing.T) {
	// SCENARIO:
	// Track a message, then untrack it before timeout.
	// Verify onExpired is NOT called.

	expiredCount := int64(0)
	onExpired := func(msg *InFlightMessage) {
		atomic.AddInt64(&expiredCount, 1)
	}

	config := testReliabilityConfig()
	config.VisibilityCheckIntervalMs = 5

	vt := NewVisibilityTracker(config, onExpired)
	defer vt.Close()

	msg := createTestInFlightMessage("test-topic", 0, 42, 50*time.Millisecond)

	vt.Track(msg)

	// Untrack before timeout
	time.Sleep(10 * time.Millisecond)
	vt.Untrack(msg.ReceiptHandle)

	// Wait past original timeout
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt64(&expiredCount) != 0 {
		t.Error("onExpired should not have been called after untrack")
	}
}

// =============================================================================
// EXTEND VISIBILITY TESTS
// =============================================================================

func TestVisibilityTracker_ExtendVisibility(t *testing.T) {
	// SCENARIO:
	// Track a message with short timeout, extend it, verify it doesn't expire early.

	expiredCount := int64(0)
	onExpired := func(msg *InFlightMessage) {
		atomic.AddInt64(&expiredCount, 1)
	}

	config := testReliabilityConfig()
	config.VisibilityCheckIntervalMs = 5

	vt := NewVisibilityTracker(config, onExpired)
	defer vt.Close()

	msg := createTestInFlightMessage("test-topic", 0, 42, 30*time.Millisecond)

	vt.Track(msg)

	// Wait 20ms (before timeout), then extend by 50ms
	time.Sleep(20 * time.Millisecond)
	newDeadline, err := vt.ExtendVisibility(msg.ReceiptHandle, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("ExtendVisibility() failed: %v", err)
	}

	// Verify new deadline is in the future
	if newDeadline.Before(time.Now()) {
		t.Error("new deadline should be in the future")
	}

	// Wait until original timeout would have expired
	time.Sleep(20 * time.Millisecond)

	// Should still be tracked (not expired yet)
	if atomic.LoadInt64(&expiredCount) != 0 {
		t.Error("message should not have expired yet after extension")
	}

	// Wait for extended timeout to expire
	time.Sleep(60 * time.Millisecond)

	if atomic.LoadInt64(&expiredCount) != 1 {
		t.Errorf("expiredCount = %d, want 1", atomic.LoadInt64(&expiredCount))
	}
}

// =============================================================================
// HEAP ORDERING TESTS
// =============================================================================

func TestVisibilityTracker_HeapOrdering(t *testing.T) {
	// SCENARIO:
	// Track multiple messages with different timeouts.
	// Verify they expire in the correct order (earliest first).

	var expiredOffsets []int64
	var mu sync.Mutex
	onExpired := func(msg *InFlightMessage) {
		mu.Lock()
		expiredOffsets = append(expiredOffsets, msg.Offset)
		mu.Unlock()
	}

	config := testReliabilityConfig()
	config.VisibilityCheckIntervalMs = 5

	vt := NewVisibilityTracker(config, onExpired)
	defer vt.Close()

	// Track messages with different timeouts
	// Offset 3 should expire first, then 1, then 2
	vt.Track(createTestInFlightMessage("topic", 0, 1, 40*time.Millisecond))
	vt.Track(createTestInFlightMessage("topic", 0, 2, 60*time.Millisecond))
	vt.Track(createTestInFlightMessage("topic", 0, 3, 20*time.Millisecond))

	// Wait for all to expire
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(expiredOffsets) != 3 {
		t.Fatalf("expected 3 expired messages, got %d", len(expiredOffsets))
	}

	// Verify order: 3, 1, 2 (based on timeout times)
	expected := []int64{3, 1, 2}
	for i, exp := range expected {
		if expiredOffsets[i] != exp {
			t.Errorf("expired[%d] = %d, want %d", i, expiredOffsets[i], exp)
		}
	}
}

// =============================================================================
// CONCURRENT ACCESS TESTS
// =============================================================================

func TestVisibilityTracker_ConcurrentTrackUntrack(t *testing.T) {
	// SCENARIO:
	// Multiple goroutines tracking and untracking messages simultaneously.
	// Verifies thread safety.

	onExpired := func(msg *InFlightMessage) {}
	vt := NewVisibilityTracker(testReliabilityConfig(), onExpired)
	defer vt.Close()

	const numGoroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				offset := int64(goroutineID*opsPerGoroutine + i)
				msg := createTestInFlightMessage("topic", goroutineID, offset, 100*time.Millisecond)

				err := vt.Track(msg)
				if err != nil {
					continue // May fail if duplicate
				}

				// Immediately untrack
				vt.Untrack(msg.ReceiptHandle)
			}
		}(g)
	}

	wg.Wait()

	// Verify all messages were cleaned up
	stats := vt.Stats()
	if stats.CurrentInFlight != 0 {
		t.Errorf("CurrentInFlight after concurrent ops = %d, want 0", stats.CurrentInFlight)
	}
}

// =============================================================================
// BENCHMARK
// =============================================================================

func BenchmarkVisibilityTracker_Track(b *testing.B) {
	onExpired := func(msg *InFlightMessage) {}
	vt := NewVisibilityTracker(testReliabilityConfig(), onExpired)
	defer vt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := createTestInFlightMessage("topic", 0, int64(i), 30*time.Second)
		vt.Track(msg)
	}
}

func BenchmarkVisibilityTracker_Untrack(b *testing.B) {
	onExpired := func(msg *InFlightMessage) {}
	vt := NewVisibilityTracker(testReliabilityConfig(), onExpired)
	defer vt.Close()

	// Pre-track messages
	msgs := make([]*InFlightMessage, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i] = createTestInFlightMessage("topic", 0, int64(i), 30*time.Second)
		vt.Track(msgs[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vt.Untrack(msgs[i].ReceiptHandle)
	}
}
