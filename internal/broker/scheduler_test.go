// =============================================================================
// SCHEDULER TESTS
// =============================================================================
//
// Tests for the delay scheduler that coordinates timer wheel, delay index,
// and broker integration.
//
// TEST CATEGORIES:
//   - Basic scheduling
//   - Cancellation
//   - Delivery callback
//   - Recovery
//   - Query operations
//   - Stats
//
// =============================================================================

package broker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func createTestScheduler(t *testing.T) (*Scheduler, string) {
	t.Helper()

	dir := t.TempDir()
	config := SchedulerConfig{
		DataDir:                dir,
		MaxDelayedPerTopic:     1000,
		MaxDelayedPerPartition: 100,
		MaxDelay:               24 * time.Hour,
	}

	scheduler, err := NewScheduler(config)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	return scheduler, dir
}

// =============================================================================
// BASIC SCHEDULING TESTS
// =============================================================================

func TestScheduler_Schedule(t *testing.T) {
	// WHAT: Scheduling a delayed message
	// WHY: Core functionality

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	delay := 1 * time.Hour
	err := scheduler.Schedule("test-topic", 0, 100, delay)
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	// Verify it's tracked
	if !scheduler.IsDelayed("test-topic", 0, 100) {
		t.Error("message should be marked as delayed")
	}
}

func TestScheduler_ScheduleAt(t *testing.T) {
	// WHAT: Scheduling at absolute time
	// WHY: Alternative API for specific delivery time

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	deliverAt := time.Now().Add(1 * time.Hour)
	err := scheduler.ScheduleAt("test-topic", 0, 100, deliverAt)
	if err != nil {
		t.Fatalf("ScheduleAt failed: %v", err)
	}

	if !scheduler.IsDelayed("test-topic", 0, 100) {
		t.Error("message should be marked as delayed")
	}
}

func TestScheduler_SchedulePastTime(t *testing.T) {
	// WHAT: Scheduling with past time (should fire immediately)
	// WHY: Past times should not error, just deliver immediately

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	var delivered atomic.Bool
	scheduler.OnDeliver(func(topic string, partition int, offset int64) {
		delivered.Store(true)
	})

	pastTime := time.Now().Add(-1 * time.Hour)
	err := scheduler.ScheduleAt("test-topic", 0, 100, pastTime)
	if err != nil {
		t.Fatalf("ScheduleAt with past time failed: %v", err)
	}

	// Should deliver almost immediately
	time.Sleep(50 * time.Millisecond)

	if !delivered.Load() {
		t.Error("past time message should be delivered immediately")
	}
}

func TestScheduler_ScheduleZeroDelay(t *testing.T) {
	// WHAT: Scheduling with zero delay
	// WHY: Should be treated as immediate

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	var delivered atomic.Bool
	scheduler.OnDeliver(func(topic string, partition int, offset int64) {
		delivered.Store(true)
	})

	err := scheduler.Schedule("test-topic", 0, 100, 0)
	if err != nil {
		t.Fatalf("Schedule with zero delay failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if !delivered.Load() {
		t.Error("zero delay message should be delivered immediately")
	}
}

// =============================================================================
// CANCELLATION TESTS
// =============================================================================

func TestScheduler_Cancel(t *testing.T) {
	// WHAT: Canceling a scheduled message
	// WHY: Users need ability to cancel delays

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	scheduler.Schedule("test-topic", 0, 100, 1*time.Hour)

	err := scheduler.Cancel("test-topic", 0, 100)
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	if scheduler.IsDelayed("test-topic", 0, 100) {
		t.Error("canceled message should not be delayed")
	}
}

func TestScheduler_CancelNonExistent(t *testing.T) {
	// WHAT: Canceling non-existent message
	// WHY: Should return appropriate error

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	err := scheduler.Cancel("test-topic", 0, 999)
	if err == nil {
		t.Error("expected error canceling non-existent message")
	}
}

func TestScheduler_CancelPreventsDelivery(t *testing.T) {
	// WHAT: Canceled messages don't get delivered
	// WHY: Cancel must actually prevent delivery

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	var delivered atomic.Bool
	scheduler.OnDeliver(func(topic string, partition int, offset int64) {
		delivered.Store(true)
	})

	scheduler.Schedule("test-topic", 0, 100, 20*time.Millisecond)
	scheduler.Cancel("test-topic", 0, 100)

	// Wait for when it would have delivered
	time.Sleep(50 * time.Millisecond)

	if delivered.Load() {
		t.Error("canceled message should not be delivered")
	}
}

// =============================================================================
// DELIVERY CALLBACK TESTS
// =============================================================================

func TestScheduler_DeliveryCallback(t *testing.T) {
	// WHAT: Delivery callback fires when delay expires
	// WHY: This is how messages become visible

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	var (
		deliveredTopic     string
		deliveredPartition int
		deliveredOffset    int64
		delivered          atomic.Bool
	)

	scheduler.OnDeliver(func(topic string, partition int, offset int64) {
		deliveredTopic = topic
		deliveredPartition = partition
		deliveredOffset = offset
		delivered.Store(true)
	})

	scheduler.Schedule("my-topic", 5, 42, 20*time.Millisecond)

	// Wait for delivery
	time.Sleep(50 * time.Millisecond)

	if !delivered.Load() {
		t.Fatal("callback should have been called")
	}

	if deliveredTopic != "my-topic" {
		t.Errorf("expected topic my-topic, got %s", deliveredTopic)
	}
	if deliveredPartition != 5 {
		t.Errorf("expected partition 5, got %d", deliveredPartition)
	}
	if deliveredOffset != 42 {
		t.Errorf("expected offset 42, got %d", deliveredOffset)
	}
}

func TestScheduler_MultipleCallbacks(t *testing.T) {
	// WHAT: Multiple messages deliver correctly
	// WHY: Must handle concurrent delivery

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	var deliveredCount atomic.Int32

	scheduler.OnDeliver(func(topic string, partition int, offset int64) {
		deliveredCount.Add(1)
	})

	// Schedule multiple with short delays
	for i := int64(0); i < 10; i++ {
		scheduler.Schedule("test-topic", 0, i, 20*time.Millisecond)
	}

	// Wait for all
	time.Sleep(100 * time.Millisecond)

	if deliveredCount.Load() != 10 {
		t.Errorf("expected 10 deliveries, got %d", deliveredCount.Load())
	}
}

// =============================================================================
// QUERY TESTS
// =============================================================================

func TestScheduler_GetDelayedMessage(t *testing.T) {
	// WHAT: Getting info about a delayed message
	// WHY: Users need to inspect scheduled messages

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	scheduler.Schedule("test-topic", 3, 100, 1*time.Hour)

	info, err := scheduler.GetDelayedMessage("test-topic", 3, 100)
	if err != nil {
		t.Fatalf("GetDelayedMessage failed: %v", err)
	}

	if info.Offset != 100 {
		t.Errorf("expected offset 100, got %d", info.Offset)
	}
	if info.Partition != 3 {
		t.Errorf("expected partition 3, got %d", info.Partition)
	}
	if info.TimeRemaining < 59*time.Minute {
		t.Error("time remaining should be close to 1 hour")
	}
}

func TestScheduler_GetDelayedMessages(t *testing.T) {
	// WHAT: Listing all delayed messages for a topic
	// WHY: Users need to see what's scheduled

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	scheduler.Schedule("test-topic", 0, 100, 1*time.Hour)
	scheduler.Schedule("test-topic", 1, 200, 2*time.Hour)
	scheduler.Schedule("test-topic", 2, 300, 3*time.Hour)

	// Different topic shouldn't be included
	scheduler.Schedule("other-topic", 0, 400, 1*time.Hour)

	messages := scheduler.GetDelayedMessages("test-topic", 10, 0)

	if len(messages) != 3 {
		t.Errorf("expected 3 messages, got %d", len(messages))
	}
}

func TestScheduler_GetDelayedMessagesPagination(t *testing.T) {
	// WHAT: Pagination for delayed messages list
	// WHY: Large lists need pagination

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	for i := int64(0); i < 25; i++ {
		scheduler.Schedule("test-topic", 0, i, 1*time.Hour)
	}

	// First page
	page1 := scheduler.GetDelayedMessages("test-topic", 10, 0)
	if len(page1) != 10 {
		t.Errorf("expected 10 in page 1, got %d", len(page1))
	}

	// Second page
	page2 := scheduler.GetDelayedMessages("test-topic", 10, 10)
	if len(page2) != 10 {
		t.Errorf("expected 10 in page 2, got %d", len(page2))
	}

	// Third page (partial)
	page3 := scheduler.GetDelayedMessages("test-topic", 10, 20)
	if len(page3) != 5 {
		t.Errorf("expected 5 in page 3, got %d", len(page3))
	}
}

// =============================================================================
// LIMIT TESTS
// =============================================================================

func TestScheduler_MaxDelay(t *testing.T) {
	// WHAT: Enforcing maximum delay
	// WHY: Prevent scheduling too far in future

	dir := t.TempDir()
	config := SchedulerConfig{
		DataDir:                dir,
		MaxDelayedPerTopic:     1000,
		MaxDelayedPerPartition: 100,
		MaxDelay:               1 * time.Hour, // Short max for test
	}

	scheduler, _ := NewScheduler(config)
	defer scheduler.Close()

	// Within limit
	err := scheduler.Schedule("test-topic", 0, 100, 30*time.Minute)
	if err != nil {
		t.Errorf("schedule within limit failed: %v", err)
	}

	// Exceeds limit - should fail with wrapped ErrDelayTooLong
	err = scheduler.Schedule("test-topic", 0, 101, 2*time.Hour)
	if err == nil {
		t.Error("expected error for delay exceeding max")
	}
	if !errors.Is(err, ErrDelayTooLong) {
		t.Errorf("expected ErrDelayTooLong, got %v", err)
	}
}

func TestScheduler_MaxPerTopic(t *testing.T) {
	// WHAT: Enforcing per-topic limit
	// WHY: Prevent topic from monopolizing delay capacity

	dir := t.TempDir()
	config := SchedulerConfig{
		DataDir:                dir,
		MaxDelayedPerTopic:     5, // Very small for test
		MaxDelayedPerPartition: 100,
		MaxDelay:               24 * time.Hour,
	}

	scheduler, _ := NewScheduler(config)
	defer scheduler.Close()

	// Fill to limit
	for i := int64(0); i < 5; i++ {
		err := scheduler.Schedule("test-topic", 0, i, 1*time.Hour)
		if err != nil {
			t.Fatalf("schedule %d failed: %v", i, err)
		}
	}

	// Exceed limit - should fail with delay index full error
	err := scheduler.Schedule("test-topic", 0, 100, 1*time.Hour)
	if err == nil {
		t.Error("expected error when exceeding limit")
	}
}

// =============================================================================
// STATS TESTS
// =============================================================================

func TestScheduler_Stats(t *testing.T) {
	// WHAT: Getting scheduler statistics
	// WHY: Observability

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	scheduler.Schedule("topic-a", 0, 100, 1*time.Hour)
	scheduler.Schedule("topic-a", 1, 101, 2*time.Hour)
	scheduler.Schedule("topic-b", 0, 200, 1*time.Hour)

	stats := scheduler.Stats()

	if stats.TotalPending != 3 {
		t.Errorf("expected 3 total pending, got %d", stats.TotalPending)
	}

	if len(stats.ByTopic) != 2 {
		t.Errorf("expected 2 topics, got %d", len(stats.ByTopic))
	}

	if stats.ByTopic["topic-a"] != 2 {
		t.Errorf("expected 2 for topic-a, got %d", stats.ByTopic["topic-a"])
	}
}

// =============================================================================
// RECOVERY TESTS
// =============================================================================

func TestScheduler_Recovery(t *testing.T) {
	// WHAT: Scheduler recovers pending delays on restart
	// WHY: Must not lose scheduled messages across restarts

	dir := t.TempDir()
	config := SchedulerConfig{
		DataDir:                dir,
		MaxDelayedPerTopic:     1000,
		MaxDelayedPerPartition: 100,
		MaxDelay:               24 * time.Hour,
	}

	// Create and schedule
	s1, _ := NewScheduler(config)
	s1.Schedule("test-topic", 0, 100, 1*time.Hour)
	s1.Schedule("test-topic", 1, 200, 2*time.Hour)
	s1.Close()

	// Reopen
	s2, _ := NewScheduler(config)
	defer s2.Close()

	// Explicitly recover
	err := s2.RecoverTopic("test-topic")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Check messages are still tracked
	if !s2.IsDelayed("test-topic", 0, 100) {
		t.Error("message 100 should still be delayed after recovery")
	}
	if !s2.IsDelayed("test-topic", 1, 200) {
		t.Error("message 200 should still be delayed after recovery")
	}
}

func TestScheduler_RecoveryPastDue(t *testing.T) {
	// WHAT: Past-due messages deliver immediately on recovery
	// WHY: Messages that expired during downtime should deliver

	dir := t.TempDir()
	config := SchedulerConfig{
		DataDir:                dir,
		MaxDelayedPerTopic:     1000,
		MaxDelayedPerPartition: 100,
		MaxDelay:               24 * time.Hour,
	}

	// Create scheduler and schedule a short delay
	s1, _ := NewScheduler(config)

	// Schedule 10ms delay but we'll take longer to restart
	s1.Schedule("test-topic", 0, 100, 10*time.Millisecond)
	s1.Close()

	// Wait for delay to pass
	time.Sleep(50 * time.Millisecond)

	// Reopen
	s2, _ := NewScheduler(config)
	defer s2.Close()

	var delivered atomic.Bool
	s2.OnDeliver(func(topic string, partition int, offset int64) {
		delivered.Store(true)
	})

	// Recover
	s2.RecoverTopic("test-topic")

	// Should deliver immediately since past due
	time.Sleep(50 * time.Millisecond)

	if !delivered.Load() {
		t.Error("past-due message should deliver immediately after recovery")
	}
}

// =============================================================================
// CONCURRENT ACCESS TESTS
// =============================================================================

func TestScheduler_ConcurrentScheduling(t *testing.T) {
	// WHAT: Concurrent scheduling operations
	// WHY: Must be thread-safe

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	var wg sync.WaitGroup

	// Concurrent schedulers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			scheduler.Schedule("test-topic", n%5, int64(n), 1*time.Hour)
		}(i)
	}

	wg.Wait()

	stats := scheduler.Stats()
	if stats.TotalPending != 100 {
		t.Errorf("expected 100 pending, got %d", stats.TotalPending)
	}
}

// =============================================================================
// MULTI-TOPIC TESTS
// =============================================================================

func TestScheduler_MultipleTopics(t *testing.T) {
	// WHAT: Scheduling across multiple topics
	// WHY: Must isolate topics correctly

	scheduler, _ := createTestScheduler(t)
	defer scheduler.Close()

	scheduler.Schedule("topic-a", 0, 100, 1*time.Hour)
	scheduler.Schedule("topic-b", 0, 100, 1*time.Hour)
	scheduler.Schedule("topic-c", 0, 100, 1*time.Hour)

	// Same offset in different topics should be tracked separately
	if !scheduler.IsDelayed("topic-a", 0, 100) {
		t.Error("topic-a offset 100 should be delayed")
	}
	if !scheduler.IsDelayed("topic-b", 0, 100) {
		t.Error("topic-b offset 100 should be delayed")
	}
	if !scheduler.IsDelayed("topic-c", 0, 100) {
		t.Error("topic-c offset 100 should be delayed")
	}

	// Cancel one shouldn't affect others
	scheduler.Cancel("topic-a", 0, 100)

	if scheduler.IsDelayed("topic-a", 0, 100) {
		t.Error("topic-a offset 100 should not be delayed after cancel")
	}
	if !scheduler.IsDelayed("topic-b", 0, 100) {
		t.Error("topic-b offset 100 should still be delayed")
	}
}

// =============================================================================
// CLOSE TESTS
// =============================================================================

func TestScheduler_CloseCleanly(t *testing.T) {
	// WHAT: Clean shutdown
	// WHY: Must not leak resources

	scheduler, _ := createTestScheduler(t)

	scheduler.Schedule("test-topic", 0, 100, 1*time.Hour)

	err := scheduler.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Operations after close should fail
	err = scheduler.Schedule("test-topic", 0, 200, 1*time.Hour)
	if !errors.Is(err, ErrSchedulerClosed) {
		t.Errorf("expected ErrSchedulerClosed after close, got %v", err)
	}
}

func TestScheduler_CloseWithPending(t *testing.T) {
	// WHAT: Close with pending timers
	// WHY: Must persist state before shutdown

	dir := t.TempDir()
	config := SchedulerConfig{
		DataDir:                dir,
		MaxDelayedPerTopic:     1000,
		MaxDelayedPerPartition: 100,
		MaxDelay:               24 * time.Hour,
	}

	s1, _ := NewScheduler(config)

	// Schedule some messages
	for i := int64(0); i < 10; i++ {
		s1.Schedule("test-topic", 0, i, 1*time.Hour)
	}

	s1.Close()

	// Reopen and verify
	s2, _ := NewScheduler(config)
	defer s2.Close()

	s2.RecoverTopic("test-topic")

	stats := s2.Stats()
	if stats.ByTopic["test-topic"] != 10 {
		t.Errorf("expected 10 recovered, got %d", stats.ByTopic["test-topic"])
	}
}
