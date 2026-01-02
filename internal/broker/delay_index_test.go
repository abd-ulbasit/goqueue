// =============================================================================
// DELAY INDEX TESTS
// =============================================================================
//
// Tests for persistent delay index storage.
//
// TEST CATEGORIES:
//   - Basic CRUD operations
//   - Persistence and recovery
//   - State transitions
//   - Concurrent access
//   - Edge cases
//
// =============================================================================

package broker

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func createTestDelayIndex(t *testing.T) (*DelayIndex, string) {
	t.Helper()

	dir := t.TempDir()
	config := DelayIndexConfig{
		DataDir:    dir,
		Topic:      "test-topic",
		MaxEntries: 1000,
	}

	idx, err := NewDelayIndex(config)
	if err != nil {
		t.Fatalf("failed to create delay index: %v", err)
	}

	return idx, dir
}

// =============================================================================
// BASIC TESTS
// =============================================================================

func TestDelayIndex_CreateNew(t *testing.T) {
	// WHAT: Creating a new delay index
	// WHY: Must be able to initialize empty index

	idx, dir := createTestDelayIndex(t)
	defer idx.Close()

	// Verify file was created
	path := filepath.Join(dir, "delays", "test-topic", "delay.index")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("index file not created at %s", path)
	}

	// Verify initial stats
	stats := idx.Stats()
	if stats.Topic != "test-topic" {
		t.Errorf("expected topic test-topic, got %s", stats.Topic)
	}
	if stats.PendingCount != 0 {
		t.Errorf("expected pending count 0, got %d", stats.PendingCount)
	}
}

func TestDelayIndex_Add(t *testing.T) {
	// WHAT: Adding delay entries
	// WHY: Core functionality for tracking delayed messages

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	deliverAt := time.Now().Add(1 * time.Hour)

	err := idx.Add(100, 0, deliverAt)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if idx.PendingCount() != 1 {
		t.Errorf("expected pending count 1, got %d", idx.PendingCount())
	}
}

func TestDelayIndex_Get(t *testing.T) {
	// WHAT: Retrieving delay entries
	// WHY: Must be able to look up by offset

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	deliverAt := time.Now().Add(1 * time.Hour)
	idx.Add(100, 2, deliverAt)

	entry, err := idx.Get(100)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if entry.Offset != 100 {
		t.Errorf("expected offset 100, got %d", entry.Offset)
	}
	if entry.Partition != 2 {
		t.Errorf("expected partition 2, got %d", entry.Partition)
	}
	if entry.State != delayStatePending {
		t.Errorf("expected state pending, got %d", entry.State)
	}
}

func TestDelayIndex_GetNotFound(t *testing.T) {
	// WHAT: Getting non-existent entry
	// WHY: Should return appropriate error

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	_, err := idx.Get(999)
	if err != ErrDelayEntryNotFound {
		t.Errorf("expected ErrDelayEntryNotFound, got %v", err)
	}
}

// =============================================================================
// STATE TRANSITION TESTS
// =============================================================================

func TestDelayIndex_MarkDelivered(t *testing.T) {
	// WHAT: Marking entry as delivered
	// WHY: State must update when message becomes visible

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	idx.Add(100, 0, time.Now().Add(1*time.Hour))

	err := idx.MarkDelivered(100)
	if err != nil {
		t.Fatalf("MarkDelivered failed: %v", err)
	}

	// Entry should be removed from pending
	if idx.PendingCount() != 0 {
		t.Errorf("expected pending count 0, got %d", idx.PendingCount())
	}

	// Get should fail (no longer pending)
	_, err = idx.Get(100)
	if err != ErrDelayEntryNotFound {
		t.Errorf("expected ErrDelayEntryNotFound after delivery, got %v", err)
	}
}

func TestDelayIndex_MarkCancelled(t *testing.T) {
	// WHAT: Marking entry as cancelled
	// WHY: User can cancel scheduled delays

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	idx.Add(100, 0, time.Now().Add(1*time.Hour))

	err := idx.MarkCancelled(100)
	if err != nil {
		t.Fatalf("MarkCancelled failed: %v", err)
	}

	if idx.PendingCount() != 0 {
		t.Errorf("expected pending count 0, got %d", idx.PendingCount())
	}
}

func TestDelayIndex_MarkExpired(t *testing.T) {
	// WHAT: Marking entry as expired
	// WHY: Final state after message is consumed

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	idx.Add(100, 0, time.Now().Add(1*time.Hour))

	err := idx.MarkExpired(100)
	if err != nil {
		t.Fatalf("MarkExpired failed: %v", err)
	}

	if idx.PendingCount() != 0 {
		t.Errorf("expected pending count 0, got %d", idx.PendingCount())
	}
}

// =============================================================================
// PERSISTENCE TESTS
// =============================================================================

func TestDelayIndex_Persistence(t *testing.T) {
	// WHAT: Entries survive close and reopen
	// WHY: Must be durable across restarts

	dir := t.TempDir()
	config := DelayIndexConfig{
		DataDir:    dir,
		Topic:      "test-topic",
		MaxEntries: 1000,
	}

	// Create and add entries
	idx1, err := NewDelayIndex(config)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	deliverAt := time.Now().Add(1 * time.Hour)
	idx1.Add(100, 0, deliverAt)
	idx1.Add(200, 1, deliverAt)
	idx1.Add(300, 2, deliverAt)

	idx1.Close()

	// Reopen and verify
	idx2, err := NewDelayIndex(config)
	if err != nil {
		t.Fatalf("failed to reopen index: %v", err)
	}
	defer idx2.Close()

	if idx2.PendingCount() != 3 {
		t.Errorf("expected 3 pending after reopen, got %d", idx2.PendingCount())
	}

	entry, err := idx2.Get(200)
	if err != nil {
		t.Fatalf("Get after reopen failed: %v", err)
	}
	if entry.Partition != 1 {
		t.Errorf("expected partition 1, got %d", entry.Partition)
	}
}

func TestDelayIndex_PersistenceWithStateChanges(t *testing.T) {
	// WHAT: State changes persist across reopens
	// WHY: Must not lose state change information

	dir := t.TempDir()
	config := DelayIndexConfig{
		DataDir:    dir,
		Topic:      "test-topic",
		MaxEntries: 1000,
	}

	// Create and modify
	idx1, _ := NewDelayIndex(config)
	deliverAt := time.Now().Add(1 * time.Hour)

	idx1.Add(100, 0, deliverAt)
	idx1.Add(200, 1, deliverAt)
	idx1.Add(300, 2, deliverAt)

	idx1.MarkDelivered(100) // This one is delivered
	idx1.MarkCancelled(200) // This one is cancelled
	// 300 stays pending

	idx1.Close()

	// Reopen
	idx2, _ := NewDelayIndex(config)
	defer idx2.Close()

	// Only 300 should be pending
	if idx2.PendingCount() != 1 {
		t.Errorf("expected 1 pending after reopen, got %d", idx2.PendingCount())
	}

	// 300 should still be gettable
	entry, err := idx2.Get(300)
	if err != nil {
		t.Fatalf("Get(300) failed: %v", err)
	}
	if entry.State != delayStatePending {
		t.Errorf("expected pending state, got %d", entry.State)
	}
}

// =============================================================================
// QUERY TESTS
// =============================================================================

func TestDelayIndex_GetPendingEntries(t *testing.T) {
	// WHAT: Listing all pending entries
	// WHY: Needed for recovery and inspection

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	now := time.Now()
	idx.Add(100, 0, now.Add(1*time.Hour))
	idx.Add(200, 1, now.Add(2*time.Hour))
	idx.Add(300, 2, now.Add(3*time.Hour))

	entries := idx.GetPendingEntries()

	if len(entries) != 3 {
		t.Errorf("expected 3 pending entries, got %d", len(entries))
	}
}

func TestDelayIndex_GetReadyEntries(t *testing.T) {
	// WHAT: Listing entries ready for delivery
	// WHY: Used during recovery to find past-due messages

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	now := time.Now()

	// Past (ready)
	idx.Add(100, 0, now.Add(-1*time.Hour))
	idx.Add(200, 1, now.Add(-30*time.Minute))

	// Future (not ready)
	idx.Add(300, 2, now.Add(1*time.Hour))

	ready := idx.GetReadyEntries()

	if len(ready) != 2 {
		t.Errorf("expected 2 ready entries, got %d", len(ready))
	}
}

func TestDelayIndex_IsReady(t *testing.T) {
	// WHAT: Checking if entry is ready
	// WHY: Helper for determining visibility

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	// Past delivery time
	idx.Add(100, 0, time.Now().Add(-1*time.Hour))

	// Future delivery time
	idx.Add(200, 1, time.Now().Add(1*time.Hour))

	entry1, _ := idx.Get(100)
	if !entry1.IsReady() {
		t.Error("past entry should be ready")
	}

	entry2, _ := idx.Get(200)
	if entry2.IsReady() {
		t.Error("future entry should not be ready")
	}
}

// =============================================================================
// LIMIT TESTS
// =============================================================================

func TestDelayIndex_MaxEntries(t *testing.T) {
	// WHAT: Enforcing maximum entries limit
	// WHY: Prevent unbounded memory/disk growth

	dir := t.TempDir()
	config := DelayIndexConfig{
		DataDir:    dir,
		Topic:      "test-topic",
		MaxEntries: 5, // Very small limit for testing
	}

	idx, _ := NewDelayIndex(config)
	defer idx.Close()

	deliverAt := time.Now().Add(1 * time.Hour)

	// Add up to limit
	for i := int64(0); i < 5; i++ {
		err := idx.Add(i, 0, deliverAt)
		if err != nil {
			t.Fatalf("Add %d failed: %v", i, err)
		}
	}

	// Next add should fail
	err := idx.Add(100, 0, deliverAt)
	if err != ErrDelayIndexFull {
		t.Errorf("expected ErrDelayIndexFull, got %v", err)
	}
}

// =============================================================================
// CONCURRENT ACCESS TESTS
// =============================================================================

func TestDelayIndex_ConcurrentAccess(t *testing.T) {
	// WHAT: Concurrent reads and writes
	// WHY: Must be thread-safe

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	var wg sync.WaitGroup

	// Concurrent writers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			idx.Add(int64(n), n%3, time.Now().Add(1*time.Hour))
		}(i)
	}

	// Concurrent readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx.GetPendingEntries()
		}()
	}

	wg.Wait()

	if idx.PendingCount() != 50 {
		t.Errorf("expected 50 pending, got %d", idx.PendingCount())
	}
}

// =============================================================================
// SYNC TESTS
// =============================================================================

func TestDelayIndex_Sync(t *testing.T) {
	// WHAT: Explicit sync to disk
	// WHY: Ensure data is flushed on demand

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	idx.Add(100, 0, time.Now().Add(1*time.Hour))

	err := idx.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

// =============================================================================
// TIME UNTIL DELIVERY TESTS
// =============================================================================

func TestDelayEntry_TimeUntilDelivery(t *testing.T) {
	// WHAT: Calculating remaining time
	// WHY: Useful for display and scheduling decisions

	entry := &DelayEntry{
		DeliverAt: time.Now().Add(1 * time.Hour).UnixNano(),
	}

	remaining := entry.TimeUntilDelivery()

	// Should be approximately 1 hour (allow some tolerance)
	if remaining < 59*time.Minute || remaining > 61*time.Minute {
		t.Errorf("expected ~1 hour, got %v", remaining)
	}
}

func TestDelayEntry_TimeUntilDelivery_Past(t *testing.T) {
	// WHAT: Time until delivery for past time
	// WHY: Should return 0, not negative

	entry := &DelayEntry{
		DeliverAt: time.Now().Add(-1 * time.Hour).UnixNano(),
	}

	remaining := entry.TimeUntilDelivery()

	if remaining != 0 {
		t.Errorf("expected 0 for past time, got %v", remaining)
	}
}

// =============================================================================
// EMPTY INDEX TESTS
// =============================================================================

func TestDelayIndex_EmptyOperations(t *testing.T) {
	// WHAT: Operations on empty index
	// WHY: Should handle gracefully

	idx, _ := createTestDelayIndex(t)
	defer idx.Close()

	// Get pending from empty
	entries := idx.GetPendingEntries()
	if len(entries) != 0 {
		t.Errorf("expected 0 pending, got %d", len(entries))
	}

	// Get ready from empty
	ready := idx.GetReadyEntries()
	if len(ready) != 0 {
		t.Errorf("expected 0 ready, got %d", len(ready))
	}

	// Mark non-existent
	err := idx.MarkDelivered(999)
	if err != ErrDelayEntryNotFound {
		t.Errorf("expected ErrDelayEntryNotFound, got %v", err)
	}
}
