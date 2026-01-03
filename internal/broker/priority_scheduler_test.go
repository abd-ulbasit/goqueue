// =============================================================================
// PRIORITY SCHEDULER TESTS
// =============================================================================
//
// These tests validate the Weighted Fair Queuing (WFQ) implementation using
// Deficit Round Robin (DRR) algorithm.
//
// TEST CATEGORIES:
//   1. Basic Operations - Enqueue/Dequeue single messages
//   2. Priority Ordering - Higher priority gets served first
//   3. WFQ Fairness - Messages distributed according to weights
//   4. Starvation Prevention - Lower priorities eventually get served
//   5. Edge Cases - Empty queues, single priority, etc.
//
// =============================================================================

package broker

import (
	"testing"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// BASIC OPERATIONS
// =============================================================================

func TestPriorityScheduler_EnqueueDequeue(t *testing.T) {
	config := DefaultPrioritySchedulerConfig()
	scheduler := NewPriorityScheduler(config)

	// Create a test message
	msg := &storage.Message{
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
		Priority:  storage.PriorityNormal,
		Timestamp: time.Now().UnixNano(),
	}

	// Enqueue
	scheduler.Enqueue(msg)

	if scheduler.Len() != 1 {
		t.Errorf("Len() = %d, want 1", scheduler.Len())
	}

	// Dequeue
	dequeued, ok := scheduler.Dequeue()
	if !ok {
		t.Fatal("Dequeue() returned ok=false, want ok=true")
	}

	if string(dequeued.Key) != "test-key" {
		t.Errorf("Dequeued key = %s, want test-key", dequeued.Key)
	}

	if scheduler.Len() != 0 {
		t.Errorf("Len() after dequeue = %d, want 0", scheduler.Len())
	}
}

func TestPriorityScheduler_DequeueEmpty(t *testing.T) {
	config := DefaultPrioritySchedulerConfig()
	scheduler := NewPriorityScheduler(config)

	// Dequeue from empty
	_, ok := scheduler.Dequeue()
	if ok {
		t.Error("Dequeue() from empty returned ok=true, want ok=false")
	}
}

func TestPriorityScheduler_Peek(t *testing.T) {
	config := DefaultPrioritySchedulerConfig()
	scheduler := NewPriorityScheduler(config)

	// Peek empty
	if peeked := scheduler.Peek(); peeked != nil {
		t.Errorf("Peek() on empty = %v, want nil", peeked)
	}

	msg := &storage.Message{
		Key:      []byte("peek-key"),
		Value:    []byte("peek-value"),
		Priority: storage.PriorityCritical,
	}
	scheduler.Enqueue(msg)

	// Peek should not remove
	peeked := scheduler.Peek()
	if peeked == nil {
		t.Fatal("Peek() returned nil")
	}
	if string(peeked.Key) != "peek-key" {
		t.Errorf("Peek() key = %s, want peek-key", peeked.Key)
	}

	// Should still have one message
	if scheduler.Len() != 1 {
		t.Errorf("Len() after Peek = %d, want 1", scheduler.Len())
	}
}

// =============================================================================
// PRIORITY ORDERING
// =============================================================================

func TestPriorityScheduler_PriorityOrdering(t *testing.T) {
	config := DefaultPrioritySchedulerConfig()
	scheduler := NewPriorityScheduler(config)

	// Enqueue messages in reverse priority order (background first, critical last)
	priorities := []storage.Priority{
		storage.PriorityBackground,
		storage.PriorityLow,
		storage.PriorityNormal,
		storage.PriorityHigh,
		storage.PriorityCritical,
	}

	for _, p := range priorities {
		msg := &storage.Message{
			Key:      []byte(p.String()),
			Value:    []byte("value"),
			Priority: p,
		}
		scheduler.Enqueue(msg)
	}

	// Dequeue all - should come out in priority order (critical first)
	expectedOrder := []string{"critical", "high", "normal", "low", "background"}
	for i, expected := range expectedOrder {
		msg, ok := scheduler.Dequeue()
		if !ok {
			t.Fatalf("Dequeue %d returned ok=false", i)
		}
		if string(msg.Key) != expected {
			t.Errorf("Dequeue %d key = %s, want %s", i, msg.Key, expected)
		}
	}
}

func TestPriorityScheduler_CriticalAlwaysFirst(t *testing.T) {
	config := DefaultPrioritySchedulerConfig()
	scheduler := NewPriorityScheduler(config)

	// Add many low priority messages
	for i := 0; i < 100; i++ {
		scheduler.Enqueue(&storage.Message{
			Key:      []byte("low"),
			Value:    []byte("value"),
			Priority: storage.PriorityLow,
		})
	}

	// Add one critical message
	scheduler.Enqueue(&storage.Message{
		Key:      []byte("critical"),
		Value:    []byte("urgent"),
		Priority: storage.PriorityCritical,
	})

	// Critical should be dequeued first
	msg, ok := scheduler.Dequeue()
	if !ok {
		t.Fatal("Dequeue returned ok=false")
	}
	if string(msg.Key) != "critical" {
		t.Errorf("First dequeue key = %s, want critical", msg.Key)
	}
}

// =============================================================================
// WFQ FAIRNESS
// =============================================================================

func TestPriorityScheduler_WFQDistribution(t *testing.T) {
	// Skip this test in short mode as it takes a while
	if testing.Short() {
		t.Skip("skipping WFQ distribution test in short mode")
	}

	config := DefaultPrioritySchedulerConfig()
	scheduler := NewPriorityScheduler(config)

	// Add equal number of messages at each priority
	messagesPerPriority := 1000
	for p := storage.PriorityCritical; p <= storage.PriorityBackground; p++ {
		for i := 0; i < messagesPerPriority; i++ {
			scheduler.Enqueue(&storage.Message{
				Key:       []byte(p.String()),
				Value:     []byte("value"),
				Priority:  p,
				Timestamp: time.Now().UnixNano(),
			})
		}
	}

	// Dequeue all and count by priority
	counts := make(map[storage.Priority]int)
	totalDequeued := 0
	for {
		msg, ok := scheduler.Dequeue()
		if !ok {
			break
		}
		counts[msg.Priority]++
		totalDequeued++
	}

	// Verify all messages were dequeued
	expectedTotal := messagesPerPriority * 5
	if totalDequeued != expectedTotal {
		t.Errorf("Total dequeued = %d, want %d", totalDequeued, expectedTotal)
	}

	// With default weights [50,25,15,7,3], the distribution should roughly follow
	// However, since all priorities have equal input, WFQ should drain higher
	// priorities faster initially, then lower priorities.
	// All messages should eventually be delivered.
	for p := storage.PriorityCritical; p <= storage.PriorityBackground; p++ {
		if counts[p] != messagesPerPriority {
			t.Errorf("Priority %s: dequeued %d, want %d", p, counts[p], messagesPerPriority)
		}
	}
}

// =============================================================================
// STARVATION PREVENTION
// =============================================================================

func TestPriorityScheduler_StarvationPrevention(t *testing.T) {
	// Configure with a very short starvation timeout for testing
	config := PrioritySchedulerConfig{
		Weights:             DefaultPriorityWeights(),
		StarvationTimeoutMs: 10, // Very short for test (10ms)
	}
	scheduler := NewPriorityScheduler(config)

	// Add a background message that will become "old"
	oldMsg := &storage.Message{
		Key:       []byte("starving"),
		Value:     []byte("please help"),
		Priority:  storage.PriorityBackground,
		Timestamp: time.Now().UnixNano(),
	}
	scheduler.Enqueue(oldMsg)

	// Wait for starvation timeout
	time.Sleep(20 * time.Millisecond)

	// Add critical messages
	for i := 0; i < 10; i++ {
		scheduler.Enqueue(&storage.Message{
			Key:       []byte("critical"),
			Value:     []byte("value"),
			Priority:  storage.PriorityCritical,
			Timestamp: time.Now().UnixNano(),
		})
	}

	// The starving background message should be promoted and dequeued
	// within the first few dequeues (exact position depends on implementation)
	foundStarving := false
	for i := 0; i < 5; i++ {
		msg, ok := scheduler.Dequeue()
		if !ok {
			t.Fatal("Unexpected empty queue")
		}
		if string(msg.Key) == "starving" {
			foundStarving = true
			break
		}
	}

	if !foundStarving {
		t.Error("Starving message was not promoted within first 5 dequeues")
	}
}

// =============================================================================
// EDGE CASES
// =============================================================================

func TestPriorityScheduler_SinglePriority(t *testing.T) {
	config := DefaultPrioritySchedulerConfig()
	scheduler := NewPriorityScheduler(config)

	// Only use one priority level
	for i := 0; i < 5; i++ {
		scheduler.Enqueue(&storage.Message{
			Key:       []byte{byte('a' + i)},
			Value:     []byte("value"),
			Priority:  storage.PriorityNormal,
			Timestamp: time.Now().UnixNano(),
		})
	}

	// Should dequeue in FIFO order within priority
	for i := 0; i < 5; i++ {
		msg, ok := scheduler.Dequeue()
		if !ok {
			t.Fatalf("Dequeue %d returned ok=false", i)
		}
		expectedKey := byte('a' + i)
		if msg.Key[0] != expectedKey {
			t.Errorf("Dequeue %d key = %c, want %c", i, msg.Key[0], expectedKey)
		}
	}
}

func TestPriorityScheduler_DequeueByPriority(t *testing.T) {
	config := DefaultPrioritySchedulerConfig()
	scheduler := NewPriorityScheduler(config)

	// Add messages at each priority
	for p := storage.PriorityCritical; p <= storage.PriorityBackground; p++ {
		scheduler.Enqueue(&storage.Message{
			Key:      []byte(p.String()),
			Value:    []byte("value"),
			Priority: p,
		})
	}

	// Dequeue specifically from Normal priority
	msg, ok := scheduler.DequeueByPriority(storage.PriorityNormal)
	if !ok {
		t.Fatal("DequeueByPriority(Normal) returned ok=false")
	}
	if msg.Priority != storage.PriorityNormal {
		t.Errorf("DequeueByPriority(Normal) priority = %s, want normal", msg.Priority)
	}

	// Try to dequeue from Normal again (should fail, it's empty)
	_, ok = scheduler.DequeueByPriority(storage.PriorityNormal)
	if ok {
		t.Error("DequeueByPriority(Normal) on empty returned ok=true")
	}
}

func TestPriorityScheduler_DequeueN(t *testing.T) {
	config := DefaultPrioritySchedulerConfig()
	scheduler := NewPriorityScheduler(config)

	// Add 10 messages
	for i := 0; i < 10; i++ {
		scheduler.Enqueue(&storage.Message{
			Key:      []byte{byte('a' + i)},
			Value:    []byte("value"),
			Priority: storage.Priority(i % 5), // Distribute across priorities
		})
	}

	// Dequeue 5
	messages := scheduler.DequeueN(5)
	if len(messages) != 5 {
		t.Errorf("DequeueN(5) returned %d messages, want 5", len(messages))
	}

	// Should have 5 remaining
	if scheduler.Len() != 5 {
		t.Errorf("Len() after DequeueN = %d, want 5", scheduler.Len())
	}

	// Dequeue more than remaining
	messages = scheduler.DequeueN(10)
	if len(messages) != 5 {
		t.Errorf("DequeueN(10) from 5 remaining returned %d, want 5", len(messages))
	}

	if scheduler.Len() != 0 {
		t.Errorf("Len() after DequeueN = %d, want 0", scheduler.Len())
	}
}

func TestPriorityScheduler_EnqueueBatch(t *testing.T) {
	config := DefaultPrioritySchedulerConfig()
	scheduler := NewPriorityScheduler(config)

	messages := []*storage.Message{
		{Key: []byte("a"), Value: []byte("1"), Priority: storage.PriorityCritical},
		{Key: []byte("b"), Value: []byte("2"), Priority: storage.PriorityNormal},
		{Key: []byte("c"), Value: []byte("3"), Priority: storage.PriorityBackground},
	}

	scheduler.EnqueueBatch(messages)

	if scheduler.Len() != 3 {
		t.Errorf("Len() after EnqueueBatch = %d, want 3", scheduler.Len())
	}

	// Critical should come first
	msg, _ := scheduler.Dequeue()
	if string(msg.Key) != "a" {
		t.Errorf("First dequeue key = %s, want a", msg.Key)
	}
}

// =============================================================================
// STATS
// =============================================================================

func TestPriorityScheduler_Stats(t *testing.T) {
	config := DefaultPrioritySchedulerConfig()
	scheduler := NewPriorityScheduler(config)

	// Enqueue messages
	scheduler.Enqueue(&storage.Message{Priority: storage.PriorityCritical})
	scheduler.Enqueue(&storage.Message{Priority: storage.PriorityNormal})
	scheduler.Enqueue(&storage.Message{Priority: storage.PriorityNormal})

	stats := scheduler.Stats()

	if stats.EnqueueCount[storage.PriorityCritical] != 1 {
		t.Errorf("EnqueueCount[Critical] = %d, want 1", stats.EnqueueCount[storage.PriorityCritical])
	}
	if stats.EnqueueCount[storage.PriorityNormal] != 2 {
		t.Errorf("EnqueueCount[Normal] = %d, want 2", stats.EnqueueCount[storage.PriorityNormal])
	}

	// Dequeue one
	scheduler.Dequeue()

	stats = scheduler.Stats()
	if stats.DequeueCount[storage.PriorityCritical] != 1 {
		t.Errorf("DequeueCount[Critical] = %d, want 1", stats.DequeueCount[storage.PriorityCritical])
	}
}
