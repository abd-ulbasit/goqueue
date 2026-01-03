// =============================================================================
// PRIORITY SCHEDULER - WEIGHTED FAIR QUEUING (WFQ)
// =============================================================================
//
// WHAT IS A PRIORITY SCHEDULER?
// A priority scheduler determines the order in which messages are delivered to
// consumers when messages have different priority levels. Without scheduling,
// high-priority messages would always starve low-priority ones.
//
// WHY WEIGHTED FAIR QUEUING?
// Simple strict priority (always serve highest first) causes starvation:
//   - If Critical queue always has messages, Background never gets served
//   - In practice, some low-priority messages may wait forever
//
// WFQ guarantees:
//   - Higher priority gets MORE bandwidth, not ALL bandwidth
//   - Every priority level makes progress (no starvation)
//   - Configurable weights control the distribution
//
// ALGORITHM: DEFICIT ROUND ROBIN (DRR)
// =============================================================================
//
// DRR is used by Linux packet schedulers (tc-drr) and network equipment.
// It's simpler and more efficient than other WFQ variants.
//
// HOW DRR WORKS:
//
//   Each priority queue has:
//   - A message queue (FIFO within priority)
//   - A "quantum" (weight) - how much "credit" it gets per round
//   - A "deficit counter" - accumulated credit
//
//   Algorithm:
//   1. Visit each priority queue in order (Critical → Background)
//   2. Add quantum to deficit counter
//   3. While deficit > 0 AND queue not empty:
//      - Dequeue one message
//      - Decrease deficit by 1 (or by message size for byte-weighted)
//   4. If queue is empty, reset deficit to 0
//   5. Move to next priority, repeat
//
//   EXAMPLE (weights: Critical=5, High=3, Normal=2):
//
//   Round 1:
//     Critical: deficit=0+5=5 → dequeue 5 msgs → deficit=0
//     High:     deficit=0+3=3 → dequeue 3 msgs → deficit=0
//     Normal:   deficit=0+2=2 → dequeue 2 msgs → deficit=0
//
//   If Critical only has 3 msgs:
//     Critical: deficit=0+5=5 → dequeue 3 msgs → deficit=0 (reset, queue empty)
//     High:     deficit=0+3=3 → dequeue 3 msgs → deficit=0
//     ...
//
//   DEFICIT CARRYOVER:
//   If a queue has large messages (or we're count-based and it's empty),
//   unused deficit carries over to next round:
//     Critical: deficit=5 → only 2 msgs → dequeue 2 → deficit=3 (carried over)
//     Next round: deficit=3+5=8 → can serve more
//
// COMPARISON:
//   ┌────────────────┬────────────────────┬─────────────────────────────────┐
//   │ Algorithm      │ Complexity         │ Properties                      │
//   ├────────────────┼────────────────────┼─────────────────────────────────┤
//   │ Strict Priority│ O(1)               │ Simple, but causes starvation   │
//   │ Round Robin    │ O(1)               │ Fair, but ignores priority      │
//   │ Weighted RR    │ O(1)               │ Fair with weights, simple       │
//   │ DRR            │ O(1) amortized     │ WRR + handles variable sizes    │
//   │ WFQ (ideal)    │ O(log n)           │ Perfect fairness, complex       │
//   │ Virtual Clock  │ O(log n)           │ Good fairness, needs timestamps │
//   └────────────────┴────────────────────┴─────────────────────────────────┘
//
//   We use DRR because:
//   - O(1) dequeue (critical for queue performance)
//   - Simple to implement and understand
//   - Handles empty queues gracefully
//   - Used in production systems (Linux, Cisco, Juniper)
//
// DEFAULT WEIGHTS:
//   Priority    │ Weight │ % Bandwidth │ Use Case
//   ────────────┼────────┼─────────────┼─────────────────────────────────
//   Critical    │   50   │    50%      │ System alerts, circuit breakers
//   High        │   25   │    25%      │ Payments, real-time events
//   Normal      │   15   │    15%      │ Standard operations
//   Low         │    7   │     7%      │ Batch jobs, reports
//   Background  │    3   │     3%      │ Analytics, logs
//   ────────────┼────────┼─────────────┼─────────────────────────────────
//   Total       │  100   │   100%      │
//
// =============================================================================

package broker

import (
	"container/heap"
	"sync"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// CONFIGURATION
// =============================================================================

// PriorityWeights defines the DRR quantum (weight) for each priority level.
// Higher weight = more bandwidth share.
//
// Default weights sum to 100 for easy percentage interpretation:
//   - Critical: 50% when all queues are full
//   - High: 25%
//   - Normal: 15%
//   - Low: 7%
//   - Background: 3%
type PriorityWeights [storage.PriorityCount]int

// DefaultPriorityWeights returns the default weight configuration.
//
// DESIGN RATIONALE:
// - Critical at 50%: Ensures system-critical messages get half the bandwidth
// - Exponential decay: Each level gets roughly half of the previous
// - Background at 3%: Still gets service, but very low priority
//
// These can be tuned per-topic for different workloads:
// - High-throughput analytics: Lower Critical, higher Background
// - Payment processing: Higher Critical and High weights
func DefaultPriorityWeights() PriorityWeights {
	return PriorityWeights{
		50, // Critical: 50%
		25, // High: 25%
		15, // Normal: 15%
		7,  // Low: 7%
		3,  // Background: 3%
	}
}

// PrioritySchedulerConfig holds configuration for the priority scheduler.
type PrioritySchedulerConfig struct {
	// Weights controls the bandwidth share for each priority level
	Weights PriorityWeights

	// StarvationTimeoutMs is how long a message can wait before being promoted.
	// If a Background message waits longer than this, it gets served next.
	// Set to 0 to disable (strict weighted priority).
	// Default: 30000 (30 seconds)
	//
	// WHY STARVATION TIMEOUT?
	// Even with WFQ, extreme load imbalance can cause long waits:
	//   - 1000 Critical msgs/sec, 1 Background msg/day
	//   - Background msg waits until Critical queue empties
	//   - With timeout: Background msg promoted after 30s regardless
	StarvationTimeoutMs int
}

// DefaultPrioritySchedulerConfig returns sensible defaults.
func DefaultPrioritySchedulerConfig() PrioritySchedulerConfig {
	return PrioritySchedulerConfig{
		Weights:             DefaultPriorityWeights(),
		StarvationTimeoutMs: 30000, // 30 seconds
	}
}

// =============================================================================
// SCHEDULED MESSAGE
// =============================================================================

// PriorityMessage wraps a message with scheduling metadata.
// Used internally by the priority scheduler to track message state.
type PriorityMessage struct {
	// Message is the actual message content
	Message *storage.Message

	// EnqueuedAt is when this message was added to the scheduler
	// Used for starvation timeout calculation
	EnqueuedAt time.Time

	// index is used by the heap implementation (internal)
	index int
}

// =============================================================================
// MESSAGE HEAP - PER-PRIORITY FIFO QUEUE
// =============================================================================
//
// We use a min-heap ordered by EnqueuedAt (oldest first) to maintain FIFO
// within each priority level. This is more efficient than a linked list for
// our use case because:
//   - O(log n) insert and remove
//   - O(1) peek at oldest
//   - Memory-efficient (array-backed)
//

// messageHeap implements heap.Interface for PriorityMessage.
// Orders by EnqueuedAt (oldest first) for FIFO within priority.
type messageHeap []*PriorityMessage

func (h messageHeap) Len() int           { return len(h) }
func (h messageHeap) Less(i, j int) bool { return h[i].EnqueuedAt.Before(h[j].EnqueuedAt) }
func (h messageHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *messageHeap) Push(x any) {
	n := len(*h)
	item := x.(*PriorityMessage)
	item.index = n
	*h = append(*h, item)
}

func (h *messageHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // GC help
	item.index = -1 // Mark as removed
	*h = old[0 : n-1]
	return item
}

// Peek returns the oldest message without removing it.
func (h messageHeap) Peek() *PriorityMessage {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

// =============================================================================
// PRIORITY SCHEDULER
// =============================================================================

// PriorityScheduler implements Weighted Fair Queuing using Deficit Round Robin.
// It manages multiple priority queues and determines the order of message delivery.
//
// THREAD SAFETY:
// All methods are safe for concurrent use. Internal locking ensures consistency.
//
// USAGE:
//
//	scheduler := NewPriorityScheduler(config)
//	scheduler.Enqueue(msg)           // Add message (uses msg.Priority)
//	msg, ok := scheduler.Dequeue()   // Get next message per WFQ
//	count := scheduler.Len()         // Total messages across all priorities
type PriorityScheduler struct {
	// queues holds one heap per priority level
	// queues[0] = Critical, queues[4] = Background
	queues [storage.PriorityCount]*messageHeap

	// deficits tracks the deficit counter for each priority (DRR algorithm)
	deficits [storage.PriorityCount]int

	// config holds scheduler configuration
	config PrioritySchedulerConfig

	// currentPriority tracks which priority we're currently serving (DRR state)
	currentPriority storage.Priority

	// mu protects all state
	mu sync.Mutex

	// stats tracks per-priority statistics
	stats PrioritySchedulerStats
}

// PrioritySchedulerStats holds scheduling statistics.
type PrioritySchedulerStats struct {
	// EnqueueCount is total messages enqueued per priority
	EnqueueCount [storage.PriorityCount]int64

	// DequeueCount is total messages dequeued per priority
	DequeueCount [storage.PriorityCount]int64

	// StarvationPromotions is how many times starvation timeout triggered
	StarvationPromotions int64
}

// NewPriorityScheduler creates a new priority scheduler with the given configuration.
//
// EXAMPLE:
//
//	// Default configuration
//	scheduler := NewPriorityScheduler(DefaultPrioritySchedulerConfig())
//
//	// Custom weights
//	config := PrioritySchedulerConfig{
//	    Weights: PriorityWeights{40, 30, 20, 7, 3}, // More balanced
//	    StarvationTimeoutMs: 60000, // 1 minute timeout
//	}
//	scheduler := NewPriorityScheduler(config)
func NewPriorityScheduler(config PrioritySchedulerConfig) *PriorityScheduler {
	ps := &PriorityScheduler{
		config:          config,
		currentPriority: storage.PriorityCritical, // Start with highest
	}

	// Initialize all priority queues
	for i := range ps.queues {
		h := &messageHeap{}
		heap.Init(h)
		ps.queues[i] = h
	}

	return ps
}

// =============================================================================
// ENQUEUE - ADD MESSAGE TO SCHEDULER
// =============================================================================

// Enqueue adds a message to the appropriate priority queue.
// The message's Priority field determines which queue it goes into.
//
// COMPLEXITY: O(log n) where n is messages in that priority queue
//
// THREAD SAFETY: Safe for concurrent use.
func (ps *PriorityScheduler) Enqueue(msg *storage.Message) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	priority := msg.Priority
	if !priority.IsValid() {
		priority = storage.PriorityNormal
	}

	sm := &PriorityMessage{
		Message:    msg,
		EnqueuedAt: time.Now(),
	}

	heap.Push(ps.queues[priority], sm)
	ps.stats.EnqueueCount[priority]++
}

// EnqueueBatch adds multiple messages to the scheduler.
// More efficient than calling Enqueue repeatedly (single lock acquisition).
func (ps *PriorityScheduler) EnqueueBatch(msgs []*storage.Message) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	now := time.Now()
	for _, msg := range msgs {
		priority := msg.Priority
		if !priority.IsValid() {
			priority = storage.PriorityNormal
		}

		sm := &PriorityMessage{
			Message:    msg,
			EnqueuedAt: now,
		}

		heap.Push(ps.queues[priority], sm)
		ps.stats.EnqueueCount[priority]++
	}
}

// =============================================================================
// DEQUEUE - GET NEXT MESSAGE (WFQ ALGORITHM)
// =============================================================================

// Dequeue returns the next message according to Weighted Fair Queuing.
// Returns nil, false if all queues are empty.
//
// ALGORITHM:
//  1. Check for starvation (any message waiting too long)
//  2. Use Deficit Round Robin to select next message
//  3. Higher priority queues get more "turns" based on their weight
//
// COMPLEXITY: O(log n) amortized for the pop operation
//
// THREAD SAFETY: Safe for concurrent use.
func (ps *PriorityScheduler) Dequeue() (*storage.Message, bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// -------------------------------------------------------------------------
	// STEP 1: Check for starvation
	// -------------------------------------------------------------------------
	// If any message has been waiting too long, serve it immediately.
	// This prevents indefinite waiting under extreme load imbalance.
	if ps.config.StarvationTimeoutMs > 0 {
		if msg := ps.checkStarvation(); msg != nil {
			return msg, true
		}
	}

	// -------------------------------------------------------------------------
	// STEP 2: Check if all queues are empty
	// -------------------------------------------------------------------------
	if ps.isEmpty() {
		return nil, false
	}

	// -------------------------------------------------------------------------
	// STEP 3: Deficit Round Robin
	// -------------------------------------------------------------------------
	// Visit priorities starting from current, add quantum, try to dequeue.
	// This ensures fair distribution based on weights.
	return ps.drrDequeue()
}

// drrDequeue implements the core Deficit Round Robin algorithm.
// Called with lock held.
func (ps *PriorityScheduler) drrDequeue() (*storage.Message, bool) {
	// We'll visit each priority at most twice to handle edge cases
	// (first pass adds quantum, second pass dequeues with accumulated deficit)
	maxIterations := int(storage.PriorityCount) * 2

	for i := 0; i < maxIterations; i++ {
		priority := ps.currentPriority
		queue := ps.queues[priority]

		// Add quantum to deficit for this priority
		ps.deficits[priority] += ps.config.Weights[priority]

		// Try to dequeue while we have deficit and messages
		if queue.Len() > 0 && ps.deficits[priority] > 0 {
			// Dequeue one message
			sm := heap.Pop(queue).(*PriorityMessage)
			ps.deficits[priority]-- // Consume one unit of deficit
			ps.stats.DequeueCount[priority]++

			// Move to next priority for fairness
			ps.advancePriority()

			return sm.Message, true
		}

		// Queue is empty - reset deficit (DRR rule)
		if queue.Len() == 0 {
			ps.deficits[priority] = 0
		}

		// Move to next priority
		ps.advancePriority()
	}

	// Shouldn't reach here if isEmpty() was false, but safety first
	return nil, false
}

// advancePriority moves to the next priority level (wraps around).
func (ps *PriorityScheduler) advancePriority() {
	ps.currentPriority = (ps.currentPriority + 1) % storage.PriorityCount
}

// checkStarvation looks for messages that have waited too long.
// If found, returns the starved message and removes it from queue.
// Called with lock held.
func (ps *PriorityScheduler) checkStarvation() *storage.Message {
	timeout := time.Duration(ps.config.StarvationTimeoutMs) * time.Millisecond
	now := time.Now()

	// Check from lowest priority up (most likely to be starved)
	// Note: We use int for loop counter because Priority is uint8 and
	// decrementing from 0 would wrap to 255.
	for p := int(storage.PriorityBackground); p >= int(storage.PriorityCritical); p-- {
		queue := ps.queues[p]
		if queue.Len() == 0 {
			continue
		}

		oldest := queue.Peek()
		if now.Sub(oldest.EnqueuedAt) > timeout {
			// This message has been waiting too long - promote it
			sm := heap.Pop(queue).(*PriorityMessage)
			ps.stats.StarvationPromotions++
			ps.stats.DequeueCount[p]++
			return sm.Message
		}
	}

	return nil
}

// isEmpty returns true if all priority queues are empty.
// Called with lock held.
func (ps *PriorityScheduler) isEmpty() bool {
	for _, q := range ps.queues {
		if q.Len() > 0 {
			return false
		}
	}
	return true
}

// =============================================================================
// UTILITY METHODS
// =============================================================================

// Len returns the total number of messages across all priority queues.
func (ps *PriorityScheduler) Len() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	total := 0
	for _, q := range ps.queues {
		total += q.Len()
	}
	return total
}

// LenByPriority returns the message count for each priority level.
func (ps *PriorityScheduler) LenByPriority() [storage.PriorityCount]int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	var counts [storage.PriorityCount]int
	for i, q := range ps.queues {
		counts[i] = q.Len()
	}
	return counts
}

// Stats returns a copy of the scheduler statistics.
func (ps *PriorityScheduler) Stats() PrioritySchedulerStats {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.stats
}

// Clear removes all messages from all priority queues.
// Useful for testing or partition reassignment.
func (ps *PriorityScheduler) Clear() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for i := range ps.queues {
		h := &messageHeap{}
		heap.Init(h)
		ps.queues[i] = h
		ps.deficits[i] = 0
	}
	ps.currentPriority = storage.PriorityCritical
}

// =============================================================================
// DEQUEUE WITH OPTIONS
// =============================================================================

// DequeueN returns up to n messages according to WFQ.
// Useful for batch consumption.
//
// RETURNS: Slice of messages (may be less than n if queues are exhausted)
func (ps *PriorityScheduler) DequeueN(n int) []*storage.Message {
	if n <= 0 {
		return nil
	}

	msgs := make([]*storage.Message, 0, n)
	for i := 0; i < n; i++ {
		msg, ok := ps.Dequeue()
		if !ok {
			break
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

// DequeueByPriority returns the next message from a specific priority queue.
// Bypasses WFQ - useful for priority-specific consumers.
func (ps *PriorityScheduler) DequeueByPriority(priority storage.Priority) (*storage.Message, bool) {
	if !priority.IsValid() {
		return nil, false
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	queue := ps.queues[priority]
	if queue.Len() == 0 {
		return nil, false
	}

	sm := heap.Pop(queue).(*PriorityMessage)
	ps.stats.DequeueCount[priority]++
	return sm.Message, true
}

// Peek returns the next message that would be dequeued without removing it.
// Returns nil if all queues are empty.
//
// NOTE: This is approximate - concurrent operations may change the result.
func (ps *PriorityScheduler) Peek() *storage.Message {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Check starvation first (mirrors Dequeue behavior)
	if ps.config.StarvationTimeoutMs > 0 {
		timeout := time.Duration(ps.config.StarvationTimeoutMs) * time.Millisecond
		now := time.Now()

		// Note: Use int to avoid uint8 underflow when p-- from 0
		for p := int(storage.PriorityBackground); p >= int(storage.PriorityCritical); p-- {
			queue := ps.queues[p]
			if queue.Len() == 0 {
				continue
			}
			oldest := queue.Peek()
			if now.Sub(oldest.EnqueuedAt) > timeout {
				return oldest.Message
			}
		}
	}

	// Return first non-empty queue's oldest message
	for _, q := range ps.queues {
		if q.Len() > 0 {
			return q.Peek().Message
		}
	}
	return nil
}

// =============================================================================
// CONFIGURATION UPDATES
// =============================================================================

// UpdateWeights changes the priority weights.
// Takes effect immediately for subsequent dequeue operations.
func (ps *PriorityScheduler) UpdateWeights(weights PriorityWeights) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.config.Weights = weights
}

// UpdateStarvationTimeout changes the starvation timeout.
// Set to 0 to disable starvation prevention.
func (ps *PriorityScheduler) UpdateStarvationTimeout(timeoutMs int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.config.StarvationTimeoutMs = timeoutMs
}
