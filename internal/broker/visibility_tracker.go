// =============================================================================
// VISIBILITY TRACKER - MANAGING MESSAGE VISIBILITY TIMEOUTS
// =============================================================================
//
// WHAT IS VISIBILITY TIMEOUT?
// When a consumer receives a message, that message becomes "invisible" to other
// consumers for a period of time. This prevents multiple consumers from
// processing the same message simultaneously.
//
// If the consumer:
//   - ACKs the message → message is deleted (done)
//   - NACKs the message → message is scheduled for retry
//   - Does nothing (timeout) → message becomes visible again
//
// WHY VISIBILITY TIMEOUT?
//
//   PROBLEM: Consumer crashes mid-processing
//   ┌──────────┐     poll     ┌──────────┐     crash!
//   │  Queue   │ ───────────► │ Consumer │ ──────────► Message stuck forever?
//   │          │              │          │
//   └──────────┘              └──────────┘
//
//   SOLUTION: Visibility timeout
//   ┌──────────┐     poll     ┌──────────┐     crash!    ┌──────────┐
//   │  Queue   │ ───────────► │ Consumer │ ──────────►   │ 30s pass │
//   │ (msg     │              │ (msg     │               │          │
//   │ invisible│              │ inflight)│               │ msg      │
//   │ 30s)     │              └──────────┘               │ visible! │
//   └──────────┘                                         └──────────┘
//                                                             │
//                                                             ▼
//                                                        Redelivered
//                                                        to another
//                                                        consumer
//
// DATA STRUCTURE: MIN-HEAP
//
// We use a min-heap ordered by visibility deadline (earliest deadline at top).
// This allows O(1) to find the next expiring message and O(log n) for
// insert/remove operations.
//
//   ┌─────────────────────────────────────────────────────────────┐
//   │                    VISIBILITY HEAP                          │
//   │                                                             │
//   │                    ┌───────────────┐                        │
//   │                    │ deadline=10:30│  ← Next to expire      │
//   │                    │ msg: order-1  │                        │
//   │                    └───────────────┘                        │
//   │                   /                 \                       │
//   │       ┌───────────────┐        ┌───────────────┐            │
//   │       │ deadline=10:31│        │ deadline=10:32│            │
//   │       │ msg: order-2  │        │ msg: order-3  │            │
//   │       └───────────────┘        └───────────────┘            │
//   │                                                             │
//   └─────────────────────────────────────────────────────────────┘
//
// BACKGROUND GOROUTINE:
// A ticker runs every 100ms (configurable) to:
//   1. Pop expired entries from the heap
//   2. For each expired: increment retry count, schedule redelivery
//   3. Sleep until next tick
//
// COMPARISON:
//   - SQS: Server-side visibility timeout, similar to our approach
//   - Kafka: No visibility timeout (offset-based only)
//   - RabbitMQ: Unacked message tracking per channel
//
// =============================================================================

package broker

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// =============================================================================
// HEAP ITEM
// =============================================================================

// visibilityItem represents an item in the visibility timeout heap.
type visibilityItem struct {
	// receiptHandle uniquely identifies this delivery attempt
	receiptHandle string

	// deadline is when this message's visibility timeout expires
	deadline time.Time

	// message is a reference to the in-flight message
	message *InFlightMessage

	// index is maintained by the heap implementation
	index int

	// removed marks items for lazy deletion
	// (removing from middle of heap is expensive, so we mark and skip)
	removed bool
}

// =============================================================================
// VISIBILITY HEAP (MIN-HEAP BY DEADLINE)
// =============================================================================

// visibilityHeap implements heap.Interface for visibility timeout tracking.
//
// WHY HEAP?
//   - O(1) to peek next expiring item
//   - O(log n) to insert new item
//   - O(log n) to remove expired item
//   - Perfect for "process items in deadline order"
//
// ALTERNATIVE APPROACHES:
//   - Sorted list: O(n) insert, O(1) remove → bad for high throughput
//   - Timer per message: Too many goroutines, memory pressure
//   - Timer wheel: More complex, better for millions of items (M5)
type visibilityHeap []*visibilityItem

func (h visibilityHeap) Len() int           { return len(h) }
func (h visibilityHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h visibilityHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *visibilityHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*visibilityItem)
	item.index = n
	*h = append(*h, item)
}

func (h *visibilityHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // Avoid memory leak
	item.index = -1 // For safety
	*h = old[0 : n-1]
	return item
}

// =============================================================================
// VISIBILITY TRACKER
// =============================================================================

// VisibilityTracker manages visibility timeouts for in-flight messages.
//
// RESPONSIBILITIES:
//   - Track all in-flight messages across all consumers
//   - Detect visibility timeout expiration
//   - Trigger redelivery for expired messages
//   - Support visibility extension (for long-running tasks)
//
// THREAD SAFETY:
//   - All operations are protected by mutex
//   - Background goroutine runs checks periodically
//   - Safe for concurrent use by multiple consumers
type VisibilityTracker struct {
	// heap is the min-heap of visibility deadlines
	heap visibilityHeap

	// byReceipt maps receipt handle to heap item for O(1) lookup
	byReceipt map[string]*visibilityItem

	// byMessageID maps message ID to heap item
	// Used to prevent duplicate deliveries of same message
	byMessageID map[string]*visibilityItem

	// config holds reliability configuration
	config ReliabilityConfig

	// onExpired is called when a message's visibility expires
	// Parameters: the in-flight message that expired
	onExpired func(*InFlightMessage)

	// mu protects all fields
	mu sync.Mutex

	// ctx for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc

	// wg for waiting on background goroutine
	wg sync.WaitGroup

	// logger for visibility operations
	logger *slog.Logger

	// stats for monitoring
	stats VisibilityStats
}

// VisibilityStats tracks visibility tracker metrics.
type VisibilityStats struct {
	// TotalTracked is the total number of messages ever tracked
	TotalTracked int64

	// CurrentInFlight is the current number of in-flight messages
	CurrentInFlight int

	// TotalExpired is the total number of visibility timeouts that fired
	TotalExpired int64

	// TotalAcked is the total number of messages successfully ACKed
	TotalAcked int64

	// TotalExtended is the total number of visibility extensions
	TotalExtended int64
}

// NewVisibilityTracker creates a new visibility tracker.
//
// PARAMETERS:
//   - config: Reliability configuration (timeouts, intervals)
//   - onExpired: Callback when visibility expires (for redelivery)
//
// The onExpired callback is called from the background goroutine, so it
// should be fast and non-blocking. Typically it schedules the message
// for retry via a channel.
func NewVisibilityTracker(config ReliabilityConfig, onExpired func(*InFlightMessage)) *VisibilityTracker {
	ctx, cancel := context.WithCancel(context.Background())

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	vt := &VisibilityTracker{
		heap:        make(visibilityHeap, 0),
		byReceipt:   make(map[string]*visibilityItem),
		byMessageID: make(map[string]*visibilityItem),
		config:      config,
		onExpired:   onExpired,
		ctx:         ctx,
		cancel:      cancel,
		logger:      logger,
	}

	heap.Init(&vt.heap)

	// Start background checker
	vt.wg.Add(1)
	go vt.checkLoop()

	return vt
}

// =============================================================================
// TRACKING OPERATIONS
// =============================================================================

// Track adds a message to visibility tracking.
//
// PARAMETERS:
//   - msg: The in-flight message to track
//
// RETURNS:
//   - error if message is already being tracked
//
// After calling Track, the message is considered "in-flight" and will be
// checked for visibility timeout expiration.
func (vt *VisibilityTracker) Track(msg *InFlightMessage) error {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	// Check if already tracking this message
	msgID := MessageID(msg.Topic, msg.Partition, msg.Offset)
	if _, exists := vt.byMessageID[msgID]; exists {
		return fmt.Errorf("message %s is already in-flight", msgID)
	}

	// Create heap item
	item := &visibilityItem{
		receiptHandle: msg.ReceiptHandle,
		deadline:      msg.VisibilityDeadline,
		message:       msg,
	}

	// Add to heap and maps
	heap.Push(&vt.heap, item)
	vt.byReceipt[msg.ReceiptHandle] = item
	vt.byMessageID[msgID] = item

	// Update stats
	vt.stats.TotalTracked++
	vt.stats.CurrentInFlight++

	vt.logger.Debug("tracking message",
		"receipt", msg.ReceiptHandle,
		"deadline", msg.VisibilityDeadline,
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset)

	return nil
}

// Untrack removes a message from visibility tracking (successful ACK).
//
// PARAMETERS:
//   - receiptHandle: The receipt handle of the message to untrack
//
// RETURNS:
//   - The in-flight message that was removed
//   - ErrReceiptNotFound if receipt handle not found
//   - ErrReceiptExpired if visibility already expired
//
// Called when consumer successfully ACKs a message.
func (vt *VisibilityTracker) Untrack(receiptHandle string) (*InFlightMessage, error) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	item, exists := vt.byReceipt[receiptHandle]
	if !exists {
		return nil, ErrReceiptNotFound
	}

	if item.removed {
		return nil, ErrReceiptExpired
	}

	// Mark as removed (lazy deletion from heap)
	item.removed = true

	// Remove from maps
	delete(vt.byReceipt, receiptHandle)
	msgID := MessageID(item.message.Topic, item.message.Partition, item.message.Offset)
	delete(vt.byMessageID, msgID)

	// Update stats
	vt.stats.CurrentInFlight--
	vt.stats.TotalAcked++

	vt.logger.Debug("untracked message",
		"receipt", receiptHandle,
		"topic", item.message.Topic,
		"partition", item.message.Partition,
		"offset", item.message.Offset)

	return item.message, nil
}

// ExtendVisibility extends the visibility timeout for a message.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle of the message
//   - extension: Additional time to add to visibility timeout
//
// RETURNS:
//   - New visibility deadline
//   - ErrReceiptNotFound if receipt handle not found
//   - ErrReceiptExpired if visibility already expired
//
// WHY EXTEND VISIBILITY?
// Some messages take longer to process than the default timeout.
// Instead of failing and retrying, consumer can extend the timeout.
//
// EXAMPLE:
//  1. Consumer receives message (30s visibility)
//  2. Processing takes 25s so far, needs more time
//  3. Consumer calls ExtendVisibility(+30s)
//  4. New deadline is now+30s
//  5. Consumer continues processing
//
// COMPARISON:
//   - SQS: ChangeMessageVisibility API
//   - Kafka: N/A (no visibility concept)
//   - RabbitMQ: N/A (can reject and requeue with delay plugin)
func (vt *VisibilityTracker) ExtendVisibility(receiptHandle string, extension time.Duration) (time.Time, error) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	item, exists := vt.byReceipt[receiptHandle]
	if !exists {
		return time.Time{}, ErrReceiptNotFound
	}

	if item.removed {
		return time.Time{}, ErrReceiptExpired
	}

	// Check if already expired
	if time.Now().After(item.deadline) {
		return time.Time{}, ErrReceiptExpired
	}

	// Update deadline
	newDeadline := time.Now().Add(extension)
	item.deadline = newDeadline
	item.message.VisibilityDeadline = newDeadline

	// Fix heap ordering (deadline changed)
	heap.Fix(&vt.heap, item.index)

	// Update stats
	vt.stats.TotalExtended++

	vt.logger.Debug("extended visibility",
		"receipt", receiptHandle,
		"new_deadline", newDeadline,
		"extension", extension)

	return newDeadline, nil
}

// IsTracking checks if a message is currently in-flight.
func (vt *VisibilityTracker) IsTracking(topic string, partition int, offset int64) bool {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	msgID := MessageID(topic, partition, offset)
	item, exists := vt.byMessageID[msgID]
	return exists && !item.removed
}

// GetInFlight returns the in-flight message for a receipt handle.
func (vt *VisibilityTracker) GetInFlight(receiptHandle string) (*InFlightMessage, bool) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	item, exists := vt.byReceipt[receiptHandle]
	if !exists || item.removed {
		return nil, false
	}
	return item.message, true
}

// Stats returns current visibility tracker statistics.
func (vt *VisibilityTracker) Stats() VisibilityStats {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	return vt.stats
}

// =============================================================================
// BACKGROUND CHECKER
// =============================================================================

// checkLoop runs periodically to find and process expired visibility timeouts.
//
// ALGORITHM:
//  1. Lock the tracker
//  2. While heap is not empty and top item is expired:
//     a. Pop the item
//     b. If not removed (lazy delete), call onExpired callback
//  3. Unlock and sleep until next check interval
//
// WHY PERIODIC CHECK (not per-item timer)?
//   - Fewer goroutines (one vs thousands)
//   - More efficient batching of expired items
//   - Simpler shutdown handling
//   - Good enough precision (100ms is fine for 30s timeouts)
func (vt *VisibilityTracker) checkLoop() {
	defer vt.wg.Done()

	ticker := time.NewTicker(time.Duration(vt.config.VisibilityCheckIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-vt.ctx.Done():
			return
		case <-ticker.C:
			vt.processExpired()
		}
	}
}

// processExpired finds and processes all expired visibility timeouts.
func (vt *VisibilityTracker) processExpired() {
	now := time.Now()
	var expired []*InFlightMessage

	vt.mu.Lock()

	// Pop all expired items from heap
	for vt.heap.Len() > 0 {
		// Peek at top
		item := vt.heap[0]

		// If not expired yet, stop (heap is ordered by deadline)
		if item.deadline.After(now) {
			break
		}

		// Pop the item
		heap.Pop(&vt.heap)

		// Skip if already removed (lazy delete)
		if item.removed {
			continue
		}

		// Remove from maps
		delete(vt.byReceipt, item.receiptHandle)
		msgID := MessageID(item.message.Topic, item.message.Partition, item.message.Offset)
		delete(vt.byMessageID, msgID)

		// Update stats
		vt.stats.CurrentInFlight--
		vt.stats.TotalExpired++

		// Collect for callback (call outside lock)
		expired = append(expired, item.message)
	}

	vt.mu.Unlock()

	// Call onExpired callback for each expired message
	// Done outside lock to prevent deadlocks if callback acquires locks
	for _, msg := range expired {
		vt.logger.Info("visibility timeout expired",
			"receipt", msg.ReceiptHandle,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"delivery_count", msg.DeliveryCount)

		if vt.onExpired != nil {
			vt.onExpired(msg)
		}
	}
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Close shuts down the visibility tracker.
//
// SHUTDOWN PROCESS:
//  1. Cancel context (signals background goroutine to stop)
//  2. Wait for background goroutine to finish
//  3. All remaining in-flight messages will be redelivered on restart
//     (this is fine for at-least-once semantics)
func (vt *VisibilityTracker) Close() error {
	vt.cancel()
	vt.wg.Wait()

	vt.mu.Lock()
	remaining := vt.stats.CurrentInFlight
	vt.mu.Unlock()

	if remaining > 0 {
		vt.logger.Info("visibility tracker closed with in-flight messages",
			"remaining", remaining)
	}

	return nil
}
