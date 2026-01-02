// =============================================================================
// HIERARCHICAL TIMER WHEEL - O(1) DELAY SCHEDULING
// =============================================================================
//
// WHAT IS A TIMER WHEEL?
// A timer wheel is a data structure for efficiently managing large numbers of
// timers (scheduled events). Instead of using a heap (O(log n) operations),
// a timer wheel uses bucketed slots for O(1) insert/delete operations.
//
// WHY HIERARCHICAL?
//
//   SIMPLE TIMER WHEEL:
//   ┌─┬─┬─┬─┬─┬─┬─┬─┐
//   │0│1│2│3│4│5│6│7│  8 buckets × 1 second = 8 second range
//   └─┴─┴─┴─┴─┴─┴─┴─┘
//   Problem: To cover 7 days with 10ms precision, we'd need:
//   7 days × 24 hours × 60 min × 60 sec × 100 ticks = 60,480,000 buckets!
//
//   HIERARCHICAL TIMER WHEEL:
//   Instead of one flat wheel, we use multiple levels with increasing granularity:
//
//   Level 0 (10ms precision):   256 buckets × 10ms   = 2.56 seconds
//   Level 1 (2.56s precision):  64 buckets × 2.56s   = 2.73 minutes
//   Level 2 (2.73m precision):  64 buckets × 2.73m   = 2.91 hours
//   Level 3 (2.91h precision):  64 buckets × 2.91h   = 7.76 days
//
//   Total: 256 + 64 + 64 + 64 = 448 buckets (vs 60M for flat wheel!)
//
// HOW IT WORKS:
//
//   INSERT (delay=5 minutes):
//   1. Calculate which level can hold this delay
//   2. For 5 min: Level 1 can hold up to 2.73 min, so use Level 2
//   3. Calculate bucket index: 5 min / 2.73 min per bucket ≈ bucket 1
//   4. Insert into Level 2, bucket 1
//
//   TICK (every 10ms):
//   1. Advance Level 0 cursor
//   2. Process all timers in current bucket
//   3. When Level 0 wraps around (every 2.56s):
//      - Advance Level 1 cursor
//      - "Cascade down": Move Level 1 timers to Level 0
//   4. Same cascade happens at each level boundary
//
//   TIMER EXPIRATION:
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Level 3:  [5d]                                                         │
//   │              │ cascade after 2.91h                                     │
//   │              ▼                                                         │
//   │ Level 2:     [2h15m]                                                   │
//   │                   │ cascade after 2.73m                                │
//   │                   ▼                                                    │
//   │ Level 1:         [45s]                                                 │
//   │                      │ cascade after 2.56s                             │
//   │                      ▼                                                 │
//   │ Level 0:            [500ms] → FIRE!                                    │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// USED BY:
//   - Linux kernel (scheduling timers)
//   - Netty (I/O timeouts)
//   - Kafka's "purgatory" (delayed operations)
//   - goqueue (delayed message delivery)
//
// COMPARISON WITH ALTERNATIVES:
//
//   ┌────────────────┬──────────┬──────────┬─────────────┬───────────────────┐
//   │ Data Structure │ Insert   │ Delete   │ FindMin     │ Memory            │
//   ├────────────────┼──────────┼──────────┼─────────────┼───────────────────┤
//   │ Min-Heap       │ O(log n) │ O(log n) │ O(1)        │ O(n)              │
//   │ Simple Wheel   │ O(1)     │ O(1)     │ O(1) amort  │ O(range/granular) │
//   │ Hier. Wheel    │ O(1)     │ O(1)     │ O(1) amort  │ O(levels × slots) │
//   │ Skip List      │ O(log n) │ O(log n) │ O(1)        │ O(n log n)        │
//   └────────────────┴──────────┴──────────┴─────────────┴───────────────────┘
//
//   Hierarchical wheel wins for:
//   - Large number of timers (100K+ delayed messages)
//   - Wide time range (milliseconds to days)
//   - Predictable latency (no O(log n) worst case)
//
// =============================================================================

package broker

import (
	"container/list"
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// CONFIGURATION
// =============================================================================

// Timer wheel level configuration
const (
	// Level 0: Fine-grained timing (10ms buckets)
	level0Buckets = 256
	level0TickMs  = 10
	level0SpanMs  = level0Buckets * level0TickMs // 2560ms = 2.56s

	// Level 1: Second-scale timing
	level1Buckets = 64
	level1SpanMs  = level1Buckets * level0SpanMs // 163840ms ≈ 2.73 min

	// Level 2: Minute-scale timing
	level2Buckets = 64
	level2SpanMs  = level2Buckets * level1SpanMs // 10485760ms ≈ 2.91 hours

	// Level 3: Hour-scale timing (covers up to ~7.76 days)
	level3Buckets = 64
	level3SpanMs  = level3Buckets * level2SpanMs // 671088640ms ≈ 7.76 days

	// Total levels
	numLevels = 4
)

// MaxDelay is the maximum supported delay duration
const MaxDelay = time.Duration(level3SpanMs) * time.Millisecond

// =============================================================================
// ERRORS
// =============================================================================

var (
	// ErrDelayTooLong means the requested delay exceeds MaxDelay
	ErrDelayTooLong = errors.New("delay exceeds maximum supported duration")

	// ErrDelayNegative means the requested delay is negative
	ErrDelayNegative = errors.New("delay cannot be negative")

	// ErrTimerNotFound means the timer ID doesn't exist
	ErrTimerNotFound = errors.New("timer not found")

	// ErrTimerWheelClosed means the timer wheel has been stopped
	ErrTimerWheelClosed = errors.New("timer wheel is closed")
)

// =============================================================================
// TIMER ENTRY
// =============================================================================

// TimerEntry represents a scheduled timer in the wheel.
//
// DESIGN NOTES:
//   - ID is unique and used for cancellation
//   - DeliverAt is absolute time (not relative delay)
//   - Data carries the payload (delayed message info)
//   - element is for efficient list removal
type TimerEntry struct {
	// ID uniquely identifies this timer (for cancellation)
	ID string

	// DeliverAt is when this timer should fire (absolute time)
	DeliverAt time.Time

	// Data is the payload associated with this timer
	// For delayed messages: contains topic, partition, offset
	Data interface{}

	// Internal: pointer to list element for O(1) removal
	element *list.Element

	// Internal: which level and bucket this timer is in
	level  int
	bucket int
}

// =============================================================================
// TIMER CALLBACK
// =============================================================================

// TimerCallback is called when a timer expires.
// The callback receives the timer entry and should process it quickly.
// Long-running operations should be dispatched to a separate goroutine.
type TimerCallback func(entry *TimerEntry)

// =============================================================================
// TIMER WHEEL
// =============================================================================

// TimerWheel implements a hierarchical timer wheel for O(1) timer scheduling.
//
// THREAD SAFETY:
// All public methods are safe for concurrent use.
// The wheel runs a background goroutine that processes ticks.
type TimerWheel struct {
	// levels holds the bucket arrays for each level
	// levels[0] has finest granularity (10ms)
	// levels[3] has coarsest granularity (~2.91 hours)
	levels [numLevels][]*list.List

	// cursors track current position in each level
	// cursor advances on each tick at that level's granularity
	cursors [numLevels]int

	// timers maps timer ID to entry for O(1) lookup/cancellation
	timers map[string]*TimerEntry

	// callback is invoked when timers expire
	callback TimerCallback

	// startTime is when the wheel started (for calculating elapsed ticks)
	startTime time.Time

	// currentTick is the total number of level-0 ticks since start
	currentTick uint64

	// mu protects all wheel state
	mu sync.Mutex

	// tickerDone signals the ticker goroutine to stop
	tickerDone chan struct{}

	// wg waits for background goroutine
	wg sync.WaitGroup

	// logger for timer wheel operations
	logger *slog.Logger

	// closed indicates the wheel has been stopped
	closed atomic.Bool

	// stats
	totalScheduled atomic.Uint64
	totalExpired   atomic.Uint64
	totalCancelled atomic.Uint64
}

// TimerWheelConfig holds timer wheel configuration.
type TimerWheelConfig struct {
	// Callback is invoked when timers expire
	Callback TimerCallback
}

// NewTimerWheel creates and starts a new hierarchical timer wheel.
//
// The wheel immediately starts a background goroutine that:
//   - Ticks every 10ms (level 0 granularity)
//   - Processes expired timers
//   - Cascades timers from higher levels
//
// Call Close() to stop the wheel and release resources.
func NewTimerWheel(config TimerWheelConfig) *TimerWheel {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	tw := &TimerWheel{
		timers:     make(map[string]*TimerEntry),
		callback:   config.Callback,
		startTime:  time.Now(),
		tickerDone: make(chan struct{}),
		logger:     logger,
	}

	// Initialize bucket lists for each level
	bucketCounts := [numLevels]int{level0Buckets, level1Buckets, level2Buckets, level3Buckets}
	for level := 0; level < numLevels; level++ {
		tw.levels[level] = make([]*list.List, bucketCounts[level])
		for i := range tw.levels[level] {
			tw.levels[level][i] = list.New()
		}
	}

	// Start the ticker goroutine
	tw.wg.Add(1)
	go tw.runTicker()

	logger.Info("timer wheel started",
		"levels", numLevels,
		"max_delay", MaxDelay.String(),
		"tick_interval_ms", level0TickMs)

	return tw
}

// =============================================================================
// PUBLIC API
// =============================================================================

// Schedule adds a new timer to the wheel.
//
// PARAMETERS:
//   - id: Unique identifier for cancellation
//   - delay: How long until the timer fires
//   - data: Payload to pass to callback
//
// RETURNS:
//   - Error if delay is invalid or wheel is closed
//
// COMPLEXITY: O(1)
func (tw *TimerWheel) Schedule(id string, delay time.Duration, data interface{}) error {
	if tw.closed.Load() {
		return ErrTimerWheelClosed
	}

	if delay < 0 {
		return ErrDelayNegative
	}

	if delay > MaxDelay {
		return ErrDelayTooLong
	}

	// For zero or very short delays, fire immediately
	if delay == 0 {
		entry := &TimerEntry{
			ID:        id,
			DeliverAt: time.Now(),
			Data:      data,
		}
		tw.totalScheduled.Add(1)
		tw.totalExpired.Add(1)

		// Fire callback immediately (outside lock)
		if tw.callback != nil {
			tw.callback(entry)
		}
		return nil
	}

	deliverAt := time.Now().Add(delay)

	tw.mu.Lock()
	defer tw.mu.Unlock()

	// Check if timer with this ID already exists
	if _, exists := tw.timers[id]; exists {
		// Remove old timer first
		tw.removeTimerLocked(id)
	}

	entry := &TimerEntry{
		ID:        id,
		DeliverAt: deliverAt,
		Data:      data,
	}

	tw.insertTimer(entry)
	tw.timers[id] = entry
	tw.totalScheduled.Add(1)

	return nil
}

// ScheduleAt adds a timer to fire at a specific time.
//
// PARAMETERS:
//   - id: Unique identifier for cancellation
//   - deliverAt: Absolute time when timer should fire
//   - data: Payload to pass to callback
//
// RETURNS:
//   - Error if time is too far in future or wheel is closed
func (tw *TimerWheel) ScheduleAt(id string, deliverAt time.Time, data interface{}) error {
	delay := time.Until(deliverAt)
	if delay <= 0 {
		// Time is in the past or now - fire immediately
		entry := &TimerEntry{
			ID:        id,
			DeliverAt: deliverAt,
			Data:      data,
		}
		tw.totalScheduled.Add(1)
		tw.totalExpired.Add(1)

		// Fire callback immediately (outside lock)
		if tw.callback != nil {
			tw.callback(entry)
		}
		return nil
	}
	return tw.Schedule(id, delay, data)
}

// Cancel removes a timer from the wheel.
//
// PARAMETERS:
//   - id: Timer identifier from Schedule()
//
// RETURNS:
//   - true if timer was found and cancelled
//   - false if timer doesn't exist (already fired or never scheduled)
//
// COMPLEXITY: O(1)
func (tw *TimerWheel) Cancel(id string) bool {
	if tw.closed.Load() {
		return false
	}

	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.removeTimerLocked(id) {
		tw.totalCancelled.Add(1)
		return true
	}
	return false
}

// Get retrieves a timer entry by ID without removing it.
func (tw *TimerWheel) Get(id string) (*TimerEntry, bool) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	entry, exists := tw.timers[id]
	if !exists {
		return nil, false
	}

	// Return a copy to prevent external modification
	return &TimerEntry{
		ID:        entry.ID,
		DeliverAt: entry.DeliverAt,
		Data:      entry.Data,
	}, true
}

// Reschedule changes the delay for an existing timer.
//
// This is more efficient than Cancel + Schedule for extending timeouts.
func (tw *TimerWheel) Reschedule(id string, newDelay time.Duration) error {
	if tw.closed.Load() {
		return ErrTimerWheelClosed
	}

	tw.mu.Lock()
	defer tw.mu.Unlock()

	entry, exists := tw.timers[id]
	if !exists {
		return ErrTimerNotFound
	}

	// Remove from current bucket
	tw.removeFromBucket(entry)

	// Update deliver time and reinsert
	entry.DeliverAt = time.Now().Add(newDelay)
	tw.insertTimer(entry)

	return nil
}

// Size returns the number of active timers.
func (tw *TimerWheel) Size() int {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	return len(tw.timers)
}

// Stats returns timer wheel statistics.
type TimerWheelStats struct {
	TotalScheduled uint64
	TotalExpired   uint64
	TotalCancelled uint64
	CurrentActive  int
	CurrentTick    uint64
}

func (tw *TimerWheel) Stats() TimerWheelStats {
	tw.mu.Lock()
	active := len(tw.timers)
	tick := tw.currentTick
	tw.mu.Unlock()

	return TimerWheelStats{
		TotalScheduled: tw.totalScheduled.Load(),
		TotalExpired:   tw.totalExpired.Load(),
		TotalCancelled: tw.totalCancelled.Load(),
		CurrentActive:  active,
		CurrentTick:    tick,
	}
}

// Close stops the timer wheel and releases resources.
//
// Any pending timers will NOT fire after Close() returns.
// This method blocks until the background goroutine exits.
func (tw *TimerWheel) Close() error {
	if tw.closed.Swap(true) {
		return nil // Already closed
	}

	close(tw.tickerDone)
	tw.wg.Wait()

	tw.logger.Info("timer wheel stopped",
		"total_scheduled", tw.totalScheduled.Load(),
		"total_expired", tw.totalExpired.Load(),
		"total_cancelled", tw.totalCancelled.Load())

	return nil
}

// =============================================================================
// INTERNAL: TIMER INSERTION
// =============================================================================

// insertTimer places a timer in the appropriate bucket.
//
// ALGORITHM:
// 1. Calculate delay in milliseconds
// 2. Find the level that can hold this delay
// 3. Calculate bucket index within that level
// 4. Insert into the bucket's linked list
func (tw *TimerWheel) insertTimer(entry *TimerEntry) {
	delay := time.Until(entry.DeliverAt)
	if delay < 0 {
		delay = 0
	}
	delayMs := delay.Milliseconds()

	// Determine which level and bucket
	level, bucket := tw.calculateBucket(delayMs)
	entry.level = level
	entry.bucket = bucket

	// Insert into bucket list
	bucketList := tw.levels[level][bucket]
	entry.element = bucketList.PushBack(entry)
}

// calculateBucket determines which level and bucket for a given delay.
//
// LEVEL SELECTION:
//   - Level 0: delays up to 2.56s (256 × 10ms)
//   - Level 1: delays up to 2.73min (64 × 2.56s)
//   - Level 2: delays up to 2.91h (64 × 2.73min)
//   - Level 3: delays up to 7.76d (64 × 2.91h)
func (tw *TimerWheel) calculateBucket(delayMs int64) (level int, bucket int) {
	// Adjust delay based on current cursor position
	// This accounts for time already elapsed within the current tick cycle

	if delayMs < level0SpanMs {
		// Level 0: 10ms buckets
		bucket = (tw.cursors[0] + int(delayMs/level0TickMs)) % level0Buckets
		return 0, bucket
	}

	if delayMs < level1SpanMs {
		// Level 1: 2.56s buckets
		bucket = (tw.cursors[1] + int(delayMs/level0SpanMs)) % level1Buckets
		return 1, bucket
	}

	if delayMs < level2SpanMs {
		// Level 2: 2.73min buckets
		bucket = (tw.cursors[2] + int(delayMs/level1SpanMs)) % level2Buckets
		return 2, bucket
	}

	// Level 3: 2.91h buckets
	bucket = (tw.cursors[3] + int(delayMs/level2SpanMs)) % level3Buckets
	return 3, bucket
}

// =============================================================================
// INTERNAL: TIMER REMOVAL
// =============================================================================

// removeTimerLocked removes a timer by ID (must hold lock).
func (tw *TimerWheel) removeTimerLocked(id string) bool {
	entry, exists := tw.timers[id]
	if !exists {
		return false
	}

	tw.removeFromBucket(entry)
	delete(tw.timers, id)
	return true
}

// removeFromBucket removes a timer from its bucket list.
func (tw *TimerWheel) removeFromBucket(entry *TimerEntry) {
	if entry.element != nil {
		bucketList := tw.levels[entry.level][entry.bucket]
		bucketList.Remove(entry.element)
		entry.element = nil
	}
}

// =============================================================================
// INTERNAL: TICK PROCESSING
// =============================================================================

// runTicker is the background goroutine that advances the wheel.
func (tw *TimerWheel) runTicker() {
	defer tw.wg.Done()

	ticker := time.NewTicker(time.Duration(level0TickMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-tw.tickerDone:
			return
		case <-ticker.C:
			tw.tick()
		}
	}
}

// tick processes one tick of the timer wheel.
//
// ALGORITHM:
// 1. Advance level 0 cursor
// 2. Process all timers in the current level 0 bucket
// 3. If level 0 wrapped around:
//   - Advance level 1 cursor
//   - Cascade timers from level 1 to level 0
//
// 4. Repeat cascade for each level that wraps
func (tw *TimerWheel) tick() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	tw.currentTick++

	// Advance level 0 cursor
	tw.cursors[0] = (tw.cursors[0] + 1) % level0Buckets

	// Process expired timers in current level 0 bucket
	tw.processExpiredBucket(0, tw.cursors[0])

	// Check if we need to cascade from higher levels
	if tw.cursors[0] == 0 {
		// Level 0 wrapped - cascade from level 1
		tw.cursors[1] = (tw.cursors[1] + 1) % level1Buckets
		tw.cascadeBucket(1, tw.cursors[1])

		if tw.cursors[1] == 0 {
			// Level 1 wrapped - cascade from level 2
			tw.cursors[2] = (tw.cursors[2] + 1) % level2Buckets
			tw.cascadeBucket(2, tw.cursors[2])

			if tw.cursors[2] == 0 {
				// Level 2 wrapped - cascade from level 3
				tw.cursors[3] = (tw.cursors[3] + 1) % level3Buckets
				tw.cascadeBucket(3, tw.cursors[3])
			}
		}
	}
}

// processExpiredBucket fires all timers in a bucket.
//
// IMPORTANT: We fire all timers in the bucket unconditionally.
// The bucket placement already accounts for timing - if a timer is in this bucket,
// it's time to fire it. Checking DeliverAt again can cause issues if ticks run
// slightly early due to scheduler variance.
func (tw *TimerWheel) processExpiredBucket(level, bucket int) {
	bucketList := tw.levels[level][bucket]

	// Process all timers in bucket
	for e := bucketList.Front(); e != nil; {
		entry := e.Value.(*TimerEntry)
		next := e.Next()

		// Remove from bucket and map
		bucketList.Remove(e)
		delete(tw.timers, entry.ID)
		tw.totalExpired.Add(1)

		// Fire callback (release lock to prevent deadlock)
		if tw.callback != nil {
			tw.mu.Unlock()
			tw.callback(entry)
			tw.mu.Lock()
		}

		e = next
	}
}

// cascadeBucket moves timers from a higher level bucket to lower levels.
//
// When a higher-level bucket's time comes, its timers need to be
// redistributed to lower levels with finer granularity.
func (tw *TimerWheel) cascadeBucket(level, bucket int) {
	bucketList := tw.levels[level][bucket]

	// Move all timers to appropriate lower levels
	for e := bucketList.Front(); e != nil; {
		entry := e.Value.(*TimerEntry)
		next := e.Next()

		// Remove from current bucket
		bucketList.Remove(e)
		entry.element = nil

		// Recalculate position based on remaining time
		remaining := time.Until(entry.DeliverAt)
		if remaining <= 0 {
			// Timer should fire now
			delete(tw.timers, entry.ID)
			tw.totalExpired.Add(1)

			if tw.callback != nil {
				tw.mu.Unlock()
				tw.callback(entry)
				tw.mu.Lock()
			}
		} else {
			// Reinsert at appropriate level
			tw.insertTimer(entry)
		}

		e = next
	}
}

// =============================================================================
// DELAYED MESSAGE DATA
// =============================================================================

// DelayedMessageData holds information about a delayed message.
// This is stored as the Data field in TimerEntry.
type DelayedMessageData struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
}

// =============================================================================
// HELPER: FIRE IMMEDIATELY (for testing and edge cases)
// =============================================================================

// FireNow processes a timer immediately without waiting for tick.
// Used primarily for testing.
func (tw *TimerWheel) FireNow(id string) bool {
	tw.mu.Lock()

	entry, exists := tw.timers[id]
	if !exists {
		tw.mu.Unlock()
		return false
	}

	// Remove from bucket and map
	tw.removeFromBucket(entry)
	delete(tw.timers, id)
	tw.totalExpired.Add(1)

	tw.mu.Unlock()

	// Fire callback
	if tw.callback != nil {
		tw.callback(entry)
	}

	return true
}

// =============================================================================
// CONTEXT-AWARE WAITING
// =============================================================================

// WaitForEmpty blocks until all timers have fired or context is cancelled.
// Useful for graceful shutdown.
func (tw *TimerWheel) WaitForEmpty(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if tw.Size() == 0 {
				return nil
			}
		}
	}
}
