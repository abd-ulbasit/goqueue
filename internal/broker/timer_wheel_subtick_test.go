// =============================================================================
// TIMER WHEEL SUB-TICK PLACEMENT TESTS
// =============================================================================
//
// tick() advances cursors[0] and then drains the bucket it lands on, so
// cursors[0] always names the bucket that was most recently processed.
// calculateBucket placed a timer at (cursors[0] + delayMs/10) % 256, and
// integer division sends every delay under one tick to offset 0 -- i.e. into
// the bucket that was just drained, which is not revisited until the cursor
// completes a full 256-tick revolution.
//
// The result was a 2.56 second delivery delay for:
//
//   - every past-due timer recovered at startup (remaining delay 0), and
//   - any schedule whose remaining delay dropped below 10ms between the caller
//     computing DeliverAt and insertTimer running -- Scheduler.scheduleAt
//     writes a persistent delay-index record in between, which is a disk write.
//
// Nothing errored and nothing was lost; the message simply arrived 2.56s late,
// which is 256x the wheel's advertised 10ms granularity.
//
// These tests drive placement and tick directly, so they are deterministic
// rather than dependent on how long a disk write happens to take.
//
// =============================================================================

package broker

import (
	"testing"
	"time"
)

// TestTimerWheel_SubTickDelayNeverLandsOnCurrentBucket checks the placement
// arithmetic in isolation, across the whole sub-tick range and the boundaries
// of every level.
func TestTimerWheel_SubTickDelayNeverLandsOnCurrentBucket(t *testing.T) {
	tw := newBucketTestWheel(nil)

	// Park the cursors somewhere non-zero so an off-by-one cannot pass by
	// accident on a fresh wheel.
	tw.cursors = [numLevels]int{7, 3, 5, 2}

	cases := []struct {
		name    string
		delayMs int64
		level   int
	}{
		{"zero (past due)", 0, 0},
		{"1ms", 1, 0},
		{"9ms (just under one tick)", 9, 0},
		{"10ms (exactly one tick)", 10, 0},
		{"11ms", 11, 0},
		{"level 0 upper bound", level0SpanMs - 1, 0},
		{"level 1 lower bound", level0SpanMs, 1},
		{"level 2 lower bound", level1SpanMs, 2},
		{"level 3 lower bound", level2SpanMs, 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			level, bucket := tw.calculateBucket(tc.delayMs)
			if level != tc.level {
				t.Fatalf("level = %d, want %d", level, tc.level)
			}
			if bucket == tw.cursors[level] {
				t.Fatalf("delay %dms placed at level %d bucket %d, which is the "+
					"cursor position -- that bucket was just drained and is not "+
					"revisited for a full revolution",
					tc.delayMs, level, bucket)
			}
		})
	}
}

// TestTimerWheel_SubTickDelayFiresOnNextTick is the end-to-end statement: a
// timer that is already due fires on the very next tick, not a revolution
// later.
func TestTimerWheel_SubTickDelayFiresOnNextTick(t *testing.T) {
	fired := make(chan string, 4)
	tw := newBucketTestWheel(func(entry *TimerEntry) {
		fired <- entry.ID
	})

	// A timer whose remaining delay has already fallen to zero. insertTimer
	// clamps a negative delay to 0, so this is exactly the shape of a past-due
	// timer restored by Scheduler.RecoverTopic.
	tw.mu.Lock()
	entry := &TimerEntry{ID: "past-due", DeliverAt: time.Now().Add(-time.Hour)}
	tw.timers[entry.ID] = entry
	tw.insertTimer(entry)
	placedBucket := entry.bucket
	cursor := tw.cursors[0]
	tw.mu.Unlock()

	if placedBucket == cursor {
		t.Fatalf("past-due timer landed in bucket %d, the bucket tick() just drained; "+
			"it would wait %d ticks (%v) to be seen again",
			placedBucket, level0Buckets,
			time.Duration(level0SpanMs)*time.Millisecond)
	}

	tw.tick()

	select {
	case id := <-fired:
		if id != "past-due" {
			t.Fatalf("fired timer ID = %q, want %q", id, "past-due")
		}
	default:
		t.Fatal("past-due timer did not fire on the next tick")
	}

	tw.mu.Lock()
	remaining := len(tw.timers)
	tw.mu.Unlock()
	if remaining != 0 {
		t.Errorf("timers still registered after firing: %d, want 0", remaining)
	}
}
