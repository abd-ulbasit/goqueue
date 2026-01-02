// =============================================================================
// TIMER WHEEL TESTS
// =============================================================================
//
// Tests for the hierarchical timer wheel implementation.
//
// TEST CATEGORIES:
//   - Basic scheduling and cancellation
//   - Timing accuracy
//   - Edge cases (zero delay, max delay)
//   - Concurrent access
//   - Cascading behavior
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
// BASIC TESTS
// =============================================================================

func TestTimerWheel_ScheduleAndFire(t *testing.T) {
	// WHAT: Basic timer scheduling and firing
	// WHY: Verify the fundamental timer wheel functionality works

	fired := make(chan string, 10)

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fired <- entry.ID
		},
	})
	defer tw.Close()

	// Schedule a timer for 50ms
	err := tw.Schedule("timer-1", 50*time.Millisecond, "test-data")
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	// Verify timer is tracked
	if tw.Size() != 1 {
		t.Errorf("expected size 1, got %d", tw.Size())
	}

	// Wait for timer to fire
	select {
	case id := <-fired:
		if id != "timer-1" {
			t.Errorf("expected timer-1, got %s", id)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer did not fire within timeout")
	}

	// Verify timer is removed
	if tw.Size() != 0 {
		t.Errorf("expected size 0 after fire, got %d", tw.Size())
	}
}

func TestTimerWheel_Cancel(t *testing.T) {
	// WHAT: Timer cancellation
	// WHY: Must be able to cancel scheduled delays

	fired := make(chan string, 10)

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fired <- entry.ID
		},
	})
	defer tw.Close()

	// Schedule a timer
	err := tw.Schedule("timer-1", 100*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	// Cancel it
	cancelled := tw.Cancel("timer-1")
	if !cancelled {
		t.Error("expected Cancel to return true")
	}

	// Verify it's removed
	if tw.Size() != 0 {
		t.Errorf("expected size 0, got %d", tw.Size())
	}

	// Wait and verify it doesn't fire
	select {
	case id := <-fired:
		t.Errorf("cancelled timer fired: %s", id)
	case <-time.After(200 * time.Millisecond):
		// Good - timer did not fire
	}
}

func TestTimerWheel_CancelNonExistent(t *testing.T) {
	// WHAT: Cancelling a non-existent timer
	// WHY: Should handle gracefully without error

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {},
	})
	defer tw.Close()

	cancelled := tw.Cancel("non-existent")
	if cancelled {
		t.Error("expected Cancel to return false for non-existent timer")
	}
}

func TestTimerWheel_ZeroDelay(t *testing.T) {
	// WHAT: Timer with zero delay
	// WHY: Should fire on next tick (almost immediately)

	fired := make(chan string, 10)

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fired <- entry.ID
		},
	})
	defer tw.Close()

	err := tw.Schedule("immediate", 0, nil)
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	// Should fire very quickly
	select {
	case id := <-fired:
		if id != "immediate" {
			t.Errorf("expected immediate, got %s", id)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("zero delay timer did not fire")
	}
}

func TestTimerWheel_NegativeDelay(t *testing.T) {
	// WHAT: Timer with negative delay
	// WHY: Should return error

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {},
	})
	defer tw.Close()

	err := tw.Schedule("negative", -1*time.Second, nil)
	if err != ErrDelayNegative {
		t.Errorf("expected ErrDelayNegative, got %v", err)
	}
}

func TestTimerWheel_MaxDelay(t *testing.T) {
	// WHAT: Timer at maximum delay
	// WHY: Should accept delays up to MaxDelay

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {},
	})
	defer tw.Close()

	// This should succeed
	err := tw.Schedule("max-delay", MaxDelay-time.Second, nil)
	if err != nil {
		t.Errorf("expected success for MaxDelay, got %v", err)
	}
}

func TestTimerWheel_ExceedMaxDelay(t *testing.T) {
	// WHAT: Timer exceeding maximum delay
	// WHY: Should return error

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {},
	})
	defer tw.Close()

	err := tw.Schedule("too-long", MaxDelay+time.Hour, nil)
	if err != ErrDelayTooLong {
		t.Errorf("expected ErrDelayTooLong, got %v", err)
	}
}

// =============================================================================
// TIMING ACCURACY TESTS
// =============================================================================

func TestTimerWheel_TimingAccuracy(t *testing.T) {
	// WHAT: Verify timer fires within acceptable tolerance
	// WHY: 10ms tick means ~20ms tolerance is reasonable

	fired := make(chan time.Duration, 1)
	start := time.Now()

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fired <- time.Since(start)
		},
	})
	defer tw.Close()

	targetDelay := 100 * time.Millisecond
	err := tw.Schedule("timing-test", targetDelay, nil)
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	select {
	case actual := <-fired:
		// Allow 30ms tolerance
		tolerance := 30 * time.Millisecond
		if actual < targetDelay-tolerance || actual > targetDelay+tolerance {
			t.Errorf("timing off: expected ~%v, got %v", targetDelay, actual)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timer did not fire")
	}

}

func TestTimerWheel_MultipleTimers(t *testing.T) {
	// WHAT: Multiple timers with different delays
	// WHY: Verify proper ordering of timer expiration

	results := make(chan string, 10)

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			results <- entry.ID
		},
	})
	defer tw.Close()

	// Schedule in reverse order
	tw.Schedule("300ms", 300*time.Millisecond, nil)
	tw.Schedule("100ms", 100*time.Millisecond, nil)
	tw.Schedule("200ms", 200*time.Millisecond, nil)

	// Collect results
	var order []string
	timeout := time.After(500 * time.Millisecond)

	for i := 0; i < 3; i++ {
		select {
		case id := <-results:
			order = append(order, id)
		case <-timeout:
			t.Fatalf("only received %d timers", len(order))
		}
	}

	// Verify order
	expected := []string{"100ms", "200ms", "300ms"}
	for i, id := range order {
		if id != expected[i] {
			t.Errorf("position %d: expected %s, got %s", i, expected[i], id)
		}
	}
}

// =============================================================================
// CONCURRENCY TESTS
// =============================================================================

func TestTimerWheel_ConcurrentSchedule(t *testing.T) {
	// WHAT: Concurrent timer scheduling
	// WHY: Scheduler must be thread-safe

	var fired atomic.Int64

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fired.Add(1)
		},
	})
	defer tw.Close()

	// Schedule 100 timers concurrently
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := string(rune('a'+n%26)) + string(rune('0'+n/26))
			tw.Schedule(id, 50*time.Millisecond, nil)
		}(i)
	}
	wg.Wait()

	// Wait for all to fire
	time.Sleep(200 * time.Millisecond)

	if fired.Load() != 100 {
		t.Errorf("expected 100 timers to fire, got %d", fired.Load())
	}
}

func TestTimerWheel_ConcurrentCancelAndFire(t *testing.T) {
	// WHAT: Concurrent cancellation while timers fire
	// WHY: Must handle race between cancel and fire

	var fired, cancelled atomic.Int64

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fired.Add(1)
		},
	})
	defer tw.Close()

	// Schedule 50 timers
	for i := 0; i < 50; i++ {
		tw.Schedule(string(rune('a'+i)), 50*time.Millisecond, nil)
	}

	// Cancel half concurrently
	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if tw.Cancel(string(rune('a' + n))) {
				cancelled.Add(1)
			}
		}(i)
	}
	wg.Wait()

	// Wait for remaining to fire
	time.Sleep(200 * time.Millisecond)

	// Verify: cancelled + fired should equal 50
	total := cancelled.Load() + fired.Load()
	if total != 50 {
		t.Errorf("expected cancelled(%d) + fired(%d) = 50, got %d",
			cancelled.Load(), fired.Load(), total)
	}
}

// =============================================================================
// RESCHEDULE TESTS
// =============================================================================

func TestTimerWheel_Reschedule(t *testing.T) {
	// WHAT: Rescheduling an existing timer
	// WHY: Must support extending delays

	fired := make(chan string, 1)
	start := time.Now()

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fired <- entry.ID
		},
	})
	defer tw.Close()

	// Schedule for 50ms
	tw.Schedule("reschedule-test", 50*time.Millisecond, nil)

	// Immediately reschedule for 150ms
	time.Sleep(10 * time.Millisecond) // Small delay to ensure first schedule processed
	err := tw.Reschedule("reschedule-test", 150*time.Millisecond)
	if err != nil {
		t.Fatalf("Reschedule failed: %v", err)
	}

	// Should fire around 160ms (10ms + 150ms), not 50ms
	select {
	case <-fired:
		elapsed := time.Since(start)
		if elapsed < 100*time.Millisecond {
			t.Errorf("timer fired too early: %v", elapsed)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("rescheduled timer did not fire")
	}
}

func TestTimerWheel_RescheduleNonExistent(t *testing.T) {
	// WHAT: Rescheduling non-existent timer
	// WHY: Should return error

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {},
	})
	defer tw.Close()

	err := tw.Reschedule("non-existent", 100*time.Millisecond)
	if err != ErrTimerNotFound {
		t.Errorf("expected ErrTimerNotFound, got %v", err)
	}
}

// =============================================================================
// GET TESTS
// =============================================================================

func TestTimerWheel_Get(t *testing.T) {
	// WHAT: Retrieving timer entry by ID
	// WHY: Needed for inspection without removal

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {},
	})
	defer tw.Close()

	deliverAt := time.Now().Add(1 * time.Hour)
	tw.ScheduleAt("get-test", deliverAt, "my-data")

	entry, exists := tw.Get("get-test")
	if !exists {
		t.Fatal("expected entry to exist")
	}

	if entry.ID != "get-test" {
		t.Errorf("expected ID get-test, got %s", entry.ID)
	}

	// DeliverAt should be close to what we set
	if entry.DeliverAt.Sub(deliverAt) > time.Second {
		t.Errorf("DeliverAt mismatch: expected %v, got %v", deliverAt, entry.DeliverAt)
	}
}

func TestTimerWheel_GetNonExistent(t *testing.T) {
	// WHAT: Getting non-existent timer
	// WHY: Should return false

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {},
	})
	defer tw.Close()

	_, exists := tw.Get("non-existent")
	if exists {
		t.Error("expected non-existent timer to return false")
	}
}

// =============================================================================
// STATS TESTS
// =============================================================================

func TestTimerWheel_Stats(t *testing.T) {
	// WHAT: Statistics tracking
	// WHY: Observability is important

	var fired atomic.Int64

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fired.Add(1)
		},
	})
	defer tw.Close()

	// Schedule some timers
	tw.Schedule("s1", 30*time.Millisecond, nil)
	tw.Schedule("s2", 30*time.Millisecond, nil)
	tw.Schedule("s3", 30*time.Millisecond, nil)
	tw.Cancel("s3")

	// Wait for timers to fire
	time.Sleep(100 * time.Millisecond)

	stats := tw.Stats()

	if stats.TotalScheduled != 3 {
		t.Errorf("expected TotalScheduled=3, got %d", stats.TotalScheduled)
	}

	if stats.TotalCancelled != 1 {
		t.Errorf("expected TotalCancelled=1, got %d", stats.TotalCancelled)
	}

	if stats.TotalExpired != 2 {
		t.Errorf("expected TotalExpired=2, got %d", stats.TotalExpired)
	}
}

// =============================================================================
// CLOSE TESTS
// =============================================================================

func TestTimerWheel_Close(t *testing.T) {
	// WHAT: Closing the timer wheel
	// WHY: Must stop cleanly without firing pending timers

	fired := make(chan string, 10)

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fired <- entry.ID
		},
	})

	// Schedule a timer far in the future
	tw.Schedule("pending", 1*time.Hour, nil)

	// Close immediately
	err := tw.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Scheduling after close should fail
	err = tw.Schedule("after-close", 1*time.Second, nil)
	if err != ErrTimerWheelClosed {
		t.Errorf("expected ErrTimerWheelClosed, got %v", err)
	}

	// Pending timer should not fire
	select {
	case id := <-fired:
		t.Errorf("pending timer fired after close: %s", id)
	case <-time.After(50 * time.Millisecond):
		// Good
	}
}

func TestTimerWheel_DoubleClose(t *testing.T) {
	// WHAT: Closing twice
	// WHY: Should be idempotent

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {},
	})

	err1 := tw.Close()
	err2 := tw.Close()

	if err1 != nil || err2 != nil {
		t.Errorf("double close should not error: err1=%v, err2=%v", err1, err2)
	}
}

// =============================================================================
// LEVEL CASCADING TESTS
// =============================================================================

func TestTimerWheel_LevelCascade(t *testing.T) {
	// WHAT: Timer that starts in higher level cascades down
	// WHY: Verify hierarchical behavior works correctly

	// Skip in short mode - this takes a while
	if testing.Short() {
		t.Skip("skipping cascade test in short mode")
	}

	fired := make(chan string, 1)

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fired <- entry.ID
		},
	})
	defer tw.Close()

	// Schedule for 3 seconds (will be in level 1 initially)
	// level0SpanMs = 2560ms, so 3s > level 0 capacity
	tw.Schedule("cascade", 3*time.Second, nil)

	// Wait for it to fire
	select {
	case id := <-fired:
		if id != "cascade" {
			t.Errorf("expected cascade, got %s", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cascaded timer did not fire")
	}
}

// =============================================================================
// REPLACE EXISTING TIMER TEST
// =============================================================================

func TestTimerWheel_ScheduleReplacesExisting(t *testing.T) {
	// WHAT: Scheduling with same ID replaces existing timer
	// WHY: Useful for updating delays without explicit cancel

	var fireCount atomic.Int64

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fireCount.Add(1)
		},
	})
	defer tw.Close()

	// Schedule, then replace
	tw.Schedule("replace", 100*time.Millisecond, nil)
	tw.Schedule("replace", 50*time.Millisecond, nil) // This replaces

	// Should only have one timer
	if tw.Size() != 1 {
		t.Errorf("expected size 1, got %d", tw.Size())
	}

	// Wait for fire
	time.Sleep(200 * time.Millisecond)

	// Should only fire once
	if fireCount.Load() != 1 {
		t.Errorf("expected 1 fire, got %d", fireCount.Load())
	}
}

// =============================================================================
// FIRE NOW TEST
// =============================================================================

func TestTimerWheel_FireNow(t *testing.T) {
	// WHAT: Immediately firing a timer
	// WHY: Useful for testing and special cases

	fired := make(chan string, 1)

	tw := NewTimerWheel(TimerWheelConfig{
		Callback: func(entry *TimerEntry) {
			fired <- entry.ID
		},
	})
	defer tw.Close()

	// Schedule far in the future
	tw.Schedule("fire-now", 1*time.Hour, nil)

	// Fire immediately
	if !tw.FireNow("fire-now") {
		t.Error("FireNow should return true")
	}

	// Should have fired
	select {
	case id := <-fired:
		if id != "fire-now" {
			t.Errorf("expected fire-now, got %s", id)
		}
	default:
		t.Error("timer should have fired immediately")
	}

	// Timer should be removed
	if tw.Size() != 0 {
		t.Errorf("expected size 0, got %d", tw.Size())
	}
}
