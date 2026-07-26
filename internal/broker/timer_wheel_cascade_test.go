// =============================================================================
// TIMER WHEEL BUCKET-DRAIN CONCURRENCY TESTS
// =============================================================================
//
// These tests pin down the lock discipline of processExpiredBucket() and
// cascadeBucket(). Both drain a bucket's linked list while dropping tw.mu to
// invoke the user callback, so they are the only places where wheel state can
// be mutated by another goroutine mid-iteration.
//
// The regression they guard against:
//
//	container/list.Remove(e) sets e.next = e.prev = nil and e.list = nil, and
//	Element.Next() returns nil whenever e.list == nil. So any *list.Element
//	held across an unlock is a live handle into memory another goroutine may
//	unlink. If a concurrent Cancel() unlinks the element the drain loop is
//	about to advance to, that element's Next() reports nil and the loop
//	terminates -- silently abandoning every timer still in the bucket.
//
// The tests drive the drain functions directly rather than waiting on the
// 10ms ticker, so the interleaving is deterministic instead of probabilistic.
//
// =============================================================================

package broker

import (
	"container/list"
	"io"
	"log/slog"
	"testing"
	"time"
)

// newBucketTestWheel builds a wheel with no ticker goroutine so a test can
// call tick internals directly and control the interleaving exactly.
func newBucketTestWheel(callback TimerCallback) *TimerWheel {
	tw := &TimerWheel{
		timers:     make(map[string]*TimerEntry),
		callback:   callback,
		startTime:  time.Now(),
		tickerDone: make(chan struct{}),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	bucketCounts := [numLevels]int{level0Buckets, level1Buckets, level2Buckets, level3Buckets}
	for level := 0; level < numLevels; level++ {
		tw.levels[level] = make([]*list.List, bucketCounts[level])
		for i := range tw.levels[level] {
			tw.levels[level][i] = list.New()
		}
	}
	return tw
}

// placeTimer inserts an entry directly into a specific level/bucket, bypassing
// calculateBucket so the test controls exactly which bucket gets drained.
func placeTimer(tw *TimerWheel, id string, level, bucket int, deliverAt time.Time) *TimerEntry {
	entry := &TimerEntry{
		ID:        id,
		DeliverAt: deliverAt,
		level:     level,
		bucket:    bucket,
	}
	entry.element = tw.levels[level][bucket].PushBack(entry)
	tw.timers[id] = entry
	return entry
}

// TestTimerWheel_CascadeSurvivesConcurrentCancel reproduces the orphaning bug.
//
// SETUP: five overdue timers sit in level-1 bucket 3. cascadeBucket sees
// remaining <= 0 for all of them, so each one fires.
//
// RACE: the callback for t0 blocks while holding no lock (cascadeBucket has
// released tw.mu). During that window the test cancels t1 -- the timer whose
// list element the drain loop captured as `next` before unlocking.
//
// BUG: on resume the loop does e = next, and next.Next() returns nil because
// Cancel unlinked it. t2/t3/t4 are never drained: they stay in the bucket and
// in tw.timers, invisible until the level-1 cursor wraps 2.73 minutes later.
func TestTimerWheel_CascadeSurvivesConcurrentCancel(t *testing.T) {
	const (
		level  = 1
		bucket = 3
		total  = 5
	)

	fired := make(map[string]bool, total)
	inCallback := make(chan struct{})
	release := make(chan struct{})

	var tw *TimerWheel
	tw = newBucketTestWheel(func(entry *TimerEntry) {
		fired[entry.ID] = true
		if entry.ID == "t0" {
			// Hold the wheel inside the unlocked callback window.
			close(inCallback)
			<-release
			// Cancel landed while we were parked here.
			_ = tw
		}
	})

	past := time.Now().Add(-time.Second)
	for i := 0; i < total; i++ {
		placeTimer(tw, timerID(i), level, bucket, past)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		tw.mu.Lock()
		tw.cascadeBucket(level, bucket)
		tw.mu.Unlock()
	}()

	<-inCallback
	// tw.mu is free here: cascadeBucket released it to invoke the callback.
	// Cancel t1, the timer whose list element the drain loop used to capture
	// as `next` before unlocking.
	canceled := tw.Cancel("t1")
	close(release)
	<-done

	// The timers that were neither the callback in flight nor the cancel
	// target must always be drained. Under the old loop they were abandoned
	// in the bucket, unreachable until the level-1 cursor wrapped.
	for _, id := range []string{"t0", "t2", "t3", "t4"} {
		if !fired[id] {
			t.Errorf("timer %s was orphaned: never fired", id)
		}
	}

	// Cancel must not be able to report success for a timer that fires anyway.
	// Either it wins (t1 never fires) or it loses (t1 fires, Cancel says false).
	if canceled && fired["t1"] {
		t.Error("Cancel(t1) reported success but t1 fired anyway")
	}
	if !canceled && !fired["t1"] {
		t.Error("Cancel(t1) reported failure but t1 never fired either")
	}

	if got := tw.levels[level][bucket].Len(); got != 0 {
		t.Errorf("bucket still holds %d orphaned timers after cascade, want 0", got)
	}
	if got := len(tw.timers); got != 0 {
		t.Errorf("tw.timers still holds %d entries after cascade, want 0", got)
	}
}

// TestTimerWheel_ExpireSurvivesConcurrentCancel is the level-0 twin of the
// cascade test. processExpiredBucket has the same drain loop, so it has the
// same failure mode: orphaned timers wait a full 2.56s wheel revolution.
func TestTimerWheel_ExpireSurvivesConcurrentCancel(t *testing.T) {
	const (
		level  = 0
		bucket = 7
		total  = 5
	)

	fired := make(map[string]bool, total)
	inCallback := make(chan struct{})
	release := make(chan struct{})

	tw := newBucketTestWheel(func(entry *TimerEntry) {
		fired[entry.ID] = true
		if entry.ID == "t0" {
			close(inCallback)
			<-release
		}
	})

	now := time.Now()
	for i := 0; i < total; i++ {
		placeTimer(tw, timerID(i), level, bucket, now)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		tw.mu.Lock()
		tw.processExpiredBucket(level, bucket)
		tw.mu.Unlock()
	}()

	<-inCallback
	tw.Cancel("t1")
	close(release)
	<-done

	for _, id := range []string{"t0", "t2", "t3", "t4"} {
		if !fired[id] {
			t.Errorf("timer %s was orphaned: never fired", id)
		}
	}
	if got := tw.levels[level][bucket].Len(); got != 0 {
		t.Errorf("bucket still holds %d orphaned timers after expiry, want 0", got)
	}
}

// TestTimerWheel_CascadeCancelDoesNotResurrect covers the second half of the
// same defect. A timer canceled during the callback window used to be handed
// to insertTimer anyway, putting it back in a bucket while absent from
// tw.timers -- a zombie that fires later and can never be canceled again.
func TestTimerWheel_CascadeCancelDoesNotResurrect(t *testing.T) {
	const (
		level  = 1
		bucket = 5
		total  = 4
	)

	fired := make(map[string]int, total)
	inCallback := make(chan struct{})
	release := make(chan struct{})

	tw := newBucketTestWheel(func(entry *TimerEntry) {
		fired[entry.ID]++
		if entry.ID == "t0" {
			close(inCallback)
			<-release
		}
	})

	// t0 is overdue (fires, releasing the lock). The rest are still in the
	// future, so cascade reinserts them at a lower level.
	placeTimer(tw, "t0", level, bucket, time.Now().Add(-time.Second))
	for i := 1; i < total; i++ {
		placeTimer(tw, timerID(i), level, bucket, time.Now().Add(400*time.Millisecond))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		tw.mu.Lock()
		tw.cascadeBucket(level, bucket)
		tw.mu.Unlock()
	}()

	<-inCallback
	canceled := tw.Cancel("t1")
	close(release)
	<-done

	if canceled {
		// Cancel won the race, so t1 must be gone from every structure.
		if _, stillTracked := tw.timers["t1"]; stillTracked {
			t.Error("Cancel(t1) reported success but t1 is still tracked")
		}
		total := 0
		for lvl := range tw.levels {
			for _, b := range tw.levels[lvl] {
				for e := b.Front(); e != nil; e = e.Next() {
					if entry, ok := e.Value.(*TimerEntry); ok && entry.ID == "t1" {
						total++
					}
				}
			}
		}
		if total != 0 {
			t.Errorf("canceled timer t1 was resurrected into %d bucket(s)", total)
		}
	}

	// t2 and t3 were neither canceled nor overdue: they must be reinserted,
	// not dropped on the floor.
	for _, id := range []string{"t2", "t3"} {
		if _, ok := tw.timers[id]; !ok {
			t.Errorf("timer %s was lost during cascade", id)
		}
	}
	if fired["t0"] != 1 {
		t.Errorf("t0 fired %d times, want 1", fired["t0"])
	}
}

func timerID(i int) string {
	return "t" + string(rune('0'+i))
}
