package cluster

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskGroup_WaitJoinsRunningGoroutines(t *testing.T) {
	var g taskGroup
	var done atomic.Int32

	release := make(chan struct{})
	for range 8 {
		if !g.Go(func() {
			<-release
			done.Add(1)
		}) {
			t.Fatal("Go should start work on an open group")
		}
	}

	close(release)
	g.Wait()

	// Wait must not return until every goroutine has finished, so the counter
	// is fully settled by the time we read it.
	if got := done.Load(); got != 8 {
		t.Errorf("finished goroutines = %d, want 8", got)
	}
}

func TestTaskGroup_GoAfterWaitIsRefused(t *testing.T) {
	var g taskGroup
	g.Wait()

	var ran atomic.Bool
	if g.Go(func() { ran.Store(true) }) {
		t.Error("Go should refuse to start work on a closed group")
	}

	// Give a stray goroutine a chance to run, so the assertion below is
	// meaningful rather than just fast.
	time.Sleep(20 * time.Millisecond)
	if ran.Load() {
		t.Error("a refused task must not run")
	}
}

func TestTaskGroup_WaitIsIdempotent(t *testing.T) {
	var g taskGroup
	g.Go(func() {})
	g.Wait()
	g.Wait() // must not panic or block
}

// TestTaskGroup_ConcurrentGoAndWait is the reason the group latches closed
// rather than calling wg.Add at each spawn site: sync.WaitGroup forbids an Add
// that begins once Wait is blocked on a zero counter, and these goroutines are
// spawned from event handlers that race shutdown. Run with -race.
func TestTaskGroup_ConcurrentGoAndWait(t *testing.T) {
	for range 50 {
		var g taskGroup
		var spawners sync.WaitGroup

		for range 8 {
			spawners.Add(1)
			go func() {
				defer spawners.Done()
				g.Go(func() { time.Sleep(time.Millisecond) })
			}()
		}

		g.Wait()
		spawners.Wait()
	}
}
