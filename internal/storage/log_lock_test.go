// =============================================================================
// LOG LOCKING REGRESSION TESTS
// =============================================================================
//
// Go's sync.RWMutex is NOT reentrant for readers. From the sync package docs:
//
//	"If any goroutine calls Lock while the lock is already held by one or more
//	 readers, concurrent calls to RLock will block until the writer has acquired
//	 (and released) the lock, so as to ensure that the lock eventually becomes
//	 available to the writer."
//
// That writer-preference rule turns recursive read locking into a permanent
// deadlock rather than a slow path:
//
//	goroutine A: l.mu.RLock()            // acquired
//	goroutine B: l.mu.Lock()             // queued, waits for A
//	goroutine A: l.mu.RLock()            // blocked behind B, which waits on A
//
// Nothing breaks the cycle. `go test -race` does not report it because no data
// race occurs: goroutine A simply parks forever. Only a timeout catches it,
// which is why every test in this file runs the work in a goroutine and races
// it against time.After.
//
// =============================================================================

package storage

import (
	"sync"
	"testing"
	"time"
)

// runWithDeadlockTimeout runs fn in its own goroutine and fails the test if it
// has not returned within limit. A recursive-RLock deadlock is unrecoverable,
// so the goroutine is deliberately abandoned rather than waited on.
func runWithDeadlockTimeout(t *testing.T, limit time.Duration, fn func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	select {
	case <-done:
	case <-time.After(limit):
		t.Fatalf("deadlock: operation did not complete within %v", limit)
	}
}

// TestLog_ReadFrom_ConcurrentAppendDoesNotDeadlock pins the exact hazard that
// used to live in Log.ReadFrom: it took l.mu.RLock and then, while still
// holding it, called the *exported* l.EarliestOffset, which takes l.mu.RLock a
// second time. A single Append arriving in that window wedged the partition
// permanently.
//
// The readers loop so that at any instant some goroutine is inside ReadFrom
// between the outer RLock and the inner offset lookup; the writers repeatedly
// queue on l.mu.Lock. One overlap is enough, and the wedge is permanent, so the
// stop signal below is never observed and the WaitGroup never drains.
func TestLog_ReadFrom_ConcurrentAppendDoesNotDeadlock(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	// Deliberately not deferred: Close takes l.mu.Lock, so on failure it would
	// queue behind the wedged readers and hang the test binary instead of
	// letting the timeout below report the deadlock. Closed on success only.

	// ReadFrom returns early when startOffset >= nextOffset, before it ever
	// reaches the offset lookup. Seed the log so the hazardous line executes.
	for i := 0; i < 16; i++ {
		if _, err := log.Append(NewMessage([]byte("k"), []byte("seed"))); err != nil {
			t.Fatalf("seed Append failed: %v", err)
		}
	}

	const (
		readers  = 8
		writers  = 2
		duration = 300 * time.Millisecond
	)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	readErrs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := log.ReadFrom(0, 4); err != nil {
					readErrs <- err
					return
				}
			}
		}()
	}

	writeErrs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := log.Append(NewMessage([]byte("k"), []byte("v"))); err != nil {
					writeErrs <- err
					return
				}
			}
		}()
	}

	time.Sleep(duration)
	close(stop)

	// If ReadFrom re-entered its own read lock while an Append was queued, the
	// reader goroutine is parked forever and this wait never returns.
	runWithDeadlockTimeout(t, 10*time.Second, wg.Wait)

	select {
	case err := <-readErrs:
		t.Fatalf("ReadFrom failed: %v", err)
	default:
	}
	select {
	case err := <-writeErrs:
		t.Fatalf("Append failed: %v", err)
	default:
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestLog_EarliestOffsetLocked_MatchesExported asserts the two entry points
// stay in agreement, so the lock-free variant used on the read path can never
// silently drift from the exported accessor.
func TestLog_EarliestOffsetLocked_MatchesExported(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	check := func(stage string) {
		t.Helper()
		want := log.EarliestOffset()
		log.mu.RLock()
		got := log.earliestOffsetLocked()
		log.mu.RUnlock()
		if got != want {
			t.Errorf("%s: earliestOffsetLocked() = %d, EarliestOffset() = %d", stage, got, want)
		}
	}

	check("empty log")

	for i := 0; i < 8; i++ {
		if _, err := log.Append(NewMessage([]byte("k"), []byte("v"))); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	check("after appends")

	if err := log.DeleteSegmentsBefore(log.NextOffset()); err != nil {
		t.Fatalf("DeleteSegmentsBefore failed: %v", err)
	}
	check("after retention cleanup")
}
