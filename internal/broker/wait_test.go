package broker

import (
	"testing"
	"time"
)

// =============================================================================
// TEST TIMING HELPERS
// =============================================================================
//
// time.Sleep(d) guarantees only that *at least* d has elapsed. On a loaded
// machine it routinely overshoots by tens of milliseconds. Any test shaped as
//
//	schedule(20ms); time.Sleep(50ms); assertHappened()
//
// is therefore asserting that the host scheduler woke this goroutine promptly,
// not that the code under test worked. Those tests fail under `go test ./...`
// with other packages competing for cores, and pass in isolation.
//
// The fix is to separate the two kinds of assertion:
//
//   - "X eventually happens" is a liveness property. Poll for it with a
//     deadline far larger than the expected latency (waitFor). A slow host
//     makes the test slower, never red.
//   - "X has NOT happened yet" is a safety property with a real deadline. It
//     needs a bare sleep, but the sleep must be short relative to the window
//     it is testing, so overshoot cannot cross the boundary.

// waitFor polls cond every 2ms until it returns true or timeout elapses.
// It reports whether cond became true.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// mustEventually fails the test with msg if cond does not become true within
// timeout.
func mustEventually(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	if !waitFor(timeout, cond) {
		t.Fatalf("%s (not satisfied within %v)", msg, timeout)
	}
}
