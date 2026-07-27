// =============================================================================
// TASK GROUP - TRACKING GOROUTINES SO Stop() ACTUALLY STOPS
// =============================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ WHY THIS EXISTS                                                             │
// │                                                                             │
// │ Cluster components spawn goroutines from two places: their Start(), and     │
// │ event handlers that fire later. The Start() ones were tracked by a          │
// │ sync.WaitGroup and joined in Stop(). The later ones were bare `go f()`      │
// │ and were not joined by anything.                                            │
// │                                                                             │
// │ Several of those untracked goroutines write to the data directory —         │
// │ Membership.SetController and UpdateNodeStatus both persist cluster/         │
// │ state.json. So Stop() could return while a write was still in flight, and   │
// │ the file would reappear after the caller believed shutdown was complete.    │
// │                                                                             │
// │ In tests that surfaces as t.TempDir() cleanup failing with "directory not   │
// │ empty". In production it is a write racing teardown.                        │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// WHY NOT JUST CALL wg.Add(1) AT EACH `go` SITE:
//
//	sync.WaitGroup forbids an Add that starts while Wait is already blocked and
//	the counter has reached zero. These goroutines are spawned from event
//	handlers, which can fire concurrently with Stop — exactly that case. The
//	group therefore latches closed before waiting, so a spawn attempt that
//	loses the race is refused rather than racing the counter.
//
// =============================================================================

package cluster

import "sync"

// taskGroup tracks background goroutines so a component's Stop() can join them.
//
// The zero value is ready to use.
type taskGroup struct {
	// mu guards closed. It is held for reading while a goroutine is being
	// registered, and for writing while the group latches closed, so a
	// registration can never begin after Wait has started.
	mu sync.RWMutex

	// closed is set by Wait and never cleared. A group is single-use: once a
	// component has stopped it does not start again.
	closed bool

	wg sync.WaitGroup
}

// Go runs fn in a tracked goroutine.
//
// It reports whether fn was started. A false return means Wait has already
// been called and the component is shutting down, so fn is dropped — the
// caller is asking for work that shutdown is in the middle of tearing down.
func (g *taskGroup) Go(fn func()) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.closed {
		return false
	}

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		fn()
	}()

	return true
}

// Wait latches the group closed and blocks until every goroutine started
// through Go has returned.
//
// It is safe to call more than once, and safe to call concurrently with Go.
// Callers should cancel whatever context their goroutines select on before
// calling Wait, otherwise this blocks for as long as they run.
func (g *taskGroup) Wait() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()

	g.wg.Wait()
}
