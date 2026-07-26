// =============================================================================
// REPLICA MANAGER SHUTDOWN CONCURRENCY TESTS
// =============================================================================
//
// Stop() used to close(rm.eventCh) while emitEvent() could still be sending on
// it. A send on a closed channel panics, and the select/default in emitEvent
// does not protect against that -- default only covers "buffer full", never
// "channel closed". The window is real in production: Broker.Close() reaches
// ReplicaManager.Stop() while a metadata-change listener goroutine is still
// inside BecomeLeader() -> emitEvent().
//
// AddListener() had the matching problem: it appended to rm.listeners with no
// lock while the dispatcher goroutine ranged over the same slice.
//
// =============================================================================

package cluster

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func newShutdownTestManager(t *testing.T) *ReplicaManager {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewReplicaManager("node-1", ReplicationConfig{}, nil, t.TempDir(), logger)
}

// TestReplicaManager_EmitDuringStop drives emitEvent concurrently with Stop.
// Against the previous implementation this panics with
// "send on closed channel" (or trips the race detector on the channel state).
func TestReplicaManager_EmitDuringStop(t *testing.T) {
	for attempt := 0; attempt < 50; attempt++ {
		rm := newShutdownTestManager(t)

		var wg sync.WaitGroup
		start := make(chan struct{})

		// Emitters race Stop, mirroring BecomeLeader() firing while the broker
		// is tearing down.
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for j := 0; j < 200; j++ {
					rm.emitEvent(ReplicaEvent{
						Type:      ReplicaEventReplicaAdded,
						Topic:     "orders",
						Partition: j,
					})
				}
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := rm.Stop(); err != nil {
				t.Errorf("Stop returned error: %v", err)
			}
		}()

		close(start)
		wg.Wait()
	}
}

// TestReplicaManager_AddListenerDuringDispatch races listener registration
// against the dispatcher goroutine reading the same slice.
func TestReplicaManager_AddListenerDuringDispatch(t *testing.T) {
	rm := newShutdownTestManager(t)
	defer func() { _ = rm.Stop() }()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				rm.emitEvent(ReplicaEvent{Type: ReplicaEventReplicaAdded, Topic: "t"})
			}
		}
	}()

	for i := 0; i < 100; i++ {
		rm.AddListener(func(ReplicaEvent) {})
	}

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestReplicaManager_StopIsIdempotent guards against a double close panic.
// Broker shutdown paths can reach Stop more than once.
func TestReplicaManager_StopIsIdempotent(t *testing.T) {
	rm := newShutdownTestManager(t)

	if err := rm.Stop(); err != nil {
		t.Fatalf("first Stop failed: %v", err)
	}
	if err := rm.Stop(); err != nil {
		t.Fatalf("second Stop failed: %v", err)
	}
}

// TestReplicaManager_EmitAfterStopDoesNotPanic covers the late-emitter case:
// a goroutine that was already in flight reaches emitEvent after Stop returned.
func TestReplicaManager_EmitAfterStopDoesNotPanic(t *testing.T) {
	rm := newShutdownTestManager(t)
	if err := rm.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	rm.emitEvent(ReplicaEvent{Type: ReplicaEventReplicaAdded, Topic: "late"})
}
