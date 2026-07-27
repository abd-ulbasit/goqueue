package cluster

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestCoordinator_StopLeavesNoWritesBehind is the regression test for the
// intermittent
//
//	TempDir RemoveAll cleanup: .../001/cluster: directory not empty
//
// failure in TestCoordinator_StartAndStop_SingleNodeLifecycle.
//
// THE BUG:
//
//	ControllerElector.startElectionLocked spawned `go ce.requestVotes()`
//	without tracking it. On a single-node cluster that goroutine runs straight
//	through becomeControllerLocked -> Membership.SetController ->
//	persistStateLocked, which writes <dataDir>/cluster/state.json.
//
//	Elector.Stop() joined only its election loop, so Stop() — and with it
//	Coordinator.Stop() — could return while that write was still pending. The
//	test would then finish, t.TempDir() cleanup would delete the tree, and the
//	straggler would recreate cluster/state.json underneath it. FailureDetector
//	had the same shape: an untracked goroutine calling UpdateNodeStatus, which
//	persists the same file.
//
// THE INVARIANT:
//
//	Once Stop() returns, nothing may touch the data directory again. This test
//	states that directly — delete the directory after Stop and require that it
//	stays deleted.
//
// WHY THE LOOP:
//
//	The original failure was a race, reproducing in roughly half of 50-run
//	batches. One iteration would be a weak guard, so this repeats the full
//	lifecycle enough times that a regression is caught reliably while still
//	running in well under a second.
func TestCoordinator_StopLeavesNoWritesBehind(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(ioDiscard{}, &slog.HandlerOptions{Level: slog.LevelError}))

	for i := range 40 {
		dataDir := t.TempDir()

		cfg := (&ClusterConfig{
			NodeID:         "node-A",
			ClientAddress:  "127.0.0.1:18080",
			ClusterAddress: "127.0.0.1:19000",
			QuorumSize:     1,
			// Short timers so the election fires immediately and the racing
			// write is in flight around Stop().
			HeartbeatInterval:  50 * time.Millisecond,
			SuspectTimeout:     150 * time.Millisecond,
			DeadTimeout:        250 * time.Millisecond,
			LeaseTimeout:       300 * time.Millisecond,
			LeaseRenewInterval: 100 * time.Millisecond,
			BootstrapTimeout:   100 * time.Millisecond,
		}).WithDefaults()

		c, err := NewCoordinator(cfg, dataDir, logger)
		if err != nil {
			t.Fatalf("iteration %d: NewCoordinator failed: %v", i, err)
		}

		startCtx, startCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := c.Start(startCtx); err != nil {
			startCancel()
			t.Fatalf("iteration %d: Start failed: %v", i, err)
		}
		startCancel()

		select {
		case <-c.Ready():
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: coordinator never became ready", i)
		}

		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = c.Stop(stopCtx)
		stopCancel()
		if err != nil {
			t.Fatalf("iteration %d: Stop failed: %v", i, err)
		}

		// Stop() has returned. Everything it started must be joined, so
		// clearing the directory has to be final.
		clusterDir := filepath.Join(dataDir, "cluster")
		if err := os.RemoveAll(clusterDir); err != nil {
			t.Fatalf("iteration %d: could not clear %s: %v", i, clusterDir, err)
		}

		// Give any straggler a chance to write, so a failure here is a real
		// leak rather than a test that simply looked too early.
		time.Sleep(5 * time.Millisecond)

		if entries, err := os.ReadDir(clusterDir); err == nil {
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Fatalf("iteration %d: a goroutine wrote to %s after Stop() returned: %v",
				i, clusterDir, names)
		}
	}
}

// TestControllerElector_StopJoinsElectionGoroutine pins the specific leak: the
// elector must not report itself stopped while an election it started is still
// running and still able to persist membership state.
func TestControllerElector_StopJoinsElectionGoroutine(t *testing.T) {
	for i := range 40 {
		dataDir := t.TempDir()

		cfg := (&ClusterConfig{
			NodeID:             "node-A",
			ClientAddress:      "127.0.0.1:18080",
			ClusterAddress:     "127.0.0.1:19000",
			QuorumSize:         1,
			HeartbeatInterval:  10 * time.Millisecond,
			SuspectTimeout:     30 * time.Millisecond,
			DeadTimeout:        50 * time.Millisecond,
			LeaseTimeout:       60 * time.Millisecond,
			LeaseRenewInterval: 20 * time.Millisecond,
		}).WithDefaults()

		node, err := NewNode(cfg)
		if err != nil {
			t.Fatalf("iteration %d: NewNode failed: %v", i, err)
		}

		membership := NewMembership(node, cfg, dataDir)
		if err := membership.RegisterSelf(); err != nil {
			t.Fatalf("iteration %d: RegisterSelf failed: %v", i, err)
		}

		elector := NewControllerElector(node, membership, cfg)
		elector.Start()

		// Drive an election, then stop while it is in flight. On a single-node
		// cluster this reaches SetController, which persists state.json.
		elector.TriggerElection()
		elector.Stop()

		clusterDir := filepath.Join(dataDir, "cluster")
		if err := os.RemoveAll(clusterDir); err != nil {
			t.Fatalf("iteration %d: could not clear %s: %v", i, clusterDir, err)
		}

		time.Sleep(5 * time.Millisecond)

		if _, err := os.Stat(clusterDir); err == nil {
			t.Fatalf("iteration %d: the elector persisted state after Stop() returned", i)
		}
	}
}
