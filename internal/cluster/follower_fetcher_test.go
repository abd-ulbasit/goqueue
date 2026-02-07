package cluster

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFollowerFetcher_HandleOffsetOutOfRange_SnapshotsDisabledResetsToZero(t *testing.T) {
	// =========================================================================
	// FOLLOWER FETCHER: OFFSET-OUT-OF-RANGE WITH SNAPSHOTS DISABLED
	// =========================================================================
	//
	// WHY:
	//   Followers can become "too far behind" (leader truncated log).
	//   If snapshots are disabled, our fallback is a blunt instrument: reset to
	//   offset 0 (data loss) so replication can proceed.
	//
	// THIS TEST ENSURES:
	//   - We take the snapshots-disabled branch without touching the network.
	//   - Fetch offset is reset to 0.
	// =========================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := DefaultReplicationConfig()
	cfg.SnapshotEnabled = false
	rm := NewReplicaManager("f1", cfg, nil, t.TempDir(), logger)
	t.Cleanup(func() { _ = rm.Stop() })

	ffCfg := FollowerFetcherConfig{
		Topic:           "orders",
		Partition:       0,
		LeaderID:        "l1",
		LeaderAddr:      "127.0.0.1:1", // never contacted
		LeaderEpoch:     1,
		ReplicaID:       "f1",
		FetchIntervalMs: 10,
		FetchMaxBytes:   1024,
		SnapshotDir:     "", // snapshots disabled
	}

	ff := NewFollowerFetcher(ffCfg, rm, nil, logger)
	ff.fetchOffset = 10

	ff.handleOffsetOutOfRange(&FetchResponse{LogEndOffset: 100})

	if got := ff.GetFetchOffset(); got != 0 {
		t.Fatalf("fetchOffset=%d want=0", got)
	}
}

func TestFollowerFetcher_HandleOffsetOutOfRange_AheadOfLeaderResetsToLeaderLEO(t *testing.T) {
	// =========================================================================
	// FOLLOWER FETCHER: SAFETY WHEN FOLLOWER IS *AHEAD* OF LEADER
	// =========================================================================
	//
	// WHY:
	//   This is an abnormal scenario (usually implies leader failover + data loss),
	//   but the code explicitly handles it by clamping the follower offset down to
	//   the leader's LEO to regain a consistent state.
	// =========================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := DefaultReplicationConfig()
	cfg.SnapshotEnabled = false
	rm := NewReplicaManager("f1", cfg, nil, t.TempDir(), logger)
	t.Cleanup(func() { _ = rm.Stop() })

	ff := NewFollowerFetcher(FollowerFetcherConfig{SnapshotDir: ""}, rm, nil, logger)
	ff.fetchOffset = 200

	ff.handleOffsetOutOfRange(&FetchResponse{LogEndOffset: 100})

	if got := ff.GetFetchOffset(); got != 100 {
		t.Fatalf("fetchOffset=%d want=100", got)
	}
}

func TestFollowerFetcher_HandleOffsetOutOfRange_SnapshotCatchupDownloadsAndApplies(t *testing.T) {
	// =========================================================================
	// FOLLOWER FETCHER: SNAPSHOT CATCH-UP PATH (END-TO-END)
	// =========================================================================
	//
	// WHY:
	//   Offset-out-of-range is the classic "I'm too far behind" replication failure.
	//   The correct recovery is to request a snapshot, download it, atomically swap
	//   local log state, then resume fetching after the snapshot boundary.
	//
	// THIS TEST EXERCISES:
	//   - RequestSnapshot() HTTP call
	//   - DownloadSnapshot() HTTP call + disk write (including dest dir creation)
	//   - SnapshotManager.LoadSnapshot() extraction + directory swap
	//   - fetchOffset update to LastIncludedOffset + 1
	// =========================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Build an in-memory tar.gz snapshot containing one file.
	snapshotBytes, checksum := buildTestSnapshotTarGz(t, map[string][]byte{
		"00000000000000000000.log": []byte("hello-from-snapshot"),
	})

	// Leader HTTP server that implements the snapshot endpoints used by ClusterClient.
	mux := http.NewServeMux()
	mux.HandleFunc("/cluster/snapshot/create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req SnapshotRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		// For this test we always return a snapshot at offset 99.
		resp := SnapshotResponse{
			ErrorCode: SnapshotErrorNone,
			Snapshot: &Snapshot{
				Topic:              req.Topic,
				Partition:          req.Partition,
				LastIncludedOffset: 99,
				LastIncludedEpoch:  1,
				SizeBytes:          int64(len(snapshotBytes)),
				Checksum:           checksum,
				CreatedAt:          timeNowUTC(),
			},
			DownloadURL: "/cluster/snapshot/orders/0/99",
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&resp)
	})
	mux.HandleFunc("/cluster/snapshot/orders/0/99", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(snapshotBytes)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	leaderAddr := srv.Listener.Addr().String()

	// Cluster client configured to talk to our httptest leader.
	cc := newTestClusterClient(t, srv.Client())

	// ReplicaManager with snapshots enabled, so FollowerFetcher can call LoadSnapshot().
	dataDir := t.TempDir()
	replCfg := DefaultReplicationConfig()
	replCfg.SnapshotEnabled = true
	rm := NewReplicaManager("f1", replCfg, cc, dataDir, logger)
	t.Cleanup(func() { _ = rm.Stop() })

	// Intentionally choose a directory that does *not* exist yet.
	// This catches the classic bug: os.Create(destPath) fails unless we MkdirAll.
	destDir := filepath.Join(t.TempDir(), "cluster-client-download", "nested")

	ffCfg := FollowerFetcherConfig{
		Topic:           "orders",
		Partition:       0,
		LeaderID:        "l1",
		LeaderAddr:      leaderAddr,
		LeaderEpoch:     1,
		ReplicaID:       "f1",
		FetchIntervalMs: 10,
		FetchMaxBytes:   1024,
		SnapshotDir:     destDir,
	}

	ff := NewFollowerFetcher(ffCfg, rm, cc, logger)
	ff.fetchOffset = 10

	ff.handleOffsetOutOfRange(&FetchResponse{LogEndOffset: 100})

	// After applying snapshot at offset 99, we must resume at 100.
	if got := ff.GetFetchOffset(); got != 100 {
		t.Fatalf("fetchOffset=%d want=100", got)
	}

	// Ensure snapshot file exists on disk (download step).
	downloadedPath := filepath.Join(destDir, "snapshot.tar.gz")
	if _, err := os.Stat(downloadedPath); err != nil {
		t.Fatalf("expected downloaded snapshot file at %s: %v", downloadedPath, err)
	}

	// Ensure snapshot contents were extracted into the local log directory.
	logDir := rm.GetLogDir("orders", 0)
	extracted := filepath.Join(logDir, "00000000000000000000.log")
	b, err := os.ReadFile(extracted)
	if err != nil {
		t.Fatalf("expected extracted file at %s: %v", extracted, err)
	}
	if string(b) != "hello-from-snapshot" {
		t.Fatalf("extracted content=%q want=%q", string(b), "hello-from-snapshot")
	}

	stats := ff.Stats()
	if stats.SnapshotDownloads != 1 {
		t.Fatalf("SnapshotDownloads=%d want=1", stats.SnapshotDownloads)
	}
}

func TestFollowerFetcher_BackoffAndOffsetSetters(t *testing.T) {
	// =========================================================================
	// FOLLOWER FETCHER: BACKOFF CALCULATION + OFFSET SETTERS
	// =========================================================================
	//
	// WHY:
	//   We want backoff to be deterministic and capped, and we rely on
	//   GetFetchOffset/SetFetchOffset for snapshot catch-up + debugging.
	// =========================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := DefaultReplicationConfig()
	rm := NewReplicaManager("f1", cfg, nil, t.TempDir(), logger)
	t.Cleanup(func() { _ = rm.Stop() })

	ff := NewFollowerFetcher(FollowerFetcherConfig{}, rm, nil, logger)

	ff.consecutiveErrors = 0
	if got := ff.calculateBackoff(); got != 100*time.Millisecond {
		t.Fatalf("backoff(errors=0)=%v want=%v", got, 100*time.Millisecond)
	}
	ff.consecutiveErrors = 1
	if got := ff.calculateBackoff(); got != 200*time.Millisecond {
		t.Fatalf("backoff(errors=1)=%v want=%v", got, 200*time.Millisecond)
	}
	ff.consecutiveErrors = 6
	if got := ff.calculateBackoff(); got != 5*time.Second {
		t.Fatalf("backoff(errors=6)=%v want=%v (cap)", got, 5*time.Second)
	}

	ff.SetFetchOffset(42)
	if got := ff.GetFetchOffset(); got != 42 {
		t.Fatalf("GetFetchOffset=%d want=42", got)
	}
}

func TestFollowerFetcher_FetchLoop_SuccessUpdatesOffsetAndReplicaState(t *testing.T) {
	// =========================================================================
	// FOLLOWER FETCHER: END-TO-END SUCCESSFUL FETCH LOOP
	// =========================================================================
	//
	// WHAT THIS TEST COVERS:
	//   - Start() starts the background fetchLoop()
	//   - doFetch() success path (EMA latency, offset advance)
	//   - ApplyFetchedMessages() updates local follower LEO/HW
	// =========================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Leader HTTP server that implements /cluster/fetch.
	var callMu sync.Mutex
	callCount := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/cluster/fetch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req FetchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		callMu.Lock()
		callCount++
		thisCall := callCount
		callMu.Unlock()

		resp := FetchResponse{
			Topic:         req.Topic,
			Partition:     req.Partition,
			ErrorCode:     FetchErrorNone,
			HighWatermark: 1,
			LogEndOffset:  1,
			LeaderEpoch:   req.LeaderEpoch,
		}

		// First call returns one message to exercise the "messages fetched" branch.
		if thisCall == 1 {
			resp.Messages = []ReplicatedMessage{{
				Offset:    req.FromOffset,
				Timestamp: time.Now().UnixMilli(),
				Key:       []byte("k"),
				Value:     []byte("v"),
				Priority:  1,
			}}
			resp.NextFetchOffset = req.FromOffset + 1
		} else {
			resp.NextFetchOffset = req.FromOffset
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&resp)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()
	leaderAddr := srv.Listener.Addr().String()

	cc := newTestClusterClient(t, srv.Client())

	replCfg := DefaultReplicationConfig()
	replCfg.SnapshotEnabled = false
	replCfg.FetchIntervalMs = 5
	replCfg.FetchMaxBytes = 1024

	rm := NewReplicaManager("f1", replCfg, cc, t.TempDir(), logger)
	defer func() { _ = rm.Stop() }()

	if err := rm.BecomeFollower("orders", 0, "l1", leaderAddr, 1, 0); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}

	// Wait until at least one fetch applied and the replica state advanced.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		state := rm.GetReplicaState("orders", 0)
		if state != nil && state.LogEndOffset >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	state := rm.GetReplicaState("orders", 0)
	t.Fatalf("timed out waiting for fetch loop; state=%+v", state)
}

func TestFollowerFetcher_NotLeader_DoesNotDeadlockOnStop(t *testing.T) {
	// =========================================================================
	// FOLLOWER FETCHER: TERMINAL ERROR INSIDE FETCH LOOP MUST NOT DEADLOCK
	// =========================================================================
	//
	// BUG THIS CATCHES:
	//   If handleResponseError() calls Stop() from inside fetchLoop(), Stop()
	//   waits on ff.wg which includes the *current* fetchLoop goroutine.
	//   That is a self-wait deadlock.
	//
	// EXPECTATION:
	//   A NotLeader response should cause the fetcher to stop without hanging,
	//   and ReplicaManager.Stop() must complete promptly.
	// =========================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/cluster/fetch", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&FetchResponse{
			Topic:        "orders",
			Partition:    0,
			ErrorCode:    FetchErrorNotLeader,
			ErrorMessage: "not leader",
			LeaderEpoch:  2,
		})
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()
	leaderAddr := srv.Listener.Addr().String()

	cc := newTestClusterClient(t, srv.Client())

	replCfg := DefaultReplicationConfig()
	replCfg.SnapshotEnabled = false
	replCfg.FetchIntervalMs = 5

	rm := NewReplicaManager("f1", replCfg, cc, t.TempDir(), logger)

	if err := rm.BecomeFollower("orders", 0, "l1", leaderAddr, 1, 0); err != nil {
		_ = rm.Stop()
		t.Fatalf("BecomeFollower: %v", err)
	}

	// Ensure at least one fetch attempt happened so we know the NotLeader path ran.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if calls.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() == 0 {
		_ = rm.Stop()
		t.Fatalf("expected at least one /cluster/fetch call")
	}

	done := make(chan struct{})
	go func() {
		_ = rm.Stop()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(1 * time.Second):
		t.Fatalf("replica manager Stop() appears hung (possible fetcher deadlock)")
	}
}

// buildTestSnapshotTarGz builds a gzipped tar archive in-memory.
// We keep it local to this test file because different tests may want
// different file sets.
func buildTestSnapshotTarGz(t *testing.T, files map[string][]byte) ([]byte, string) {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tr := tar.NewWriter(gz)

	for name, content := range files {
		hdr := &tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(content)),
		}
		if err := tr.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader(%s): %v", name, err)
		}
		if _, err := tr.Write(content); err != nil {
			t.Fatalf("Write(%s): %v", name, err)
		}
	}

	if err := tr.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	payload := buf.Bytes()
	h := sha256.Sum256(payload)
	checksum := hex.EncodeToString(h[:])
	return payload, checksum
}

// newTestClusterClient creates a ClusterClient suitable for talking to an
// httptest server. We intentionally bypass membership lookups because the
// FollowerFetcher code paths we test use explicit leader addresses.
func newTestClusterClient(t *testing.T, httpClient *http.Client) *ClusterClient {
	t.Helper()

	cfg := (&ClusterConfig{NodeID: "f1", ClusterAddress: "127.0.0.1:0", QuorumSize: 1}).WithDefaults()
	node, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	m := NewMembership(node, cfg, t.TempDir())
	if err := m.RegisterSelf(); err != nil {
		t.Fatalf("RegisterSelf: %v", err)
	}

	cc := NewClusterClient(node, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	cc.httpClient = httpClient
	return cc
}

// timeNowUTC is a small helper so tests don't depend on local timezone.
func timeNowUTC() time.Time { return time.Now().UTC() }

// Ensure we don't accidentally leak a background context in this file.
var _ = context.Background
