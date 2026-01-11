package cluster

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotManager_CreateLoadDeleteAndStats(t *testing.T) {
	// ============================================================================
	// SNAPSHOT MANAGER TEST (LEADER + FOLLOWER PATHS)
	// ============================================================================
	//
	// WHY THIS TEST EXISTS:
	//   Snapshotting is a *critical* reliability feature for replication.
	//   If snapshots are broken, a lagging follower may never catch up.
	//
	// WHAT WE VERIFY:
	//   1) Leader can create a tar.gz snapshot from a log directory (including nested files)
	//   2) Snapshot metadata is recorded and discoverable via GetSnapshot/GetSnapshotPath
	//   3) Follower can load (verify checksum + extract + swap directories)
	//   4) Corruption is detected via checksum mismatch
	//   5) Cleanup/Delete remove snapshot files and internal bookkeeping
	//
	// FAILURE MODES CAUGHT:
	//   - Missing/incorrect tar header paths
	//   - Incomplete gzip/tar close leading to corrupted archives
	//   - Checksum computation bugs
	//   - Snapshot extraction not preserving directory structure
	//   - Unsafe directory swap leaving broken state
	// ============================================================================

	dataDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	sm := NewSnapshotManager(dataDir, logger)

	// Build a fake partition log directory with nested files.
	logDir := filepath.Join(dataDir, "logs", "orders", "0")
	if err := os.MkdirAll(filepath.Join(logDir, "segments"), 0o755); err != nil {
		t.Fatalf("mkdir logDir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(logDir, "00000000000000000000.log"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write log file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "segments", "00000000000000000000.idx"), []byte("world\n"), 0o644); err != nil {
		t.Fatalf("write idx file: %v", err)
	}

	// Create a snapshot as if we are the leader.
	snap, err := sm.CreateSnapshot("orders", 0, logDir, 123, 7)
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap == nil {
		t.Fatalf("expected snapshot, got nil")
	}
	if snap.Topic != "orders" || snap.Partition != 0 {
		t.Fatalf("unexpected snapshot identity: topic=%q partition=%d", snap.Topic, snap.Partition)
	}
	if snap.LastIncludedOffset != 123 || snap.LastIncludedEpoch != 7 {
		t.Fatalf("unexpected snapshot offsets: lastOffset=%d epoch=%d", snap.LastIncludedOffset, snap.LastIncludedEpoch)
	}
	if snap.Checksum == "" {
		t.Fatalf("expected checksum to be set")
	}

	if _, err := os.Stat(snap.FilePath); err != nil {
		t.Fatalf("expected snapshot file to exist: %v", err)
	}

	// Ensure discovery helpers work.
	if got := sm.GetSnapshot("orders", 0); got == nil {
		t.Fatalf("expected GetSnapshot to return snapshot")
	}
	if got := sm.GetSnapshotPath("orders", 0); got != snap.FilePath {
		t.Fatalf("GetSnapshotPath mismatch: got=%q want=%q", got, snap.FilePath)
	}

	// Stats should reflect one snapshot.
	stats := sm.GetStats()
	if stats.TotalSnapshots != 1 {
		t.Fatalf("TotalSnapshots: got=%d want=1", stats.TotalSnapshots)
	}
	if stats.TotalSizeBytes <= 0 {
		t.Fatalf("expected TotalSizeBytes > 0")
	}
	if stats.OldestSnapshot.IsZero() || stats.NewestSnapshot.IsZero() {
		t.Fatalf("expected non-zero oldest/newest timestamps")
	}

	// Load snapshot into a target directory.
	// Create a pre-existing directory so we also exercise the "backup existing log" path.
	targetDir := filepath.Join(dataDir, "restored", "orders", "0")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir targetDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(targetDir, "stale.txt"), []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale file: %v", err)
	}

	if err := sm.LoadSnapshot(snap.FilePath, targetDir, snap.Checksum); err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	// Verify extracted contents match original.
	b1, err := os.ReadFile(filepath.Join(targetDir, "00000000000000000000.log"))
	if err != nil {
		t.Fatalf("read restored log file: %v", err)
	}
	if string(b1) != "hello\n" {
		t.Fatalf("restored log content mismatch: got=%q", string(b1))
	}

	b2, err := os.ReadFile(filepath.Join(targetDir, "segments", "00000000000000000000.idx"))
	if err != nil {
		t.Fatalf("read restored idx file: %v", err)
	}
	if string(b2) != "world\n" {
		t.Fatalf("restored idx content mismatch: got=%q", string(b2))
	}

	// Corruption path: checksum mismatch should fail fast.
	badTarget := filepath.Join(dataDir, "restored-bad")
	if err := sm.LoadSnapshot(snap.FilePath, badTarget, "not-a-real-checksum"); err == nil {
		t.Fatalf("expected checksum mismatch error, got nil")
	}

	// Cleanup path: mark snapshot old and clean it up.
	// NOTE: snapshots map stores pointers, so mutating CreatedAt is safe for test.
	snap.CreatedAt = time.Now().Add(-48 * time.Hour)
	removed := sm.CleanupOldSnapshots(24 * time.Hour)
	if removed != 1 {
		t.Fatalf("CleanupOldSnapshots removed=%d want=1", removed)
	}
	if _, err := os.Stat(snap.FilePath); err == nil {
		t.Fatalf("expected snapshot file to be deleted by cleanup")
	}

	// DeleteSnapshot should be a no-op if snapshot does not exist.
	if err := sm.DeleteSnapshot("orders", 0); err != nil {
		t.Fatalf("DeleteSnapshot (after cleanup) should be no-op: %v", err)
	}
}
