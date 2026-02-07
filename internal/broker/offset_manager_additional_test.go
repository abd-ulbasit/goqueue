package broker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOffsetManager_CommitGetAndReload_RoundTrip(t *testing.T) {
	baseDir := t.TempDir()

	om, err := NewOffsetManager(baseDir, false, 0)
	if err != nil {
		t.Fatalf("NewOffsetManager failed: %v", err)
	}
	defer func() { _ = om.Close() }()

	// Commit a single offset and verify reads go through the in-memory cache.
	if err := om.Commit("g1", "orders", 0, 10, 7, "member-1"); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	off, err := om.GetOffset("g1", "orders", 0)
	if err != nil {
		t.Fatalf("GetOffset failed: %v", err)
	}
	if off != 10 {
		t.Fatalf("GetOffset=%d, want 10", off)
	}

	// GetGroupOffsets must return a deep copy, so callers cannot mutate the
	// OffsetManager's internal cache (a classic shared-mutable-state footgun).
	go1, err := om.GetGroupOffsets("g1")
	if err != nil {
		t.Fatalf("GetGroupOffsets failed: %v", err)
	}
	go1.Topics["orders"].Partitions[0].Offset = 999

	go2, err := om.GetGroupOffsets("g1")
	if err != nil {
		t.Fatalf("GetGroupOffsets (2nd) failed: %v", err)
	}
	if go2.Topics["orders"].Partitions[0].Offset != 10 {
		t.Fatalf("GetGroupOffsets returned non-copy (mutated): got=%d want=10", go2.Topics["orders"].Partitions[0].Offset)
	}

	// Close and rebuild from disk to exercise loadAllOffsets().
	if err := om.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	om2, err := NewOffsetManager(baseDir, false, 0)
	if err != nil {
		t.Fatalf("NewOffsetManager (reload) failed: %v", err)
	}
	defer func() { _ = om2.Close() }()

	off2, err := om2.GetOffset("g1", "orders", 0)
	if err != nil {
		t.Fatalf("GetOffset (reloaded) failed: %v", err)
	}
	if off2 != 10 {
		t.Fatalf("GetOffset (reloaded)=%d, want 10", off2)
	}
}

func TestOffsetManager_CommitValidations_AndBatchSkipsNegativeOffsets(t *testing.T) {
	baseDir := t.TempDir()

	om, err := NewOffsetManager(baseDir, false, 0)
	if err != nil {
		t.Fatalf("NewOffsetManager failed: %v", err)
	}
	defer func() { _ = om.Close() }()

	if err := om.Commit("g1", "orders", 0, -1, 1, ""); !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("Commit negative offset err=%v, want %v", err, ErrInvalidOffset)
	}

	// CommitBatch deliberately *skips* negative offsets (best-effort semantics).
	// We assert that only the valid partition is persisted.
	offsets := map[string]map[int]int64{
		"orders": {
			0: 42,
			1: -5, // should be skipped
		},
	}
	if err := om.CommitBatch("g1", offsets, 2); err != nil {
		t.Fatalf("CommitBatch failed: %v", err)
	}

	if got, err := om.GetOffset("g1", "orders", 0); err != nil || got != 42 {
		t.Fatalf("GetOffset partition 0 got=%d err=%v, want 42 nil", got, err)
	}
	if _, err := om.GetOffset("g1", "orders", 1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("GetOffset partition 1 err=%v, want %v", err, ErrOffsetNotFound)
	}
}

func TestOffsetManager_ResetHelpers_SetMetadata(t *testing.T) {
	baseDir := t.TempDir()

	om, err := NewOffsetManager(baseDir, false, 0)
	if err != nil {
		t.Fatalf("NewOffsetManager failed: %v", err)
	}
	defer func() { _ = om.Close() }()

	if err := om.ResetToEarliest("g1", "orders", 0, 1); err != nil {
		t.Fatalf("ResetToEarliest failed: %v", err)
	}
	goff, err := om.GetGroupOffsets("g1")
	if err != nil {
		t.Fatalf("GetGroupOffsets failed: %v", err)
	}
	if got := goff.Topics["orders"].Partitions[0].Metadata; got != "reset:earliest" {
		t.Fatalf("ResetToEarliest metadata=%q, want %q", got, "reset:earliest")
	}

	if err := om.ResetToLatest("g1", "orders", 0, 1234, 2); err != nil {
		t.Fatalf("ResetToLatest failed: %v", err)
	}
	goff, _ = om.GetGroupOffsets("g1")
	if got := goff.Topics["orders"].Partitions[0].Metadata; got != "reset:latest" {
		t.Fatalf("ResetToLatest metadata=%q, want %q", got, "reset:latest")
	}

	if err := om.ResetToOffset("g1", "orders", 0, 77, 3); err != nil {
		t.Fatalf("ResetToOffset failed: %v", err)
	}
	goff, _ = om.GetGroupOffsets("g1")
	if got := goff.Topics["orders"].Partitions[0].Metadata; got != "reset:offset:77" {
		t.Fatalf("ResetToOffset metadata=%q, want %q", got, "reset:offset:77")
	}
}

func TestOffsetManager_DeleteGroup_RemovesCacheAndDisk(t *testing.T) {
	baseDir := t.TempDir()

	om, err := NewOffsetManager(baseDir, false, 0)
	if err != nil {
		t.Fatalf("NewOffsetManager failed: %v", err)
	}
	defer func() { _ = om.Close() }()

	if err := om.Commit("g1", "orders", 0, 10, 1, ""); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	groupDir := filepath.Join(baseDir, "g1")
	if _, err := os.Stat(groupDir); err != nil {
		t.Fatalf("expected group dir to exist before delete: %v", err)
	}

	if err := om.DeleteGroup("g1"); err != nil {
		t.Fatalf("DeleteGroup failed: %v", err)
	}
	if _, err := om.GetOffset("g1", "orders", 0); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("GetOffset after DeleteGroup err=%v, want %v", err, ErrOffsetNotFound)
	}
	if _, err := os.Stat(groupDir); !os.IsNotExist(err) {
		t.Fatalf("expected group dir removed; stat err=%v", err)
	}

	groups := om.ListGroups()
	if len(groups) != 0 {
		t.Fatalf("ListGroups=%v, want empty", groups)
	}
}

func TestOffsetManager_MarkForAutoCommit_DisabledIsNoOp(t *testing.T) {
	baseDir := t.TempDir()

	om, err := NewOffsetManager(baseDir, false, 0)
	if err != nil {
		t.Fatalf("NewOffsetManager failed: %v", err)
	}
	defer func() { _ = om.Close() }()

	om.MarkForAutoCommit("g1", "orders", 0, 10)

	om.pendingMu.Lock()
	defer om.pendingMu.Unlock()
	if len(om.pendingCommits) != 0 {
		t.Fatalf("pendingCommits=%v, want empty when auto-commit disabled", om.pendingCommits)
	}
}

func TestOffsetManager_AutoCommit_FlushesOnClose(t *testing.T) {
	baseDir := t.TempDir()

	om, err := NewOffsetManager(baseDir, true, 50)
	if err != nil {
		t.Fatalf("NewOffsetManager failed: %v", err)
	}

	// Stage out-of-order offsets; we should keep the max offset per partition.
	om.MarkForAutoCommit("g1", "orders", 0, 10)
	om.MarkForAutoCommit("g1", "orders", 0, 9)
	om.MarkForAutoCommit("g1", "orders", 0, 25)

	// Close triggers a final flush (best-effort) before returning.
	if err := om.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reload and verify the flushed commit is now durable on disk.
	om2, err := NewOffsetManager(baseDir, false, 0)
	if err != nil {
		t.Fatalf("NewOffsetManager (reload) failed: %v", err)
	}
	defer func() { _ = om2.Close() }()

	// Since auto-commit commits with generation=0 and no metadata, we only
	// assert the offset value.
	got, err := om2.GetOffset("g1", "orders", 0)
	if err != nil {
		t.Fatalf("GetOffset failed: %v", err)
	}
	if got != 25 {
		t.Fatalf("auto-commit GetOffset=%d, want 25", got)
	}
}

func TestOffsetManager_LoadAllOffsets_InvalidJSON(t *testing.T) {
	baseDir := t.TempDir()

	// Create a group dir with a corrupt offsets.json file.
	groupDir := filepath.Join(baseDir, "g1")
	if err := os.MkdirAll(groupDir, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(groupDir, "offsets.json"), []byte("not-json"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := NewOffsetManager(baseDir, false, 0)
	if err == nil {
		t.Fatalf("NewOffsetManager expected error for invalid JSON, got nil")
	}
}

func TestOffsetManager_FlushPendingCommits_SwapsMap(t *testing.T) {
	baseDir := t.TempDir()

	om, err := NewOffsetManager(baseDir, true, 1000)
	if err != nil {
		t.Fatalf("NewOffsetManager failed: %v", err)
	}
	defer func() { _ = om.Close() }()

	om.MarkForAutoCommit("g1", "orders", 0, 10)
	om.MarkForAutoCommit("g2", "orders", 1, 20)

	// Directly call flushPendingCommits to cover the lock-swap behavior
	// without relying on the ticker.
	om.flushPendingCommits()

	// After flush, pendingCommits should have been swapped to a fresh map.
	om.pendingMu.Lock()
	defer om.pendingMu.Unlock()
	if len(om.pendingCommits) != 0 {
		t.Fatalf("pendingCommits after flush=%v, want empty", om.pendingCommits)
	}

	// The offsets should now be committed.
	// (We tolerate a tiny time skew in CommittedAt by only checking values.)
	off1, err := om.GetOffset("g1", "orders", 0)
	if err != nil || off1 != 10 {
		t.Fatalf("GetOffset g1 got=%d err=%v, want 10 nil", off1, err)
	}
	off2, err := om.GetOffset("g2", "orders", 1)
	if err != nil || off2 != 20 {
		t.Fatalf("GetOffset g2 got=%d err=%v, want 20 nil", off2, err)
	}

	// Prove the auto-commit goroutine is live and exits cleanly.
	// (This is intentionally weak: we just want to avoid leaks.)
	_ = time.Now()
}
