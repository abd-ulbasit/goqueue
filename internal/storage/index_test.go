// =============================================================================
// INDEX TESTS
// =============================================================================
//
// Tests for the sparse index that maps offset → byte position.
//
// KEY BEHAVIORS TO TEST:
//   - Index entries are added at granularity intervals
//   - Binary search lookup finds correct floor entry
//   - Index survives restart (load/save)
//   - Truncation works correctly
//
// =============================================================================

package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIndex_NewAndClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	idx, err := NewIndex(path, 0)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("Index file was not created")
	}

	if err := idx.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestIndex_ForceAppend(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(filepath.Join(dir, "test.index"), 0)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	// Force append should always add entry
	if err := idx.ForceAppend(0, 0); err != nil {
		t.Fatalf("ForceAppend failed: %v", err)
	}
	if err := idx.ForceAppend(100, 4096); err != nil {
		t.Fatalf("ForceAppend failed: %v", err)
	}

	if idx.EntryCount() != 2 {
		t.Errorf("Expected 2 entries, got %d", idx.EntryCount())
	}
}

func TestIndex_MaybeAppend_RespectsGranularity(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(filepath.Join(dir, "test.index"), 0)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	// First entry at position 0
	idx.ForceAppend(0, 0)

	// Entry at position 1000 (less than 4KB granularity) - should skip
	added, err := idx.MaybeAppend(10, 1000)
	if err != nil {
		t.Fatalf("MaybeAppend failed: %v", err)
	}
	if added {
		t.Error("Should not add entry within granularity")
	}

	// Entry at position 5000 (more than 4KB) - should add
	added, err = idx.MaybeAppend(50, 5000)
	if err != nil {
		t.Fatalf("MaybeAppend failed: %v", err)
	}
	if !added {
		t.Error("Should add entry past granularity threshold")
	}

	// Entry at position 6000 (less than 4KB from last) - should skip
	added, err = idx.MaybeAppend(60, 6000)
	if err != nil {
		t.Fatalf("MaybeAppend failed: %v", err)
	}
	if added {
		t.Error("Should not add entry within granularity of last entry")
	}
}

func TestIndex_Lookup(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(filepath.Join(dir, "test.index"), 0)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	// Add some entries
	idx.ForceAppend(0, 0)
	idx.ForceAppend(100, 4096)
	idx.ForceAppend(200, 8192)
	idx.ForceAppend(300, 12288)

	testCases := []struct {
		targetOffset   int64
		expectedOffset int64
		expectedPos    int64
	}{
		{0, 0, 0},         // Exact match on first
		{50, 0, 0},        // Between first and second
		{100, 100, 4096},  // Exact match
		{150, 100, 4096},  // Between entries
		{200, 200, 8192},  // Exact match
		{250, 200, 8192},  // Between entries
		{300, 300, 12288}, // Exact match on last
		{400, 300, 12288}, // Past last entry
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			entry, err := idx.Lookup(tc.targetOffset)
			if err != nil {
				t.Fatalf("Lookup(%d) failed: %v", tc.targetOffset, err)
			}
			if entry.Offset != tc.expectedOffset || entry.Position != tc.expectedPos {
				t.Errorf("Lookup(%d) = {%d, %d}, want {%d, %d}",
					tc.targetOffset, entry.Offset, entry.Position,
					tc.expectedOffset, tc.expectedPos)
			}
		})
	}
}

func TestIndex_LoadExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	// Create index with some entries
	idx1, err := NewIndex(path, 0)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}

	idx1.ForceAppend(0, 0)
	idx1.ForceAppend(100, 4096)
	idx1.ForceAppend(200, 8192)
	idx1.Close()

	// Load existing index
	idx2, err := LoadIndex(path, 0)
	if err != nil {
		t.Fatalf("LoadIndex failed: %v", err)
	}
	defer idx2.Close()

	// Verify entries were loaded
	if idx2.EntryCount() != 3 {
		t.Errorf("Expected 3 entries after load, got %d", idx2.EntryCount())
	}

	// Verify lookup still works
	entry, err := idx2.Lookup(150)
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if entry.Offset != 100 {
		t.Errorf("Lookup(150) returned offset %d, want 100", entry.Offset)
	}
}

func TestIndex_LastOffset(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(filepath.Join(dir, "test.index"), 0)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	// Empty index
	if idx.LastOffset() != -1 {
		t.Errorf("Empty index should return -1, got %d", idx.LastOffset())
	}

	// Add entries
	idx.ForceAppend(0, 0)
	if idx.LastOffset() != 0 {
		t.Errorf("Expected 0, got %d", idx.LastOffset())
	}

	idx.ForceAppend(100, 4096)
	if idx.LastOffset() != 100 {
		t.Errorf("Expected 100, got %d", idx.LastOffset())
	}
}

func TestIndex_TruncateTo(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(filepath.Join(dir, "test.index"), 0)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	// Add entries
	idx.ForceAppend(0, 0)
	idx.ForceAppend(100, 4096)
	idx.ForceAppend(200, 8192)
	idx.ForceAppend(300, 12288)

	// Truncate to offset 150 (should keep entries 0 and 100)
	if err := idx.TruncateTo(150); err != nil {
		t.Fatalf("TruncateTo failed: %v", err)
	}

	if idx.EntryCount() != 2 {
		t.Errorf("Expected 2 entries after truncate, got %d", idx.EntryCount())
	}
	if idx.LastOffset() != 100 {
		t.Errorf("Expected last offset 100, got %d", idx.LastOffset())
	}
}

func TestIndex_ErrorPaths(t *testing.T) {
	// Test loading index with non-existent directory
	_, err := LoadIndex("/non/existent/path/index", 0)
	if err == nil {
		t.Error("LoadIndex should fail for non-existent path")
	}

	// Test operations on closed index
	dir := t.TempDir()
	idx, err := NewIndex(filepath.Join(dir, "test.index"), 0)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}

	idx.Close()

	// These operations should fail on closed index
	if err := idx.ForceAppend(0, 0); err == nil {
		t.Error("ForceAppend should fail on closed index")
	}
	// Note: Lookup doesn't check for closed file - it only checks if entries slice is empty
	// So we can't test for failure on closed index here
	if err := idx.Sync(); err == nil {
		t.Error("Sync should fail on closed index")
	}
	if err := idx.TruncateTo(0); err == nil {
		t.Error("TruncateTo should fail on closed index")
	}
}
