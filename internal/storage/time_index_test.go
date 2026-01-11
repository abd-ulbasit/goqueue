// =============================================================================
// TIME INDEX TESTS
// =============================================================================

package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestNewTimeIndex tests creating a new time index.
func TestNewTimeIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("failed to create time index: %v", err)
	}
	defer ti.Close()

	// Verify file was created
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("time index file was not created")
	}

	// Verify initial state
	if ti.GetEntryCount() != 0 {
		t.Errorf("expected 0 entries, got %d", ti.GetEntryCount())
	}
}

// TestTimeIndexMaybeAppend tests adding entries with granularity control.
func TestTimeIndexMaybeAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("failed to create time index: %v", err)
	}
	defer ti.Close()

	baseTime := time.Now().UnixMilli()

	// First append should succeed (position = 0, which triggers first entry)
	added, err := ti.MaybeAppend(baseTime, 0, TimeIndexGranularity)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	if !added {
		t.Error("first append should have been added")
	}

	// Second append at same position should not add (within granularity)
	added, err = ti.MaybeAppend(baseTime+1000, 1, TimeIndexGranularity+100)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	if added {
		t.Error("second append should not have been added (within granularity)")
	}

	// Third append at next granularity boundary should add
	added, err = ti.MaybeAppend(baseTime+2000, 100, 2*TimeIndexGranularity)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	if !added {
		t.Error("third append should have been added (crossed granularity)")
	}

	// Verify entry count
	if ti.GetEntryCount() != 2 {
		t.Errorf("expected 2 entries, got %d", ti.GetEntryCount())
	}
}

// TestTimeIndexLookup tests timestamp-to-offset lookup.
func TestTimeIndexLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("failed to create time index: %v", err)
	}
	defer ti.Close()

	// Add some entries
	// Time: 1000 -> Offset: 0
	// Time: 2000 -> Offset: 100
	// Time: 3000 -> Offset: 200
	// Time: 4000 -> Offset: 300
	entries := []struct {
		timestamp int64
		offset    int64
		position  int64
	}{
		{1000, 0, TimeIndexGranularity},
		{2000, 100, 2 * TimeIndexGranularity},
		{3000, 200, 3 * TimeIndexGranularity},
		{4000, 300, 4 * TimeIndexGranularity},
	}

	for _, e := range entries {
		_, err := ti.MaybeAppend(e.timestamp, e.offset, e.position)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	tests := []struct {
		name           string
		timestamp      int64
		expectedOffset int64
	}{
		{"exact match first", 1000, 0},
		{"exact match middle", 2000, 100},
		{"exact match last", 4000, 300},
		{"between entries", 1500, 0},     // Should return offset for timestamp 1000
		{"between entries 2", 2500, 100}, // Should return offset for timestamp 2000
		{"before first", 500, 0},         // Should return first offset
		{"after last", 5000, 300},        // Should return last offset
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, err := ti.Lookup(tt.timestamp)
			if err != nil {
				t.Errorf("lookup failed: %v", err)
				return
			}
			if offset != tt.expectedOffset {
				t.Errorf("expected offset %d, got %d", tt.expectedOffset, offset)
			}
		})
	}
}

// TestTimeIndexLookupEmpty tests lookup on empty index.
func TestTimeIndexLookupEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("failed to create time index: %v", err)
	}
	defer ti.Close()

	_, err = ti.Lookup(1000)
	if err != ErrTimestampNotFound {
		t.Errorf("expected ErrTimestampNotFound, got %v", err)
	}
}

// TestTimeIndexLookupRange tests time range queries.
func TestTimeIndexLookupRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("failed to create time index: %v", err)
	}
	defer ti.Close()

	// Add entries
	entries := []struct {
		timestamp int64
		offset    int64
		position  int64
	}{
		{1000, 0, TimeIndexGranularity},
		{2000, 100, 2 * TimeIndexGranularity},
		{3000, 200, 3 * TimeIndexGranularity},
		{4000, 300, 4 * TimeIndexGranularity},
	}

	for _, e := range entries {
		_, err := ti.MaybeAppend(e.timestamp, e.offset, e.position)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	tests := []struct {
		name          string
		startTime     int64
		endTime       int64
		expectedStart int64
		expectedEnd   int64 // -1 means "to end"
	}{
		{"exact range", 1000, 3000, 0, 200},
		{"wide range", 500, 5000, 0, -1},
		{"middle range", 1500, 3500, 0, 300},
		{"narrow range", 2000, 2500, 100, 200}, // end is 3000's offset since 2500 < 3000
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startOffset, endOffset, err := ti.LookupRange(tt.startTime, tt.endTime)
			if err != nil {
				t.Errorf("lookup range failed: %v", err)
				return
			}
			if startOffset != tt.expectedStart {
				t.Errorf("expected start offset %d, got %d", tt.expectedStart, startOffset)
			}
			if endOffset != tt.expectedEnd {
				t.Errorf("expected end offset %d, got %d", tt.expectedEnd, endOffset)
			}
		})
	}
}

// TestTimeIndexPersistence tests that time index survives restart.
func TestTimeIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	// Create and populate index
	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("failed to create time index: %v", err)
	}

	entries := []struct {
		timestamp int64
		offset    int64
		position  int64
	}{
		{1000, 0, TimeIndexGranularity},
		{2000, 100, 2 * TimeIndexGranularity},
		{3000, 200, 3 * TimeIndexGranularity},
	}

	for _, e := range entries {
		_, err := ti.MaybeAppend(e.timestamp, e.offset, e.position)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	ti.Close()

	// Reload index
	ti2, err := LoadTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("failed to load time index: %v", err)
	}
	defer ti2.Close()

	// Verify entries
	if ti2.GetEntryCount() != 3 {
		t.Errorf("expected 3 entries, got %d", ti2.GetEntryCount())
	}

	// Verify lookups work
	offset, err := ti2.Lookup(2000)
	if err != nil {
		t.Errorf("lookup failed: %v", err)
	}
	if offset != 100 {
		t.Errorf("expected offset 100, got %d", offset)
	}
}

// TestTimeIndexTruncate tests truncating entries after an offset.
func TestTimeIndexTruncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("failed to create time index: %v", err)
	}
	defer ti.Close()

	// Add entries
	entries := []struct {
		timestamp int64
		offset    int64
		position  int64
	}{
		{1000, 0, TimeIndexGranularity},
		{2000, 100, 2 * TimeIndexGranularity},
		{3000, 200, 3 * TimeIndexGranularity},
		{4000, 300, 4 * TimeIndexGranularity},
	}

	for _, e := range entries {
		_, err := ti.MaybeAppend(e.timestamp, e.offset, e.position)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	// Truncate after offset 100
	err = ti.Truncate(100)
	if err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	// Verify only 2 entries remain
	if ti.GetEntryCount() != 2 {
		t.Errorf("expected 2 entries after truncate, got %d", ti.GetEntryCount())
	}

	// Verify last timestamp is 2000
	lastTs, err := ti.GetLastTimestamp()
	if err != nil {
		t.Errorf("failed to get last timestamp: %v", err)
	}
	if lastTs != 2000 {
		t.Errorf("expected last timestamp 2000, got %d", lastTs)
	}
}

// TestTimeIndexGetFirstLastTimestamp tests getting boundary timestamps.
func TestTimeIndexGetFirstLastTimestamp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer ti.Close()

	// Test empty index
	_, err = ti.GetFirstTimestamp()
	if err != ErrTimestampNotFound {
		t.Errorf("expected ErrTimestampNotFound for empty index, got %v", err)
	}

	_, err = ti.GetLastTimestamp()
	if err != ErrTimestampNotFound {
		t.Errorf("expected ErrTimestampNotFound for empty index, got %v", err)
	}

	// Add entries
	ti.MaybeAppend(1000, 0, TimeIndexGranularity)
	ti.MaybeAppend(2000, 100, 2*TimeIndexGranularity)
	ti.MaybeAppend(3000, 200, 3*TimeIndexGranularity)

	// Test first timestamp
	first, err := ti.GetFirstTimestamp()
	if err != nil {
		t.Errorf("failed to get first timestamp: %v", err)
	}
	if first != 1000 {
		t.Errorf("expected first timestamp 1000, got %d", first)
	}

	// Test last timestamp
	last, err := ti.GetLastTimestamp()
	if err != nil {
		t.Errorf("failed to get last timestamp: %v", err)
	}
	if last != 3000 {
		t.Errorf("expected last timestamp 3000, got %d", last)
	}
}

func TestTimeIndex_Close_ErrorHandling(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer ti.Close()

	// Add some data
	ti.MaybeAppend(1000, 0, TimeIndexGranularity)

	// Close should succeed
	if err := ti.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Note: Close is not idempotent for TimeIndex because it closes the underlying file
	// The defer will handle cleanup
}

func TestTimeIndex_GetLastTimestamp_Empty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer ti.Close()

	// Test empty index
	_, err = ti.GetLastTimestamp()
	if err != ErrTimestampNotFound {
		t.Errorf("expected ErrTimestampNotFound for empty index, got %v", err)
	}

	// Test empty index
	_, err = ti.GetFirstTimestamp()
	if err != ErrTimestampNotFound {
		t.Errorf("expected ErrTimestampNotFound for empty index, got %v", err)
	}

	// Add entries
	ti.MaybeAppend(1000, 0, TimeIndexGranularity)
	ti.MaybeAppend(2000, 100, 2*TimeIndexGranularity)
	ti.MaybeAppend(3000, 200, 3*TimeIndexGranularity)

	// Test first timestamp
	first, err := ti.GetFirstTimestamp()
	if err != nil {
		t.Errorf("failed to get first timestamp: %v", err)
	}
	if first != 1000 {
		t.Errorf("expected first timestamp 1000, got %d", first)
	}

	// Test last timestamp
	last, err := ti.GetLastTimestamp()
	if err != nil {
		t.Errorf("failed to get last timestamp: %v", err)
	}
	if last != 3000 {
		t.Errorf("expected last timestamp 3000, got %d", last)
	}
}

// TestLoadCorruptedTimeIndex tests loading a corrupted index file.
func TestLoadCorruptedTimeIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	// Create a file with invalid size (not multiple of entry size)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	f.Write([]byte{1, 2, 3, 4, 5}) // 5 bytes, not multiple of 16
	f.Close()

	_, err = LoadTimeIndex(path, 0)
	if err == nil {
		t.Error("expected error loading corrupted index")
	}
}

func TestRebuildTimeIndex_ScansLogFile(t *testing.T) {
	// =============================================================================
	// TIME INDEX REBUILD (CORRUPTION / MISSING FILE RECOVERY)
	// =============================================================================
	//
	// WHY: If the .timeindex is missing/corrupted, we must be able to recover by
	// scanning the segment log and rebuilding the timestamp→offset mapping.
	//
	// This test intentionally:
	//   1) Writes a real segment log (using Segment.Append)
	//   2) Deletes the generated .timeindex
	//   3) Calls RebuildTimeIndex(logPath, indexPath)
	//   4) Validates first/last timestamps and lookup behavior
	// =============================================================================

	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	baseTS := time.Now().UnixNano()
	step := int64(time.Second)

	// Write messages large enough to ensure time-index granularity thresholds are crossed.
	for i := 0; i < 5; i++ {
		value := make([]byte, TimeIndexGranularity)
		value[0] = byte(i)

		msg := NewMessage([]byte{byte(i)}, value)
		msg.Timestamp = baseTS + int64(i)*step
		if _, err := seg.Append(msg); err != nil {
			_ = seg.Close()
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	logPath := filepath.Join(dir, SegmentFileName(0))
	indexPath := filepath.Join(dir, TimeIndexFileName(0))

	// Simulate corruption/missing index.
	_ = os.Remove(indexPath)

	ti, err := RebuildTimeIndex(logPath, indexPath, 0)
	if err != nil {
		t.Fatalf("RebuildTimeIndex failed: %v", err)
	}
	defer ti.Close()

	// Ensure the rebuilt file exists.
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("rebuilt time index file not found: %v", err)
	}

	first, err := ti.GetFirstTimestamp()
	if err != nil {
		t.Fatalf("GetFirstTimestamp failed: %v", err)
	}
	if first != baseTS {
		t.Fatalf("first timestamp=%d, want %d", first, baseTS)
	}

	last, err := ti.GetLastTimestamp()
	if err != nil {
		t.Fatalf("GetLastTimestamp failed: %v", err)
	}
	expectedLast := baseTS + 4*step
	if last != expectedLast {
		t.Fatalf("last timestamp=%d, want %d", last, expectedLast)
	}

	// Lookup should return the offset at or before the target timestamp.
	off, err := ti.Lookup(baseTS + 2*step)
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if off != 2 {
		t.Fatalf("Lookup returned offset=%d, want 2", off)
	}
}

// BenchmarkTimeIndexLookup benchmarks lookup performance.
func TestTimeIndex_ErrorPaths(t *testing.T) {
	// Test loading time index with non-existent directory
	_, err := LoadTimeIndex("/non/existent/path/timeindex", 0)
	if err == nil {
		t.Error("LoadTimeIndex should fail for non-existent path")
	}

	// Test operations on closed time index
	dir := t.TempDir()
	ti, err := NewTimeIndex(filepath.Join(dir, TimeIndexFileName(0)), 0)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}

	ti.Close()

	// These operations should fail on closed time index
	if _, err := ti.MaybeAppend(1000, 0, TimeIndexGranularity); err == nil {
		t.Error("MaybeAppend should fail on closed time index")
	}
	if _, err := ti.Lookup(1000); err == nil {
		t.Error("Lookup should fail on closed time index")
	}
	if _, _, err := ti.LookupRange(1000, 2000); err == nil {
		t.Error("LookupRange should fail on closed time index")
	}
	// Note: Truncate on TimeIndex doesn't check for closed file
	// So we can't test for failure here
}

func BenchmarkTimeIndexLookup(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, TimeIndexFileName(0))

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		b.Fatalf("failed to create time index: %v", err)
	}
	defer ti.Close()

	// Add 10000 entries
	for i := 0; i < 10000; i++ {
		ti.MaybeAppend(int64(i*1000), int64(i*100), int64((i+1)*TimeIndexGranularity))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ti.Lookup(5000000) // Lookup middle timestamp
	}
}
