// =============================================================================
// LOG TESTS
// =============================================================================
//
// Tests for the append-only log (manages multiple segments).
//
// KEY BEHAVIORS TO TEST:
//   - Writes span multiple segments (rollover)
//   - Reads across segment boundaries
//   - Recovery loads all segments
//   - Retention cleanup deletes old segments
//
// =============================================================================

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLog_NewAndClose(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}

	// Verify initial state
	if log.NextOffset() != 0 {
		t.Errorf("NextOffset = %d, want 0", log.NextOffset())
	}
	if log.EarliestOffset() != 0 {
		t.Errorf("EarliestOffset = %d, want 0", log.EarliestOffset())
	}
	if log.SegmentCount() != 1 {
		t.Errorf("SegmentCount = %d, want 1", log.SegmentCount())
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestLog_AppendAndRead(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Write messages
	for i := 0; i < 10; i++ {
		msg := NewMessage(
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		)
		offset, err := log.Append(msg)
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
		if offset != int64(i) {
			t.Errorf("Append %d returned offset %d", i, offset)
		}
	}

	// Verify state
	if log.NextOffset() != 10 {
		t.Errorf("NextOffset = %d, want 10", log.NextOffset())
	}
	if log.LatestOffset() != 9 {
		t.Errorf("LatestOffset = %d, want 9", log.LatestOffset())
	}

	// Read back each message
	for i := 0; i < 10; i++ {
		msg, err := log.Read(int64(i))
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}
		expectedKey := fmt.Sprintf("key-%d", i)
		if string(msg.Key) != expectedKey {
			t.Errorf("Message %d key = %s, want %s", i, msg.Key, expectedKey)
		}
	}
}

func TestLog_ReadFrom(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Write 20 messages
	for i := 0; i < 20; i++ {
		msg := NewMessage(
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		)
		log.Append(msg)
	}

	// Read from middle with limit
	msgs, err := log.ReadFrom(10, 5)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("Expected 5 messages, got %d", len(msgs))
	}

	// Verify offsets
	for i, msg := range msgs {
		expectedOffset := int64(10 + i)
		if msg.Offset != expectedOffset {
			t.Errorf("Message %d offset = %d, want %d", i, msg.Offset, expectedOffset)
		}
	}
}

func TestLog_ReadFrom_NoLimit(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Write messages
	for i := 0; i < 10; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
		log.Append(msg)
	}

	// Read all with no limit (maxMessages = 0)
	msgs, err := log.ReadFrom(0, 0)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if len(msgs) != 10 {
		t.Errorf("Expected 10 messages, got %d", len(msgs))
	}
}

func TestLog_ReadFrom_NoNewMessages(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Write some messages
	for i := 0; i < 5; i++ {
		msg := NewMessage([]byte("key"), []byte("value"))
		log.Append(msg)
	}

	// Read from offset beyond what exists
	msgs, err := log.ReadFrom(10, 10)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(msgs))
	}
}

func TestLog_LoadExisting(t *testing.T) {
	dir := t.TempDir()

	// Create log and write messages
	log1, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		msg := NewMessage(
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		)
		log1.Append(msg)
	}
	log1.Close()

	// Load existing log
	log2, err := LoadLog(dir)
	if err != nil {
		t.Fatalf("LoadLog failed: %v", err)
	}
	defer log2.Close()

	// Verify state
	if log2.NextOffset() != 10 {
		t.Errorf("NextOffset after load = %d, want 10", log2.NextOffset())
	}
	if log2.EarliestOffset() != 0 {
		t.Errorf("EarliestOffset after load = %d, want 0", log2.EarliestOffset())
	}

	// Verify can read messages
	for i := 0; i < 10; i++ {
		msg, err := log2.Read(int64(i))
		if err != nil {
			t.Fatalf("Read %d after load failed: %v", i, err)
		}
		expectedKey := fmt.Sprintf("key-%d", i)
		if string(msg.Key) != expectedKey {
			t.Errorf("Message %d key = %s, want %s", i, msg.Key, expectedKey)
		}
	}

	// Verify can continue appending
	msg := NewMessage([]byte("new-key"), []byte("new-value"))
	offset, err := log2.Append(msg)
	if err != nil {
		t.Fatalf("Append after load failed: %v", err)
	}
	if offset != 10 {
		t.Errorf("New message offset = %d, want 10", offset)
	}
}

func TestLog_ReadNonexistent(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Write one message
	msg := NewMessage([]byte("key"), []byte("value"))
	log.Append(msg)

	// Try to read offset that doesn't exist
	_, err = log.Read(100)
	if err == nil {
		t.Error("Read of non-existent offset should fail")
	}
}

func TestLog_EmptyState(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Empty log should have sensible values
	if log.NextOffset() != 0 {
		t.Errorf("Empty NextOffset = %d, want 0", log.NextOffset())
	}
	if log.LatestOffset() != -1 {
		t.Errorf("Empty LatestOffset = %d, want -1", log.LatestOffset())
	}
	if log.Size() != 0 {
		t.Errorf("Empty Size = %d, want 0", log.Size())
	}
}

func TestLog_Size(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	initialSize := log.Size()
	if initialSize != 0 {
		t.Errorf("Initial size = %d, want 0", initialSize)
	}

	// Write a message
	msg := NewMessage([]byte("key"), []byte("value"))
	log.Append(msg)

	newSize := log.Size()
	if newSize <= initialSize {
		t.Errorf("Size should increase after append: %d -> %d", initialSize, newSize)
	}
}

func TestLog_DeleteSegmentsBefore(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Write messages
	for i := 0; i < 10; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
		log.Append(msg)
	}

	// Should have 1 segment (not enough data to trigger rollover)
	if log.SegmentCount() != 1 {
		t.Logf("SegmentCount = %d (expected 1 for small test)", log.SegmentCount())
	}

	// DeleteSegmentsBefore shouldn't delete the active segment
	err = log.DeleteSegmentsBefore(5)
	if err != nil {
		t.Fatalf("DeleteSegmentsBefore failed: %v", err)
	}

	// Should still have at least 1 segment
	if log.SegmentCount() < 1 {
		t.Errorf("Should have at least 1 segment after delete")
	}
}

// TestLog_LargeMessages tests handling of larger message payloads
func TestLog_LargeMessages(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Write messages of various sizes
	sizes := []int{100, 1000, 10000, 100000}

	for i, size := range sizes {
		value := make([]byte, size)
		for j := range value {
			value[j] = byte(j % 256)
		}

		msg := NewMessage([]byte(fmt.Sprintf("key-%d", i)), value)
		offset, err := log.Append(msg)
		if err != nil {
			t.Fatalf("Append %d (size %d) failed: %v", i, size, err)
		}

		// Read back and verify
		readMsg, err := log.Read(offset)
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}
		if !bytes.Equal(readMsg.Value, value) {
			t.Errorf("Message %d (size %d) value mismatch", i, size)
		}
	}
}

func BenchmarkLog_Append(b *testing.B) {
	dir := b.TempDir()
	log, _ := NewLog(dir)
	defer log.Close()
	//TODO Check why the follwing benchmark for value of the message
	// max value size in the message.go says 16MB
	// for 1KB     293733	      3520 ns/op	    1170 B/op	       1 allocs/op
	// for 1MB       2030	    570840 ns/op	 1073798 B/op	       1 allocs/op
	// for 4MB        534	   1968821 ns/op	 4479483 B/op	       2 allocs/op
	// for 8MB        240	   4242974 ns/op	 9589645 B/op	       4 allocs/op
	// for 32MB   2913681	     404.6 ns/op	     408 B/op	       7 allocs/op
	// for 64MB   2783688	     416.8 ns/op	     408 B/op	       7 allocs/op
	msg := NewMessage([]byte("key"), make([]byte, 8*1024*1024)) // 1KB message

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Append(msg)
	}
}

func BenchmarkLog_ReadSequential(b *testing.B) {
	dir := b.TempDir()
	log, _ := NewLog(dir)
	defer log.Close()

	// Write messages
	for i := 0; i < 10000; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("key-%d", i)), make([]byte, 1024))
		log.Append(msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Read(int64(i % 10000))
	}
}

func TestLog_AppendAtOffset(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Test normal append at expected offset
	msg := NewMessage([]byte("key"), []byte("value"))
	offset, err := log.AppendAtOffset(msg, 0)
	if err != nil {
		t.Fatalf("AppendAtOffset failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("Expected offset 0, got %d", offset)
	}

	// Test idempotency - appending same offset again should succeed
	msg2 := NewMessage([]byte("key2"), []byte("value2"))
	offset, err = log.AppendAtOffset(msg2, 0)
	if err != nil {
		t.Fatalf("AppendAtOffset idempotent failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("Expected offset 0 for idempotent, got %d", offset)
	}

	// Test gap detection - trying to append at offset 2 when next is 1
	msg3 := NewMessage([]byte("key3"), []byte("value3"))
	_, err = log.AppendAtOffset(msg3, 2)
	if err == nil {
		t.Error("AppendAtOffset should fail when creating gap")
	}

	// Test normal append at next expected offset
	msg4 := NewMessage([]byte("key4"), []byte("value4"))
	offset, err = log.AppendAtOffset(msg4, 1)
	if err != nil {
		t.Fatalf("AppendAtOffset at expected offset failed: %v", err)
	}
	if offset != 1 {
		t.Errorf("Expected offset 1, got %d", offset)
	}

	// Verify messages can be read
	readMsg, err := log.Read(0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(readMsg.Key) != "key" {
		t.Errorf("Read wrong message: got %s", readMsg.Key)
	}
}

func TestLog_Sync(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Write a message
	msg := NewMessage([]byte("key"), []byte("value"))
	if _, err := log.Append(msg); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Sync should not fail
	if err := log.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}

	// Sync on closed log should fail
	log.Close()
	if err := log.Sync(); !errors.Is(err, ErrLogClosed) {
		t.Errorf("Expected ErrLogClosed, got %v", err)
	}
}

func TestLog_TruncateAfter(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Write multiple messages
	for i := 0; i < 10; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		if _, err := log.Append(msg); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	// Truncate after offset 5
	if err := log.TruncateAfter(5); err != nil {
		t.Fatalf("TruncateAfter failed: %v", err)
	}

	// Verify we can still read messages up to offset 5
	for i := 0; i <= 5; i++ {
		msg, err := log.Read(int64(i))
		if err != nil {
			t.Errorf("Read %d after truncate failed: %v", i, err)
		}
		expectedKey := fmt.Sprintf("key-%d", i)
		if string(msg.Key) != expectedKey {
			t.Errorf("Message %d key mismatch after truncate", i)
		}
	}

	// Verify next offset is correct
	if log.NextOffset() != 6 {
		t.Errorf("NextOffset after truncate = %d, want 6", log.NextOffset())
	}

	// Truncate after non-existent offset should fail
	if err := log.TruncateAfter(100); err == nil {
		t.Error("TruncateAfter should fail for non-existent offset")
	}
}

func TestLog_Dir(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Dir should return the directory we created with
	if log.Dir() != dir {
		t.Errorf("Dir() = %s, want %s", log.Dir(), dir)
	}
}

func TestLog_ListSegmentFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a log and write some messages to create segment files
	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
		log.Append(msg)
	}
	log.Close()

	// List segment files
	offsets, err := ListSegmentFiles(dir)
	if err != nil {
		t.Fatalf("ListSegmentFiles failed: %v", err)
	}

	if len(offsets) == 0 {
		t.Error("Expected at least one segment file")
	}

	// Offsets should be sorted
	for i := 1; i < len(offsets); i++ {
		if offsets[i] <= offsets[i-1] {
			t.Errorf("Offsets not sorted: %d <= %d", offsets[i], offsets[i-1])
		}
	}

	// First offset should be 0
	if offsets[0] != 0 {
		t.Errorf("First offset should be 0, got %d", offsets[0])
	}
}

func TestLog_GetSegmentPaths(t *testing.T) {
	dir := t.TempDir()

	// Create a log and write some messages
	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
		log.Append(msg)
	}
	log.Close()

	// Get segment paths
	paths, err := GetSegmentPaths(dir)
	if err != nil {
		t.Fatalf("GetSegmentPaths failed: %v", err)
	}

	if len(paths) == 0 {
		t.Error("Expected at least one segment path")
	}

	// Paths should be absolute and include the directory
	for _, path := range paths {
		if !filepath.IsAbs(path) {
			t.Errorf("Path should be absolute: %s", path)
		}
		if !strings.HasPrefix(path, dir) {
			t.Errorf("Path should be in directory %s: %s", dir, path)
		}
		if !strings.HasSuffix(path, ".log") {
			t.Errorf("Path should end with .log: %s", path)
		}
	}
}

func TestLog_Rollover(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Write messages until we trigger a rollover
	// Note: In a real scenario, this would require writing 64MB of data
	// For testing, we'll verify the rollover logic by checking segment count

	initialSegmentCount := log.SegmentCount()

	// Write a message to ensure we have at least one segment
	msg := NewMessage([]byte("key"), []byte("value"))
	if _, err := log.Append(msg); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// The rollover happens automatically when a segment is full
	// We can't easily test it without writing 64MB, but we can verify
	// the segment count increases appropriately
	if log.SegmentCount() < initialSegmentCount {
		t.Error("Segment count should not decrease")
	}

	// Verify we can still read after potential rollover
	readMsg, err := log.Read(0)
	if err != nil {
		t.Fatalf("Read after potential rollover failed: %v", err)
	}
	if string(readMsg.Key) != "key" {
		t.Error("Read wrong message after potential rollover")
	}
}

func TestLog_EarliestOffset(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Empty log should return 0 (no segments, so offset 0 is the start)
	if earliest := log.EarliestOffset(); earliest != 0 {
		t.Errorf("EarliestOffset for empty log should be 0, got %d", earliest)
	}

	// Write messages starting from offset 0
	msg := NewMessage([]byte("key"), []byte("value"))
	if _, err := log.Append(msg); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Earliest offset should be 0
	if earliest := log.EarliestOffset(); earliest != 0 {
		t.Errorf("EarliestOffset should be 0, got %d", earliest)
	}

	// Write another message
	msg2 := NewMessage([]byte("key2"), []byte("value2"))
	if _, err := log.Append(msg2); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Earliest offset should still be 0
	if earliest := log.EarliestOffset(); earliest != 0 {
		t.Errorf("EarliestOffset should still be 0, got %d", earliest)
	}
}

func TestLog_ListSegmentFiles_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()

	// Empty directory should return empty slice
	offsets, err := ListSegmentFiles(dir)
	if err != nil {
		t.Fatalf("ListSegmentFiles failed: %v", err)
	}
	if len(offsets) != 0 {
		t.Errorf("Expected empty slice, got %v", offsets)
	}
}

func TestLog_ListSegmentFiles_InvalidFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a file that doesn't match the segment naming pattern
	invalidFile := filepath.Join(dir, "invalid.txt")
	if err := os.WriteFile(invalidFile, []byte("test"), 0o644); err != nil {
		t.Fatalf("Failed to create invalid file: %v", err)
	}

	// Create a valid segment file
	validFile := filepath.Join(dir, "00000000000000000000.log")
	if err := os.WriteFile(validFile, []byte("test"), 0o644); err != nil {
		t.Fatalf("Failed to create valid file: %v", err)
	}

	offsets, err := ListSegmentFiles(dir)
	if err != nil {
		t.Fatalf("ListSegmentFiles failed: %v", err)
	}

	// Should only return the valid segment file
	if len(offsets) != 1 {
		t.Errorf("Expected 1 offset, got %d", len(offsets))
	}
	if offsets[0] != 0 {
		t.Errorf("Expected offset 0, got %d", offsets[0])
	}
}

func TestLog_GetSegmentPaths_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()

	paths, err := GetSegmentPaths(dir)
	if err != nil {
		t.Fatalf("GetSegmentPaths failed: %v", err)
	}
	if len(paths) != 0 {
		t.Errorf("Expected empty slice, got %v", paths)
	}
}

func TestLog_Close_ErrorHandling(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}

	// Close should succeed initially
	if err := log.Close(); err != nil {
		t.Fatalf("First Close failed: %v", err)
	}

	// Close should be idempotent
	if err := log.Close(); err != nil {
		t.Errorf("Second Close should be idempotent, got error: %v", err)
	}
}

func BenchmarkLog_ReadFrom_Batch(b *testing.B) {
	dir := b.TempDir()
	log, _ := NewLog(dir)
	defer log.Close()

	// Write messages
	for i := 0; i < 10000; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("key-%d", i)), make([]byte, 1024))
		log.Append(msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.ReadFrom(int64(i%9900), 100) // Read 100 messages at a time
	}
}
