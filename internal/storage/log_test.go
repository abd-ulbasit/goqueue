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
	"fmt"
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
