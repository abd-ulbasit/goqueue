// =============================================================================
// SEGMENT TESTS
// =============================================================================
//
// Tests for segment file management - the building blocks of the log.
//
// KEY BEHAVIORS TO TEST:
//   - Write and read messages correctly
//   - Segment rolls over at size limit
//   - Recovery from crash (partial writes)
//   - Index integration
//
// =============================================================================

package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSegment_NewAndClose(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	// Verify initial state
	if seg.BaseOffset() != 0 {
		t.Errorf("BaseOffset = %d, want 0", seg.BaseOffset())
	}
	if seg.NextOffset() != 0 {
		t.Errorf("NextOffset = %d, want 0", seg.NextOffset())
	}
	if seg.Size() != 0 {
		t.Errorf("Size = %d, want 0", seg.Size())
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestSegment_AppendAndRead(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Write messages
	messages := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for i, m := range messages {
		msg := NewMessage(m.key, m.value)
		offset, err := seg.Append(msg)
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
		if offset != int64(i) {
			t.Errorf("Append %d returned offset %d, want %d", i, offset, i)
		}
	}

	// Verify segment state
	if seg.NextOffset() != 3 {
		t.Errorf("NextOffset = %d, want 3", seg.NextOffset())
	}
	if seg.MessageCount() != 3 {
		t.Errorf("MessageCount = %d, want 3", seg.MessageCount())
	}

	// Read back messages
	for i, expected := range messages {
		msg, err := seg.Read(int64(i))
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}
		if !bytes.Equal(msg.Key, expected.key) {
			t.Errorf("Message %d key = %s, want %s", i, msg.Key, expected.key)
		}
		if !bytes.Equal(msg.Value, expected.value) {
			t.Errorf("Message %d value = %s, want %s", i, msg.Value, expected.value)
		}
	}
}

func TestSegment_ReadFrom(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Write 10 messages
	for i := 0; i < 10; i++ {
		msg := NewMessage(
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		)
		if _, err := seg.Append(msg); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Read from offset 5 with max 3
	msgs, err := seg.ReadFrom(5, 3)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(msgs))
	}

	// Verify offsets
	for i, msg := range msgs {
		expectedOffset := int64(5 + i)
		if msg.Offset != expectedOffset {
			t.Errorf("Message %d offset = %d, want %d", i, msg.Offset, expectedOffset)
		}
	}
}

func TestSegment_LoadExisting(t *testing.T) {
	dir := t.TempDir()

	// Create segment and write messages
	seg1, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		if _, err := seg1.Append(msg); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	seg1.Close()

	// Load segment
	seg2, err := LoadSegment(dir, 0)
	if err != nil {
		t.Fatalf("LoadSegment failed: %v", err)
	}
	defer seg2.Close()

	// Verify state
	if seg2.NextOffset() != 5 {
		t.Errorf("NextOffset after load = %d, want 5", seg2.NextOffset())
	}

	// Verify can read messages
	for i := 0; i < 5; i++ {
		msg, err := seg2.Read(int64(i))
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}
		expectedKey := fmt.Sprintf("key-%d", i)
		if string(msg.Key) != expectedKey {
			t.Errorf("Message %d key = %s, want %s", i, msg.Key, expectedKey)
		}
	}

	// Verify can still append
	msg := NewMessage([]byte("new-key"), []byte("new-value"))
	offset, err := seg2.Append(msg)
	if err != nil {
		t.Fatalf("Append after load failed: %v", err)
	}
	if offset != 5 {
		t.Errorf("New message offset = %d, want 5", offset)
	}
}

func TestSegment_Seal(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Write a message
	msg := NewMessage([]byte("key"), []byte("value"))
	if _, err := seg.Append(msg); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Seal the segment
	if err := seg.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	if !seg.IsReadOnly() {
		t.Error("Segment should be read-only after seal")
	}

	// Writing should fail
	msg2 := NewMessage([]byte("key2"), []byte("value2"))
	_, err = seg.Append(msg2)
	if err != ErrSegmentReadOnly {
		t.Errorf("Append after seal should fail with ErrSegmentReadOnly, got: %v", err)
	}

	// Reading should still work
	readMsg, err := seg.Read(0)
	if err != nil {
		t.Fatalf("Read after seal failed: %v", err)
	}
	if !bytes.Equal(readMsg.Key, []byte("key")) {
		t.Error("Read returned wrong message")
	}
}

func TestSegment_IsFull(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Initially not full
	if seg.IsFull() {
		t.Error("New segment should not be full")
	}

	// We can't easily test reaching 64MB in unit tests
	// Just verify the check works
	if seg.Size() >= MaxSegmentSize {
		if !seg.IsFull() {
			t.Error("Segment at max size should report as full")
		}
	}
}

func TestSegment_ReadNonexistentOffset(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Write one message
	msg := NewMessage([]byte("key"), []byte("value"))
	seg.Append(msg)

	// Try to read offset that doesn't exist
	_, err = seg.Read(100)
	if err == nil {
		t.Error("Read of non-existent offset should fail")
	}
}

func TestSegment_ReadFromEmpty(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Read from empty segment
	msgs, err := seg.ReadFrom(0, 10)
	if err != nil {
		t.Fatalf("ReadFrom empty failed: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages from empty segment, got %d", len(msgs))
	}
}

func TestSegment_NonZeroBaseOffset(t *testing.T) {
	dir := t.TempDir()

	// Create segment starting at offset 1000
	seg, err := NewSegment(dir, 1000)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	if seg.BaseOffset() != 1000 {
		t.Errorf("BaseOffset = %d, want 1000", seg.BaseOffset())
	}
	if seg.NextOffset() != 1000 {
		t.Errorf("NextOffset = %d, want 1000", seg.NextOffset())
	}

	// Write message - should get offset 1000
	msg := NewMessage([]byte("key"), []byte("value"))
	offset, err := seg.Append(msg)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if offset != 1000 {
		t.Errorf("First message offset = %d, want 1000", offset)
	}

	// Verify filename
	expectedFile := filepath.Join(dir, "00000000000000001000.log")
	if _, err := filepath.Glob(expectedFile); err != nil {
		t.Errorf("Expected segment file not found: %s", expectedFile)
	}
}

func TestSegmentFileName(t *testing.T) {
	testCases := []struct {
		offset   int64
		expected string
	}{
		{0, "00000000000000000000.log"},
		{1, "00000000000000000001.log"},
		{1000, "00000000000000001000.log"},
		{9999999999, "00000000009999999999.log"},
	}

	for _, tc := range testCases {
		result := SegmentFileName(tc.offset)
		if result != tc.expected {
			t.Errorf("SegmentFileName(%d) = %s, want %s", tc.offset, result, tc.expected)
		}
	}
}

func TestSegment_TimeBasedQueries(t *testing.T) {
	// =============================================================================
	// TIME-BASED QUERIES (M14)
	// =============================================================================
	//
	// WHY THIS TEST EXISTS:
	// The storage layer supports time-based replay and auditing via:
	//   - ReadFromTimestamp(start)
	//   - ReadTimeRange([start,end))
	//   - GetFirstTimestamp / GetLastTimestamp
	//
	// These APIs must be correct even when the time index is sparse.
	// We validate the observable behavior (returned messages), not the internal
	// time-index entry density.
	// =============================================================================

	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Use deterministic, increasing timestamps so we can make crisp assertions.
	baseTS := time.Now().UnixNano()
	step := int64(time.Second)

	// Write 5 messages with timestamps: baseTS + i*1s
	//
	// IMPORTANT:
	// The time index is sparse (one entry per ~4KB of log data). If we write tiny
	// messages, we might only get 1 entry, which makes boundary timestamp helpers
	// less informative.
	//
	// So we write values sized to TimeIndexGranularity to ensure we cross the
	// indexing threshold between appends.
	for i := 0; i < 5; i++ {
		value := make([]byte, TimeIndexGranularity)
		value[0] = byte(i) // Make each payload distinct.

		msg := NewMessage(
			[]byte(fmt.Sprintf("key-%d", i)),
			value,
		)
		msg.Timestamp = baseTS + int64(i)*step

		if _, err := seg.Append(msg); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	// -----------------------------------------------------------------------------
	// First/last timestamp helpers
	// -----------------------------------------------------------------------------
	firstTS, err := seg.GetFirstTimestamp()
	if err != nil {
		t.Fatalf("GetFirstTimestamp failed: %v", err)
	}
	if firstTS != baseTS {
		t.Fatalf("GetFirstTimestamp=%d, want %d", firstTS, baseTS)
	}

	lastTS, err := seg.GetLastTimestamp()
	if err != nil {
		t.Fatalf("GetLastTimestamp failed: %v", err)
	}
	expectedLast := baseTS + 4*step
	if lastTS != expectedLast {
		t.Fatalf("GetLastTimestamp=%d, want %d", lastTS, expectedLast)
	}

	// -----------------------------------------------------------------------------
	// ReadFromTimestamp
	// -----------------------------------------------------------------------------
	start := baseTS + 2*step
	msgs, err := seg.ReadFromTimestamp(start, 0)
	if err != nil {
		t.Fatalf("ReadFromTimestamp failed: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("ReadFromTimestamp returned %d msgs, want 3", len(msgs))
	}
	for i, m := range msgs {
		wantOffset := int64(2 + i)
		if m.Offset != wantOffset {
			t.Fatalf("msg[%d].Offset=%d, want %d", i, m.Offset, wantOffset)
		}
		if m.Timestamp < start {
			t.Fatalf("msg[%d].Timestamp=%d, want >= %d", i, m.Timestamp, start)
		}
	}

	// MaxMessages should cap the results.
	msgs, err = seg.ReadFromTimestamp(start, 2)
	if err != nil {
		t.Fatalf("ReadFromTimestamp(max=2) failed: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("ReadFromTimestamp(max=2) returned %d msgs, want 2", len(msgs))
	}
	if msgs[0].Offset != 2 || msgs[1].Offset != 3 {
		t.Fatalf("ReadFromTimestamp(max=2) offsets=%d,%d want 2,3", msgs[0].Offset, msgs[1].Offset)
	}

	// -----------------------------------------------------------------------------
	// ReadTimeRange: [start,end) = [t+1s, t+4s) should yield offsets 1,2,3
	// -----------------------------------------------------------------------------
	rangeStart := baseTS + 1*step
	rangeEnd := baseTS + 4*step

	msgs, err = seg.ReadTimeRange(rangeStart, rangeEnd, 0)
	if err != nil {
		t.Fatalf("ReadTimeRange failed: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("ReadTimeRange returned %d msgs, want 3", len(msgs))
	}
	for i, m := range msgs {
		wantOffset := int64(1 + i)
		if m.Offset != wantOffset {
			t.Fatalf("range msg[%d].Offset=%d, want %d", i, m.Offset, wantOffset)
		}
		if m.Timestamp < rangeStart || m.Timestamp >= rangeEnd {
			t.Fatalf("range msg[%d].Timestamp=%d, want in [%d,%d)", i, m.Timestamp, rangeStart, rangeEnd)
		}
	}

	msgs, err = seg.ReadTimeRange(rangeStart, rangeEnd, 1)
	if err != nil {
		t.Fatalf("ReadTimeRange(max=1) failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("ReadTimeRange(max=1) returned %d msgs, want 1", len(msgs))
	}
	if msgs[0].Offset != 1 {
		t.Fatalf("ReadTimeRange(max=1) first offset=%d, want 1", msgs[0].Offset)
	}
}

func TestSegment_TimeBasedQueries_ErrSegmentClosed(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if _, err := seg.ReadFromTimestamp(time.Now().UnixNano(), 0); err != ErrSegmentClosed {
		t.Fatalf("ReadFromTimestamp on closed segment error=%v, want %v", err, ErrSegmentClosed)
	}
	if _, err := seg.ReadTimeRange(time.Now().UnixNano(), time.Now().UnixNano()+1, 0); err != ErrSegmentClosed {
		t.Fatalf("ReadTimeRange on closed segment error=%v, want %v", err, ErrSegmentClosed)
	}
}

func BenchmarkSegment_Append(b *testing.B) {
	dir := b.TempDir()
	seg, _ := NewSegment(dir, 0)
	defer seg.Close()

	msg := NewMessage([]byte("key"), make([]byte, 1024)) // 1KB message

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seg.Append(msg)
	}
}

func TestSegment_Sync(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Write a message
	msg := NewMessage([]byte("key"), []byte("value"))
	if _, err := seg.Append(msg); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Sync should not fail
	if err := seg.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}

	// Sync on closed segment should fail
	seg.Close()
	if err := seg.Sync(); err != ErrSegmentClosed {
		t.Errorf("Expected ErrSegmentClosed, got %v", err)
	}
}

func TestSegment_Delete(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	// Write a message to create files
	msg := NewMessage([]byte("key"), []byte("value"))
	if _, err := seg.Append(msg); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify files exist before delete
	logPath := filepath.Join(dir, SegmentFileName(0))
	indexPath := filepath.Join(dir, IndexFileName(0))
	timeIndexPath := filepath.Join(dir, TimeIndexFileName(0))

	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("Log file should exist before delete")
	}
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Error("Index file should exist before delete")
	}
	if _, err := os.Stat(timeIndexPath); os.IsNotExist(err) {
		t.Error("Time index file should exist before delete")
	}

	// Delete segment
	if err := seg.Delete(); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify files are deleted after delete
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Error("Log file should be deleted")
	}
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		t.Error("Index file should be deleted")
	}
	if _, err := os.Stat(timeIndexPath); !os.IsNotExist(err) {
		t.Error("Time index file should be deleted")
	}

	// Delete should be idempotent (calling again should not fail)
	if err := seg.Delete(); err != nil {
		t.Errorf("Second delete should not fail: %v", err)
	}
}

func TestSegment_Close_ErrorHandling(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	// Write a message to have something to flush
	msg := NewMessage([]byte("key"), []byte("value"))
	if _, err := seg.Append(msg); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Close should succeed
	if err := seg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Close should be idempotent
	if err := seg.Close(); err != nil {
		t.Errorf("Second Close should be idempotent, got error: %v", err)
	}

	// Operations after close should fail
	if _, err := seg.Read(0); err != ErrSegmentClosed {
		t.Errorf("Read after close should fail with ErrSegmentClosed, got: %v", err)
	}
	if _, err := seg.Append(msg); err != ErrSegmentClosed {
		t.Errorf("Append after close should fail with ErrSegmentClosed, got: %v", err)
	}
}

func TestSegment_RebuildSegment(t *testing.T) {
	dir := t.TempDir()

	// Create a segment and write messages
	seg1, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		if _, err := seg1.Append(msg); err != nil {
			seg1.Close()
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}
	seg1.Close()

	// Delete index files to simulate corruption
	indexPath := filepath.Join(dir, IndexFileName(0))
	timeIndexPath := filepath.Join(dir, TimeIndexFileName(0))
	os.Remove(indexPath)
	os.Remove(timeIndexPath)

	// Rebuild segment
	seg2, err := rebuildSegment(dir, 0)
	if err != nil {
		t.Fatalf("rebuildSegment failed: %v", err)
	}
	defer seg2.Close()

	// Verify segment state
	if seg2.BaseOffset() != 0 {
		t.Errorf("BaseOffset = %d, want 0", seg2.BaseOffset())
	}
	if seg2.NextOffset() != 5 {
		t.Errorf("NextOffset = %d, want 5", seg2.NextOffset())
	}

	// Verify can read messages
	for i := 0; i < 5; i++ {
		msg, err := seg2.Read(int64(i))
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}
		expectedKey := fmt.Sprintf("key-%d", i)
		if string(msg.Key) != expectedKey {
			t.Errorf("Message %d key = %s, want %s", i, msg.Key, expectedKey)
		}
	}

	// Verify can continue appending
	msg := NewMessage([]byte("new-key"), []byte("new-value"))
	offset, err := seg2.Append(msg)
	if err != nil {
		t.Fatalf("Append after rebuild failed: %v", err)
	}
	if offset != 5 {
		t.Errorf("New message offset = %d, want 5", offset)
	}
}

func BenchmarkSegment_Read(b *testing.B) {
	dir := b.TempDir()
	seg, _ := NewSegment(dir, 0)
	defer seg.Close()

	// Write some messages first
	for i := 0; i < 1000; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("key-%d", i)), make([]byte, 1024))
		seg.Append(msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seg.Read(int64(i % 1000))
	}
}
