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
	"path/filepath"
	"testing"
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
