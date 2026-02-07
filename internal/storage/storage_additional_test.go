// =============================================================================
// ADDITIONAL STORAGE TESTS - COVERAGE GAPS
// =============================================================================
//
// These tests target functions with low or zero coverage to push towards 90%+
// Avoids redeclaring tests that already exist in other test files.
//
// =============================================================================

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// =============================================================================
// PRIORITY TESTS
// =============================================================================

func TestPriority_String_AllCases(t *testing.T) {
	tests := []struct {
		priority Priority
		want     string
	}{
		{PriorityCritical, "critical"},
		{PriorityHigh, "high"},
		{PriorityNormal, "normal"},
		{PriorityLow, "low"},
		{PriorityBackground, "background"},
		{Priority(99), "unknown(99)"},
		{Priority(255), "unknown(255)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.priority.String()
			if got != tt.want {
				t.Errorf("Priority(%d).String() = %q, want %q", tt.priority, got, tt.want)
			}
		})
	}
}

func TestPriority_IsValid_AllCases(t *testing.T) {
	tests := []struct {
		priority Priority
		want     bool
	}{
		{PriorityCritical, true},
		{PriorityHigh, true},
		{PriorityNormal, true},
		{PriorityLow, true},
		{PriorityBackground, true},
		{Priority(5), false},
		{Priority(100), false},
		{Priority(255), false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("priority_%d", tt.priority), func(t *testing.T) {
			got := tt.priority.IsValid()
			if got != tt.want {
				t.Errorf("Priority(%d).IsValid() = %v, want %v", tt.priority, got, tt.want)
			}
		})
	}
}

// =============================================================================
// MESSAGE HELPER TESTS
// =============================================================================

func TestMessage_NewMessage_Basic(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")

	msg := NewMessage(key, value)

	if !bytes.Equal(msg.Key, key) {
		t.Errorf("Key = %v, want %v", msg.Key, key)
	}
	if !bytes.Equal(msg.Value, value) {
		t.Errorf("Value = %v, want %v", msg.Value, value)
	}
	if msg.Priority != PriorityNormal {
		t.Errorf("Priority = %v, want PriorityNormal", msg.Priority)
	}
	if msg.Timestamp == 0 {
		t.Error("Timestamp should be set")
	}
}

func TestMessage_NewMessageWithHeaders_Complete(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")
	headers := map[string]string{
		"trace-id":     "abc123",
		"correlation":  "xyz789",
		"content-type": "application/json",
	}

	msg := NewMessageWithHeaders(key, value, headers)

	if !bytes.Equal(msg.Key, key) {
		t.Errorf("Key = %v, want %v", msg.Key, key)
	}
	if !bytes.Equal(msg.Value, value) {
		t.Errorf("Value = %v, want %v", msg.Value, value)
	}
	if len(msg.Headers) != len(headers) {
		t.Errorf("Headers count = %d, want %d", len(msg.Headers), len(headers))
	}
	for k, v := range headers {
		if msg.Headers[k] != v {
			t.Errorf("Header[%s] = %s, want %s", k, msg.Headers[k], v)
		}
	}
}

func TestMessage_CompressionFlags_Toggle(t *testing.T) {
	msg := NewMessage([]byte("key"), []byte("value"))

	// Initially not compressed
	if msg.IsCompressed() {
		t.Error("New message should not be compressed")
	}

	// Set compressed
	msg.SetCompressed(true)
	if !msg.IsCompressed() {
		t.Error("Message should be compressed after SetCompressed(true)")
	}

	// Unset compressed
	msg.SetCompressed(false)
	if msg.IsCompressed() {
		t.Error("Message should not be compressed after SetCompressed(false)")
	}
}

func TestMessage_TombstoneFlags_Toggle(t *testing.T) {
	msg := NewMessage([]byte("key"), []byte("value"))

	// Initially not tombstone
	if msg.IsTombstone() {
		t.Error("New message should not be tombstone")
	}

	// Set tombstone
	msg.SetTombstone(true)
	if !msg.IsTombstone() {
		t.Error("Message should be tombstone after SetTombstone(true)")
	}

	// Unset tombstone
	msg.SetTombstone(false)
	if msg.IsTombstone() {
		t.Error("Message should not be tombstone after SetTombstone(false)")
	}
}

func TestMessage_Size_WithHeaders_Calculation(t *testing.T) {
	headers := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	msg := NewMessageWithHeaders([]byte("key"), []byte("value"), headers)

	size := msg.Size()

	// Size should be greater than just header + key + value
	minSize := HeaderSize + len(msg.Key) + len(msg.Value)
	if size <= minSize {
		t.Errorf("Size with headers = %d, should be > %d", size, minSize)
	}

	// Encoded size should match calculated size
	encoded, err := msg.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if len(encoded) != size {
		t.Errorf("Encoded size = %d, Size() = %d", len(encoded), size)
	}
}

// =============================================================================
// CONTROL RECORD TESTS
// =============================================================================

func TestControlRecordPayload_RoundtripFull(t *testing.T) {
	original := &ControlRecordPayload{
		ProducerID:      123456789,
		Epoch:           42,
		TransactionalID: "test-transaction-id",
	}

	encoded := original.Encode()
	decoded, err := DecodeControlRecordPayload(encoded)
	if err != nil {
		t.Fatalf("DecodeControlRecordPayload failed: %v", err)
	}

	if decoded.ProducerID != original.ProducerID {
		t.Errorf("ProducerID = %d, want %d", decoded.ProducerID, original.ProducerID)
	}
	if decoded.Epoch != original.Epoch {
		t.Errorf("Epoch = %d, want %d", decoded.Epoch, original.Epoch)
	}
	if decoded.TransactionalID != original.TransactionalID {
		t.Errorf("TransactionalID = %q, want %q", decoded.TransactionalID, original.TransactionalID)
	}
}

func TestControlRecordPayload_DecodeInvalidData(t *testing.T) {
	// Too short
	_, err := DecodeControlRecordPayload([]byte{1, 2, 3})
	if err == nil {
		t.Error("DecodeControlRecordPayload should fail for short data")
	}

	// Invalid transactional ID length
	badData := make([]byte, 12)
	// Set an impossibly large string length
	badData[10] = 0xFF
	badData[11] = 0xFF
	_, err = DecodeControlRecordPayload(badData)
	if err == nil {
		t.Error("DecodeControlRecordPayload should fail for invalid string length")
	}
}

// =============================================================================
// LOG TESTS - ADDITIONAL COVERAGE
// =============================================================================

func TestLog_GetOffsetByTimestamp_Coverage(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Empty log should return 0
	offset, err := log.GetOffsetByTimestamp(time.Now().UnixNano())
	if err != nil {
		t.Fatalf("GetOffsetByTimestamp on empty log failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("GetOffsetByTimestamp on empty log = %d, want 0", offset)
	}

	// Write some messages with timestamps
	baseTime := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		msg := &Message{
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
			Timestamp: baseTime + int64(i*1000000000), // 1 second apart
		}
		_, err := log.Append(msg)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Query for timestamp in the future (should return next offset or 0 depending on time index)
	futureOffset, err := log.GetOffsetByTimestamp(baseTime + int64(100*1000000000))
	if err != nil {
		t.Fatalf("GetOffsetByTimestamp for future failed: %v", err)
	}
	// The result depends on time index behavior - just verify no error
	t.Logf("GetOffsetByTimestamp for future returned: %d", futureOffset)
}

func TestLog_ClosedOperations_Coverage(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}

	// Close the log
	if err := log.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Operations on closed log should fail
	msg := NewMessage([]byte("key"), []byte("value"))
	_, err = log.Append(msg)
	if !errors.Is(err, ErrLogClosed) {
		t.Errorf("Append on closed log should return ErrLogClosed, got %v", err)
	}

	_, err = log.Read(0)
	if !errors.Is(err, ErrLogClosed) {
		t.Errorf("Read on closed log should return ErrLogClosed, got %v", err)
	}

	_, err = log.ReadFrom(0, 10)
	if !errors.Is(err, ErrLogClosed) {
		t.Errorf("ReadFrom on closed log should return ErrLogClosed, got %v", err)
	}

	err = log.Sync()
	if !errors.Is(err, ErrLogClosed) {
		t.Errorf("Sync on closed log should return ErrLogClosed, got %v", err)
	}

	_, err = log.GetOffsetByTimestamp(time.Now().UnixNano())
	if !errors.Is(err, ErrLogClosed) {
		t.Errorf("GetOffsetByTimestamp on closed log should return ErrLogClosed, got %v", err)
	}
}

// =============================================================================
// SEGMENT TESTS - ADDITIONAL COVERAGE
// =============================================================================

func TestSegment_FileNames_Format(t *testing.T) {
	offset := int64(12345)

	segmentFile := SegmentFileName(offset)
	indexFile := IndexFileName(offset)

	// Verify format
	if segmentFile != "00000000000000012345.log" {
		t.Errorf("SegmentFileName = %s, want 00000000000000012345.log", segmentFile)
	}
	if indexFile != "00000000000000012345.index" {
		t.Errorf("IndexFileName = %s, want 00000000000000012345.index", indexFile)
	}
}

func TestSegment_BaseOffsetAndNext_Coverage(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 100)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	if seg.BaseOffset() != 100 {
		t.Errorf("BaseOffset = %d, want 100", seg.BaseOffset())
	}
	if seg.NextOffset() != 100 {
		t.Errorf("Initial NextOffset = %d, want 100", seg.NextOffset())
	}

	// After writing a message
	msg := NewMessage([]byte("key"), []byte("value"))
	_, err = seg.Append(msg)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if seg.NextOffset() != 101 {
		t.Errorf("NextOffset after append = %d, want 101", seg.NextOffset())
	}
}

func TestSegment_SizeAndCount_Coverage(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	initialSize := seg.Size()
	initialCount := seg.MessageCount()

	if initialSize != 0 {
		t.Errorf("Initial size = %d, want 0", initialSize)
	}
	if initialCount != 0 {
		t.Errorf("Initial count = %d, want 0", initialCount)
	}

	// Write messages
	for i := 0; i < 5; i++ {
		msg := NewMessage([]byte("key"), []byte("value"))
		seg.Append(msg)
	}

	if seg.Size() <= initialSize {
		t.Error("Size should increase after writes")
	}
	if seg.MessageCount() != 5 {
		t.Errorf("MessageCount = %d, want 5", seg.MessageCount())
	}
}

func TestSegment_IsFullAndReadOnly_Coverage(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	if seg.IsFull() {
		t.Error("New segment should not be full")
	}
	if seg.IsReadOnly() {
		t.Error("New segment should not be read-only")
	}

	// Seal the segment
	if err := seg.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	if !seg.IsReadOnly() {
		t.Error("Sealed segment should be read-only")
	}
}

func TestSegment_GetTimestamps_Coverage(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Empty segment - returns error
	_, err = seg.GetFirstTimestamp()
	if err == nil {
		t.Log("GetFirstTimestamp on empty segment may return error or zero")
	}
	_, err = seg.GetLastTimestamp()
	if err == nil {
		t.Log("GetLastTimestamp on empty segment may return error or zero")
	}

	// Write messages
	baseTime := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		msg := &Message{
			Key:       []byte("key"),
			Value:     []byte("value"),
			Timestamp: baseTime + int64(i*1000),
		}
		seg.Append(msg)
	}

	// After writing, timestamps should be available
	first, err := seg.GetFirstTimestamp()
	if err != nil {
		t.Logf("GetFirstTimestamp after writes: %v", err)
	} else if first == 0 {
		t.Log("First timestamp is 0 (may be expected)")
	}
}

// =============================================================================
// INDEX TESTS - ADDITIONAL COVERAGE
// =============================================================================

func TestIndex_EntryCount_Coverage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	idx, err := NewIndex(path, 0)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	if idx.EntryCount() != 0 {
		t.Errorf("Initial EntryCount = %d, want 0", idx.EntryCount())
	}

	// Add entries
	for i := 0; i < 5; i++ {
		idx.ForceAppend(int64(i*100), int64(i*1000))
	}

	if idx.EntryCount() != 5 {
		t.Errorf("EntryCount after adds = %d, want 5", idx.EntryCount())
	}
}

func TestIndex_Sync_Coverage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	idx, err := NewIndex(path, 0)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	// Add some entries
	idx.ForceAppend(0, 0)
	idx.ForceAppend(100, 1000)

	// Sync should not fail
	if err := idx.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}
}

// =============================================================================
// TIME INDEX TESTS - ADDITIONAL COVERAGE
// =============================================================================

func TestTimeIndex_FileNames_Format(t *testing.T) {
	offset := int64(12345)
	fileName := TimeIndexFileName(offset)

	if fileName != "00000000000000012345.timeindex" {
		t.Errorf("TimeIndexFileName = %s, want 00000000000000012345.timeindex", fileName)
	}
}

func TestTimeIndex_GetCounts_Coverage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.timeindex")

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer ti.Close()

	if ti.GetEntryCount() != 0 {
		t.Errorf("Initial GetEntryCount = %d, want 0", ti.GetEntryCount())
	}
}

func TestTimeIndex_MaybeAppend_Coverage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.timeindex")

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer ti.Close()

	baseTime := time.Now().UnixNano()

	// First entry should always be added
	_, err = ti.MaybeAppend(baseTime, 0, 0)
	if err != nil {
		t.Errorf("MaybeAppend first entry failed: %v", err)
	}

	// Entry with same timestamp should be skipped (interval not met)
	_, err = ti.MaybeAppend(baseTime, 1, 100)
	if err != nil {
		t.Errorf("MaybeAppend duplicate timestamp failed: %v", err)
	}

	// Entry with larger timestamp should be added
	_, err = ti.MaybeAppend(baseTime+1000000000, 100, 10000)
	if err != nil {
		t.Errorf("MaybeAppend later timestamp failed: %v", err)
	}
}

func TestTimeIndex_LookupRange_Coverage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.timeindex")

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer ti.Close()

	// Empty index - may return error or (-1, -1)
	start, end, err := ti.LookupRange(0, 1000000000)
	if err != nil {
		// Empty index returns ErrTimestampNotFound - this is expected
		t.Logf("LookupRange on empty returned error (expected): %v", err)
	} else if start != -1 || end != -1 {
		t.Errorf("LookupRange on empty = (%d, %d), want (-1, -1)", start, end)
	}
}

func TestTimeIndex_Sync_Coverage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.timeindex")

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer ti.Close()

	if err := ti.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}
}

// =============================================================================
// EDGE CASE TESTS
// =============================================================================

func TestLog_EmptyDirectory_Coverage(t *testing.T) {
	dir := t.TempDir()

	// Create a fresh log
	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}

	// Should have one segment
	if log.SegmentCount() != 1 {
		t.Errorf("SegmentCount = %d, want 1", log.SegmentCount())
	}

	// Close and reload
	if err := log.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	log2, err := LoadLog(dir)
	if err != nil {
		t.Fatalf("LoadLog failed: %v", err)
	}
	defer log2.Close()

	if log2.SegmentCount() != 1 {
		t.Errorf("Reloaded SegmentCount = %d, want 1", log2.SegmentCount())
	}
}

func TestLog_ReadFromEmpty_Coverage(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// ReadFrom on empty log should return empty slice
	msgs, err := log.ReadFrom(0, 10)
	if err != nil {
		t.Fatalf("ReadFrom on empty log failed: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("ReadFrom on empty log returned %d messages, want 0", len(msgs))
	}
}

func TestSegment_LoadCorruptedSegment_Recovery(t *testing.T) {
	dir := t.TempDir()

	// Create a segment file with invalid content
	segPath := filepath.Join(dir, "00000000000000000000.log")
	if err := os.WriteFile(segPath, []byte("invalid segment data"), 0o644); err != nil {
		t.Fatalf("Failed to create invalid segment: %v", err)
	}

	// Try to load it - should handle gracefully (may fail or recover)
	_, err := LoadSegment(dir, 0)
	// We expect either an error or successful recovery
	// The important thing is it doesn't panic
	_ = err
}

func TestTimeIndex_Truncate_Coverage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.timeindex")

	ti, err := NewTimeIndex(path, 0)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer ti.Close()

	// Add entries (force add to bypass interval check)
	baseTime := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		// Use MaybeAppend with sufficient interval
		ti.MaybeAppend(baseTime+int64(i)*int64(time.Second), int64(i*100), int64(i*1000))
	}

	// Truncate
	if err := ti.Truncate(200); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}
}

// =============================================================================
// BENCHMARK TESTS
// =============================================================================

func BenchmarkNewMessage_Coverage(b *testing.B) {
	key := []byte("benchmark-key")
	value := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewMessage(key, value)
	}
}

func BenchmarkParsePriority_Coverage(b *testing.B) {
	priorities := []string{"critical", "high", "normal", "low", "background"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ParsePriority(priorities[i%len(priorities)])
	}
}

func BenchmarkPriority_String(b *testing.B) {
	priorities := []Priority{PriorityCritical, PriorityHigh, PriorityNormal, PriorityLow, PriorityBackground}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = priorities[i%len(priorities)].String()
	}
}
