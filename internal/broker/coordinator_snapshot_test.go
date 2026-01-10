// =============================================================================
// COORDINATOR SNAPSHOT TESTS
// =============================================================================

package broker

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSnapshotEntryEncodeDecode tests encoding and decoding snapshot entries.
func TestSnapshotEntryEncodeDecode(t *testing.T) {
	entry := SnapshotEntry{
		Type:  RecordTypeOffsetCommit,
		Key:   []byte("test-group:test-topic:0"),
		Value: []byte("offset-data-here"),
	}

	// Encode
	data := entry.Encode()

	// Decode
	reader := &byteReader{data: data, offset: 0}
	decoded, err := DecodeSnapshotEntry(reader)
	if err != nil {
		t.Fatalf("failed to decode entry: %v", err)
	}

	// Verify
	if decoded.Type != entry.Type {
		t.Errorf("type mismatch: expected %d, got %d", entry.Type, decoded.Type)
	}
	if string(decoded.Key) != string(entry.Key) {
		t.Errorf("key mismatch: expected %s, got %s", entry.Key, decoded.Key)
	}
	if string(decoded.Value) != string(entry.Value) {
		t.Errorf("value mismatch: expected %s, got %s", entry.Value, decoded.Value)
	}
}

// TestSnapshotHeaderEncodeDecode tests encoding and decoding snapshot headers.
func TestSnapshotHeaderEncodeDecode(t *testing.T) {
	header := SnapshotHeader{
		Magic:         SnapshotMagic,
		Version:       SnapshotVersion,
		Type:          SnapshotTypeGroupCoordinator,
		CRC:           12345678,
		LastOffset:    999999,
		LastTimestamp: time.Now().UnixMilli(),
		EntryCount:    1000,
	}

	// Encode
	data := header.Encode()

	// Decode
	decoded, err := DecodeSnapshotHeader(data)
	if err != nil {
		t.Fatalf("failed to decode header: %v", err)
	}

	// Verify
	if decoded.Magic != header.Magic {
		t.Errorf("magic mismatch")
	}
	if decoded.Version != header.Version {
		t.Errorf("version mismatch")
	}
	if decoded.Type != header.Type {
		t.Errorf("type mismatch")
	}
	if decoded.CRC != header.CRC {
		t.Errorf("CRC mismatch")
	}
	if decoded.LastOffset != header.LastOffset {
		t.Errorf("last offset mismatch")
	}
	if decoded.LastTimestamp != header.LastTimestamp {
		t.Errorf("last timestamp mismatch")
	}
	if decoded.EntryCount != header.EntryCount {
		t.Errorf("entry count mismatch")
	}
}

// TestSnapshotWriterReader tests writing and reading snapshots.
func TestSnapshotWriterReader(t *testing.T) {
	dir := t.TempDir()

	// Create writer
	writer, err := NewSnapshotWriter(dir, SnapshotTypeGroupCoordinator, 10000)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Add entries
	entries := []SnapshotEntry{
		{
			Type:  RecordTypeOffsetCommit,
			Key:   []byte("group1:topic1:0"),
			Value: []byte("offset:100"),
		},
		{
			Type:  RecordTypeOffsetCommit,
			Key:   []byte("group1:topic1:1"),
			Value: []byte("offset:200"),
		},
		{
			Type:  RecordTypeGroupMetadata,
			Key:   []byte("group1"),
			Value: []byte("metadata-here"),
		},
	}

	for _, entry := range entries {
		if err := writer.AddEntry(entry); err != nil {
			t.Fatalf("failed to add entry: %v", err)
		}
	}

	// Close writer
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Open reader
	reader, err := OpenSnapshot(writer.GetPath())
	if err != nil {
		t.Fatalf("failed to open snapshot: %v", err)
	}
	defer reader.Close()

	// Verify header
	header := reader.GetHeader()
	if header.Type != SnapshotTypeGroupCoordinator {
		t.Errorf("type mismatch")
	}
	if header.LastOffset != 10000 {
		t.Errorf("last offset mismatch: expected 10000, got %d", header.LastOffset)
	}
	if header.EntryCount != 3 {
		t.Errorf("entry count mismatch: expected 3, got %d", header.EntryCount)
	}

	// Read entries
	readEntries, err := reader.ReadAllEntries()
	if err != nil {
		t.Fatalf("failed to read entries: %v", err)
	}

	if len(readEntries) != len(entries) {
		t.Fatalf("entry count mismatch: expected %d, got %d", len(entries), len(readEntries))
	}

	// Verify entries
	for i, entry := range entries {
		if readEntries[i].Type != entry.Type {
			t.Errorf("entry %d type mismatch", i)
		}
		if string(readEntries[i].Key) != string(entry.Key) {
			t.Errorf("entry %d key mismatch", i)
		}
		if string(readEntries[i].Value) != string(entry.Value) {
			t.Errorf("entry %d value mismatch", i)
		}
	}
}

// TestSnapshotCorruption tests handling of corrupted snapshots.
func TestSnapshotCorruption(t *testing.T) {
	dir := t.TempDir()

	// Create a corrupted snapshot file
	path := filepath.Join(dir, "snapshot-1-1000-1234567890.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Write invalid data
	f.Write([]byte("invalid snapshot data"))
	f.Close()

	// Try to open - should fail
	_, err = OpenSnapshot(path)
	if err == nil {
		t.Error("expected error opening corrupted snapshot")
	}
}

// TestSnapshotManager tests snapshot manager functionality.
func TestSnapshotManager(t *testing.T) {
	dir := t.TempDir()

	sm := NewSnapshotManager(dir, SnapshotTypeGroupCoordinator)

	// Initially should not need snapshot
	if sm.ShouldSnapshot() {
		t.Error("should not need snapshot initially")
	}

	// Process records up to threshold
	for i := int64(0); i < DefaultSnapshotRecordInterval; i++ {
		sm.RecordProcessed()
	}

	// Should need snapshot now
	if !sm.ShouldSnapshot() {
		t.Error("should need snapshot after record threshold")
	}

	// Reset counters
	sm.ResetCounters()

	// Should not need snapshot after reset
	if sm.ShouldSnapshot() {
		t.Error("should not need snapshot after reset")
	}
}

// TestSnapshotManagerTimeInterval tests time-based snapshot trigger.
func TestSnapshotManagerTimeInterval(t *testing.T) {
	dir := t.TempDir()

	sm := NewSnapshotManager(dir, SnapshotTypeGroupCoordinator)
	sm.timeInterval = 100 * time.Millisecond // Short interval for testing

	// Wait for time interval to pass
	time.Sleep(150 * time.Millisecond)

	// Should need snapshot now
	if !sm.ShouldSnapshot() {
		t.Error("should need snapshot after time interval")
	}
}

// TestSnapshotManagerCleanup tests snapshot cleanup.
func TestSnapshotManagerCleanup(t *testing.T) {
	dir := t.TempDir()

	sm := NewSnapshotManager(dir, SnapshotTypeGroupCoordinator)
	sm.maxSnapshots = 2

	// Create 5 snapshot files
	for i := 0; i < 5; i++ {
		writer, err := NewSnapshotWriter(dir, SnapshotTypeGroupCoordinator, int64(i*1000))
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}
		writer.AddEntry(SnapshotEntry{
			Type:  RecordTypeOffsetCommit,
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		writer.Close()
	}

	// Cleanup old snapshots
	if err := sm.CleanupOldSnapshots(); err != nil {
		t.Fatalf("failed to cleanup: %v", err)
	}

	// Should have only 2 snapshots left
	files, err := filepath.Glob(filepath.Join(dir, "snapshot-*.bin"))
	if err != nil {
		t.Fatalf("failed to glob: %v", err)
	}

	if len(files) != 2 {
		t.Errorf("expected 2 snapshots, got %d", len(files))
	}
}

// TestSnapshotManagerGetLatest tests finding the latest snapshot.
func TestSnapshotManagerGetLatest(t *testing.T) {
	dir := t.TempDir()

	sm := NewSnapshotManager(dir, SnapshotTypeGroupCoordinator)

	// Create snapshots at different offsets
	offsets := []int64{1000, 5000, 3000, 7000, 2000}
	for _, offset := range offsets {
		writer, err := NewSnapshotWriter(dir, SnapshotTypeGroupCoordinator, offset)
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}
		writer.AddEntry(SnapshotEntry{
			Type:  RecordTypeOffsetCommit,
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		writer.Close()
	}

	// Get latest
	latest, err := sm.GetLatestSnapshot()
	if err != nil {
		t.Fatalf("failed to get latest: %v", err)
	}

	// Should be the one with offset 7000
	var foundOffset int64
	fmt.Sscanf(filepath.Base(latest), "snapshot-1-%d-%d.bin", &foundOffset, new(int64))
	if foundOffset != 7000 {
		t.Errorf("expected latest offset 7000, got %d", foundOffset)
	}
}

// byteReader is a simple reader for testing.
type byteReader struct {
	data   []byte
	offset int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}
