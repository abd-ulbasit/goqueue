// =============================================================================
// LOG COMPACTION TESTS
// =============================================================================

package broker

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"goqueue/internal/storage"
)

// TestCalculateDirtyRatio tests dirty ratio calculation.
func TestCalculateDirtyRatio(t *testing.T) {
	baseDir := t.TempDir()

	// Create partition
	p, err := NewPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	defer p.Close()

	// Append records with duplicates
	// Key "key1" appears 3 times (2 duplicates)
	// Key "key2" appears 2 times (1 duplicate)
	// Total: 5 records, 2 unique keys, 3 duplicates
	records := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key1", "value1-v2"}, // Duplicate
		{"key2", "value2-v2"}, // Duplicate
		{"key1", "value1-v3"}, // Duplicate
	}

	for _, rec := range records {
		msg := storage.NewMessage([]byte(rec.key), []byte(rec.value))
		if _, err := p.ProduceMessage(msg); err != nil {
			t.Fatalf("failed to produce: %v", err)
		}
	}

	// Calculate dirty ratio
	dirtyRatio, totalRecords, uniqueKeys, err := CalculateDirtyRatio(p)
	if err != nil {
		t.Fatalf("failed to calculate dirty ratio: %v", err)
	}

	// Verify counts
	if totalRecords != 5 {
		t.Errorf("expected 5 total records, got %d", totalRecords)
	}
	if uniqueKeys != 2 {
		t.Errorf("expected 2 unique keys, got %d", uniqueKeys)
	}

	// Expected dirty ratio: (5 - 2) / 5 = 0.6 (60%)
	expected := 0.6
	if dirtyRatio < expected-0.01 || dirtyRatio > expected+0.01 {
		t.Errorf("dirty ratio mismatch: expected %.2f, got %.2f", expected, dirtyRatio)
	}
}

// TestCompactorShouldCompact tests compaction trigger logic.
func TestCompactorShouldCompact(t *testing.T) {
	baseDir := t.TempDir()

	// Create partition with low duplicates
	p, err := NewPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	defer p.Close()

	compactor := NewCompactor()

	// Add unique records (no duplicates)
	for i := 0; i < 10; i++ {
		key := []byte("unique-key-" + string(rune('a'+i)))
		msg := storage.NewMessage(key, []byte("value"))
		p.ProduceMessage(msg)
	}

	should, ratio, err := compactor.ShouldCompact(p)
	if err != nil {
		t.Fatalf("ShouldCompact failed: %v", err)
	}
	if should {
		t.Errorf("should not trigger compaction with low dirty ratio (%.2f)", ratio)
	}

	// Add duplicates to push dirty ratio > 50%
	for i := 0; i < 30; i++ {
		msg := storage.NewMessage([]byte("duplicate-key"), []byte("value"))
		p.ProduceMessage(msg)
	}

	should, ratio, err = compactor.ShouldCompact(p)
	if err != nil {
		t.Fatalf("ShouldCompact failed: %v", err)
	}
	if !should {
		t.Errorf("should trigger compaction with high dirty ratio (%.2f)", ratio)
	}
}

// TestBuildKeyMap tests building the key map for compaction.
func TestBuildKeyMap(t *testing.T) {
	baseDir := t.TempDir()

	// Create partition
	p, err := NewPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	defer p.Close()

	// Append records with updates (last value wins)
	records := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key1", "value1-updated"}, // Update key1
		{"key3", "value3"},
		{"key2", "value2-updated-v2"}, // Update key2 again
	}

	for _, rec := range records {
		msg := storage.NewMessage([]byte(rec.key), []byte(rec.value))
		p.ProduceMessage(msg)
	}

	// Build key map
	keyMap, totalRecords, _, err := buildKeyMap(p.Log(), DefaultTombstoneRetention)
	if err != nil {
		t.Fatalf("failed to build key map: %v", err)
	}

	// Verify total records
	if totalRecords != 5 {
		t.Errorf("expected 5 total records, got %d", totalRecords)
	}

	// Verify key map contains only 3 unique keys
	if len(keyMap) != 3 {
		t.Errorf("expected 3 unique keys, got %d", len(keyMap))
	}

	// Check key1 has latest value
	if string(keyMap["key1"].Value) != "value1-updated" {
		t.Errorf("key1 should have latest value, got %s", string(keyMap["key1"].Value))
	}

	// Check key2 has latest value
	if string(keyMap["key2"].Value) != "value2-updated-v2" {
		t.Errorf("key2 should have latest value, got %s", string(keyMap["key2"].Value))
	}

	// Check key3 exists
	if _, ok := keyMap["key3"]; !ok {
		t.Error("key3 should exist")
	}
}

// TestBuildKeyMapTombstones tests tombstone handling in key map.
func TestBuildKeyMapTombstones(t *testing.T) {
	baseDir := t.TempDir()

	// Create partition
	p, err := NewPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	defer p.Close()

	// Add a record
	msg1 := storage.NewMessage([]byte("key1"), []byte("value1"))
	p.ProduceMessage(msg1)

	// Add a tombstone (empty value)
	tombstone := storage.NewMessage([]byte("key1"), []byte{})
	p.ProduceMessage(tombstone)

	// Build key map with long retention (tombstone should be kept)
	keyMap, _, tombstonesRemoved, err := buildKeyMap(p.Log(), 24*time.Hour)
	if err != nil {
		t.Fatalf("failed to build key map: %v", err)
	}

	// Tombstone should be in map (not expired)
	if _, ok := keyMap["key1"]; !ok {
		t.Error("fresh tombstone should be in key map")
	}
	if tombstonesRemoved != 0 {
		t.Errorf("expected 0 tombstones removed, got %d", tombstonesRemoved)
	}

	// Build key map with zero retention (tombstone should be removed)
	keyMap, _, tombstonesRemoved, err = buildKeyMap(p.Log(), 0)
	if err != nil {
		t.Fatalf("failed to build key map: %v", err)
	}

	// Tombstone should be removed (expired)
	if _, ok := keyMap["key1"]; ok {
		t.Error("expired tombstone should be removed from key map")
	}
	if tombstonesRemoved != 1 {
		t.Errorf("expected 1 tombstone removed, got %d", tombstonesRemoved)
	}
}

// TestCompactPartition tests end-to-end compaction.
func TestCompactPartition(t *testing.T) {
	baseDir := t.TempDir()

	// Create partition
	p, err := NewPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}

	// Add records with high duplication
	for i := 0; i < 100; i++ {
		msg := storage.NewMessage([]byte("key1"), []byte("value"))
		p.ProduceMessage(msg)
	}
	// Add one more unique key
	msg := storage.NewMessage([]byte("key2"), []byte("value2"))
	p.ProduceMessage(msg)

	// Total: 101 records, 2 unique keys
	// Dirty ratio: (101 - 2) / 101 = 98%

	// Close partition before compaction
	p.Close()

	// Compact
	partitionDir := filepath.Join(baseDir, "test-topic", "0")

	// Reload partition for compaction
	p, err = LoadPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to reload partition: %v", err)
	}

	result, err := CompactPartition(p, DefaultTombstoneRetention)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	if !result.Success {
		t.Fatalf("compaction not successful: %v", result.Error)
	}

	// Verify stats
	if result.Stats.TotalRecordsBefore != 101 {
		t.Errorf("expected 101 records before, got %d", result.Stats.TotalRecordsBefore)
	}
	if result.Stats.TotalRecordsAfter != 2 {
		t.Errorf("expected 2 records after, got %d", result.Stats.TotalRecordsAfter)
	}
	if result.Stats.RecordsRemoved != 99 {
		t.Errorf("expected 99 records removed, got %d", result.Stats.RecordsRemoved)
	}

	// Reload and verify compacted partition
	pNew, err := LoadPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to reload compacted partition: %v", err)
	}
	defer pNew.Close()

	// Should have only 2 messages
	messages, err := pNew.ConsumeByOffset(0, 100)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if len(messages) != 2 {
		t.Errorf("expected 2 messages after compaction, got %d", len(messages))
	}

	// Verify partition directory exists
	if _, err := os.Stat(partitionDir); os.IsNotExist(err) {
		t.Error("partition directory should exist after compaction")
	}
}

// TestCompactPartitionMultipleKeys tests compaction with multiple keys.
func TestCompactPartitionMultipleKeys(t *testing.T) {
	baseDir := t.TempDir()

	// Create partition
	p, err := NewPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}

	// Add records for multiple keys with updates
	keys := []string{"group1:topic1:0", "group1:topic1:1", "group1:topic2:0"}
	for round := 0; round < 10; round++ {
		for _, key := range keys {
			value := []byte("offset:" + string(rune('0'+round)))
			msg := storage.NewMessage([]byte(key), value)
			p.ProduceMessage(msg)
		}
	}

	// Total: 30 records, 3 unique keys
	// Dirty ratio: (30 - 3) / 30 = 0.9 (90%)

	p.Close()

	// Reload and compact
	p, err = LoadPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to reload partition: %v", err)
	}

	result, err := CompactPartition(p, DefaultTombstoneRetention)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	if !result.Success {
		t.Fatalf("compaction not successful: %v", result.Error)
	}

	// Verify stats
	if result.Stats.TotalRecordsBefore != 30 {
		t.Errorf("expected 30 records before, got %d", result.Stats.TotalRecordsBefore)
	}
	if result.Stats.TotalRecordsAfter != 3 {
		t.Errorf("expected 3 records after, got %d", result.Stats.TotalRecordsAfter)
	}

	// Reload and verify values
	pNew, err := LoadPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to reload compacted partition: %v", err)
	}
	defer pNew.Close()

	messages, err := pNew.ConsumeByOffset(0, 100)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if len(messages) != 3 {
		t.Errorf("expected 3 messages after compaction, got %d", len(messages))
	}

	// Each key should have the last value (offset:9)
	for _, msg := range messages {
		if string(msg.Value) != "offset:9" {
			t.Errorf("expected last value 'offset:9', got '%s' for key '%s'",
				string(msg.Value), string(msg.Key))
		}
	}
}

// TestCompactPartitionEmpty tests compacting an empty partition.
func TestCompactPartitionEmpty(t *testing.T) {
	baseDir := t.TempDir()

	// Create empty partition
	p, err := NewPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	p.Close()

	// Reload and compact
	p, err = LoadPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to reload partition: %v", err)
	}

	result, err := CompactPartition(p, DefaultTombstoneRetention)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	if !result.Success {
		t.Fatalf("compaction should succeed on empty partition")
	}

	if result.Stats.TotalRecordsBefore != 0 {
		t.Errorf("expected 0 records before, got %d", result.Stats.TotalRecordsBefore)
	}
	if result.Stats.TotalRecordsAfter != 0 {
		t.Errorf("expected 0 records after, got %d", result.Stats.TotalRecordsAfter)
	}
}

// TestCompactPartitionPreservesMetadata tests that compaction preserves message metadata.
func TestCompactPartitionPreservesMetadata(t *testing.T) {
	baseDir := t.TempDir()

	// Create partition
	p, err := NewPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}

	// Add message with priority and headers - use NewMessageWithHeaders for proper encoding
	msg := storage.NewMessageWithHeaders(
		[]byte("key1"),
		[]byte("value1"),
		map[string]string{
			"trace-id": "abc123",
			"source":   "test",
		},
	)
	msg.Priority = 2 // Medium priority

	p.ProduceMessage(msg)

	// Add update (should keep this one) - use NewMessageWithHeaders for proper encoding
	msg2 := storage.NewMessageWithHeaders(
		[]byte("key1"),
		[]byte("value1-updated"),
		map[string]string{
			"trace-id": "xyz789",
			"source":   "test-v2",
		},
	)
	msg2.Priority = 1 // Higher priority

	p.ProduceMessage(msg2)

	p.Close()

	// Compact
	p, err = LoadPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to reload partition: %v", err)
	}

	result, err := CompactPartition(p, DefaultTombstoneRetention)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	if !result.Success {
		t.Fatalf("compaction not successful")
	}

	// Reload and verify metadata preserved
	pNew, err := LoadPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to reload compacted partition: %v", err)
	}
	defer pNew.Close()

	messages, err := pNew.ConsumeByOffset(0, 100)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	compactedMsg := messages[0]

	// Verify value is the latest
	if string(compactedMsg.Value) != "value1-updated" {
		t.Errorf("expected latest value")
	}

	// Verify priority preserved
	if compactedMsg.Priority != 1 {
		t.Errorf("expected priority 1, got %d", compactedMsg.Priority)
	}

	// Verify headers preserved
	if compactedMsg.Headers == nil {
		t.Fatalf("expected headers to be preserved, got nil")
	}
	traceID := compactedMsg.Headers["trace-id"]
	if traceID != "xyz789" {
		t.Errorf("expected trace-id 'xyz789', got '%s'", traceID)
	}
	source := compactedMsg.Headers["source"]
	if source != "test-v2" {
		t.Errorf("expected source 'test-v2', got '%s'", source)
	}
}

// TestCompactPartitionStats tests compaction statistics.
func TestCompactPartitionStats(t *testing.T) {
	baseDir := t.TempDir()

	// Create partition
	p, err := NewPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}

	// Add 50 records with same key
	for i := 0; i < 50; i++ {
		msg := storage.NewMessage([]byte("key"), []byte("value"))
		p.ProduceMessage(msg)
	}

	p.Close()

	// Compact
	p, err = LoadPartition(baseDir, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to reload partition: %v", err)
	}

	result, err := CompactPartition(p, DefaultTombstoneRetention)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// Verify stats
	stats := result.Stats

	if stats.TotalRecordsBefore != 50 {
		t.Errorf("expected 50 records before, got %d", stats.TotalRecordsBefore)
	}

	if stats.TotalRecordsAfter != 1 {
		t.Errorf("expected 1 record after, got %d", stats.TotalRecordsAfter)
	}

	if stats.RecordsRemoved != 49 {
		t.Errorf("expected 49 records removed, got %d", stats.RecordsRemoved)
	}

	if stats.UniqueKeys != 1 {
		t.Errorf("expected 1 unique key, got %d", stats.UniqueKeys)
	}

	if stats.DurationMS <= 0 {
		t.Error("duration should be > 0")
	}

	if stats.CompactedAt.IsZero() {
		t.Error("compacted time should be set")
	}

	// Dirty ratio should be ~0.98 (49/50)
	expectedRatio := 0.98
	if stats.DirtyRatio < expectedRatio-0.05 || stats.DirtyRatio > expectedRatio+0.05 {
		t.Errorf("expected dirty ratio ~%.2f, got %.2f", expectedRatio, stats.DirtyRatio)
	}
}

// TestNewCompactorWithConfig tests custom compactor configuration.
func TestNewCompactorWithConfig(t *testing.T) {
	compactor := NewCompactorWithConfig(0.3, 12*time.Hour, 5*time.Minute)

	if compactor.dirtyRatioThreshold != 0.3 {
		t.Errorf("expected dirty ratio 0.3, got %.2f", compactor.dirtyRatioThreshold)
	}

	if compactor.tombstoneRetention != 12*time.Hour {
		t.Errorf("expected 12h retention, got %v", compactor.tombstoneRetention)
	}

	if compactor.checkInterval != 5*time.Minute {
		t.Errorf("expected 5m interval, got %v", compactor.checkInterval)
	}
}
