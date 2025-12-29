// =============================================================================
// PARTITION TESTS
// =============================================================================
//
// Tests for the partition abstraction layer.
//
// KEY BEHAVIORS TO TEST:
//   - Produce adds messages to underlying log
//   - Consume reads messages from offset
//   - State persists across restart
//
// =============================================================================

package broker

import (
	"fmt"
	"testing"

	"goqueue/internal/storage"
)

func TestPartition_NewAndClose(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}

	if p.ID() != 0 {
		t.Errorf("ID = %d, want 0", p.ID())
	}
	if p.Topic() != "test-topic" {
		t.Errorf("Topic = %s, want test-topic", p.Topic())
	}
	if p.LatestOffset() != -1 {
		t.Errorf("LatestOffset = %d, want -1 for empty partition", p.LatestOffset())
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPartition_ProduceAndConsume(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}
	defer p.Close()

	// Produce messages
	for i := 0; i < 10; i++ {
		offset, err := p.Produce(
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		)
		if err != nil {
			t.Fatalf("Produce %d failed: %v", i, err)
		}
		if offset != int64(i) {
			t.Errorf("Produce %d returned offset %d", i, offset)
		}
	}

	// Verify state
	if p.LatestOffset() != 9 {
		t.Errorf("LatestOffset = %d, want 9", p.LatestOffset())
	}
	if p.NextOffset() != 10 {
		t.Errorf("NextOffset = %d, want 10", p.NextOffset())
	}

	// Consume from beginning
	msgs, err := p.Consume(0, 5)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("Expected 5 messages, got %d", len(msgs))
	}

	// Verify consumed messages
	for i, msg := range msgs {
		expectedKey := fmt.Sprintf("key-%d", i)
		expectedValue := fmt.Sprintf("value-%d", i)
		if string(msg.Key) != expectedKey {
			t.Errorf("Message %d key = %s, want %s", i, msg.Key, expectedKey)
		}
		if string(msg.Value) != expectedValue {
			t.Errorf("Message %d value = %s, want %s", i, msg.Value, expectedValue)
		}
		if msg.Offset != int64(i) {
			t.Errorf("Message %d offset = %d, want %d", i, msg.Offset, i)
		}
	}
}

func TestPartition_ConsumeFromMiddle(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}
	defer p.Close()

	// Produce messages
	for i := 0; i < 20; i++ {
		p.Produce([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
	}

	// Consume from middle
	msgs, err := p.Consume(10, 5)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
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

func TestPartition_ConsumeNoLimit(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}
	defer p.Close()

	// Produce messages
	for i := 0; i < 10; i++ {
		p.Produce([]byte("key"), []byte("value"))
	}

	// Consume all with maxMessages = 0
	msgs, err := p.Consume(0, 0)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	if len(msgs) != 10 {
		t.Errorf("Expected 10 messages, got %d", len(msgs))
	}
}

func TestPartition_ConsumeNoNew(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}
	defer p.Close()

	// Produce some messages
	for i := 0; i < 5; i++ {
		p.Produce([]byte("key"), []byte("value"))
	}

	// Consume from offset beyond current
	msgs, err := p.Consume(100, 10)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(msgs))
	}
}

func TestPartition_LoadExisting(t *testing.T) {
	dir := t.TempDir()

	// Create partition and write messages
	p1, err := NewPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		p1.Produce([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}
	p1.Close()

	// Load existing partition
	p2, err := LoadPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("LoadPartition failed: %v", err)
	}
	defer p2.Close()

	// Verify state
	if p2.LatestOffset() != 9 {
		t.Errorf("LatestOffset after load = %d, want 9", p2.LatestOffset())
	}
	if p2.NextOffset() != 10 {
		t.Errorf("NextOffset after load = %d, want 10", p2.NextOffset())
	}

	// Verify can read messages
	msgs, err := p2.Consume(0, 10)
	if err != nil {
		t.Fatalf("Consume after load failed: %v", err)
	}
	if len(msgs) != 10 {
		t.Errorf("Expected 10 messages, got %d", len(msgs))
	}

	// Verify can continue producing
	offset, err := p2.Produce([]byte("new-key"), []byte("new-value"))
	if err != nil {
		t.Fatalf("Produce after load failed: %v", err)
	}
	if offset != 10 {
		t.Errorf("New message offset = %d, want 10", offset)
	}
}

func TestPartition_Size(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}
	defer p.Close()

	initialSize := p.Size()
	if initialSize != 0 {
		t.Errorf("Initial size = %d, want 0", initialSize)
	}

	// Produce message
	p.Produce([]byte("key"), []byte("value"))

	newSize := p.Size()
	if newSize <= initialSize {
		t.Errorf("Size should increase after produce: %d -> %d", initialSize, newSize)
	}
}

func TestPartition_EarliestOffset(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}
	defer p.Close()

	// Initially 0
	if p.EarliestOffset() != 0 {
		t.Errorf("EarliestOffset = %d, want 0", p.EarliestOffset())
	}

	// Produce messages
	for i := 0; i < 10; i++ {
		p.Produce([]byte("key"), []byte("value"))
	}

	// Still 0 (no retention cleanup)
	if p.EarliestOffset() != 0 {
		t.Errorf("EarliestOffset = %d, want 0", p.EarliestOffset())
	}
}

func TestPartition_Name(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, "orders", 3)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}
	defer p.Close()

	if p.Name() != "orders-3" {
		t.Errorf("Name = %s, want orders-3", p.Name())
	}
}

func TestPartition_ProduceMessage(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}
	defer p.Close()

	// Create custom message
	msg := storage.NewMessage([]byte("key"), []byte("value"))
	msg.Flags = storage.FlagCompressed // Set custom flag

	offset, err := p.ProduceMessage(msg)
	if err != nil {
		t.Fatalf("ProduceMessage failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("ProduceMessage returned offset %d, want 0", offset)
	}

	// Read and verify
	readMsg, err := p.ReadMessage(0)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if readMsg.Flags != storage.FlagCompressed {
		t.Errorf("Message flags = %d, want %d", readMsg.Flags, storage.FlagCompressed)
	}
}

func TestPartition_ReadMessage(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}
	defer p.Close()

	// Produce a message
	p.Produce([]byte("key"), []byte("value"))

	// Read it
	msg, err := p.ReadMessage(0)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if string(msg.Key) != "key" {
		t.Errorf("Message key = %s, want key", msg.Key)
	}
	if string(msg.Value) != "value" {
		t.Errorf("Message value = %s, want value", msg.Value)
	}
}

func TestPartition_Sync(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, "test-topic", 0)
	if err != nil {
		t.Fatalf("NewPartition failed: %v", err)
	}
	defer p.Close()

	// Produce some messages
	for i := 0; i < 10; i++ {
		p.Produce([]byte("key"), []byte("value"))
	}

	// Sync should not error
	if err := p.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func BenchmarkPartition_Produce(b *testing.B) {
	dir := b.TempDir()
	p, _ := NewPartition(dir, "test-topic", 0)
	defer p.Close()

	key := []byte("key")
	value := make([]byte, 1024) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Produce(key, value)
	}
}

func BenchmarkPartition_Consume(b *testing.B) {
	dir := b.TempDir()
	p, _ := NewPartition(dir, "test-topic", 0)
	defer p.Close()

	// Produce messages
	for i := 0; i < 10000; i++ {
		p.Produce([]byte(fmt.Sprintf("key-%d", i)), make([]byte, 1024))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Consume(int64(i%9900), 100)
	}
}
