// =============================================================================
// TOPIC TESTS
// =============================================================================
//
// Tests for topic management and message routing.
//
// KEY BEHAVIORS TO TEST:
//   - Messages route to correct partition
//   - Same key always goes to same partition
//   - Consumption works across partitions
//
// =============================================================================

package broker

import (
	"fmt"
	"testing"
)

func TestTopic_NewAndClose(t *testing.T) {
	dir := t.TempDir()

	config := DefaultTopicConfig("test-topic")
	topic, err := NewTopic(dir, config)
	if err != nil {
		t.Fatalf("NewTopic failed: %v", err)
	}

	if topic.Name() != "test-topic" {
		t.Errorf("Name = %s, want test-topic", topic.Name())
	}

	if err := topic.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestTopic_PublishAndConsume(t *testing.T) {
	dir := t.TempDir()

	config := DefaultTopicConfig("test-topic")
	topic, err := NewTopic(dir, config)
	if err != nil {
		t.Fatalf("NewTopic failed: %v", err)
	}
	defer topic.Close()

	// Publish messages
	for i := 0; i < 10; i++ {
		_, offset, err := topic.Publish(
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
		if offset != int64(i) {
			t.Errorf("Publish %d returned offset %d", i, offset)
		}
	}

	// Consume from partition 0
	msgs, err := topic.Consume(0, 0, 10)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if len(msgs) != 10 {
		t.Fatalf("Expected 10 messages, got %d", len(msgs))
	}

	// Verify messages
	for i, msg := range msgs {
		expectedKey := fmt.Sprintf("key-%d", i)
		expectedValue := fmt.Sprintf("value-%d", i)
		if string(msg.Key) != expectedKey {
			t.Errorf("Message %d key = %s, want %s", i, msg.Key, expectedKey)
		}
		if string(msg.Value) != expectedValue {
			t.Errorf("Message %d value = %s, want %s", i, msg.Value, expectedValue)
		}
	}
}

func TestTopic_ConsumeFromMiddle(t *testing.T) {
	dir := t.TempDir()

	config := DefaultTopicConfig("test-topic")
	topic, err := NewTopic(dir, config)
	if err != nil {
		t.Fatalf("NewTopic failed: %v", err)
	}
	defer topic.Close()

	// Publish messages
	for i := 0; i < 20; i++ {
		topic.Publish([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
	}

	// Consume from offset 10
	msgs, err := topic.Consume(0, 10, 5)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	if len(msgs) != 5 {
		t.Fatalf("Expected 5 messages, got %d", len(msgs))
	}

	// Verify first message is at offset 10
	if msgs[0].Offset != 10 {
		t.Errorf("First message offset = %d, want 10", msgs[0].Offset)
	}
}

func TestTopic_ConsumeNoNewMessages(t *testing.T) {
	dir := t.TempDir()

	config := DefaultTopicConfig("test-topic")
	topic, err := NewTopic(dir, config)
	if err != nil {
		t.Fatalf("NewTopic failed: %v", err)
	}
	defer topic.Close()

	// Publish some messages
	for i := 0; i < 5; i++ {
		topic.Publish([]byte("key"), []byte("value"))
	}

	// Consume from beyond current offset
	msgs, err := topic.Consume(0, 100, 10)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(msgs))
	}
}

func TestTopic_ConsumeInvalidPartition(t *testing.T) {
	dir := t.TempDir()

	config := DefaultTopicConfig("test-topic")
	topic, err := NewTopic(dir, config)
	if err != nil {
		t.Fatalf("NewTopic failed: %v", err)
	}
	defer topic.Close()

	// Try to consume from non-existent partition
	_, err = topic.Consume(999, 0, 10)
	if err == nil {
		t.Error("Consume from invalid partition should fail")
	}
}

func TestTopic_LoadExisting(t *testing.T) {
	dir := t.TempDir()

	// Create topic and publish messages
	config := DefaultTopicConfig("test-topic")
	topic1, err := NewTopic(dir, config)
	if err != nil {
		t.Fatalf("NewTopic failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		topic1.Publish([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}
	topic1.Close()

	// Load existing topic
	topic2, err := LoadTopic(dir, "test-topic")
	if err != nil {
		t.Fatalf("LoadTopic failed: %v", err)
	}
	defer topic2.Close()

	// Verify can read messages
	msgs, err := topic2.Consume(0, 0, 10)
	if err != nil {
		t.Fatalf("Consume after load failed: %v", err)
	}
	if len(msgs) != 10 {
		t.Errorf("Expected 10 messages, got %d", len(msgs))
	}

	// Verify can continue publishing
	_, offset, err := topic2.Publish([]byte("new-key"), []byte("new-value"))
	if err != nil {
		t.Fatalf("Publish after load failed: %v", err)
	}
	if offset != 10 {
		t.Errorf("New message offset = %d, want 10", offset)
	}
}

func TestTopic_PublishToPartition(t *testing.T) {
	dir := t.TempDir()

	config := DefaultTopicConfig("test-topic")
	topic, err := NewTopic(dir, config)
	if err != nil {
		t.Fatalf("NewTopic failed: %v", err)
	}
	defer topic.Close()

	// Publish directly to partition 0
	offset, err := topic.PublishToPartition(0, []byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("PublishToPartition failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("PublishToPartition returned offset %d, want 0", offset)
	}

	// Try invalid partition
	_, err = topic.PublishToPartition(999, []byte("key"), []byte("value"))
	if err == nil {
		t.Error("PublishToPartition to invalid partition should fail")
	}
}

func TestTopic_NilKeyRoundRobin(t *testing.T) {
	dir := t.TempDir()

	config := DefaultTopicConfig("test-topic")
	topic, err := NewTopic(dir, config)
	if err != nil {
		t.Fatalf("NewTopic failed: %v", err)
	}
	defer topic.Close()

	// Publish with nil key (should round-robin)
	for i := 0; i < 5; i++ {
		_, _, err := topic.Publish(nil, []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatalf("Publish with nil key failed: %v", err)
		}
	}

	// All should go to partition 0 (single partition in M1)
	msgs, err := topic.Consume(0, 0, 5)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if len(msgs) != 5 {
		t.Errorf("Expected 5 messages, got %d", len(msgs))
	}
}

func TestTopic_LatestAndEarliestOffset(t *testing.T) {
	dir := t.TempDir()

	config := DefaultTopicConfig("test-topic")
	topic, err := NewTopic(dir, config)
	if err != nil {
		t.Fatalf("NewTopic failed: %v", err)
	}
	defer topic.Close()

	// Initially
	earliest := topic.EarliestOffset()
	if earliest != 0 {
		t.Errorf("EarliestOffset = %d, want 0", earliest)
	}

	offsets := topic.LatestOffsets()
	if offsets[0] != -1 {
		t.Errorf("LatestOffset[0] = %d, want -1 (empty)", offsets[0])
	}

	// Publish messages
	for i := 0; i < 10; i++ {
		topic.Publish([]byte("key"), []byte("value"))
	}

	offsets = topic.LatestOffsets()
	if offsets[0] != 9 {
		t.Errorf("LatestOffset[0] = %d, want 9", offsets[0])
	}
}

func BenchmarkTopic_Publish(b *testing.B) {
	dir := b.TempDir()
	config := DefaultTopicConfig("test-topic")
	topic, _ := NewTopic(dir, config)
	defer topic.Close()

	key := []byte("key")
	value := make([]byte, 1024) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.Publish(key, value)
	}
}

func BenchmarkTopic_Consume(b *testing.B) {
	dir := b.TempDir()
	config := DefaultTopicConfig("test-topic")
	topic, _ := NewTopic(dir, config)
	defer topic.Close()

	// Publish messages
	for i := 0; i < 10000; i++ {
		topic.Publish([]byte(fmt.Sprintf("key-%d", i)), make([]byte, 1024))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.Consume(0, int64(i%9900), 100)
	}
}
