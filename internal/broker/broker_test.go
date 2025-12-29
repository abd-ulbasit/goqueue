// =============================================================================
// BROKER TESTS
// =============================================================================
//
// Tests for the broker - the central coordinator of goqueue.
//
// KEY BEHAVIORS TO TEST:
//   - Topic lifecycle (create, delete, list)
//   - Publish/consume API
//   - State persistence across restart
//   - Error handling for invalid operations
//
// =============================================================================

package broker

import (
	"fmt"
	"testing"
	"time"
)

func TestBroker_NewAndClose(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}

	// Verify broker state
	if b.NodeID() != "test-node" {
		t.Errorf("NodeID = %s, want test-node", b.NodeID())
	}
	if b.DataDir() != dir {
		t.Errorf("DataDir = %s, want %s", b.DataDir(), dir)
	}
	if b.Uptime() < 0 {
		t.Errorf("Uptime should be positive")
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestBroker_CreateAndDeleteTopic(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{DataDir: dir}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic
	topicConfig := DefaultTopicConfig("test-topic")
	err = b.CreateTopic(topicConfig)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Verify topic exists
	if !b.TopicExists("test-topic") {
		t.Error("Topic should exist after creation")
	}

	// List should include topic
	topics := b.ListTopics()
	if len(topics) != 1 || topics[0] != "test-topic" {
		t.Errorf("ListTopics = %v, want [test-topic]", topics)
	}

	// Create duplicate should fail
	err = b.CreateTopic(topicConfig)
	if err == nil {
		t.Error("Creating duplicate topic should fail")
	}

	// Delete topic
	err = b.DeleteTopic("test-topic")
	if err != nil {
		t.Fatalf("DeleteTopic failed: %v", err)
	}

	// Verify topic gone
	if b.TopicExists("test-topic") {
		t.Error("Topic should not exist after deletion")
	}

	// Delete non-existent should fail
	err = b.DeleteTopic("non-existent")
	if err == nil {
		t.Error("Deleting non-existent topic should fail")
	}
}

func TestBroker_GetTopic(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{DataDir: dir}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Get non-existent topic
	_, err = b.GetTopic("non-existent")
	if err == nil {
		t.Error("GetTopic should fail for non-existent topic")
	}

	// Create topic
	topicConfig := DefaultTopicConfig("test-topic")
	b.CreateTopic(topicConfig)

	// Get topic
	topic, err := b.GetTopic("test-topic")
	if err != nil {
		t.Fatalf("GetTopic failed: %v", err)
	}
	if topic.Name() != "test-topic" {
		t.Errorf("Topic name = %s, want test-topic", topic.Name())
	}
}

func TestBroker_PublishAndConsume(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{DataDir: dir}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic
	topicConfig := DefaultTopicConfig("orders")
	b.CreateTopic(topicConfig)

	// Publish messages
	for i := 0; i < 10; i++ {
		partition, offset, err := b.Publish("orders",
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
		if partition != 0 {
			t.Errorf("Publish %d partition = %d, want 0", i, partition)
		}
		if offset != int64(i) {
			t.Errorf("Publish %d offset = %d, want %d", i, offset, i)
		}
	}

	// Consume messages
	msgs, err := b.Consume("orders", 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if len(msgs) != 10 {
		t.Fatalf("Expected 10 messages, got %d", len(msgs))
	}

	// Verify messages
	for i, msg := range msgs {
		if msg.Topic != "orders" {
			t.Errorf("Message %d topic = %s, want orders", i, msg.Topic)
		}
		if msg.Partition != 0 {
			t.Errorf("Message %d partition = %d, want 0", i, msg.Partition)
		}
		if msg.Offset != int64(i) {
			t.Errorf("Message %d offset = %d, want %d", i, msg.Offset, i)
		}
		expectedKey := fmt.Sprintf("key-%d", i)
		if string(msg.Key) != expectedKey {
			t.Errorf("Message %d key = %s, want %s", i, msg.Key, expectedKey)
		}
	}
}

func TestBroker_PublishToNonExistentTopic(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{DataDir: dir}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Publish to non-existent topic
	_, _, err = b.Publish("non-existent", []byte("key"), []byte("value"))
	if err == nil {
		t.Error("Publish to non-existent topic should fail")
	}
}

func TestBroker_ConsumeFromNonExistentTopic(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{DataDir: dir}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Consume from non-existent topic
	_, err = b.Consume("non-existent", 0, 0, 10)
	if err == nil {
		t.Error("Consume from non-existent topic should fail")
	}
}

func TestBroker_ConsumeFromMiddle(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{DataDir: dir}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic and publish
	topicConfig := DefaultTopicConfig("orders")
	b.CreateTopic(topicConfig)

	for i := 0; i < 20; i++ {
		b.Publish("orders", []byte(fmt.Sprintf("key-%d", i)), []byte("value"))
	}

	// Consume from offset 10
	msgs, err := b.Consume("orders", 0, 10, 5)
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

func TestBroker_GetOffsetBounds(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{DataDir: dir}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic
	topicConfig := DefaultTopicConfig("orders")
	b.CreateTopic(topicConfig)

	// Empty topic bounds
	earliest, latest, err := b.GetOffsetBounds("orders", 0)
	if err != nil {
		t.Fatalf("GetOffsetBounds failed: %v", err)
	}
	if earliest != 0 {
		t.Errorf("Empty topic earliest = %d, want 0", earliest)
	}
	if latest != -1 {
		t.Errorf("Empty topic latest = %d, want -1", latest)
	}

	// Publish messages
	for i := 0; i < 10; i++ {
		b.Publish("orders", []byte("key"), []byte("value"))
	}

	// Check bounds
	earliest, latest, err = b.GetOffsetBounds("orders", 0)
	if err != nil {
		t.Fatalf("GetOffsetBounds failed: %v", err)
	}
	if earliest != 0 {
		t.Errorf("Earliest = %d, want 0", earliest)
	}
	if latest != 9 {
		t.Errorf("Latest = %d, want 9", latest)
	}
}

func TestBroker_Stats(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{DataDir: dir, NodeID: "test-node"}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Initial stats
	stats := b.Stats()
	if stats.NodeID != "test-node" {
		t.Errorf("Stats NodeID = %s, want test-node", stats.NodeID)
	}
	if stats.TopicCount != 0 {
		t.Errorf("Stats TopicCount = %d, want 0", stats.TopicCount)
	}

	// Create topic and publish
	topicConfig := DefaultTopicConfig("orders")
	b.CreateTopic(topicConfig)

	for i := 0; i < 10; i++ {
		b.Publish("orders", []byte("key"), []byte("value"))
	}

	// Check updated stats
	stats = b.Stats()
	if stats.TopicCount != 1 {
		t.Errorf("Stats TopicCount = %d, want 1", stats.TopicCount)
	}
	if _, ok := stats.TopicStats["orders"]; !ok {
		t.Error("Stats should include orders topic")
	}
	if stats.TopicStats["orders"].TotalMessages != 10 {
		t.Errorf("Topic TotalMessages = %d, want 10", stats.TopicStats["orders"].TotalMessages)
	}
}

func TestBroker_LoadExisting(t *testing.T) {
	dir := t.TempDir()

	// Create broker and topic
	config := BrokerConfig{DataDir: dir}
	b1, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}

	topicConfig := DefaultTopicConfig("orders")
	b1.CreateTopic(topicConfig)

	for i := 0; i < 10; i++ {
		b1.Publish("orders", []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	b1.Close()

	// Load broker with existing data
	b2, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker (reload) failed: %v", err)
	}
	defer b2.Close()

	// Verify topic exists
	if !b2.TopicExists("orders") {
		t.Error("Topic should exist after reload")
	}

	// Verify can consume messages
	msgs, err := b2.Consume("orders", 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume after reload failed: %v", err)
	}
	if len(msgs) != 10 {
		t.Errorf("Expected 10 messages after reload, got %d", len(msgs))
	}

	// Verify can continue publishing
	_, offset, err := b2.Publish("orders", []byte("new-key"), []byte("new-value"))
	if err != nil {
		t.Fatalf("Publish after reload failed: %v", err)
	}
	if offset != 10 {
		t.Errorf("New message offset = %d, want 10", offset)
	}
}

func TestBroker_ConsumeNoNewMessages(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{DataDir: dir}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic
	topicConfig := DefaultTopicConfig("orders")
	b.CreateTopic(topicConfig)

	// Publish some messages
	for i := 0; i < 5; i++ {
		b.Publish("orders", []byte("key"), []byte("value"))
	}

	// Try to consume from beyond available
	msgs, err := b.Consume("orders", 0, 100, 10)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(msgs))
	}
}

func TestBroker_MessageTimestamp(t *testing.T) {
	dir := t.TempDir()

	config := BrokerConfig{DataDir: dir}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	defer b.Close()

	// Create topic
	topicConfig := DefaultTopicConfig("orders")
	b.CreateTopic(topicConfig)

	before := time.Now()
	b.Publish("orders", []byte("key"), []byte("value"))
	after := time.Now()

	// Consume and check timestamp
	msgs, _ := b.Consume("orders", 0, 0, 1)
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message")
	}

	// Timestamp should be between before and after
	if msgs[0].Timestamp.Before(before) || msgs[0].Timestamp.After(after) {
		t.Errorf("Timestamp %v not in expected range [%v, %v]",
			msgs[0].Timestamp, before, after)
	}
}

func BenchmarkBroker_Publish(b *testing.B) {
	dir := b.TempDir()
	config := BrokerConfig{DataDir: dir}
	broker, _ := NewBroker(config)
	defer broker.Close()

	topicConfig := DefaultTopicConfig("orders")
	broker.CreateTopic(topicConfig)

	key := []byte("key")
	value := make([]byte, 1024) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		broker.Publish("orders", key, value)
	}
}

func BenchmarkBroker_Consume(b *testing.B) {
	dir := b.TempDir()
	config := BrokerConfig{DataDir: dir}
	broker, _ := NewBroker(config)
	defer broker.Close()

	topicConfig := DefaultTopicConfig("orders")
	broker.CreateTopic(topicConfig)

	// Publish messages
	for i := 0; i < 10000; i++ {
		broker.Publish("orders", []byte(fmt.Sprintf("key-%d", i)), make([]byte, 1024))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		broker.Consume("orders", 0, int64(i%9900), 100)
	}
}
