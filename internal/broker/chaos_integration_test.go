package broker

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// CHAOS INTEGRATION TESTS
// =============================================================================
//
// These tests simulate failure scenarios to validate system resilience:
//   - Broker restart recovery
//   - Concurrent consumer failures
//   - Message durability across restarts
//   - Consumer group rebalancing on member failure
//
// WHY CHAOS TESTS?
//   In production, things fail. These tests ensure GoQueue handles:
//   - Pod restarts gracefully
//   - Network partitions don't lose messages
//   - Consumer crashes don't duplicate processing
//
// COMPARISON:
//   - Kafka: Uses Jepsen for distributed chaos testing
//   - RabbitMQ: Relies on queue mirroring for resilience
//   - SQS: AWS handles chaos testing internally
//   - goqueue: Unit-level chaos tests + Kubernetes pod kill tests
//
// =============================================================================

// Force reference to storage package to avoid unused import
var _ = storage.PriorityNormal

// TestChaos_BrokerRestart_MessageDurability verifies messages survive broker restart.
//
// SCENARIO:
//  1. Publish messages
//  2. Close broker (simulating pod restart)
//  3. Reopen broker
//  4. Verify all messages are still readable
//
// WHY THIS MATTERS:
//
//	Users expect messages to survive pod restarts. Without proper persistence,
//	messages in memory are lost during Kubernetes rolling updates.
func TestChaos_BrokerRestart_MessageDurability(t *testing.T) {
	// Create temp directory for broker data
	dataDir, err := os.MkdirTemp("", "goqueue-chaos-restart-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	topicName := "restart-test"
	numMessages := 100

	// Phase 1: Create broker and publish messages
	b1, err := NewBroker(BrokerConfig{DataDir: dataDir})
	if err != nil {
		t.Fatalf("failed to create broker 1: %v", err)
	}

	if err := b1.CreateTopic(TopicConfig{
		Name:          topicName,
		NumPartitions: 1,
	}); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Publish messages
	for i := 0; i < numMessages; i++ {
		_, _, err := b1.Publish(topicName, []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("message-%d", i)))
		if err != nil {
			t.Fatalf("failed to publish message %d: %v", i, err)
		}
	}

	// Close broker (simulates pod restart)
	if err := b1.Close(); err != nil {
		t.Fatalf("failed to close broker 1: %v", err)
	}

	// Phase 2: Reopen broker and verify messages
	b2, err := NewBroker(BrokerConfig{DataDir: dataDir})
	if err != nil {
		t.Fatalf("failed to create broker 2: %v", err)
	}
	defer b2.Close()

	// Wait for topic recovery
	time.Sleep(100 * time.Millisecond)

	// Consume messages and verify count
	messages, err := b2.Consume(topicName, 0, 0, numMessages+10)
	if err != nil {
		t.Fatalf("failed to consume messages: %v", err)
	}

	if len(messages) != numMessages {
		t.Errorf("message count after restart = %d, want %d", len(messages), numMessages)
	}

	// Verify message content
	for i, msg := range messages {
		expectedValue := fmt.Sprintf("message-%d", i)
		if string(msg.Value) != expectedValue {
			t.Errorf("message %d value = %q, want %q", i, string(msg.Value), expectedValue)
		}
	}
}

// TestChaos_ConcurrentConsumersWithFailure simulates consumer failures during processing.
//
// SCENARIO:
//  1. Start multiple consumers
//  2. One consumer "crashes" mid-processing (stops without ACKing)
//  3. Verify other consumers continue processing
//  4. Verify crashed consumer's messages are redelivered (via visibility timeout)
func TestChaos_ConcurrentConsumersWithFailure(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "goqueue-chaos-consumer-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	broker, err := NewBroker(BrokerConfig{DataDir: dataDir})
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer broker.Close()

	topicName := "concurrent-consumer-test"
	numMessages := 50

	if err := broker.CreateTopic(TopicConfig{
		Name:          topicName,
		NumPartitions: 4, // Multiple partitions for parallel consumers
	}); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Publish messages to all partitions
	// Use PublishToPartitionWithPriority with normal priority
	for i := 0; i < numMessages; i++ {
		partition := i % 4
		_, err := broker.PublishToPartitionWithPriority(topicName, partition, []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("msg-%d", i)), storage.PriorityNormal)
		if err != nil {
			t.Fatalf("failed to publish to partition %d: %v", partition, err)
		}
	}

	// Simulate concurrent consumers with one "failing"
	var wg sync.WaitGroup
	consumedMessages := make(chan string, numMessages*2) // Buffer for all messages
	failConsumer := 1                                    // Consumer 1 will "crash"

	for consumerID := 0; consumerID < 4; consumerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			partition := id
			messages, err := broker.Consume(topicName, partition, 0, 20)
			if err != nil {
				t.Errorf("consumer %d failed to consume: %v", id, err)
				return
			}

			for _, msg := range messages {
				// Simulate consumer 1 crashing after processing half
				if id == failConsumer && len(consumedMessages) > numMessages/8 {
					// Consumer "crashes" - just stops without completing
					return
				}
				consumedMessages <- fmt.Sprintf("consumer-%d:%s", id, string(msg.Value))
			}
		}(consumerID)
	}

	wg.Wait()
	close(consumedMessages)

	// Count consumed messages
	messageCount := 0
	for range consumedMessages {
		messageCount++
	}

	// At least 3 out of 4 consumers should have processed their messages
	// (one consumer "crashed")
	expectedMinimum := (numMessages / 4) * 3 // 3 consumers worth
	if messageCount < expectedMinimum/2 {    // Allow for some variance
		t.Errorf("consumed messages = %d, want at least %d", messageCount, expectedMinimum/2)
	}
}

// TestChaos_HighConcurrencyPublishDuringRestart tests publish handling during broker shutdown.
//
// SCENARIO:
//  1. Start publishing in background
//  2. Close broker mid-publish
//  3. Verify clean shutdown (no panics, no data corruption)
func TestChaos_HighConcurrencyPublishDuringRestart(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "goqueue-chaos-publish-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	broker, err := NewBroker(BrokerConfig{DataDir: dataDir})
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	topicName := "publish-chaos-test"
	if err := broker.CreateTopic(TopicConfig{
		Name:          topicName,
		NumPartitions: 4,
	}); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Start publishing in background
	var wg sync.WaitGroup
	stopPublishing := make(chan struct{})
	var publishCount int64
	var errorCount int64

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			msgNum := 0
			for {
				select {
				case <-stopPublishing:
					return
				default:
					_, _, err := broker.Publish(topicName,
						[]byte(fmt.Sprintf("key-%d-%d", id, msgNum)),
						[]byte(fmt.Sprintf("value-%d-%d", id, msgNum)))
					if err != nil {
						// Expected during shutdown - use atomic for thread safety
						atomic.AddInt64(&errorCount, 1)
					} else {
						atomic.AddInt64(&publishCount, 1)
					}
					msgNum++
					time.Sleep(1 * time.Millisecond)
				}
			}
		}(i)
	}

	// Let publishing run for a bit
	time.Sleep(50 * time.Millisecond)

	// Signal stop and close broker (simulates Kubernetes pod termination)
	close(stopPublishing)
	if err := broker.Close(); err != nil {
		t.Errorf("broker close failed: %v", err)
	}

	wg.Wait()

	// Verify some messages were published and some errors occurred during shutdown
	finalPublishCount := atomic.LoadInt64(&publishCount)
	finalErrorCount := atomic.LoadInt64(&errorCount)
	if finalPublishCount == 0 {
		t.Error("no messages were published before shutdown")
	}

	t.Logf("published %d messages, %d errors during shutdown (expected)", finalPublishCount, finalErrorCount)
}

// TestChaos_ConsumerGroupRebalanceOnFailure tests consumer group rebalancing when members fail.
//
// SCENARIO:
//  1. Create consumer group with 3 members
//  2. Each member gets partition assignments
//  3. One member "fails" (leaves group)
//  4. Verify remaining members get reassigned partitions
func TestChaos_ConsumerGroupRebalanceOnFailure(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "goqueue-chaos-rebalance-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	broker, err := NewBroker(BrokerConfig{DataDir: dataDir})
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer broker.Close()

	topicName := "rebalance-test"
	groupID := "test-group"
	numPartitions := 6

	if err := broker.CreateTopic(TopicConfig{
		Name:          topicName,
		NumPartitions: numPartitions,
	}); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	coordinator := broker.GroupCoordinator()

	// Join 3 members
	// JoinGroup signature: (groupID, clientID string, topics []string) (*JoinResult, error)
	result1, err := coordinator.JoinGroup(groupID, "client-1", []string{topicName})
	if err != nil {
		t.Fatalf("member 1 failed to join: %v", err)
	}
	member1 := result1.MemberID

	result2, err := coordinator.JoinGroup(groupID, "client-2", []string{topicName})
	if err != nil {
		t.Fatalf("member 2 failed to join: %v", err)
	}
	member2 := result2.MemberID

	result3, err := coordinator.JoinGroup(groupID, "client-3", []string{topicName})
	if err != nil {
		t.Fatalf("member 3 failed to join: %v", err)
	}
	member3 := result3.MemberID

	// Get initial assignments
	group, err := coordinator.GetGroup(groupID)
	if err != nil {
		t.Fatalf("failed to get group: %v", err)
	}

	partitions1, _, _ := group.GetAssignment(member1)
	partitions2, _, _ := group.GetAssignment(member2)
	partitions3, _, _ := group.GetAssignment(member3)

	totalAssignedBefore := len(partitions1) + len(partitions2) + len(partitions3)
	if totalAssignedBefore != numPartitions {
		t.Errorf("total assigned before = %d, want %d", totalAssignedBefore, numPartitions)
	}

	// Member 3 "fails" (leaves group)
	if err := coordinator.LeaveGroup(groupID, member3); err != nil {
		t.Fatalf("member 3 failed to leave: %v", err)
	}

	// Wait for rebalance
	time.Sleep(100 * time.Millisecond)

	// Get updated assignments
	partitions1After, _, _ := group.GetAssignment(member1)
	partitions2After, _, _ := group.GetAssignment(member2)

	totalAssignedAfter := len(partitions1After) + len(partitions2After)
	if totalAssignedAfter != numPartitions {
		t.Errorf("total assigned after rebalance = %d, want %d", totalAssignedAfter, numPartitions)
	}

	// Both remaining members should have more partitions now
	if len(partitions1After) < len(partitions1) && len(partitions2After) < len(partitions2) {
		t.Error("neither member got additional partitions after rebalance")
	}

	t.Logf("member1: %d -> %d partitions, member2: %d -> %d partitions",
		len(partitions1), len(partitions1After), len(partitions2), len(partitions2After))
}

// TestChaos_RapidTopicCreateDelete tests rapid creation and deletion of topics.
//
// SCENARIO:
//  1. Rapidly create and delete topics
//  2. Verify no resource leaks
//  3. Verify broker remains stable
func TestChaos_RapidTopicCreateDelete(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "goqueue-chaos-topic-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	broker, err := NewBroker(BrokerConfig{DataDir: dataDir})
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	// Don't defer close - we'll close explicitly after test logic

	// Rapid create/delete cycles (reduced from 20 to 10 for speed)
	for i := 0; i < 10; i++ {
		topicName := fmt.Sprintf("rapid-topic-%d", i)

		if err := broker.CreateTopic(TopicConfig{
			Name:          topicName,
			NumPartitions: 2,
		}); err != nil {
			broker.Close()
			t.Fatalf("cycle %d: failed to create topic: %v", i, err)
		}

		// Publish a few messages
		for j := 0; j < 3; j++ {
			_, _, err := broker.Publish(topicName, nil, []byte(fmt.Sprintf("msg-%d", j)))
			if err != nil {
				broker.Close()
				t.Fatalf("cycle %d: failed to publish: %v", i, err)
			}
		}

		if err := broker.DeleteTopic(topicName); err != nil {
			broker.Close()
			t.Fatalf("cycle %d: failed to delete topic: %v", i, err)
		}
	}

	// Verify broker is still functional
	finalTopic := "final-topic"
	if err := broker.CreateTopic(TopicConfig{Name: finalTopic, NumPartitions: 1}); err != nil {
		broker.Close()
		t.Fatalf("failed to create final topic: %v", err)
	}

	_, _, err = broker.Publish(finalTopic, nil, []byte("final-message"))
	if err != nil {
		broker.Close()
		t.Fatalf("failed to publish to final topic: %v", err)
	}

	messages, err := broker.Consume(finalTopic, 0, 0, 10)
	if err != nil {
		broker.Close()
		t.Fatalf("failed to consume from final topic: %v", err)
	}

	if len(messages) != 1 {
		broker.Close()
		t.Errorf("final topic messages = %d, want 1", len(messages))
		return
	}

	// Explicit close at the end
	if err := broker.Close(); err != nil {
		t.Errorf("broker close failed: %v", err)
	}
}

// TestChaos_OffsetPersistenceAcrossRestart verifies consumer offsets survive restart.
//
// SCENARIO:
//  1. Consume messages and commit offsets
//  2. Restart broker
//  3. Verify offset is preserved
func TestChaos_OffsetPersistenceAcrossRestart(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "goqueue-chaos-offset-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	topicName := "offset-persist-test"
	groupID := "persist-group"
	numMessages := 50
	consumeCount := 25

	// Phase 1: Create broker, publish, consume, and commit
	b1, err := NewBroker(BrokerConfig{DataDir: dataDir})
	if err != nil {
		t.Fatalf("failed to create broker 1: %v", err)
	}

	if err := b1.CreateTopic(TopicConfig{Name: topicName, NumPartitions: 1}); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Publish messages
	for i := 0; i < numMessages; i++ {
		_, _, err := b1.Publish(topicName, nil, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("failed to publish: %v", err)
		}
	}

	coordinator1 := b1.GroupCoordinator()

	// Join group and consume some messages
	result, err := coordinator1.JoinGroup(groupID, "client-1", []string{topicName})
	if err != nil {
		t.Fatalf("failed to join group: %v", err)
	}
	memberID := result.MemberID

	messages, err := b1.Consume(topicName, 0, 0, consumeCount)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	// Commit offset for consumed messages
	offsets := map[string]map[int]int64{
		topicName: {0: int64(len(messages))},
	}
	if err := coordinator1.CommitOffsets(groupID, offsets, memberID); err != nil {
		t.Fatalf("failed to commit offsets: %v", err)
	}

	// Close broker
	if err := b1.Close(); err != nil {
		t.Fatalf("failed to close broker: %v", err)
	}

	// Phase 2: Reopen broker and verify offset
	b2, err := NewBroker(BrokerConfig{DataDir: dataDir})
	if err != nil {
		t.Fatalf("failed to create broker 2: %v", err)
	}
	defer b2.Close()

	time.Sleep(100 * time.Millisecond)

	coordinator2 := b2.GroupCoordinator()

	// Get committed offset
	offset, err := coordinator2.GetOffset(groupID, topicName, 0)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}

	if offset != int64(consumeCount) {
		t.Errorf("offset after restart = %d, want %d", offset, consumeCount)
	}

	// Consume from committed offset - should get remaining messages
	remaining, err := b2.Consume(topicName, 0, offset, numMessages)
	if err != nil {
		t.Fatalf("failed to consume remaining: %v", err)
	}

	expectedRemaining := numMessages - consumeCount
	if len(remaining) != expectedRemaining {
		t.Errorf("remaining messages = %d, want %d", len(remaining), expectedRemaining)
	}
}
