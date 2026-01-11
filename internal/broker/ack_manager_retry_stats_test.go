package broker

import (
	"testing"
	"time"
)

func TestAckManager_Nack_SchedulesRetry_ProcessRetry_RedeliversAndStats(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 1)

	cfg := DefaultReliabilityConfig()
	cfg.MaxInFlightPerConsumer = 10
	cfg.VisibilityTimeoutMs = 10_000
	cfg.BackoffBaseMs = 0
	cfg.BackoffMaxMs = 0
	am := replaceAckManager(t, b, cfg)

	// Make retry queue small and deterministic for this test.
	am.retryQueue = make(chan *InFlightMessage, 1)

	// The AckManager starts a background retry processor goroutine that can race
	// with our assertions by draining retryQueue before we can inspect it.
	//
	// We cancel the manager context here to stop the retryProcessor. We still
	// call processRetry manually (synchronously) to cover that logic.
	am.cancel()
	am.wg.Wait()

	msg := &Message{Topic: "orders", Partition: 0, Offset: 0, Timestamp: time.Now(), Key: []byte("k"), Value: []byte("v")}
	receipt, err := am.TrackDelivery(msg, "c1", "g1", 30*time.Second)
	if err != nil {
		t.Fatalf("TrackDelivery failed: %v", err)
	}

	// Nack should remove from visibility tracker and enqueue for retry.
	res, err := am.Nack(receipt, "boom")
	if err != nil {
		t.Fatalf("Nack failed: %v", err)
	}
	if res.Action != "requeued" {
		t.Fatalf("res.Action=%q, want requeued", res.Action)
	}

	// Pull the inflight from the retry queue and process synchronously.
	var inflight *InFlightMessage
	select {
	case inflight = <-am.retryQueue:
		// ok
	default:
		t.Fatalf("expected inflight message to be queued for retry")
	}
	inflight.NextRetryTime = time.Now()
	am.processRetry(inflight)

	// After processRetry, the inflight should be visible in the visibility tracker
	// under a new receipt handle.
	newReceipt := inflight.ReceiptHandle
	if newReceipt == receipt {
		t.Fatalf("expected new receipt after retry")
	}

	// ACK the re-delivered receipt and confirm committed offset updates.
	ackRes, err := am.Ack(newReceipt)
	if err != nil {
		t.Fatalf("Ack(newReceipt) failed: %v", err)
	}
	if !ackRes.Success {
		t.Fatalf("expected ack success")
	}
	if got := am.GetCommittedOffset("c1", "g1", "orders", 0); got != 0 {
		t.Fatalf("GetCommittedOffset=%d, want 0", got)
	}

	// Stats should reflect nack, retry and ack activity.
	stats := am.Stats()
	if stats.TotalNacks == 0 {
		t.Fatalf("expected TotalNacks > 0")
	}
	if stats.TotalRetries == 0 {
		t.Fatalf("expected TotalRetries > 0")
	}
	if stats.TotalAcks == 0 {
		t.Fatalf("expected TotalAcks > 0")
	}

	// Cover consumer state eviction API too.
	am.RemoveConsumerState("c1", "g1", "orders", 0)
	if got := am.GetCommittedOffset("c1", "g1", "orders", 0); got != -1 {
		t.Fatalf("GetCommittedOffset after RemoveConsumerState=%d, want -1", got)
	}
}

func TestAckManager_VisibilityExpiry_QueuesRetry_AndQueueBackpressureRoutesToDLQ(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 1)

	cfg := DefaultReliabilityConfig()
	cfg.MaxInFlightPerConsumer = 10
	cfg.VisibilityTimeoutMs = 1
	cfg.BackoffBaseMs = 0
	cfg.BackoffMaxMs = 0
	cfg.MaxRetries = 1
	am := replaceAckManager(t, b, cfg)

	// Force retryQueue to be unbuffered so onVisibilityExpired's non-blocking send
	// will fall through to DLQ routing.
	am.retryQueue = make(chan *InFlightMessage)

	msg := &Message{Topic: "orders", Partition: 0, Offset: 0, Timestamp: time.Now(), Key: []byte("k"), Value: []byte("v")}
	receipt, err := am.TrackDelivery(msg, "c1", "g1", 30*time.Second)
	if err != nil {
		t.Fatalf("TrackDelivery failed: %v", err)
	}

	// Extract the in-flight record and invoke expiry handler directly.
	am.visibilityTracker.mu.Lock()
	item := am.visibilityTracker.byReceipt[receipt]
	am.visibilityTracker.mu.Unlock()
	if item == nil || item.message == nil {
		t.Fatalf("expected receipt to be tracked")
	}

	am.onVisibilityExpired(item.message)

	// The message should have been routed to DLQ because the retry queue was full.
	dlqTopic := cfg.DLQTopicName("orders")
	msgs, err := b.Consume(dlqTopic, 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume(DLQ) failed: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatalf("expected DLQ message")
	}
	parsed, err := ParseDLQMessage(msgs[0].Value)
	if err != nil {
		t.Fatalf("ParseDLQMessage failed: %v", err)
	}
	if parsed.OriginalTopic != "orders" {
		t.Fatalf("dlq.OriginalTopic=%q, want orders", parsed.OriginalTopic)
	}
}
