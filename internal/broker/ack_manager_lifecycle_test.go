package broker

import (
	"errors"
	"log/slog"
	"testing"
	"time"
)

// newTestBroker creates a broker with a temp data dir.
//
// We keep this helper in broker package tests (not broker_test) so we can
// swap internal components (like AckManager) without exposing test-only APIs.
func newTestBroker(t *testing.T) *Broker {
	t.Helper()

	dir := t.TempDir()

	b, err := NewBroker(BrokerConfig{
		DataDir:  dir,
		NodeID:   "test-node",
		LogLevel: slog.LevelError, // keep tests quiet
		// ClusterEnabled intentionally left false
	})
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}

	t.Cleanup(func() {
		_ = b.Close()
	})

	return b
}

func replaceAckManager(t *testing.T, b *Broker, cfg ReliabilityConfig) *AckManager {
	t.Helper()

	// Shut down the broker's default ACK manager to avoid extra goroutines.
	if b.ackManager != nil {
		_ = b.ackManager.Close()
	}

	b.reliabilityConfig = cfg
	b.ackManager = NewAckManager(b, cfg)

	t.Cleanup(func() {
		_ = b.ackManager.Close()
	})

	return b.ackManager
}

func mustCreateTopic(t *testing.T, b *Broker, name string, partitions int) {
	t.Helper()

	if err := b.CreateTopic(TopicConfig{Name: name, NumPartitions: partitions}); err != nil {
		t.Fatalf("CreateTopic(%q) failed: %v", name, err)
	}
}

func TestAckManager_TrackDelivery_Backpressure(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 1)

	cfg := DefaultReliabilityConfig()
	cfg.MaxInFlightPerConsumer = 1
	cfg.VisibilityTimeoutMs = 10_000 // avoid expiry firing mid-test
	am := replaceAckManager(t, b, cfg)

	msg0 := &Message{Topic: "orders", Partition: 0, Offset: 0, Timestamp: time.Now(), Key: []byte("k0"), Value: []byte("v0")}
	msg1 := &Message{Topic: "orders", Partition: 0, Offset: 1, Timestamp: time.Now(), Key: []byte("k1"), Value: []byte("v1")}

	r0, err := am.TrackDelivery(msg0, "c1", "g1", 30*time.Second)
	if err != nil {
		t.Fatalf("TrackDelivery(msg0) failed: %v", err)
	}

	if _, err := am.TrackDelivery(msg1, "c1", "g1", 30*time.Second); err == nil {
		t.Fatalf("TrackDelivery(msg1) expected ErrBackpressure")
	} else if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("TrackDelivery(msg1) error=%v, want %v", err, ErrBackpressure)
	}

	// Releasing the in-flight slot should allow another delivery.
	if _, err := am.Ack(r0); err != nil {
		t.Fatalf("Ack(msg0) failed: %v", err)
	}

	if _, err := am.TrackDelivery(msg1, "c1", "g1", 30*time.Second); err != nil {
		t.Fatalf("TrackDelivery(msg1) after ACK failed: %v", err)
	}
}

func TestAckManager_Ack_OutOfOrder_CommitJumps(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 1)

	cfg := DefaultReliabilityConfig()
	cfg.VisibilityTimeoutMs = 10_000
	am := replaceAckManager(t, b, cfg)

	msgs := []*Message{
		{Topic: "orders", Partition: 0, Offset: 0, Timestamp: time.Now(), Key: []byte("k0"), Value: []byte("v0")},
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: time.Now(), Key: []byte("k1"), Value: []byte("v1")},
		{Topic: "orders", Partition: 0, Offset: 2, Timestamp: time.Now(), Key: []byte("k2"), Value: []byte("v2")},
	}

	receipts := make([]string, 0, len(msgs))
	for _, m := range msgs {
		r, err := am.TrackDelivery(m, "c1", "g1", 30*time.Second)
		if err != nil {
			t.Fatalf("TrackDelivery(offset=%d) failed: %v", m.Offset, err)
		}
		receipts = append(receipts, r)
	}

	// ACK out of order: ACK 1 and 2 first should NOT advance commit.
	res1, err := am.Ack(receipts[1])
	if err != nil {
		t.Fatalf("Ack(offset=1) failed: %v", err)
	}
	if res1.NewCommittedOffset != -1 {
		t.Fatalf("Ack(offset=1) committed=%d, want -1 (gap at 0)", res1.NewCommittedOffset)
	}

	res2, err := am.Ack(receipts[2])
	if err != nil {
		t.Fatalf("Ack(offset=2) failed: %v", err)
	}
	if res2.NewCommittedOffset != -1 {
		t.Fatalf("Ack(offset=2) committed=%d, want -1 (gap at 0)", res2.NewCommittedOffset)
	}

	// Now ACK 0; commit should jump to 2 and clean up pending state.
	res0, err := am.Ack(receipts[0])
	if err != nil {
		t.Fatalf("Ack(offset=0) failed: %v", err)
	}
	if res0.NewCommittedOffset != 2 {
		t.Fatalf("Ack(offset=0) committed=%d, want 2", res0.NewCommittedOffset)
	}
}

func TestAckManager_Reject_RoutesToDLQ_WithMetadata(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 1)

	cfg := DefaultReliabilityConfig()
	cfg.VisibilityTimeoutMs = 10_000
	am := replaceAckManager(t, b, cfg)

	msg := &Message{Topic: "orders", Partition: 0, Offset: 0, Timestamp: time.Now(), Key: []byte("order-123"), Value: []byte("payload")}
	receipt, err := am.TrackDelivery(msg, "c1", "g1", 30*time.Second)
	if err != nil {
		t.Fatalf("TrackDelivery failed: %v", err)
	}

	res, err := am.Reject(receipt, "poison")
	if err != nil {
		t.Fatalf("Reject failed: %v", err)
	}
	if res.Action != "dlq" {
		t.Fatalf("Reject action=%q, want %q", res.Action, "dlq")
	}
	if res.DLQTopic == "" {
		t.Fatalf("Reject should return DLQ topic")
	}

	// Verify the message was actually published to the DLQ and is parseable.
	dlqMsgs, err := b.Consume(res.DLQTopic, 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume(DLQ) failed: %v", err)
	}
	if len(dlqMsgs) != 1 {
		t.Fatalf("DLQ message count=%d, want 1", len(dlqMsgs))
	}

	dlq, err := ParseDLQMessage(dlqMsgs[0].Value)
	if err != nil {
		t.Fatalf("ParseDLQMessage failed: %v", err)
	}
	if dlq.OriginalTopic != "orders" || dlq.OriginalOffset != 0 {
		t.Fatalf("DLQ original=(%s,%d), want (orders,0)", dlq.OriginalTopic, dlq.OriginalOffset)
	}
	if dlq.Reason != DLQReasonRejected {
		t.Fatalf("DLQ reason=%s, want %s", dlq.Reason, DLQReasonRejected)
	}
	if string(dlq.OriginalKey) != "order-123" {
		t.Fatalf("DLQ key=%q, want %q", string(dlq.OriginalKey), "order-123")
	}
}

func TestBroker_ConsumeWithReceipts_AckRejectLagAndVisibility(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 1)

	// Tune reliability so tests are fast but not flaky.
	cfg := DefaultReliabilityConfig()
	cfg.VisibilityTimeoutMs = 30_000
	cfg.BackoffBaseMs = 10_000 // avoid retries running immediately in background
	cfg.BackoffMaxMs = 10_000
	cfg.VisibilityCheckIntervalMs = 1_000
	replaceAckManager(t, b, cfg)

	// Publish a few messages so the broker has offsets [0..2].
	for i := 0; i < 3; i++ {
		_, _, err := b.Publish("orders", []byte{byte('a' + i)}, []byte{byte('0' + i)})
		if err != nil {
			t.Fatalf("Publish(%d) failed: %v", i, err)
		}
	}

	msgs, err := b.ConsumeWithReceipts("orders", 0, 0, 3, "c1", "g1")
	if err != nil {
		t.Fatalf("ConsumeWithReceipts failed: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("ConsumeWithReceipts len=%d, want 3", len(msgs))
	}
	if msgs[0].ReceiptHandle == "" || msgs[1].ReceiptHandle == "" || msgs[2].ReceiptHandle == "" {
		t.Fatalf("all messages should have receipt handles")
	}

	// Initial lag should reflect 3 in-flight messages and committed=-1.
	lag0, err := b.GetConsumerLag("c1", "g1", "orders", 0)
	if err != nil {
		t.Fatalf("GetConsumerLag failed: %v", err)
	}
	if lag0.CommittedOffset != -1 {
		t.Fatalf("CommittedOffset=%d, want -1", lag0.CommittedOffset)
	}
	if lag0.InFlightCount != 3 {
		t.Fatalf("InFlightCount=%d, want 3", lag0.InFlightCount)
	}

	// ACK offset 0 → committed should advance to 0.
	ackRes, err := b.Ack(msgs[0].ReceiptHandle)
	if err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
	if ackRes.NewCommittedOffset != 0 {
		t.Fatalf("Ack committed=%d, want 0", ackRes.NewCommittedOffset)
	}

	// Extend visibility for message 1 (common long-processing scenario).
	if _, err := b.ExtendVisibility(msgs[1].ReceiptHandle, 30*time.Second); err != nil {
		t.Fatalf("ExtendVisibility failed: %v", err)
	}

	// Reject offset 1 → should go to DLQ and keep commit pinned at 0 (gap).
	rejRes, err := b.Reject(msgs[1].ReceiptHandle, "bad format")
	if err != nil {
		t.Fatalf("Reject failed: %v", err)
	}
	if rejRes.DLQTopic == "" {
		t.Fatalf("Reject should return DLQ topic")
	}

	// ACK offset 2 while offset 1 is missing → commit should remain 0.
	ack2Res, err := b.Ack(msgs[2].ReceiptHandle)
	if err != nil {
		t.Fatalf("Ack(offset=2) failed: %v", err)
	}
	if ack2Res.NewCommittedOffset != 0 {
		t.Fatalf("Ack(offset=2) committed=%d, want 0 (gap at 1)", ack2Res.NewCommittedOffset)
	}

	lag1, err := b.GetConsumerLag("c1", "g1", "orders", 0)
	if err != nil {
		t.Fatalf("GetConsumerLag failed: %v", err)
	}
	if lag1.CommittedOffset != 0 {
		t.Fatalf("CommittedOffset=%d, want 0", lag1.CommittedOffset)
	}
	if lag1.InFlightCount != 0 {
		t.Fatalf("InFlightCount=%d, want 0", lag1.InFlightCount)
	}
}
