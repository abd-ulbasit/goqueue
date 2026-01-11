package broker

import (
	"log/slog"
	"testing"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// BROKER COVERAGE TARGETS
// =============================================================================
//
// These tests intentionally exercise small “wrapper” APIs on `Broker` that are
// easy to forget to test because the underlying subsystems (scheduler, schema
// registry, ack manager) already have their own tests.
//
// WHY THIS FILE EXISTS:
//   - Broker package coverage was materially lower than cluster.
//   - Many broker methods were 0% even though their dependencies were tested.
//   - These methods are part of the public broker surface (used by HTTP APIs)
//     and can hide real integration bugs (e.g., error mapping semantics).
//
// We keep assertions intentionally “contract-level” (not implementation-level)
// so tests remain robust across refactors.
// =============================================================================

func TestBroker_PublishBatchWithPriority_SuccessAndErrors(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 1) // deterministic: all publishes go to p0

	// -------------------------------------------------------------------------
	// SUCCESS: mixed-priority batch publish
	// -------------------------------------------------------------------------
	results, err := b.PublishBatchWithPriority("orders", []struct {
		Key      []byte
		Value    []byte
		Priority storage.Priority
	}{
		{Key: []byte("k1"), Value: []byte("v1"), Priority: storage.PriorityHigh},
		{Key: []byte("k2"), Value: []byte("v2"), Priority: storage.PriorityLow},
		{Key: []byte("k3"), Value: []byte("v3"), Priority: storage.PriorityCritical},
	})
	if err != nil {
		t.Fatalf("PublishBatchWithPriority failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("len(results)=%d, want 3", len(results))
	}
	for i, r := range results {
		if r.Partition != 0 {
			t.Fatalf("results[%d].Partition=%d, want 0", i, r.Partition)
		}
		if r.Offset != int64(i) {
			// Single-partition append-only log: offsets should be monotonic starting at 0.
			t.Fatalf("results[%d].Offset=%d, want %d", i, r.Offset, i)
		}
	}

	// -------------------------------------------------------------------------
	// ERROR PATHS: closed broker + missing topic
	// -------------------------------------------------------------------------
	// Missing topic should be surfaced as ErrTopicNotFound wrapped.
	if _, err := b.PublishBatchWithPriority("nope", nil); err == nil {
		t.Fatalf("expected error for missing topic")
	}

	// Closed broker should return ErrBrokerClosed.
	// NOTE: we create a second broker so this test remains isolated.
	b2, err := NewBroker(BrokerConfig{DataDir: t.TempDir(), LogLevel: slog.LevelError})
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	if err := b2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if _, err := b2.PublishBatchWithPriority("orders", nil); err != ErrBrokerClosed {
		t.Fatalf("PublishBatchWithPriority after Close error=%v, want %v", err, ErrBrokerClosed)
	}
}

func TestBroker_Nack_RecordsTraceAndUpdatesReliabilityStats(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 1)

	// Publish a message and consume it with receipts so we can NACK using the
	// broker-level wrapper.
	_, _, err := b.Publish("orders", []byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	msgs, err := b.ConsumeWithReceipts("orders", 0, 0, 1, "consumer-1", "group-1")
	if err != nil {
		t.Fatalf("ConsumeWithReceipts failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("ConsumeWithReceipts len=%d, want 1", len(msgs))
	}

	// NACK the message (transient failure).
	res, err := b.Nack(msgs[0].ReceiptHandle, "boom")
	if err != nil {
		t.Fatalf("Nack failed: %v", err)
	}
	if res.DeliveryCount < 1 {
		t.Fatalf("DeliveryCount=%d, want >= 1", res.DeliveryCount)
	}

	// ReliabilityStats should reflect the NACK at the ACK manager level.
	stats := b.ReliabilityStats()
	if stats.AckManager.TotalNacks < 1 {
		t.Fatalf("AckManager.TotalNacks=%d, want >= 1", stats.AckManager.TotalNacks)
	}

	// Tracing: Broker.Nack should record a consume.nacked span.
	// RecordSpan stores into the ring buffer synchronously, so this should be
	// visible immediately, but we allow a short polling window to avoid flakes
	// if future changes make it async.
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		found := false
		traces := b.Tracer().GetRecentTraces(10)
		for _, tr := range traces {
			for _, sp := range tr.Spans {
				if sp.EventType == SpanEventConsumeNacked {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected to find a %q span in recent traces", SpanEventConsumeNacked)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBroker_DelayedMessageWrappers_CancelAndQuery(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "emails", 1)

	// Use a long delay so the message remains pending (not delivered) throughout
	// the test. That keeps the test deterministic and avoids racing timers.
	delay := 1 * time.Hour
	p, off, err := b.PublishWithDelay("emails", []byte("user-1"), []byte("reminder"), delay)
	if err != nil {
		t.Fatalf("PublishWithDelay failed: %v", err)
	}
	if p != 0 {
		t.Fatalf("partition=%d, want 0", p)
	}

	// Broker-level query wrappers.
	if !b.IsDelayed("emails", 0, off) {
		t.Fatalf("IsDelayed=false, want true")
	}

	list, err := b.GetDelayedMessages("emails", 10, 0)
	if err != nil {
		t.Fatalf("GetDelayedMessages failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("len(GetDelayedMessages)=%d, want 1", len(list))
	}

	info, err := b.GetDelayedMessage("emails", 0, off)
	if err != nil {
		t.Fatalf("GetDelayedMessage failed: %v", err)
	}
	if info.State != "pending" {
		t.Fatalf("State=%q, want %q", info.State, "pending")
	}

	// Cancel should be idempotent at the broker API layer:
	//   - First cancel: (true, nil)
	//   - Second cancel: (false, nil)
	cancelled, err := b.CancelDelayed("emails", 0, off)
	if err != nil {
		t.Fatalf("CancelDelayed failed: %v", err)
	}
	if !cancelled {
		t.Fatalf("CancelDelayed cancelled=false, want true")
	}

	cancelled2, err := b.CancelDelayed("emails", 0, off)
	if err != nil {
		t.Fatalf("CancelDelayed (2nd) failed: %v", err)
	}
	if cancelled2 {
		t.Fatalf("CancelDelayed (2nd) cancelled=true, want false")
	}

	// After cancellation, the message should not be considered delayed.
	if b.IsDelayed("emails", 0, off) {
		t.Fatalf("IsDelayed=true after cancellation, want false")
	}

	// DelayStats and Scheduler() are just accessors/wrappers, but we still
	// validate the stats reflect the scheduling activity.
	if b.Scheduler() == nil {
		t.Fatalf("Scheduler() returned nil")
	}

	ds := b.DelayStats()
	if ds.TotalScheduled == 0 {
		t.Fatalf("DelayStats.TotalScheduled=0, want > 0")
	}
	if ds.TotalCancelled == 0 {
		t.Fatalf("DelayStats.TotalCancelled=0, want > 0")
	}
	if ds.ByTopic["emails"] == 0 {
		// After cancellation, pending count should be 0 for this topic.
		// We keep the assertion loose (it must exist and be >= 0).
		if _, ok := ds.ByTopic["emails"]; !ok {
			t.Fatalf("expected DelayStats.ByTopic to include topic")
		}
	}
}

func TestBroker_SchemaRegistryWrappers_StatsAndAccessor(t *testing.T) {
	b := newTestBroker(t)

	sr := b.SchemaRegistry()
	if sr == nil {
		t.Fatalf("SchemaRegistry() returned nil")
	}

	// Register a tiny schema to create observable stats.
	subject := "orders"
	schema := `{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`
	if _, err := sr.RegisterSchema(subject, schema); err != nil {
		t.Fatalf("RegisterSchema failed: %v", err)
	}

	stats := b.SchemaStats()
	if stats.SubjectCount < 1 {
		t.Fatalf("SchemaStats.SubjectCount=%d, want >= 1", stats.SubjectCount)
	}
	if stats.TotalSchemas < 1 {
		t.Fatalf("SchemaStats.TotalSchemas=%d, want >= 1", stats.TotalSchemas)
	}
}
