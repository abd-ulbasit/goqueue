package broker

import (
	"testing"

	"goqueue/internal/storage"
)

func TestBroker_PublishBatch_AndPriorityConsume_MarkConsumed_Stats(t *testing.T) {
	b := newTestBroker(t)

	// Cover a handful of simple accessors (these were previously 0%):
	_ = DefaultBrokerConfig()
	if b.AckManager() == nil {
		t.Fatalf("expected AckManager accessor to return non-nil")
	}
	if b.Tracer() == nil {
		t.Fatalf("expected Tracer accessor to return non-nil")
	}
	_ = b.ReliabilityConfig()
	_ = b.GroupCoordinator()
	_ = b.CooperativeGroupCoordinator()
	_ = b.Uptime()

	mustCreateTopic(t, b, "orders", 1)

	// -------------------------------------------------------------------------
	// PublishBatch (0% previously)
	// -------------------------------------------------------------------------
	results, err := b.PublishBatch("orders", []struct {
		Key   []byte
		Value []byte
	}{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	})
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("len(results)=%d, want 2", len(results))
	}

	// -------------------------------------------------------------------------
	// Priority publish + ConsumeByPriority/ConsumeByPriorityWFQ (0% previously)
	// -------------------------------------------------------------------------
	p0, o0, err := b.PublishWithPriority("orders", []byte("k0"), []byte("critical"), storage.PriorityCritical)
	if err != nil {
		t.Fatalf("PublishWithPriority(critical) failed: %v", err)
	}
	p1, o1, err := b.PublishWithPriority("orders", []byte("k1"), []byte("high"), storage.PriorityHigh)
	if err != nil {
		t.Fatalf("PublishWithPriority(high) failed: %v", err)
	}
	p2, o2, err := b.PublishWithPriority("orders", []byte("k2"), []byte("low"), storage.PriorityLow)
	if err != nil {
		t.Fatalf("PublishWithPriority(low) failed: %v", err)
	}
	if p0 != 0 || p1 != 0 || p2 != 0 {
		t.Fatalf("expected all publishes to go to partition 0")
	}
	if !(o0 <= o1 && o1 <= o2) {
		t.Fatalf("expected offsets to be monotonic: o0=%d o1=%d o2=%d", o0, o1, o2)
	}

	msgs, err := b.ConsumeByPriority("orders", 0, 0, 10)
	if err != nil {
		t.Fatalf("ConsumeByPriority failed: %v", err)
	}
	if len(msgs) < 3 {
		t.Fatalf("expected at least 3 messages, got %d", len(msgs))
	}
	// Strict priority consumption should surface the highest priority first.
	if msgs[0].Priority != storage.PriorityCritical {
		t.Fatalf("msgs[0].Priority=%d, want Critical(0)", msgs[0].Priority)
	}

	// Consume via WFQ path too (we don't assert the exact ordering here; WFQ is
	// intentionally fairness-biased rather than strictly-priority).
	scheduler := NewPriorityScheduler(DefaultPrioritySchedulerConfig())
	wfqMsgs, err := b.ConsumeByPriorityWFQ("orders", 0, 0, 10, scheduler)
	if err != nil {
		t.Fatalf("ConsumeByPriorityWFQ failed: %v", err)
	}
	if len(wfqMsgs) == 0 {
		t.Fatalf("expected some messages from WFQ consume")
	}

	// -------------------------------------------------------------------------
	// MarkConsumed (0% previously) + PriorityStats (0% previously)
	// -------------------------------------------------------------------------
	// Mark one of the consumed messages as processed so it disappears from the
	// priority index (similar to "ack" but for the priority query side).
	toMark := msgs[0]
	if err := b.MarkConsumed(toMark.Topic, toMark.Partition, toMark.Offset); err != nil {
		t.Fatalf("MarkConsumed failed: %v", err)
	}

	after, err := b.ConsumeByPriority("orders", 0, 0, 10)
	if err != nil {
		t.Fatalf("ConsumeByPriority(after MarkConsumed) failed: %v", err)
	}
	for _, m := range after {
		if m.Offset == toMark.Offset {
			t.Fatalf("expected offset %d to be filtered after MarkConsumed", toMark.Offset)
		}
	}

	ps := b.PriorityStats()
	if ps.Topics["orders"] == nil {
		t.Fatalf("expected PriorityStats to include topic")
	}
	partStats := ps.Topics["orders"].Partitions[0]
	if partStats == nil {
		t.Fatalf("expected PriorityStats to include partition 0")
	}

	// We only make light assertions to keep this test robust across
	// implementation changes.
	if partStats.Total[storage.PriorityCritical] == 0 && partStats.Total[storage.PriorityHigh] == 0 {
		t.Fatalf("expected some total counts in priority stats")
	}
}
