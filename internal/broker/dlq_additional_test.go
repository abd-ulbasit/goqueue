package broker

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDLQRouter_Route_DisabledIsNoop(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 1)

	cfg := DefaultReliabilityConfig()
	cfg.DLQEnabled = false

	r := NewDLQRouter(b, cfg)

	msg := &DLQMessage{
		OriginalTopic:     "orders",
		OriginalPartition: 0,
		OriginalOffset:    7,
		OriginalTimestamp: time.Now(),
		OriginalKey:       []byte("k"),
		OriginalValue:     []byte("v"),
		DeliveryAttempts:  1,
		FirstDelivery:     time.Now(),
		LastDelivery:      time.Now(),
		LastConsumer:      "c1",
		LastGroup:         "g1",
		Reason:            DLQReasonMaxRetries,
		DLQTime:           time.Now(),
	}

	if err := r.Route(msg); err != nil {
		t.Fatalf("Route (disabled) failed: %v", err)
	}

	if b.TopicExists(cfg.DLQTopicName("orders")) {
		t.Fatalf("DLQ topic should not be created when DLQ is disabled")
	}

	stats := r.Stats()
	if stats.TotalRouted != 0 {
		t.Fatalf("TotalRouted=%d, want 0", stats.TotalRouted)
	}
}

func TestDLQRouter_Route_AutoCreatesDLQAndUpdatesStats(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 2)

	cfg := DefaultReliabilityConfig()
	cfg.DLQEnabled = true
	cfg.DLQRetentionHours = 24

	r := NewDLQRouter(b, cfg)

	msg := &DLQMessage{
		OriginalTopic:     "orders",
		OriginalPartition: 1,
		OriginalOffset:    42,
		OriginalTimestamp: time.Now(),
		OriginalKey:       []byte("order-42"),
		OriginalValue:     []byte("{\"id\":42}"),
		DeliveryAttempts:  4,
		FirstDelivery:     time.Now().Add(-5 * time.Minute),
		LastDelivery:      time.Now(),
		LastConsumer:      "c1",
		LastGroup:         "g1",
		Reason:            DLQReasonRejected,
		LastError:         "nope",
		DLQTime:           time.Now(),
	}

	if err := r.Route(msg); err != nil {
		t.Fatalf("Route failed: %v", err)
	}

	dlqTopic := cfg.DLQTopicName("orders")
	if !b.TopicExists(dlqTopic) {
		t.Fatalf("expected DLQ topic %q to be created", dlqTopic)
	}

	dlq, err := b.GetTopic(dlqTopic)
	if err != nil {
		t.Fatalf("GetTopic(%q) failed: %v", dlqTopic, err)
	}
	if dlq.NumPartitions() != 2 {
		t.Fatalf("DLQ partitions=%d, want 2", dlq.NumPartitions())
	}

	stats := r.Stats()
	if stats.TotalRouted != 1 {
		t.Fatalf("TotalRouted=%d, want 1", stats.TotalRouted)
	}
	if stats.RoutesByReason[DLQReasonRejected] != 1 {
		t.Fatalf("RoutesByReason[REJECTED]=%d, want 1", stats.RoutesByReason[DLQReasonRejected])
	}
	if stats.RoutesByTopic["orders"] != 1 {
		t.Fatalf("RoutesByTopic[orders]=%d, want 1", stats.RoutesByTopic["orders"])
	}

	// Stats must be a deep copy (mutating the returned map should not affect router).
	stats.RoutesByTopic["orders"] = 999
	stats2 := r.Stats()
	if stats2.RoutesByTopic["orders"] != 1 {
		t.Fatalf("Stats deep copy broken: got %d, want 1", stats2.RoutesByTopic["orders"])
	}
}

func TestDLQRouter_TopicHelpers(t *testing.T) {
	b := newTestBroker(t)
	cfg := DefaultReliabilityConfig()
	r := NewDLQRouter(b, cfg)

	if r.GetDLQTopic("orders") != "orders"+cfg.DLQSuffix {
		t.Fatalf("GetDLQTopic unexpected")
	}
	if r.IsDLQTopic("x") {
		t.Fatalf("IsDLQTopic(x)=true, want false")
	}
	if !r.IsDLQTopic("orders" + cfg.DLQSuffix) {
		t.Fatalf("IsDLQTopic(orders.dlq)=false, want true")
	}
	if r.GetOriginalTopic("not-a-dlq") != "" {
		t.Fatalf("GetOriginalTopic(non-dlq) expected empty")
	}
	if r.GetOriginalTopic("orders"+cfg.DLQSuffix) != "orders" {
		t.Fatalf("GetOriginalTopic(dlq) unexpected")
	}
}

func TestParseDLQMessage_InvalidJSON(t *testing.T) {
	if _, err := ParseDLQMessage([]byte("{")); err == nil {
		t.Fatalf("ParseDLQMessage expected error")
	}
}

func TestDLQRouter_Reprocess_SkipsErrorsAndRepublishes(t *testing.T) {
	b := newTestBroker(t)
	mustCreateTopic(t, b, "orders", 1)
	if err := b.CreateTopic(TopicConfig{Name: "orders.dlq", NumPartitions: 1, RetentionHours: 24}); err != nil {
		t.Fatalf("CreateTopic(orders.dlq) failed: %v", err)
	}

	cfg := DefaultReliabilityConfig()
	cfg.DLQEnabled = true
	r := NewDLQRouter(b, cfg)

	// Message 0: invalid JSON (ParseDLQMessage should fail) -> Errors++.
	if _, _, err := b.Publish("orders.dlq", nil, []byte("{bad")); err != nil {
		t.Fatalf("Publish(invalid dlq msg) failed: %v", err)
	}

	// Message 1: valid DLQ message but filtered out -> Skipped++.
	dlq1 := &DLQMessage{
		OriginalTopic:     "orders",
		OriginalPartition: 0,
		OriginalOffset:    1,
		OriginalTimestamp: time.Now(),
		OriginalKey:       []byte("k1"),
		OriginalValue:     []byte("v1"),
		DeliveryAttempts:  3,
		FirstDelivery:     time.Now().Add(-1 * time.Minute),
		LastDelivery:      time.Now(),
		LastConsumer:      "c1",
		LastGroup:         "g1",
		Reason:            DLQReasonMaxRetries,
		DLQTime:           time.Now(),
	}
	b1, _ := json.Marshal(dlq1)
	if _, _, err := b.Publish("orders.dlq", nil, b1); err != nil {
		t.Fatalf("Publish(dlq1) failed: %v", err)
	}

	// Message 2: valid and passes filter -> Processed++.
	dlq2 := &DLQMessage{
		OriginalTopic:     "orders",
		OriginalPartition: 0,
		OriginalOffset:    2,
		OriginalTimestamp: time.Now(),
		OriginalKey:       []byte("k2"),
		OriginalValue:     []byte("v2"),
		DeliveryAttempts:  2,
		FirstDelivery:     time.Now().Add(-30 * time.Second),
		LastDelivery:      time.Now(),
		LastConsumer:      "c2",
		LastGroup:         "g1",
		Reason:            DLQReasonRejected,
		DLQTime:           time.Now(),
	}
	b2, _ := json.Marshal(dlq2)
	if _, _, err := b.Publish("orders.dlq", nil, b2); err != nil {
		t.Fatalf("Publish(dlq2) failed: %v", err)
	}

	res, err := r.Reprocess(ReprocessRequest{
		DLQTopic:    "orders.dlq",
		TargetTopic: "", // use original_topic
		FromOffset:  0,
		MaxMessages: 10,
		Filter: func(m *DLQMessage) bool {
			return m.OriginalOffset >= 2
		},
	})
	if err != nil {
		t.Fatalf("Reprocess failed: %v", err)
	}

	if res.Errors != 1 || res.Skipped != 1 || res.Processed != 1 {
		t.Fatalf("result=%+v, want Errors=1 Skipped=1 Processed=1", res)
	}
	if res.LastOffset < 2 {
		t.Fatalf("LastOffset=%d, want >=2", res.LastOffset)
	}
}
