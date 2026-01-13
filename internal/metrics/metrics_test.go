package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewRegistry(t *testing.T) {
	config := DefaultConfig()
	config.IncludeGoCollector = false
	config.IncludeProcessCollector = false

	registry := NewRegistry(config)

	if registry == nil {
		t.Fatal("expected registry to be non-nil")
	}

	if registry.Broker == nil {
		t.Error("expected Broker metrics to be initialized")
	}
	if registry.Storage == nil {
		t.Error("expected Storage metrics to be initialized")
	}
	if registry.Consumer == nil {
		t.Error("expected Consumer metrics to be initialized")
	}
	if registry.Cluster == nil {
		t.Error("expected Cluster metrics to be initialized")
	}
}

func TestBrokerMetrics_RecordPublish(t *testing.T) {
	config := DefaultConfig()
	config.IncludeGoCollector = false
	config.IncludeProcessCollector = false
	registry := NewRegistry(config)

	// RecordPublish(topic string, bytes int, latency float64)
	registry.Broker.RecordPublish("orders", 1024, 0.005)
	registry.Broker.RecordPublish("orders", 2048, 0.010)
	registry.Broker.RecordPublish("events", 512, 0.002)

	// Use WithLabelValues to get specific counter for "orders" topic
	ordersCount := testutil.ToFloat64(registry.Broker.MessagesPublished.WithLabelValues("orders"))
	if ordersCount != 2 {
		t.Errorf("MessagesPublished for orders: expected 2, got %v", ordersCount)
	}

	eventsCount := testutil.ToFloat64(registry.Broker.MessagesPublished.WithLabelValues("events"))
	if eventsCount != 1 {
		t.Errorf("MessagesPublished for events: expected 1, got %v", eventsCount)
	}

	// Check bytes for "orders" topic
	ordersBytes := testutil.ToFloat64(registry.Broker.BytesPublished.WithLabelValues("orders"))
	expectedOrdersBytes := float64(1024 + 2048)
	if ordersBytes != expectedOrdersBytes {
		t.Errorf("BytesPublished for orders: expected %v, got %v", expectedOrdersBytes, ordersBytes)
	}
}

func TestBrokerMetrics_RecordConsume(t *testing.T) {
	config := DefaultConfig()
	config.IncludeGoCollector = false
	config.IncludeProcessCollector = false
	registry := NewRegistry(config)

	// RecordConsume(topic, consumerGroup string, count int, bytes int, latency float64)
	registry.Broker.RecordConsume("orders", "group-1", 10, 10240, 0.003)
	registry.Broker.RecordConsume("orders", "group-1", 5, 5120, 0.002)
	registry.Broker.RecordConsume("events", "group-2", 20, 20480, 0.005)

	// Check messages consumed for orders/group-1
	ordersGroup1 := testutil.ToFloat64(registry.Broker.MessagesConsumed.WithLabelValues("orders", "group-1"))
	if ordersGroup1 != 15 {
		t.Errorf("MessagesConsumed for orders/group-1: expected 15, got %v", ordersGroup1)
	}

	// Check events/group-2
	eventsGroup2 := testutil.ToFloat64(registry.Broker.MessagesConsumed.WithLabelValues("events", "group-2"))
	if eventsGroup2 != 20 {
		t.Errorf("MessagesConsumed for events/group-2: expected 20, got %v", eventsGroup2)
	}
}

func TestStorageMetrics_RecordWrite(t *testing.T) {
	config := DefaultConfig()
	config.IncludeGoCollector = false
	config.IncludeProcessCollector = false
	registry := NewRegistry(config)

	// RecordWrite(topic string, partition int, bytes int)
	registry.Storage.RecordWrite("orders", 0, 1024)
	registry.Storage.RecordWrite("orders", 0, 2048)

	// Check bytes written for orders topic
	ordersBytes := testutil.ToFloat64(registry.Storage.BytesWritten.WithLabelValues("orders"))
	expected := float64(1024 + 2048)
	if ordersBytes != expected {
		t.Errorf("BytesWritten for orders: expected %v, got %v", expected, ordersBytes)
	}
}

func TestConsumerMetrics_SetGroupMembers(t *testing.T) {
	config := DefaultConfig()
	config.IncludeGoCollector = false
	config.IncludeProcessCollector = false
	registry := NewRegistry(config)

	// SetGroupMembers(consumerGroup string, count int)
	registry.Consumer.SetGroupMembers("group-1", 3)
	registry.Consumer.SetGroupMembers("group-2", 5)

	// Check group-1
	group1 := testutil.ToFloat64(registry.Consumer.GroupMembers.WithLabelValues("group-1"))
	if group1 != 3 {
		t.Errorf("GroupMembers for group-1: expected 3, got %v", group1)
	}

	// Check group-2
	group2 := testutil.ToFloat64(registry.Consumer.GroupMembers.WithLabelValues("group-2"))
	if group2 != 5 {
		t.Errorf("GroupMembers for group-2: expected 5, got %v", group2)
	}
}

func TestClusterMetrics_SetNodeCounts(t *testing.T) {
	config := DefaultConfig()
	config.IncludeGoCollector = false
	config.IncludeProcessCollector = false
	registry := NewRegistry(config)

	// SetNodeCounts(total, healthy int)
	registry.Cluster.SetNodeCounts(5, 4)

	if got := testutil.ToFloat64(registry.Cluster.NodesTotal); got != 5 {
		t.Errorf("NodesTotal: expected 5, got %v", got)
	}
	if got := testutil.ToFloat64(registry.Cluster.NodesHealthy); got != 4 {
		t.Errorf("NodesHealthy: expected 4, got %v", got)
	}
}

func TestHandler_ProducesPrometheusOutput(t *testing.T) {
	config := DefaultConfig()
	config.IncludeGoCollector = false
	config.IncludeProcessCollector = false
	registry := NewRegistry(config)

	registry.Broker.RecordPublish("orders", 1024, 0.005)
	registry.Storage.RecordWrite("orders", 0, 3072)
	registry.Consumer.SetGroupMembers("group-1", 3)
	registry.Cluster.SetNodeCounts(5, 5)

	handler := registry.Handler()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	expectedMetrics := []string{
		"goqueue_broker_messages_published_total",
		"goqueue_broker_bytes_published_total",
		"goqueue_storage_bytes_written_total",
		"goqueue_consumer_group_members",
		"goqueue_cluster_nodes_total",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(body, metric) {
			t.Errorf("expected metric %s in output, not found", metric)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if !config.Enabled {
		t.Error("expected Enabled to be true by default")
	}
	if config.Namespace != "goqueue" {
		t.Errorf("expected Namespace goqueue, got %s", config.Namespace)
	}
	if config.IncludePartitionLabel {
		t.Error("expected IncludePartitionLabel to be false by default")
	}
}

func TestDefaultLatencyBuckets(t *testing.T) {
	config := DefaultConfig()
	buckets := config.HistogramBuckets

	if buckets[0] != 0.0005 {
		t.Errorf("expected first bucket to be 0.5ms, got %v", buckets[0])
	}

	lastBucket := buckets[len(buckets)-1]
	if lastBucket < 1 {
		t.Errorf("expected last bucket to be >= 1s, got %v", lastBucket)
	}

	for i := 1; i < len(buckets); i++ {
		if buckets[i] <= buckets[i-1] {
			t.Errorf("buckets not in ascending order: %v <= %v", buckets[i], buckets[i-1])
		}
	}
}
