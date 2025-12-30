package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"goqueue/internal/broker"
)

// setupTestServer creates a test broker and API server.
func setupTestServer(t *testing.T) (*Server, func()) {
	t.Helper()

	dir := t.TempDir()
	config := broker.BrokerConfig{
		DataDir: dir,
		NodeID:  "test-node",
	}

	b, err := broker.NewBroker(config)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	server := NewServer(b, DefaultServerConfig())

	cleanup := func() {
		b.Close()
	}

	return server, cleanup
}

// TestHealthEndpoint tests GET /health.
func TestHealthEndpoint(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	server.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("Expected status 'ok', got %v", resp["status"])
	}
}

// TestStatsEndpoint tests GET /stats.
func TestStatsEndpoint(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()

	server.handleStats(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if resp["node_id"] != "test-node" {
		t.Errorf("Expected node_id 'test-node', got %v", resp["node_id"])
	}
}

// TestCreateTopic tests POST /topics.
func TestCreateTopic(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	tests := []struct {
		name           string
		body           CreateTopicRequest
		expectedStatus int
	}{
		{
			name:           "create topic with defaults",
			body:           CreateTopicRequest{Name: "test-topic"},
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "create topic with custom partitions",
			body:           CreateTopicRequest{Name: "test-topic-2", NumPartitions: 6},
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "missing name",
			body:           CreateTopicRequest{NumPartitions: 3},
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.body)
			req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
			rec := httptest.NewRecorder()

			server.handleTopics(rec, req)

			if rec.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d: %s", tt.expectedStatus, rec.Code, rec.Body.String())
			}
		})
	}
}

// TestCreateTopicDuplicate tests creating a topic that already exists.
func TestCreateTopicDuplicate(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(CreateTopicRequest{Name: "dup-topic"})
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.handleTopics(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("Failed to create first topic: %s", rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	server.handleTopics(rec, req)

	if rec.Code != http.StatusConflict {
		t.Errorf("Expected status 409 Conflict, got %d", rec.Code)
	}
}

// TestListTopics tests GET /topics.
func TestListTopics(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	for _, name := range []string{"topic-a", "topic-b", "topic-c"} {
		body, _ := json.Marshal(CreateTopicRequest{Name: name})
		req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		server.handleTopics(rec, req)
	}

	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	rec := httptest.NewRecorder()
	server.handleTopics(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", rec.Code)
	}

	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	topics, ok := resp["topics"].([]interface{})
	if !ok {
		t.Fatalf("Expected topics array, got %T", resp["topics"])
	}

	if len(topics) != 3 {
		t.Errorf("Expected 3 topics, got %d", len(topics))
	}
}

// TestGetTopic tests GET /topics/{name}.
func TestGetTopic(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(CreateTopicRequest{Name: "my-topic", NumPartitions: 4})
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.handleTopics(rec, req)

	req = httptest.NewRequest(http.MethodGet, "/topics/my-topic", nil)
	rec = httptest.NewRecorder()
	server.getTopic(rec, req, "my-topic")

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	if resp["name"] != "my-topic" {
		t.Errorf("Expected name 'my-topic', got %v", resp["name"])
	}
	if int(resp["partitions"].(float64)) != 4 {
		t.Errorf("Expected 4 partitions, got %v", resp["partitions"])
	}
}

// TestGetTopicNotFound tests GET /topics/{name} for non-existent topic.
func TestGetTopicNotFound(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/topics/nonexistent", nil)
	rec := httptest.NewRecorder()
	server.getTopic(rec, req, "nonexistent")

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", rec.Code)
	}
}

// TestDeleteTopic tests DELETE /topics/{name}.
func TestDeleteTopic(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(CreateTopicRequest{Name: "to-delete"})
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.handleTopics(rec, req)

	req = httptest.NewRequest(http.MethodDelete, "/topics/to-delete", nil)
	rec = httptest.NewRecorder()
	server.deleteTopic(rec, req, "to-delete")

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/topics/to-delete", nil)
	rec = httptest.NewRecorder()
	server.getTopic(rec, req, "to-delete")

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected topic to be deleted")
	}
}

// TestPublishMessages tests POST /topics/{name}/messages.
func TestPublishMessages(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(CreateTopicRequest{Name: "pub-topic", NumPartitions: 3})
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.handleTopics(rec, req)

	pubReq := PublishRequest{
		Messages: []PublishMessage{
			{Key: "key-1", Value: "value-1"},
			{Key: "key-2", Value: "value-2"},
			{Value: "value-3"},
		},
	}
	body, _ = json.Marshal(pubReq)
	req = httptest.NewRequest(http.MethodPost, "/topics/pub-topic/messages", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	server.publishMessages(rec, req, "pub-topic")

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	results, ok := resp["results"].([]interface{})
	if !ok {
		t.Fatalf("Expected results array")
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

// TestPublishWithExplicitPartition tests publishing to a specific partition.
func TestPublishWithExplicitPartition(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(CreateTopicRequest{Name: "explicit-part", NumPartitions: 3})
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.handleTopics(rec, req)

	partition := 2
	pubReq := PublishRequest{
		Messages: []PublishMessage{
			{Value: "value", Partition: &partition},
		},
	}
	body, _ = json.Marshal(pubReq)
	req = httptest.NewRequest(http.MethodPost, "/topics/explicit-part/messages", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	server.publishMessages(rec, req, "explicit-part")

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", rec.Code)
	}

	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	results := resp["results"].([]interface{})
	result := results[0].(map[string]interface{})

	if int(result["partition"].(float64)) != 2 {
		t.Errorf("Expected partition 2, got %v", result["partition"])
	}
}

// TestConsumeMessages tests GET /topics/{name}/partitions/{id}/messages.
func TestConsumeMessages(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(CreateTopicRequest{Name: "consume-topic", NumPartitions: 1})
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.handleTopics(rec, req)

	pubReq := PublishRequest{
		Messages: []PublishMessage{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
			{Key: "k3", Value: "v3"},
		},
	}
	body, _ = json.Marshal(pubReq)
	req = httptest.NewRequest(http.MethodPost, "/topics/consume-topic/messages", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	server.publishMessages(rec, req, "consume-topic")

	req = httptest.NewRequest(http.MethodGet, "/topics/consume-topic/partitions/0/messages?offset=0&limit=10", nil)
	rec = httptest.NewRecorder()
	server.consumeMessages(rec, req, "consume-topic", 0)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp ConsumeResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	if len(resp.Messages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(resp.Messages))
	}

	if resp.NextOffset != 3 {
		t.Errorf("Expected next_offset 3, got %d", resp.NextOffset)
	}
}

// TestConsumeWithOffset tests consuming from a specific offset.
func TestConsumeWithOffset(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(CreateTopicRequest{Name: "offset-topic", NumPartitions: 1})
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.handleTopics(rec, req)

	for i := 0; i < 10; i++ {
		pubReq := PublishRequest{
			Messages: []PublishMessage{{Value: fmt.Sprintf("msg-%d", i)}},
		}
		body, _ = json.Marshal(pubReq)
		req = httptest.NewRequest(http.MethodPost, "/topics/offset-topic/messages", bytes.NewReader(body))
		rec = httptest.NewRecorder()
		server.publishMessages(rec, req, "offset-topic")
	}

	req = httptest.NewRequest(http.MethodGet, "/topics/offset-topic/partitions/0/messages?offset=5&limit=10", nil)
	rec = httptest.NewRecorder()
	server.consumeMessages(rec, req, "offset-topic", 0)

	var resp ConsumeResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	if len(resp.Messages) != 5 {
		t.Errorf("Expected 5 messages (offset 5-9), got %d", len(resp.Messages))
	}

	if resp.Messages[0].Offset != 5 {
		t.Errorf("Expected first message offset 5, got %d", resp.Messages[0].Offset)
	}
}

// TestServerStartStop tests server lifecycle.
func TestServerStartStop(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.httpServer.Addr = "127.0.0.1:0"

	err := server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Stop(ctx)
	if err != nil {
		t.Errorf("Failed to stop server: %v", err)
	}
}
