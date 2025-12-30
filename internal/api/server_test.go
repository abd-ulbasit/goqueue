// ============================================================================
// API SERVER TESTS - Chi Router Based
// ============================================================================
//
// These tests validate the REST API layer using chi router.
// Tests call through the full router (ServeHTTP) rather than individual handlers
// to properly exercise URL parameter parsing, middleware, and routing.
//
// TEST PATTERNS:
//   - setupTestServer: Creates broker + API server in temp directory
//   - All tests use httptest.NewRecorder() + router.ServeHTTP()
//   - This ensures chi URL params work correctly
//
// COMPARISON WITH UNIT TESTS:
//   - Integration tests: Call through full router (what we do here)
//   - Unit tests: Would mock the broker and test handlers in isolation
//   - Both are valuable; we prioritize integration for end-to-end validation
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

// ============================================================================
// TEST HELPERS
// ============================================================================

// setupTestServer creates a test broker and API server in a temp directory.
// Returns the server and a cleanup function.
//
// WHY temp directory: Tests must be isolated - each test gets fresh state.
// Temp directories are auto-cleaned by Go's testing framework.
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

// doRequest is a helper to make HTTP requests through the router.
// This ensures chi URL params and middleware are exercised.
func doRequest(server *Server, method, path string, body interface{}) *httptest.ResponseRecorder {
	var reqBody *bytes.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reqBody = bytes.NewReader(data)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.router.ServeHTTP(rec, req)
	return rec
}

// ============================================================================
// HEALTH & STATS ENDPOINT TESTS
// ============================================================================

// TestHealthEndpoint tests GET /health.
// Health endpoint is critical for orchestration systems (Kubernetes liveness probes).
func TestHealthEndpoint(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	rec := doRequest(server, http.MethodGet, "/health", nil)

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
// Stats endpoint is used for observability and monitoring dashboards.
func TestStatsEndpoint(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	rec := doRequest(server, http.MethodGet, "/stats", nil)

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

// ============================================================================
// TOPIC CRUD TESTS
// ============================================================================

// TestCreateTopic tests POST /topics.
// Topic creation is the entry point for using the queue.
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
			rec := doRequest(server, http.MethodPost, "/topics", tt.body)

			if rec.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d: %s", tt.expectedStatus, rec.Code, rec.Body.String())
			}
		})
	}
}

// TestCreateTopicDuplicate tests creating a topic that already exists.
// Should return 409 Conflict.
func TestCreateTopicDuplicate(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	body := CreateTopicRequest{Name: "dup-topic"}
	rec := doRequest(server, http.MethodPost, "/topics", body)

	if rec.Code != http.StatusCreated {
		t.Fatalf("Failed to create first topic: %s", rec.Body.String())
	}

	// Try to create the same topic again
	rec = doRequest(server, http.MethodPost, "/topics", body)

	if rec.Code != http.StatusConflict {
		t.Errorf("Expected status 409 Conflict, got %d", rec.Code)
	}
}

// TestListTopics tests GET /topics.
func TestListTopics(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create multiple topics
	for _, name := range []string{"topic-a", "topic-b", "topic-c"} {
		rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: name})
		if rec.Code != http.StatusCreated {
			t.Fatalf("Failed to create topic %s: %s", name, rec.Body.String())
		}
	}

	// List topics
	rec := doRequest(server, http.MethodGet, "/topics", nil)

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

	// Create a topic
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "my-topic", NumPartitions: 4})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Failed to create topic: %s", rec.Body.String())
	}

	// Get the topic
	rec = doRequest(server, http.MethodGet, "/topics/my-topic", nil)

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

	rec := doRequest(server, http.MethodGet, "/topics/nonexistent", nil)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", rec.Code)
	}
}

// TestDeleteTopic tests DELETE /topics/{name}.
func TestDeleteTopic(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create a topic
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "to-delete"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Failed to create topic: %s", rec.Body.String())
	}

	// Delete the topic
	rec = doRequest(server, http.MethodDelete, "/topics/to-delete", nil)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	// Verify it's deleted
	rec = doRequest(server, http.MethodGet, "/topics/to-delete", nil)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected topic to be deleted, got status %d", rec.Code)
	}
}

// ============================================================================
// PUBLISH & CONSUME TESTS
// ============================================================================

// TestPublishMessages tests POST /topics/{name}/messages.
func TestPublishMessages(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "pub-topic", NumPartitions: 3})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Failed to create topic: %s", rec.Body.String())
	}

	// Publish messages
	pubReq := PublishRequest{
		Messages: []PublishMessage{
			{Key: "key-1", Value: "value-1"},
			{Key: "key-2", Value: "value-2"},
			{Value: "value-3"},
		},
	}
	rec = doRequest(server, http.MethodPost, "/topics/pub-topic/messages", pubReq)

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

	// Create topic
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "explicit-part", NumPartitions: 3})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Failed to create topic: %s", rec.Body.String())
	}

	// Publish to specific partition
	partition := 2
	pubReq := PublishRequest{
		Messages: []PublishMessage{
			{Value: "value", Partition: &partition},
		},
	}
	rec = doRequest(server, http.MethodPost, "/topics/explicit-part/messages", pubReq)

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

	// Create topic
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "consume-topic", NumPartitions: 1})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Failed to create topic: %s", rec.Body.String())
	}

	// Publish messages
	pubReq := PublishRequest{
		Messages: []PublishMessage{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
			{Key: "k3", Value: "v3"},
		},
	}
	rec = doRequest(server, http.MethodPost, "/topics/consume-topic/messages", pubReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("Failed to publish: %s", rec.Body.String())
	}

	// Consume messages
	rec = doRequest(server, http.MethodGet, "/topics/consume-topic/partitions/0/messages?offset=0&limit=10", nil)

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

	// Create topic
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "offset-topic", NumPartitions: 1})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Failed to create topic: %s", rec.Body.String())
	}

	// Publish 10 messages
	for i := 0; i < 10; i++ {
		pubReq := PublishRequest{
			Messages: []PublishMessage{{Value: fmt.Sprintf("msg-%d", i)}},
		}
		rec = doRequest(server, http.MethodPost, "/topics/offset-topic/messages", pubReq)
		if rec.Code != http.StatusOK {
			t.Fatalf("Failed to publish message %d: %s", i, rec.Body.String())
		}
	}

	// Consume from offset 5
	rec = doRequest(server, http.MethodGet, "/topics/offset-topic/partitions/0/messages?offset=5&limit=10", nil)

	var resp ConsumeResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	if len(resp.Messages) != 5 {
		t.Errorf("Expected 5 messages (offset 5-9), got %d", len(resp.Messages))
	}

	if resp.Messages[0].Offset != 5 {
		t.Errorf("Expected first message offset 5, got %d", resp.Messages[0].Offset)
	}
}

// ============================================================================
// CONSUMER GROUP TESTS
// ============================================================================

// TestJoinConsumerGroup tests POST /groups/{group}/join.
// Joining a group is the first step for coordinated consumption.
func TestJoinConsumerGroup(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic first (needed for partition assignment)
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "orders", NumPartitions: 3})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Failed to create topic: %s", rec.Body.String())
	}

	// Join group - uses ClientID, not ConsumerID
	joinReq := JoinGroupRequest{
		ClientID: "consumer-1",
		Topics:   []string{"orders"},
	}
	rec = doRequest(server, http.MethodPost, "/groups/order-processors/join", joinReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp JoinGroupResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	// JoinGroupResponse uses Generation, not GenerationID
	if resp.Generation != 1 {
		t.Errorf("Expected generation 1, got %d", resp.Generation)
	}

	// Response gives Partitions array directly (for the subscribed topic)
	// Single consumer should get all 3 partitions
	if len(resp.Partitions) != 3 {
		t.Errorf("Expected 3 partition assignments, got %d", len(resp.Partitions))
	}

	// Should have a MemberID assigned
	if resp.MemberID == "" {
		t.Error("Expected member_id to be assigned")
	}
}

// TestConsumerGroupRebalance tests that adding consumers triggers rebalance.
func TestConsumerGroupRebalance(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "orders", NumPartitions: 4})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Failed to create topic: %s", rec.Body.String())
	}

	// First consumer joins
	joinReq := JoinGroupRequest{
		ClientID: "consumer-1",
		Topics:   []string{"orders"},
	}
	rec = doRequest(server, http.MethodPost, "/groups/order-processors/join", joinReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("Consumer 1 join failed: %s", rec.Body.String())
	}

	var resp1 JoinGroupResponse
	json.Unmarshal(rec.Body.Bytes(), &resp1)

	// Consumer 1 should have all 4 partitions initially
	if len(resp1.Partitions) != 4 {
		t.Errorf("Expected consumer 1 to have 4 partitions, got %d", len(resp1.Partitions))
	}

	// Second consumer joins - triggers rebalance
	joinReq2 := JoinGroupRequest{
		ClientID: "consumer-2",
		Topics:   []string{"orders"},
	}
	rec = doRequest(server, http.MethodPost, "/groups/order-processors/join", joinReq2)
	if rec.Code != http.StatusOK {
		t.Fatalf("Consumer 2 join failed: %s", rec.Body.String())
	}

	var resp2 JoinGroupResponse
	json.Unmarshal(rec.Body.Bytes(), &resp2)

	// Generation should increase due to rebalance
	if resp2.Generation <= resp1.Generation {
		t.Errorf("Expected generation to increase after rebalance")
	}

	// Consumer 2 should have ~2 partitions (range assignment)
	if len(resp2.Partitions) != 2 {
		t.Errorf("Expected consumer 2 to have 2 partitions, got %d", len(resp2.Partitions))
	}
}

// TestHeartbeat tests POST /groups/{group}/heartbeat.
func TestHeartbeat(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic and join group
	doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "orders", NumPartitions: 2})

	joinReq := JoinGroupRequest{
		ClientID: "consumer-1",
		Topics:   []string{"orders"},
	}
	rec := doRequest(server, http.MethodPost, "/groups/order-processors/join", joinReq)
	var joinResp JoinGroupResponse
	json.Unmarshal(rec.Body.Bytes(), &joinResp)

	// Send heartbeat - uses MemberID and Generation
	hbReq := HeartbeatRequest{
		MemberID:   joinResp.MemberID,
		Generation: joinResp.Generation,
	}
	rec = doRequest(server, http.MethodPost, "/groups/order-processors/heartbeat", hbReq)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestHeartbeatWrongGeneration tests that stale heartbeats are rejected.
func TestHeartbeatWrongGeneration(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic and join group
	doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "orders", NumPartitions: 2})

	joinReq := JoinGroupRequest{
		ClientID: "consumer-1",
		Topics:   []string{"orders"},
	}
	rec := doRequest(server, http.MethodPost, "/groups/order-processors/join", joinReq)
	var joinResp JoinGroupResponse
	json.Unmarshal(rec.Body.Bytes(), &joinResp)

	// Send heartbeat with wrong generation
	hbReq := HeartbeatRequest{
		MemberID:   joinResp.MemberID,
		Generation: joinResp.Generation + 100, // Wrong generation
	}
	rec = doRequest(server, http.MethodPost, "/groups/order-processors/heartbeat", hbReq)

	// Should be rejected
	if rec.Code != http.StatusConflict {
		t.Errorf("Expected status 409 (conflict), got %d", rec.Code)
	}
}

// TestLeaveGroup tests POST /groups/{group}/leave.
func TestLeaveGroup(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic and join group
	doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "orders", NumPartitions: 2})

	joinReq := JoinGroupRequest{
		ClientID: "consumer-1",
		Topics:   []string{"orders"},
	}
	rec := doRequest(server, http.MethodPost, "/groups/order-processors/join", joinReq)
	var joinResp JoinGroupResponse
	json.Unmarshal(rec.Body.Bytes(), &joinResp)

	// Leave group - uses MemberID
	leaveReq := LeaveGroupRequest{
		MemberID: joinResp.MemberID,
	}
	rec = doRequest(server, http.MethodPost, "/groups/order-processors/leave", leaveReq)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify group details show no members
	rec = doRequest(server, http.MethodGet, "/groups/order-processors", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("Failed to get group details: %s", rec.Body.String())
	}

	var groupInfo map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &groupInfo)

	members := groupInfo["members"].([]interface{})
	if len(members) != 0 {
		t.Errorf("Expected 0 members after leave, got %d", len(members))
	}
}

// TestCommitOffsets tests POST /groups/{group}/offsets.
func TestCommitOffsets(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic and join group
	doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "orders", NumPartitions: 2})

	joinReq := JoinGroupRequest{
		ClientID: "consumer-1",
		Topics:   []string{"orders"},
	}
	rec := doRequest(server, http.MethodPost, "/groups/order-processors/join", joinReq)
	var joinResp JoinGroupResponse
	json.Unmarshal(rec.Body.Bytes(), &joinResp)

	// Commit offsets - uses MemberID and Generation, with string partition keys
	commitReq := CommitOffsetsRequest{
		MemberID:   joinResp.MemberID,
		Generation: joinResp.Generation,
		Offsets: map[string]map[string]int64{
			"orders": {
				"0": 100,
				"1": 50,
			},
		},
	}
	rec = doRequest(server, http.MethodPost, "/groups/order-processors/offsets", commitReq)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestGetOffsets tests GET /groups/{group}/offsets.
func TestGetOffsets(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic and join group
	doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "orders", NumPartitions: 2})

	joinReq := JoinGroupRequest{
		ClientID: "consumer-1",
		Topics:   []string{"orders"},
	}
	rec := doRequest(server, http.MethodPost, "/groups/order-processors/join", joinReq)
	var joinResp JoinGroupResponse
	json.Unmarshal(rec.Body.Bytes(), &joinResp)

	// Commit some offsets
	commitReq := CommitOffsetsRequest{
		MemberID:   joinResp.MemberID,
		Generation: joinResp.Generation,
		Offsets: map[string]map[string]int64{
			"orders": {"0": 100, "1": 50},
		},
	}
	doRequest(server, http.MethodPost, "/groups/order-processors/offsets", commitReq)

	// Fetch offsets
	rec = doRequest(server, http.MethodGet, "/groups/order-processors/offsets?topic=orders", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	// Response uses "topics" key, not "offsets"
	topics, ok := resp["topics"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected topics in response, got: %v", resp)
	}

	orderOffsets, ok := topics["orders"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected orders topic in response, got: %v", topics)
	}

	// Note: JSON unmarshals numbers as float64
	if int64(orderOffsets["0"].(float64)) != 100 {
		t.Errorf("Expected partition 0 offset 100, got %v", orderOffsets["0"])
	}
	if int64(orderOffsets["1"].(float64)) != 50 {
		t.Errorf("Expected partition 1 offset 50, got %v", orderOffsets["1"])
	}
}

// TestListGroups tests GET /groups.
func TestListGroups(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "orders", NumPartitions: 2})

	// Create multiple groups
	for _, groupID := range []string{"group-a", "group-b", "group-c"} {
		joinReq := JoinGroupRequest{
			ClientID: "consumer-1",
			Topics:   []string{"orders"},
		}
		doRequest(server, http.MethodPost, fmt.Sprintf("/groups/%s/join", groupID), joinReq)
	}

	// List groups
	rec := doRequest(server, http.MethodGet, "/groups", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	groups := resp["groups"].([]interface{})
	if len(groups) != 3 {
		t.Errorf("Expected 3 groups, got %d", len(groups))
	}
}

// TestGetGroupDetails tests GET /groups/{group}.
func TestGetGroupDetails(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic and join group with multiple consumers
	doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "orders", NumPartitions: 4})

	// Add two consumers
	for _, clientID := range []string{"consumer-1", "consumer-2"} {
		joinReq := JoinGroupRequest{
			ClientID: clientID,
			Topics:   []string{"orders"},
		}
		doRequest(server, http.MethodPost, "/groups/order-processors/join", joinReq)
	}

	// Get group details
	rec := doRequest(server, http.MethodGet, "/groups/order-processors", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	// Response uses "id" not "group_id"
	if resp["id"] != "order-processors" {
		t.Errorf("Expected id 'order-processors', got %v", resp["id"])
	}

	members := resp["members"].([]interface{})
	if len(members) != 2 {
		t.Errorf("Expected 2 members, got %d", len(members))
	}
}

// TestDeleteGroup tests DELETE /groups/{group}.
func TestDeleteGroup(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic and join group
	doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: "orders", NumPartitions: 2})

	joinReq := JoinGroupRequest{
		ClientID: "consumer-1",
		Topics:   []string{"orders"},
	}
	doRequest(server, http.MethodPost, "/groups/order-processors/join", joinReq)

	// Delete group
	rec := doRequest(server, http.MethodDelete, "/groups/order-processors", nil)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify it's gone
	rec = doRequest(server, http.MethodGet, "/groups/order-processors", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected group to be deleted, got status %d", rec.Code)
	}
}

// ============================================================================
// SERVER LIFECYCLE TESTS
// ============================================================================

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
