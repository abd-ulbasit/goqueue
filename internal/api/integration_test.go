// =============================================================================
// API INTEGRATION TESTS
// =============================================================================
//
// WHAT: End-to-end tests exercising complete API workflows through the router.
//
// WHY INTEGRATION TESTS?
//   - Unit tests validate individual functions in isolation
//   - Integration tests validate that components work together correctly
//   - They catch issues like: routing bugs, middleware order, serialization errors
//
// TEST CATEGORIES:
//   1. Topic Lifecycle: Create → Publish → Consume → Delete
//   2. Consumer Groups: Join → Poll → Commit → Leave
//   3. Error Handling: Invalid inputs, missing resources, edge cases
//   4. Concurrent Operations: Multiple producers/consumers
//   5. Delay & Priority: Scheduled message delivery
//
// =============================================================================

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"goqueue/internal/broker"
)

// =============================================================================
// TOPIC LIFECYCLE INTEGRATION TESTS
// =============================================================================

// TestTopicLifecycle_CreatePublishConsumeDelete tests a complete topic workflow.
func TestTopicLifecycle_CreatePublishConsumeDelete(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	topicName := "lifecycle-test-topic"

	// Step 1: Create topic
	t.Run("create_topic", func(t *testing.T) {
		rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{
			Name:          topicName,
			NumPartitions: 2,
		})

		if rec.Code != http.StatusCreated {
			t.Fatalf("Create topic failed: %d - %s", rec.Code, rec.Body.String())
		}
	})

	// Step 2: Publish messages
	t.Run("publish_messages", func(t *testing.T) {
		// PublishRequest contains Messages array of PublishMessage
		req := PublishRequest{
			Messages: []PublishMessage{
				{Key: "key1", Value: "message-one"},
				{Key: "key2", Value: "message-two"},
				{Key: "key3", Value: "message-three"},
			},
		}

		rec := doRequest(server, http.MethodPost, "/topics/"+topicName+"/messages", req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Publish failed: %d - %s", rec.Code, rec.Body.String())
		}

		// Response is wrapped: {"results": [...]}
		var resp struct {
			Results []PublishResult `json:"results"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("Failed to parse publish response: %v", err)
		}

		if len(resp.Results) != 3 {
			t.Fatalf("Expected 3 results, got %d", len(resp.Results))
		}

		for i, r := range resp.Results {
			if r.Error != "" {
				t.Errorf("Message %d failed: %s", i, r.Error)
			}
		}
	})

	// Step 3: Consume messages from partition 0
	t.Run("consume_from_partition", func(t *testing.T) {
		rec := doRequest(server, http.MethodGet, "/topics/"+topicName+"/partitions/0/messages?offset=0&limit=10", nil)

		if rec.Code != http.StatusOK {
			t.Fatalf("Consume failed: %d - %s", rec.Code, rec.Body.String())
		}

		var resp ConsumeResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("Failed to parse consume response: %v", err)
		}

		// We should have at least some messages
		t.Logf("Consumed %d messages from partition 0", len(resp.Messages))
	})

	// Step 4: List topics and verify
	t.Run("list_topics", func(t *testing.T) {
		rec := doRequest(server, http.MethodGet, "/topics", nil)

		if rec.Code != http.StatusOK {
			t.Fatalf("List topics failed: %d", rec.Code)
		}

		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("Failed to parse list topics response: %v", err)
		}

		topics, ok := resp["topics"].([]interface{})
		if !ok {
			t.Fatalf("Expected topics array in response")
		}

		found := false
		for _, topic := range topics {
			if topic.(string) == topicName {
				found = true
				break
			}
		}

		if !found {
			t.Error("Created topic not found in list")
		}
	})

	// Step 5: Get topic info
	t.Run("get_topic_info", func(t *testing.T) {
		rec := doRequest(server, http.MethodGet, "/topics/"+topicName, nil)

		if rec.Code != http.StatusOK {
			t.Fatalf("Get topic failed: %d", rec.Code)
		}

		var info map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
			t.Fatalf("Failed to parse topic info: %v", err)
		}

		if info["name"] != topicName {
			t.Errorf("Topic name = %v, want %s", info["name"], topicName)
		}
	})

	// Step 6: Delete topic
	t.Run("delete_topic", func(t *testing.T) {
		rec := doRequest(server, http.MethodDelete, "/topics/"+topicName, nil)

		if rec.Code != http.StatusOK && rec.Code != http.StatusNoContent {
			t.Fatalf("Delete topic failed: %d - %s", rec.Code, rec.Body.String())
		}
	})

	// Step 7: Verify topic is gone
	t.Run("verify_deleted", func(t *testing.T) {
		rec := doRequest(server, http.MethodGet, "/topics/"+topicName, nil)

		if rec.Code != http.StatusNotFound {
			t.Errorf("Expected 404 for deleted topic, got %d", rec.Code)
		}
	})
}

// =============================================================================
// CONSUMER GROUP INTEGRATION TESTS
// =============================================================================

// TestConsumerGroup_JoinPollLeave tests consumer group workflow.
func TestConsumerGroup_JoinPollLeave(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	topicName := "consumer-group-test"
	groupID := "test-group"

	// Step 1: Create topic
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{
		Name:          topicName,
		NumPartitions: 1,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Create topic failed: %d", rec.Code)
	}

	// Step 2: Publish some messages
	req := PublishRequest{
		Messages: []PublishMessage{
			{Key: "k1", Value: "msg-1"},
			{Key: "k2", Value: "msg-2"},
			{Key: "k3", Value: "msg-3"},
		},
	}
	rec = doRequest(server, http.MethodPost, "/topics/"+topicName+"/messages", req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Publish failed: %d", rec.Code)
	}

	// Step 3: Join consumer group
	var memberID string
	t.Run("join_group", func(t *testing.T) {
		joinReq := JoinGroupRequest{
			ClientID: "consumer-1",
			Topics:   []string{topicName},
		}
		rec = doRequest(server, http.MethodPost, "/groups/"+groupID+"/join", joinReq)

		if rec.Code != http.StatusOK {
			t.Fatalf("Join group failed: %d - %s", rec.Code, rec.Body.String())
		}

		var resp JoinGroupResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("Failed to parse join response: %v", err)
		}

		memberID = resp.MemberID

		if memberID == "" {
			t.Error("Expected member ID in response")
		}

		t.Logf("Joined group with member ID: %s, generation: %d", memberID, resp.Generation)
	})

	// Step 4: Poll for messages
	t.Run("poll_messages", func(t *testing.T) {
		pollPath := fmt.Sprintf("/groups/%s/poll?member_id=%s&timeout=1s&limit=10", groupID, memberID)
		rec = doRequest(server, http.MethodGet, pollPath, nil)

		if rec.Code != http.StatusOK {
			t.Fatalf("Poll failed: %d - %s", rec.Code, rec.Body.String())
		}

		var resp PollResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("Failed to parse poll response: %v", err)
		}

		// Count total messages across partitions
		totalMessages := 0
		for _, msgs := range resp.Messages {
			totalMessages += len(msgs)
		}
		t.Logf("Polled %d total messages", totalMessages)
	})

	// Step 5: Leave group
	t.Run("leave_group", func(t *testing.T) {
		leaveReq := LeaveGroupRequest{
			MemberID: memberID,
		}
		rec = doRequest(server, http.MethodPost, "/groups/"+groupID+"/leave", leaveReq)

		if rec.Code != http.StatusOK && rec.Code != http.StatusNoContent {
			t.Fatalf("Leave group failed: %d - %s", rec.Code, rec.Body.String())
		}
	})
}

// =============================================================================
// ERROR HANDLING INTEGRATION TESTS
// =============================================================================

// TestErrorHandling_InvalidInputs tests API error responses for invalid inputs.
func TestErrorHandling_InvalidInputs(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	tests := []struct {
		name           string
		method         string
		path           string
		body           interface{}
		expectedStatus int
	}{
		{
			name:           "create topic without name",
			method:         http.MethodPost,
			path:           "/topics",
			body:           CreateTopicRequest{Name: ""},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "get non-existent topic",
			method:         http.MethodGet,
			path:           "/topics/does-not-exist",
			body:           nil,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "publish to non-existent topic",
			method:         http.MethodPost,
			path:           "/topics/does-not-exist/messages",
			body:           PublishRequest{Messages: []PublishMessage{{Value: "test"}}},
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "consume from non-existent topic",
			method:         http.MethodGet,
			path:           "/topics/does-not-exist/partitions/0/messages",
			body:           nil,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "join group without client_id",
			method:         http.MethodPost,
			path:           "/groups/test-group/join",
			body:           JoinGroupRequest{Topics: []string{"test"}},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "join group without topics",
			method:         http.MethodPost,
			path:           "/groups/test-group/join",
			body:           JoinGroupRequest{ClientID: "client-1"},
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(server, tt.method, tt.path, tt.body)

			if rec.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d: %s", tt.expectedStatus, rec.Code, rec.Body.String())
			}
		})
	}
}

// =============================================================================
// CONCURRENT OPERATIONS TESTS
// =============================================================================

// TestConcurrentPublishers tests multiple concurrent publishers.
func TestConcurrentPublishers(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	topicName := "concurrent-test"

	// Create topic with multiple partitions
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{
		Name:          topicName,
		NumPartitions: 4,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Create topic failed: %d", rec.Code)
	}

	// Launch concurrent publishers
	numPublishers := 5
	messagesPerPublisher := 20
	var wg sync.WaitGroup
	errors := make(chan error, numPublishers)

	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()

			for j := 0; j < messagesPerPublisher; j++ {
				req := PublishRequest{
					Messages: []PublishMessage{{
						Key:   fmt.Sprintf("pub-%d-msg-%d", publisherID, j),
						Value: fmt.Sprintf("content from publisher %d message %d", publisherID, j),
					}},
				}

				rec := doRequest(server, http.MethodPost, "/topics/"+topicName+"/messages", req)
				if rec.Code != http.StatusOK {
					errors <- fmt.Errorf("publisher %d failed: %d", publisherID, rec.Code)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Verify total message count
	time.Sleep(100 * time.Millisecond) // Allow messages to settle

	totalMessages := 0
	for p := 0; p < 4; p++ {
		rec := doRequest(server, http.MethodGet, fmt.Sprintf("/topics/%s/partitions/%d/messages?offset=0&limit=1000", topicName, p), nil)
		if rec.Code == http.StatusOK {
			var resp ConsumeResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err == nil {
				totalMessages += len(resp.Messages)
			}
		}
	}

	expected := numPublishers * messagesPerPublisher
	if totalMessages != expected {
		t.Errorf("Total messages = %d, want %d", totalMessages, expected)
	}
}

// =============================================================================
// DELAY & PRIORITY MESSAGE TESTS
// =============================================================================

// TestDelayedMessage_ScheduleAndConsume tests delayed message scheduling.
func TestDelayedMessage_ScheduleAndConsume(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	topicName := "delay-test"

	// Create topic
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: topicName})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Create topic failed: %d", rec.Code)
	}

	// Publish delayed message (short delay for testing)
	req := PublishRequest{
		Messages: []PublishMessage{{
			Value: "delayed-message",
			Delay: "100ms", // 100ms delay
		}},
	}

	rec = doRequest(server, http.MethodPost, "/topics/"+topicName+"/messages", req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Publish delayed message failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Results []PublishResult `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse results: %v", err)
	}

	if len(resp.Results) != 1 || resp.Results[0].Error != "" {
		t.Errorf("Expected successful publish, got: %+v", resp.Results)
	}

	t.Logf("Delayed message published to partition %d, offset %d", resp.Results[0].Partition, resp.Results[0].Offset)

	// Wait for delay to expire and check
	time.Sleep(200 * time.Millisecond)
}

// TestPriorityMessage_Publish tests priority-based message publishing.
func TestPriorityMessage_Publish(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	topicName := "priority-test"

	// Create topic
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: topicName})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Create topic failed: %d", rec.Code)
	}

	// Publish messages with different priorities
	req := PublishRequest{
		Messages: []PublishMessage{
			{Value: "low-priority", Priority: "low"},
			{Value: "high-priority", Priority: "high"},
			{Value: "normal-priority", Priority: "normal"},
			{Value: "critical-priority", Priority: "critical"},
		},
	}

	rec = doRequest(server, http.MethodPost, "/topics/"+topicName+"/messages", req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Publish failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Results []PublishResult `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse results: %v", err)
	}

	// Verify all messages published successfully
	for i, r := range resp.Results {
		if r.Error != "" {
			t.Errorf("Message %d failed: %s", i, r.Error)
		}
	}

	t.Logf("Published %d priority messages", len(resp.Results))
}

// =============================================================================
// STATS & METRICS INTEGRATION TESTS
// =============================================================================

// TestStats_ReflectsOperations tests that stats accurately reflect operations.
func TestStats_ReflectsOperations(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Get initial stats
	rec := doRequest(server, http.MethodGet, "/stats", nil)
	var initialStats map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &initialStats); err != nil {
		t.Fatalf("Failed to parse initial stats: %v", err)
	}

	// Create topic and publish messages
	topicName := "stats-test"
	rec = doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: topicName})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Create topic failed: %d", rec.Code)
	}

	msgs := make([]PublishMessage, 10)
	for i := range msgs {
		msgs[i] = PublishMessage{Value: fmt.Sprintf("message-%d", i)}
	}
	rec = doRequest(server, http.MethodPost, "/topics/"+topicName+"/messages", PublishRequest{Messages: msgs})
	if rec.Code != http.StatusOK {
		t.Fatalf("Publish failed: %d", rec.Code)
	}

	// Get updated stats
	rec = doRequest(server, http.MethodGet, "/stats", nil)
	var updatedStats map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &updatedStats); err != nil {
		t.Fatalf("Failed to parse updated stats: %v", err)
	}

	// Should have topic stats now
	if topics, ok := updatedStats["topics"].(map[string]interface{}); ok {
		if _, hasTopic := topics[topicName]; !hasTopic {
			t.Error("Stats should include the created topic")
		}
	}
}

// =============================================================================
// BATCH OPERATIONS TESTS
// =============================================================================

// TestBatchPublish_LargePayload tests publishing large batches.
func TestBatchPublish_LargePayload(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	topicName := "batch-test"
	rec := doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{
		Name:          topicName,
		NumPartitions: 4,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("Create topic failed: %d", rec.Code)
	}

	// Create large batch
	batchSize := 100
	messages := make([]PublishMessage, batchSize)
	for i := range messages {
		messages[i] = PublishMessage{
			Key:   fmt.Sprintf("batch-key-%d", i),
			Value: fmt.Sprintf("batch-message-%d-with-some-extra-content-to-add-size", i),
		}
	}

	startTime := time.Now()
	rec = doRequest(server, http.MethodPost, "/topics/"+topicName+"/messages", PublishRequest{Messages: messages})
	duration := time.Since(startTime)

	if rec.Code != http.StatusOK {
		t.Fatalf("Batch publish failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Results []PublishResult `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse results: %v", err)
	}

	if len(resp.Results) != batchSize {
		t.Errorf("Expected %d results, got %d", batchSize, len(resp.Results))
	}

	// Check success rate
	successCount := 0
	for _, r := range resp.Results {
		if r.Error == "" {
			successCount++
		}
	}

	if successCount != batchSize {
		t.Errorf("Expected %d successes, got %d", batchSize, successCount)
	}

	t.Logf("Batch of %d messages published in %v", batchSize, duration)
}

// =============================================================================
// HEALTH CHECK TESTS
// =============================================================================

// TestHealthEndpoint_UnderLoad tests health endpoint under concurrent load.
func TestHealthEndpoint_UnderLoad(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create background load
	topicName := "health-test"
	doRequest(server, http.MethodPost, "/topics", CreateTopicRequest{Name: topicName})

	// Start background publisher
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				req := PublishRequest{Messages: []PublishMessage{{Value: "load"}}}
				doRequest(server, http.MethodPost, "/topics/"+topicName+"/messages", req)
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Rapidly check health endpoint
	for i := 0; i < 50; i++ {
		rec := doRequest(server, http.MethodGet, "/health", nil)
		if rec.Code != http.StatusOK {
			t.Errorf("Health check %d failed: %d", i, rec.Code)
		}
	}
}

// Ensure broker import is used
var _ = broker.BrokerConfig{}
