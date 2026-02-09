package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"goqueue/internal/broker"
	"goqueue/internal/storage"
)

// =============================================================================
// TOPIC HANDLERS
// =============================================================================

// CreateTopicRequest is the request body for topic creation.
type CreateTopicRequest struct {
	Name           string `json:"name"`
	NumPartitions  int    `json:"num_partitions,omitempty"`
	RetentionHours int    `json:"retention_hours,omitempty"`
}

func (s *Server) createTopic(w http.ResponseWriter, r *http.Request) {
	var req CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.Name == "" {
		s.errorResponse(w, http.StatusBadRequest, "name is required")
		return
	}

	if req.NumPartitions <= 0 {
		req.NumPartitions = 3
	}
	if req.RetentionHours <= 0 {
		req.RetentionHours = 168
	}

	config := broker.TopicConfig{
		Name:           req.Name,
		NumPartitions:  req.NumPartitions,
		RetentionHours: req.RetentionHours,
	}

	if err := s.broker.CreateTopic(config); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			s.errorResponse(w, http.StatusConflict, "topic already exists")
			return
		}
		// Handle not-controller error with 503 Service Unavailable + Retry-After
		// This tells the client to retry the request which may hit the controller
		if errors.Is(err, broker.ErrNotController) {
			w.Header().Set("Retry-After", "1")
			s.errorResponse(w, http.StatusServiceUnavailable, "not the controller node, please retry")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to create topic: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusCreated, map[string]interface{}{
		"name":       req.Name,
		"partitions": req.NumPartitions,
		"created":    true,
	})
}

func (s *Server) listTopics(w http.ResponseWriter, r *http.Request) {
	topics := s.broker.ListTopics()
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"topics": topics,
	})
}

func (s *Server) getTopic(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")

	topic, err := s.broker.GetTopic(topicName)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "topic not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	partitionOffsets := make(map[string]map[string]int64)
	for i := 0; i < topic.NumPartitions(); i++ {
		partition, _ := topic.Partition(i)
		partitionOffsets[strconv.Itoa(i)] = map[string]int64{
			"earliest": partition.EarliestOffset(),
			"latest":   partition.LatestOffset(),
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":              topic.Name(),
		"partitions":        topic.NumPartitions(),
		"total_messages":    topic.TotalMessages(),
		"total_size_bytes":  topic.TotalSize(),
		"partition_offsets": partitionOffsets,
	})
}

func (s *Server) deleteTopic(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")

	if err := s.broker.DeleteTopic(topicName); err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "topic not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"deleted": true,
		"name":    topicName,
	})
}

// =============================================================================
// PARTITION INFO HANDLER
// =============================================================================
//
// WHY: Operators and clients need to see partition leadership assignments.
// Essential for:
//   - Debugging: "Why is my message going to the wrong node?"
//   - Monitoring: "Are all partitions in sync?"
//   - Optimization: "Can I connect directly to the leader?"
//
// RESPONSE FORMAT:
//
//	{
//	  "topic": "orders",
//	  "partitions": [
//	    {
//	      "partition": 0,
//	      "leader": "node-0",
//	      "replicas": ["node-0", "node-1", "node-2"],
//	      "isr": ["node-0", "node-1"]  // node-2 is behind
//	    },
//	    ...
//	  ]
//	}
//
// ISR EXPLANATION:
//   ISR (In-Sync Replicas) = replicas that have caught up to the leader.
//   If ISR < replicas, some nodes are lagging (network, disk, overload).
//   If ISR is empty (except leader), data loss risk if leader fails.
//
// COMPARISON:
//   - Kafka: kafka-topics.sh --describe shows Leader/Replicas/ISR
//   - RabbitMQ: rabbitmqctl list_queues shows mirror status
//   - goqueue: GET /topics/{name}/partitions
//
// =============================================================================

func (s *Server) getTopicPartitions(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")

	// Get partition info from broker
	partitions := s.broker.GetTopicPartitions(topicName)
	if len(partitions) == 0 {
		// Topic doesn't exist or no cluster metadata
		s.errorResponse(w, http.StatusNotFound, "topic not found or no partition info available")
		return
	}

	// Convert to response format
	partitionInfos := make([]map[string]interface{}, len(partitions))
	for i, p := range partitions {
		partitionInfos[i] = map[string]interface{}{
			"partition": p.Partition,
			"leader":    p.Leader,
			"replicas":  p.Replicas,
			"isr":       p.ISR,
			"version":   p.Version,
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"topic":      topicName,
		"partitions": partitionInfos,
	})
}

// =============================================================================
// MESSAGE HANDLERS
// =============================================================================

// PublishRequest is the request body for publishing messages.
type PublishRequest struct {
	Messages []PublishMessage `json:"messages"`
}

// PublishMessage is a single message to publish.
//
// DELAY SUPPORT (M5):
// Messages can be published with a delay using either:
//   - delay: Duration string (e.g., "30s", "1h", "24h")
//   - deliverAt: RFC3339 timestamp (e.g., "2024-01-15T09:30:00Z")
//
// If both are provided, delay takes precedence.
// If neither is provided, message is delivered immediately.
//
// PRIORITY SUPPORT (M6):
// Messages can have a priority level:
//   - "critical" (0): Highest priority, processed first
//   - "high" (1): Above normal priority
//   - "normal" (2): Default priority
//   - "low" (3): Below normal priority
//   - "background" (4): Lowest priority, processed last
//
// If not provided, defaults to "normal".
//
// PRIORITY + DELAY INTERACTION:
// When a message has both delay and priority:
//  1. Message waits until deliverAt time
//  2. When ready, priority determines ordering among available messages
type PublishMessage struct {
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
	Partition *int   `json:"partition,omitempty"`
	Delay     string `json:"delay,omitempty"`     // M5: Duration string ("30s", "1h")
	DeliverAt string `json:"deliverAt,omitempty"` // M5: RFC3339 timestamp
	Priority  string `json:"priority,omitempty"`  // M6: Priority level (critical/high/normal/low/background)
}

// PublishResult is the result of publishing a single message.
type PublishResult struct {
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Priority  string `json:"priority,omitempty"`  // M6: Priority level applied
	Delayed   bool   `json:"delayed,omitempty"`   // M5: true if message is delayed
	DeliverAt string `json:"deliverAt,omitempty"` // M5: when message will be visible
	Error     string `json:"error,omitempty"`
}

func (s *Server) publishMessages(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")

	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if len(req.Messages) == 0 {
		s.errorResponse(w, http.StatusBadRequest, "at least one message required")
		return
	}

	// Verify topic exists before processing messages
	// All publish operations now go through broker methods which handle
	// cluster forwarding, so we just need to validate the topic exists
	if _, err := s.broker.GetTopic(topicName); err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "topic not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	results := make([]PublishResult, len(req.Messages))

	for i, msg := range req.Messages {
		var key []byte
		if msg.Key != "" {
			key = []byte(msg.Key)
		}
		value := []byte(msg.Value)

		var partition int
		var offset int64
		var deliverAt time.Time
		var isDelayed bool
		var err error

		// Parse priority (M6)
		// Default to Normal if not specified
		priority := storage.PriorityNormal
		if msg.Priority != "" {
			priority = storage.ParsePriority(msg.Priority)
			// Validate that the priority string was recognized
			// ParsePriority defaults to Normal for unrecognized strings,
			// so we check if input wasn't a valid priority name
			validPriorities := map[string]bool{
				"critical": true, "Critical": true, "CRITICAL": true,
				"high": true, "High": true, "HIGH": true,
				"normal": true, "Normal": true, "NORMAL": true,
				"low": true, "Low": true, "LOW": true,
				"background": true, "Background": true, "BACKGROUND": true,
			}
			if !validPriorities[msg.Priority] {
				results[i] = PublishResult{Error: "invalid priority: must be one of critical, high, normal, low, background"}
				continue
			}
		}

		// Parse delay parameters (M5)
		if msg.Delay != "" {
			delay, parseErr := time.ParseDuration(msg.Delay)
			if parseErr != nil {
				results[i] = PublishResult{Error: "invalid delay format: " + parseErr.Error()}
				continue
			}
			deliverAt = time.Now().Add(delay)
			isDelayed = delay > 0
		} else if msg.DeliverAt != "" {
			var parseErr error
			deliverAt, parseErr = time.Parse(time.RFC3339, msg.DeliverAt)
			if parseErr != nil {
				results[i] = PublishResult{Error: "invalid deliverAt format: " + parseErr.Error()}
				continue
			}
			isDelayed = deliverAt.After(time.Now())
		}

		// Publish with or without delay/priority
		// NOTE: Delayed messages with priority will be tracked in the delay index
		// and the priority will be honored when the message becomes visible.
		switch {
		case isDelayed:
			// Delayed message with priority (M5+M6 integration)
			partition, offset, err = s.broker.PublishAtWithPriority(topicName, key, value, deliverAt, priority)
			results[i] = PublishResult{
				Partition: partition,
				Offset:    offset,
				Priority:  priority.String(),
				Delayed:   true,
				DeliverAt: deliverAt.Format(time.RFC3339),
			}
		case msg.Partition != nil:
			// Direct partition publish with priority
			// Uses broker.PublishToPartitionWithPriority which handles cluster forwarding
			offset, err = s.broker.PublishToPartitionWithPriority(topicName, *msg.Partition, key, value, priority)
			partition = *msg.Partition
			results[i] = PublishResult{
				Partition: partition,
				Offset:    offset,
				Priority:  priority.String(),
			}
		default:
			// Normal publish with priority (key-based routing)
			// Uses broker.PublishWithPriority which handles cluster forwarding
			partition, offset, err = s.broker.PublishWithPriority(topicName, key, value, priority)
			results[i] = PublishResult{
				Partition: partition,
				Offset:    offset,
				Priority:  priority.String(),
			}
		}

		if err != nil {
			results[i].Error = err.Error()
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"results": results,
	})
}

// =============================================================================
// INTERNAL: FORWARDED PUBLISH HANDLER
// =============================================================================
//
// WHY: In cluster mode, only partition leaders can write messages.
// Non-leaders forward publish requests here.
//
// PROTOCOL:
//   Request:  ForwardPublishRequest {Key, Value []byte, Partition int}
//   Response: ForwardPublishResponse {Partition, Offset int64, Error string}
//
// VALIDATION:
//   - We MUST be the leader for this partition (no re-forwarding)
//   - If not leader, return error (indicates stale routing)
//
// FLOW:
//   Non-Leader Node ──POST /messages/forward──► This Handler
//                                                    │
//                                   ┌────────────────┴────────────────┐
//                                   │  1. Validate we're leader       │
//                                   │  2. Write to partition          │
//                                   │  3. Wait for ISR replication    │
//                                   │  4. Return offset                │
//                                   └─────────────────────────────────┘
//
// =============================================================================

// ForwardPublishRequest is the request body for forwarded publishes.
// Matches broker.ForwardPublishRequest for easy serialization.
type ForwardPublishRequest struct {
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
	Partition int    `json:"partition"`
}

// ForwardPublishResponse is the response for forwarded publishes.
// Matches broker.ForwardPublishResponse for easy serialization.
type ForwardPublishResponse struct {
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Error     string `json:"error,omitempty"`
}

// forwardPublishHandler handles forwarded publish requests from non-leader nodes.
func (s *Server) forwardPublishHandler(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")

	var req ForwardPublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		resp := ForwardPublishResponse{Error: "invalid JSON: " + err.Error()}
		s.writeJSON(w, http.StatusBadRequest, resp)
		return
	}

	// Get the topic
	topic, err := s.broker.GetTopic(topicName)
	if err != nil {
		resp := ForwardPublishResponse{Error: "topic not found: " + topicName}
		s.writeJSON(w, http.StatusNotFound, resp)
		return
	}

	// Write directly to the specified partition (no re-routing)
	// This bypasses PublishWithTrace's leadership check since we ARE the leader
	offset, err := topic.PublishToPartition(req.Partition, req.Key, req.Value)
	if err != nil {
		resp := ForwardPublishResponse{Error: "publish failed: " + err.Error()}
		s.writeJSON(w, http.StatusInternalServerError, resp)
		return
	}

	// Wait for ISR replication (same as regular publish)
	if s.broker.GetReplicationCoordinator() != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		if err := s.broker.GetReplicationCoordinator().WaitForReplication(ctx, topicName, req.Partition, offset); err != nil {
			// Log but don't fail - message is durable on leader
			s.logger.Warn("replication wait failed for forwarded publish",
				"topic", topicName,
				"partition", req.Partition,
				"offset", offset,
				"error", err)
		}
	}

	resp := ForwardPublishResponse{
		Partition: req.Partition,
		Offset:    offset,
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// ConsumeResponse is the response for consume requests.
type ConsumeResponse struct {
	Messages   []ConsumeMessage `json:"messages"`
	NextOffset int64            `json:"next_offset"`
}

// ConsumeMessage is a consumed message.
//
// PRIORITY (M6):
// The priority field indicates the message's priority level.
// This is useful for:
//   - Client-side priority handling
//   - Debugging priority distribution
//   - Metrics and monitoring
//
// RECEIPT HANDLE (M4):
// When messages are returned via consumer group polling (/groups/{group}/poll),
// each message includes a receipt_handle for per-message ACK/NACK/REJECT.
// The receipt_handle is only present when using the reliability layer.
type ConsumeMessage struct {
	Offset        int64  `json:"offset"`
	Timestamp     string `json:"timestamp"`
	Key           string `json:"key,omitempty"`
	Value         string `json:"value"`
	Priority      string `json:"priority,omitempty"`       // M6: Priority level (critical/high/normal/low/background)
	ReceiptHandle string `json:"receipt_handle,omitempty"` // M4: For per-message ACK (only present in /groups/{group}/poll)
}

func (s *Server) consumeMessages(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")
	partitionIDStr := chi.URLParam(r, "partitionID")

	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid partition ID")
		return
	}

	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	offset := int64(0)
	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err != nil || offset < 0 {
			s.errorResponse(w, http.StatusBadRequest, "invalid offset")
			return
		}
	}

	limit := 100
	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			s.errorResponse(w, http.StatusBadRequest, "invalid limit")
			return
		}
		if limit > 1000 {
			limit = 1000
		}
	}

	messages, err := s.broker.Consume(topicName, partitionID, offset, limit)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := ConsumeResponse{
		Messages:   make([]ConsumeMessage, len(messages)),
		NextOffset: offset,
	}

	for i, msg := range messages {
		response.Messages[i] = ConsumeMessage{
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp.Format(time.RFC3339Nano),
			Key:       string(msg.Key),
			Value:     string(msg.Value),
			Priority:  msg.Priority.String(),
		}
		if msg.Offset >= response.NextOffset {
			response.NextOffset = msg.Offset + 1
		}
	}

	s.writeJSON(w, http.StatusOK, response)
}
