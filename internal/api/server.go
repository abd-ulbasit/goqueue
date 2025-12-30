// =============================================================================
// HTTP API SERVER - REST INTERFACE FOR GOQUEUE
// =============================================================================
//
// WHAT IS THIS?
// This module provides a RESTful HTTP API for interacting with goqueue.
// It allows any HTTP client to:
//   - Manage topics (create, delete, list)
//   - Publish messages (single or batch)
//   - Consume messages (pull-based)
//   - Query broker status
//   - Manage consumer groups (M3)
//   - Commit offsets (M3)
//
// WHY CHI ROUTER?
//
//   Chi is a lightweight, idiomatic Go router that:
//   - Is stdlib net/http compatible
//   - Supports URL parameters (e.g., /topics/{name})
//   - Has middleware support
//   - Zero external dependencies
//
//   COMPARISON:
//   - gorilla/mux: Feature-rich but archived (maintenance mode)
//   - gin: Fast but opinionated, non-stdlib compatible
//   - echo: Full-featured but heavier weight
//   - chi: Perfect balance of features and simplicity
//
// ENDPOINT OVERVIEW:
//
//   TOPICS
//   POST   /topics              Create a new topic
//   GET    /topics              List all topics
//   GET    /topics/{name}       Get topic details
//   DELETE /topics/{name}       Delete a topic
//
//   MESSAGES
//   POST   /topics/{name}/messages                       Publish message(s)
//   GET    /topics/{name}/partitions/{id}/messages       Consume (simple)
//
//   CONSUMER GROUPS (M3)
//   POST   /groups/{group}/join                          Join consumer group
//   POST   /groups/{group}/heartbeat                     Send heartbeat
//   POST   /groups/{group}/leave                         Leave group
//   GET    /groups/{group}/poll                          Long-poll for messages
//   POST   /groups/{group}/offsets                       Commit offsets
//   GET    /groups/{group}/offsets                       Get committed offsets
//   GET    /groups                                       List all groups
//   GET    /groups/{group}                               Get group details
//   DELETE /groups/{group}                               Delete group
//
//   ADMIN
//   GET    /health              Health check
//   GET    /stats               Broker statistics
//
// =============================================================================

package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"goqueue/internal/broker"
)

// =============================================================================
// API SERVER
// =============================================================================

// Server is the HTTP API server for goqueue.
type Server struct {
	broker     *broker.Broker
	httpServer *http.Server
	router     *chi.Mux
	logger     *slog.Logger
	wg         sync.WaitGroup
}

// ServerConfig holds API server configuration.
type ServerConfig struct {
	Addr         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// DefaultServerConfig returns sensible defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Addr:         ":8080",
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
}

// NewServer creates a new API server.
func NewServer(b *broker.Broker, config ServerConfig) *Server {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	r := chi.NewRouter()

	s := &Server{
		broker: b,
		router: r,
		logger: logger,
	}

	// Set up middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(s.loggingMiddleware)
	r.Use(middleware.Recoverer)

	// Register routes
	s.registerRoutes()

	s.httpServer = &http.Server{
		Addr:         config.Addr,
		Handler:      r,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		IdleTimeout:  config.IdleTimeout,
	}

	return s
}

// registerRoutes sets up all API endpoints using chi router.
func (s *Server) registerRoutes() {
	// Health & Stats
	s.router.Get("/health", s.handleHealth)
	s.router.Get("/stats", s.handleStats)

	// Topics
	s.router.Route("/topics", func(r chi.Router) {
		r.Post("/", s.createTopic)
		r.Get("/", s.listTopics)

		r.Route("/{topicName}", func(r chi.Router) {
			r.Get("/", s.getTopic)
			r.Delete("/", s.deleteTopic)

			// Messages
			r.Post("/messages", s.publishMessages)

			// Partitions
			r.Route("/partitions/{partitionID}", func(r chi.Router) {
				r.Get("/messages", s.consumeMessages)
			})
		})
	})

	// Consumer Groups (M3)
	s.router.Route("/groups", func(r chi.Router) {
		r.Get("/", s.listGroups)

		r.Route("/{groupID}", func(r chi.Router) {
			r.Get("/", s.getGroup)
			r.Delete("/", s.deleteGroup)

			// Membership
			r.Post("/join", s.joinGroup)
			r.Post("/heartbeat", s.heartbeat)
			r.Post("/leave", s.leaveGroup)

			// Messages (long-poll)
			r.Get("/poll", s.pollMessages)

			// Offsets
			r.Post("/offsets", s.commitOffsets)
			r.Get("/offsets", s.getOffsets)
		})
	})
}

// loggingMiddleware logs all HTTP requests.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWrapper{ResponseWriter: w, status: 200}
		next.ServeHTTP(wrapped, r)
		s.logger.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.status,
			"duration", time.Since(start).String(),
		)
	})
}

type responseWrapper struct {
	http.ResponseWriter
	status int
}

func (rw *responseWrapper) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// =============================================================================
// SERVER LIFECYCLE
// =============================================================================

// Start begins listening for HTTP requests (non-blocking).
func (s *Server) Start() error {
	s.logger.Info("starting HTTP API server", "addr", s.httpServer.Addr)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			s.logger.Error("HTTP server error", "error", err)
		}
	}()
	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("shutting down HTTP API server")
	return s.httpServer.Shutdown(ctx)
}

// ListenAndServe starts the server and blocks until shutdown.
func (s *Server) ListenAndServe() error {
	s.logger.Info("starting HTTP API server", "addr", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

// =============================================================================
// HEALTH & STATS HANDLERS
// =============================================================================

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "ok",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.Stats()
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"node_id":          stats.NodeID,
		"uptime":           stats.Uptime.String(),
		"topics":           stats.TopicCount,
		"total_size_bytes": stats.TotalSize,
		"topic_stats":      stats.TopicStats,
	})
}

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
// MESSAGE HANDLERS
// =============================================================================

// PublishRequest is the request body for publishing messages.
type PublishRequest struct {
	Messages []PublishMessage `json:"messages"`
}

// PublishMessage is a single message to publish.
type PublishMessage struct {
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
	Partition *int   `json:"partition,omitempty"`
}

// PublishResult is the result of publishing a single message.
type PublishResult struct {
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
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

	topic, err := s.broker.GetTopic(topicName)
	if err != nil {
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

		if msg.Partition != nil {
			offset, err = topic.PublishToPartition(*msg.Partition, key, value)
			partition = *msg.Partition
		} else {
			partition, offset, err = topic.Publish(key, value)
		}

		results[i] = PublishResult{
			Partition: partition,
			Offset:    offset,
		}
		if err != nil {
			results[i].Error = err.Error()
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"results": results,
	})
}

// ConsumeResponse is the response for consume requests.
type ConsumeResponse struct {
	Messages   []ConsumeMessage `json:"messages"`
	NextOffset int64            `json:"next_offset"`
}

// ConsumeMessage is a consumed message.
type ConsumeMessage struct {
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
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
		}
		if msg.Offset >= response.NextOffset {
			response.NextOffset = msg.Offset + 1
		}
	}

	s.writeJSON(w, http.StatusOK, response)
}

// =============================================================================
// CONSUMER GROUP HANDLERS (M3)
// =============================================================================

// JoinGroupRequest is the request body for joining a consumer group.
//
// EXAMPLE:
//
//	{
//	  "client_id": "order-processor-1",
//	  "topics": ["orders"]
//	}
type JoinGroupRequest struct {
	ClientID string   `json:"client_id"`
	Topics   []string `json:"topics"`
}

// JoinGroupResponse is the response after joining a consumer group.
//
// EXAMPLE:
//
//	{
//	  "member_id": "order-processor-1-abc123",
//	  "generation": 5,
//	  "leader_id": "order-processor-1-abc123",
//	  "partitions": [0, 1, 2],
//	  "members": ["order-processor-1-abc123", "order-processor-2-def456"]
//	}
type JoinGroupResponse struct {
	MemberID   string   `json:"member_id"`
	Generation int      `json:"generation"`
	LeaderID   string   `json:"leader_id"`
	Partitions []int    `json:"partitions"`
	Members    []string `json:"members"`
}

func (s *Server) joinGroup(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	var req JoinGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.ClientID == "" {
		s.errorResponse(w, http.StatusBadRequest, "client_id is required")
		return
	}
	if len(req.Topics) == 0 {
		s.errorResponse(w, http.StatusBadRequest, "at least one topic is required")
		return
	}

	// Verify topics exist
	for _, topic := range req.Topics {
		if !s.broker.TopicExists(topic) {
			s.errorResponse(w, http.StatusNotFound, "topic not found: "+topic)
			return
		}
	}

	coordinator := s.broker.GroupCoordinator()
	result, err := coordinator.JoinGroup(groupID, req.ClientID, req.Topics)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "failed to join group: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, JoinGroupResponse{
		MemberID:   result.MemberID,
		Generation: result.Generation,
		LeaderID:   result.LeaderID,
		Partitions: result.Partitions,
		Members:    result.Members,
	})
}

// HeartbeatRequest is the request body for sending a heartbeat.
type HeartbeatRequest struct {
	MemberID   string `json:"member_id"`
	Generation int    `json:"generation"`
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	var req HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.MemberID == "" {
		s.errorResponse(w, http.StatusBadRequest, "member_id is required")
		return
	}

	coordinator := s.broker.GroupCoordinator()
	if err := coordinator.Heartbeat(groupID, req.MemberID, req.Generation); err != nil {
		switch err {
		case broker.ErrGroupNotFound:
			s.errorResponse(w, http.StatusNotFound, "group not found")
		case broker.ErrMemberNotFound:
			s.errorResponse(w, http.StatusNotFound, "member not found (may have been evicted)")
		case broker.ErrStaleGeneration:
			s.errorResponse(w, http.StatusConflict, "stale generation (rebalance occurred)")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "ok",
	})
}

// LeaveGroupRequest is the request body for leaving a consumer group.
type LeaveGroupRequest struct {
	MemberID string `json:"member_id"`
}

func (s *Server) leaveGroup(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	var req LeaveGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.MemberID == "" {
		s.errorResponse(w, http.StatusBadRequest, "member_id is required")
		return
	}

	coordinator := s.broker.GroupCoordinator()
	if err := coordinator.LeaveGroup(groupID, req.MemberID); err != nil {
		switch err {
		case broker.ErrGroupNotFound:
			s.errorResponse(w, http.StatusNotFound, "group not found")
		case broker.ErrMemberNotFound:
			s.errorResponse(w, http.StatusNotFound, "member not found")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "left",
	})
}

// =============================================================================
// LONG-POLL MESSAGE CONSUMPTION (M3)
// =============================================================================

// PollResponse is the response containing messages for assigned partitions.
type PollResponse struct {
	Messages   map[int][]ConsumeMessage `json:"messages"`    // partition -> messages
	NextOffset map[int]int64            `json:"next_offset"` // partition -> next offset
}

// pollMessages implements long-polling for consumer group members.
//
// LONG-POLLING:
// Instead of returning immediately (which wastes resources if no messages),
// the server holds the request open until:
//   - Messages are available
//   - Timeout is reached (default 30 seconds)
//
// FLOW:
//  1. Validate member is in group with correct generation
//  2. Get member's assigned partitions
//  3. For each partition, get committed offset and read messages
//  4. If no messages, wait (with short polling intervals) until timeout
//  5. Return messages grouped by partition
//
// COMPARISON:
//   - Kafka: Uses fetch request with maxWaitMs parameter
//   - SQS: ReceiveMessage with WaitTimeSeconds
//   - goqueue: timeout query parameter (default 30s)
func (s *Server) pollMessages(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	// Get query parameters
	memberID := r.URL.Query().Get("member_id")
	generationStr := r.URL.Query().Get("generation")
	timeoutStr := r.URL.Query().Get("timeout")
	limitStr := r.URL.Query().Get("limit")

	if memberID == "" {
		s.errorResponse(w, http.StatusBadRequest, "member_id query param is required")
		return
	}

	generation := 0
	if generationStr != "" {
		var err error
		generation, err = strconv.Atoi(generationStr)
		if err != nil {
			s.errorResponse(w, http.StatusBadRequest, "invalid generation")
			return
		}
	}

	// Default 30 second timeout for long-polling
	timeout := 30 * time.Second
	if timeoutStr != "" {
		parsed, err := time.ParseDuration(timeoutStr)
		if err != nil {
			s.errorResponse(w, http.StatusBadRequest, "invalid timeout format (use Go duration like 30s)")
			return
		}
		timeout = parsed
		if timeout > 60*time.Second {
			timeout = 60 * time.Second // Cap at 60 seconds
		}
	}

	limit := 100
	if limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			s.errorResponse(w, http.StatusBadRequest, "invalid limit")
			return
		}
		if limit > 1000 {
			limit = 1000
		}
	}

	coordinator := s.broker.GroupCoordinator()

	// Get group and validate member
	group, err := coordinator.GetGroup(groupID)
	if err != nil {
		s.errorResponse(w, http.StatusNotFound, "group not found")
		return
	}

	partitions, groupGen, err := group.GetAssignment(memberID)
	if err != nil {
		s.errorResponse(w, http.StatusNotFound, "member not found (may have been evicted)")
		return
	}

	// Check generation
	if generation != 0 && generation != groupGen {
		s.errorResponse(w, http.StatusConflict, "stale generation (rebalance occurred)")
		return
	}

	// Get the topic (for M3, we assume single topic per group)
	if len(group.Topics) == 0 {
		s.errorResponse(w, http.StatusInternalServerError, "group has no subscribed topics")
		return
	}
	topicName := group.Topics[0]

	// Long-polling loop
	deadline := time.Now().Add(timeout)
	pollInterval := 100 * time.Millisecond // How often to check for new messages

	for {
		response := PollResponse{
			Messages:   make(map[int][]ConsumeMessage),
			NextOffset: make(map[int]int64),
		}
		totalMessages := 0

		// Fetch messages from each assigned partition
		for _, partition := range partitions {
			// Get committed offset (start from there) or start from 0
			offset, err := coordinator.GetOffset(groupID, topicName, partition)
			if err != nil {
				offset = 0 // No committed offset, start from beginning
			}

			messages, err := s.broker.Consume(topicName, partition, offset, limit)
			if err != nil {
				continue // Skip partition on error
			}

			if len(messages) > 0 {
				partitionMessages := make([]ConsumeMessage, len(messages))
				nextOffset := offset

				for i, msg := range messages {
					partitionMessages[i] = ConsumeMessage{
						Offset:    msg.Offset,
						Timestamp: msg.Timestamp.Format(time.RFC3339Nano),
						Key:       string(msg.Key),
						Value:     string(msg.Value),
					}
					if msg.Offset >= nextOffset {
						nextOffset = msg.Offset + 1
					}
				}

				response.Messages[partition] = partitionMessages
				response.NextOffset[partition] = nextOffset
				totalMessages += len(messages)
			}
		}

		// If we have messages, return immediately
		if totalMessages > 0 {
			s.writeJSON(w, http.StatusOK, response)
			return
		}

		// Check if we've exceeded the timeout
		if time.Now().After(deadline) {
			// Return empty response (no messages)
			s.writeJSON(w, http.StatusOK, response)
			return
		}

		// Wait a bit before checking again
		time.Sleep(pollInterval)
	}
}

// =============================================================================
// OFFSET HANDLERS (M3)
// =============================================================================

// CommitOffsetsRequest is the request body for committing offsets.
//
// EXAMPLE:
//
//	{
//	  "member_id": "order-processor-1-abc123",
//	  "generation": 5,
//	  "offsets": {
//	    "orders": {
//	      "0": 150,
//	      "1": 89
//	    }
//	  }
//	}
type CommitOffsetsRequest struct {
	MemberID   string                      `json:"member_id"`
	Generation int                         `json:"generation"`
	Offsets    map[string]map[string]int64 `json:"offsets"` // topic -> partition -> offset
}

func (s *Server) commitOffsets(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	var req CommitOffsetsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.MemberID == "" {
		s.errorResponse(w, http.StatusBadRequest, "member_id is required")
		return
	}
	if len(req.Offsets) == 0 {
		s.errorResponse(w, http.StatusBadRequest, "offsets are required")
		return
	}

	// Convert string partition keys to int
	offsets := make(map[string]map[int]int64)
	for topic, partitions := range req.Offsets {
		offsets[topic] = make(map[int]int64)
		for partStr, offset := range partitions {
			partInt, err := strconv.Atoi(partStr)
			if err != nil {
				s.errorResponse(w, http.StatusBadRequest, "invalid partition ID: "+partStr)
				return
			}
			offsets[topic][partInt] = offset
		}
	}

	coordinator := s.broker.GroupCoordinator()
	if err := coordinator.CommitOffsets(groupID, offsets, req.MemberID); err != nil {
		switch err {
		case broker.ErrGroupNotFound:
			s.errorResponse(w, http.StatusNotFound, "group not found")
		case broker.ErrNotAssigned:
			s.errorResponse(w, http.StatusForbidden, "partition not assigned to this member")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "committed",
		"group":   groupID,
		"offsets": req.Offsets,
	})
}

func (s *Server) getOffsets(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	coordinator := s.broker.GroupCoordinator()
	groupOffsets, err := coordinator.GetGroupOffsets(groupID)
	if err != nil {
		if err == broker.ErrOffsetNotFound {
			// No offsets committed yet
			s.writeJSON(w, http.StatusOK, map[string]interface{}{
				"group_id": groupID,
				"topics":   map[string]interface{}{},
			})
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Convert to JSON-friendly format (string partition keys)
	topics := make(map[string]map[string]int64)
	for topicName, topicOffsets := range groupOffsets.Topics {
		topics[topicName] = make(map[string]int64)
		for partID, partOffset := range topicOffsets.Partitions {
			topics[topicName][strconv.Itoa(partID)] = partOffset.Offset
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"group_id":   groupOffsets.GroupID,
		"topics":     topics,
		"generation": groupOffsets.Generation,
		"updated_at": groupOffsets.UpdatedAt.Format(time.RFC3339),
	})
}

// =============================================================================
// GROUP MANAGEMENT HANDLERS
// =============================================================================

func (s *Server) listGroups(w http.ResponseWriter, r *http.Request) {
	coordinator := s.broker.GroupCoordinator()
	groups := coordinator.GetAllGroupsInfo()

	// Simplify for list view
	groupList := make([]map[string]interface{}, len(groups))
	for i, g := range groups {
		groupList[i] = map[string]interface{}{
			"id":         g.ID,
			"state":      g.State,
			"members":    len(g.Members),
			"generation": g.Generation,
			"topics":     g.Topics,
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"groups": groupList,
	})
}

func (s *Server) getGroup(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	coordinator := s.broker.GroupCoordinator()
	info, err := coordinator.GetGroupInfo(groupID)
	if err != nil {
		if err == broker.ErrGroupNotFound {
			s.errorResponse(w, http.StatusNotFound, "group not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Convert members to JSON-friendly format
	members := make([]map[string]interface{}, len(info.Members))
	for i, m := range info.Members {
		members[i] = map[string]interface{}{
			"id":                  m.ID,
			"client_id":           m.ClientID,
			"assigned_partitions": m.AssignedPartitions,
			"last_heartbeat":      m.LastHeartbeat.Format(time.RFC3339),
			"joined_at":           m.JoinedAt.Format(time.RFC3339),
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":         info.ID,
		"state":      info.State,
		"generation": info.Generation,
		"topics":     info.Topics,
		"members":    members,
		"created_at": info.CreatedAt.Format(time.RFC3339),
	})
}

func (s *Server) deleteGroup(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	coordinator := s.broker.GroupCoordinator()
	if err := coordinator.DeleteGroup(groupID); err != nil {
		if err == broker.ErrGroupNotFound {
			s.errorResponse(w, http.StatusNotFound, "group not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"deleted": true,
		"group":   groupID,
	})
}

// =============================================================================
// RESPONSE HELPERS
// =============================================================================

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (s *Server) errorResponse(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]interface{}{
		"error":  message,
		"status": status,
	})
}
