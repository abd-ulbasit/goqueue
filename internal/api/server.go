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
//
// WHY HTTP?
//
//   For M2, we start with HTTP for simplicity and debuggability.
//   gRPC with streaming comes in M14 for production workloads.
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
//   POST   /topics/{name}/messages                Publish message(s)
//   GET    /topics/{name}/partitions/{id}/messages?offset=0&limit=10  Consume
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

	"goqueue/internal/broker"
)

// =============================================================================
// API SERVER
// =============================================================================

// Server is the HTTP API server for goqueue.
type Server struct {
	broker     *broker.Broker
	httpServer *http.Server
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

	s := &Server{
		broker: b,
		logger: logger,
	}

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	s.httpServer = &http.Server{
		Addr:         config.Addr,
		Handler:      s.loggingMiddleware(mux),
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		IdleTimeout:  config.IdleTimeout,
	}

	return s
}

// registerRoutes sets up all API endpoints.
func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/topics", s.handleTopics)
	mux.HandleFunc("/topics/", s.handleTopicOperations)
}

// loggingMiddleware logs all requests.
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
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w, "GET")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "ok",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w, "GET")
		return
	}
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

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.createTopic(w, r)
	case http.MethodGet:
		s.listTopics(w, r)
	default:
		s.methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handleTopicOperations(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/topics/")
	parts := strings.Split(path, "/")

	if len(parts) == 0 || parts[0] == "" {
		s.errorResponse(w, http.StatusBadRequest, "topic name required")
		return
	}

	topicName := parts[0]

	// /topics/{name}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			s.getTopic(w, r, topicName)
		case http.MethodDelete:
			s.deleteTopic(w, r, topicName)
		default:
			s.methodNotAllowed(w, "GET, DELETE")
		}
		return
	}

	// /topics/{name}/messages
	if len(parts) == 2 && parts[1] == "messages" {
		if r.Method != http.MethodPost {
			s.methodNotAllowed(w, "POST")
			return
		}
		s.publishMessages(w, r, topicName)
		return
	}

	// /topics/{name}/partitions/{id}/messages
	if len(parts) == 4 && parts[1] == "partitions" && parts[3] == "messages" {
		if r.Method != http.MethodGet {
			s.methodNotAllowed(w, "GET")
			return
		}
		partitionID, err := strconv.Atoi(parts[2])
		if err != nil {
			s.errorResponse(w, http.StatusBadRequest, "invalid partition ID")
			return
		}
		s.consumeMessages(w, r, topicName, partitionID)
		return
	}

	s.errorResponse(w, http.StatusNotFound, "endpoint not found")
}

// =============================================================================
// TOPIC CRUD OPERATIONS
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

func (s *Server) getTopic(w http.ResponseWriter, r *http.Request, name string) {
	topic, err := s.broker.GetTopic(name)
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

func (s *Server) deleteTopic(w http.ResponseWriter, r *http.Request, name string) {
	if err := s.broker.DeleteTopic(name); err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "topic not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"deleted": true,
		"name":    name,
	})
}

// =============================================================================
// MESSAGE OPERATIONS
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

func (s *Server) publishMessages(w http.ResponseWriter, r *http.Request, topicName string) {
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

func (s *Server) consumeMessages(w http.ResponseWriter, r *http.Request, topicName string, partitionID int) {
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	offset := int64(0)
	if offsetStr != "" {
		var err error
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err != nil || offset < 0 {
			s.errorResponse(w, http.StatusBadRequest, "invalid offset")
			return
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

func (s *Server) methodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	s.errorResponse(w, http.StatusMethodNotAllowed, "method not allowed")
}
