// TODO: Break this file into smaller files per feature (e.g., topics.go, messages.go, groups.go, etc.)
// for better maintainability.
// TODO: Add unit tests for all handlers and edge cases.

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
//   POST   /topics/{name}/messages                       Publish message(s) with optional priority
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
//   GET    /priority/stats      Priority statistics (M6)
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
	"goqueue/internal/metrics"
	"goqueue/internal/storage"
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
	// ==========================================================================
	// KUBERNETES-READY HEALTH ENDPOINTS
	// ==========================================================================
	//
	// These endpoints follow cloud-native conventions for orchestration systems.
	//
	// ENDPOINT        PURPOSE                      K8S PROBE TYPE
	// ─────────────────────────────────────────────────────────────────────────
	// /health         General health (legacy)      -
	// /healthz        Is process alive?            livenessProbe
	// /readyz         Ready for traffic?           readinessProbe
	// /livez          Startup complete?            startupProbe
	// /version        Build/version info           -
	//
	// ==========================================================================
	s.router.Get("/health", s.handleHealth)
	s.router.Get("/healthz", s.handleHealthz)
	s.router.Get("/readyz", s.handleReadyz)
	s.router.Get("/livez", s.handleLivez)
	s.router.Get("/version", s.handleVersion)
	s.router.Get("/stats", s.handleStats)

	// ==========================================================================
	// PROMETHEUS METRICS ENDPOINT
	// ==========================================================================
	//
	// WHY /metrics?
	// This is the standard endpoint that Prometheus scrapes to collect metrics.
	// All Prometheus-compatible systems expect metrics at this path.
	//
	// HOW IT WORKS:
	//   1. Prometheus server scrapes GET /metrics every N seconds (default 15s)
	//   2. Handler returns all registered metrics in Prometheus text format
	//   3. Prometheus stores time-series data for querying
	//
	// FORMAT EXAMPLE:
	//   # HELP goqueue_broker_messages_published_total Total messages published
	//   # TYPE goqueue_broker_messages_published_total counter
	//   goqueue_broker_messages_published_total{topic="orders"} 12345
	//
	// ==========================================================================
	s.router.Get("/metrics", s.handleMetrics)

	// Topics
	s.router.Route("/topics", func(r chi.Router) {
		r.Post("/", s.createTopic)
		r.Get("/", s.listTopics)

		r.Route("/{topicName}", func(r chi.Router) {
			r.Get("/", s.getTopic)
			r.Delete("/", s.deleteTopic)

			// Messages
			r.Post("/messages", s.publishMessages)

			// ======================================================================
			// DELAYED MESSAGES (M5)
			// ======================================================================
			//
			// These endpoints provide delayed/scheduled message delivery.
			//
			// FLOW:
			//   1. Producer publishes with delay/deliverAt parameter
			//   2. Message is stored immediately but hidden from consumers
			//   3. After delay expires, message becomes visible
			//
			// ENDPOINTS:
			//   GET  /topics/{name}/delayed              List pending delayed messages
			//   GET  /topics/{name}/delayed/{offset}    Get specific delayed message
			//   DELETE /topics/{name}/delayed/{offset}  Cancel delayed message
			//
			// ======================================================================
			r.Get("/delayed", s.listDelayedMessages)
			r.Get("/delayed/{offset}", s.getDelayedMessage)
			r.Delete("/delayed/{partition}/{offset}", s.cancelDelayedMessage)

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

			// Cooperative rebalancing routes (M12)
			s.RegisterCooperativeGroupRoutes(r)
		})
	})

	// Global cooperative routes (M12)
	s.RegisterCooperativeGlobalRoutes(s.router)

	// ==========================================================================
	// MESSAGE ACKNOWLEDGMENT (M4 - RELIABILITY)
	// ==========================================================================
	//
	// These endpoints provide per-message ACK/NACK/REJECT semantics on top of
	// the consumer group polling mechanism.
	//
	// FLOW:
	//   1. Consumer polls via /groups/{group}/poll (returns receipt handles)
	//   2. For each message processed:
	//      - Success → POST /messages/ack
	//      - Transient failure → POST /messages/nack (retry)
	//      - Permanent failure → POST /messages/reject (DLQ)
	//   3. If processing takes too long → POST /messages/visibility (extend)
	//
	// ==========================================================================
	s.router.Route("/messages", func(r chi.Router) {
		r.Post("/ack", s.ackMessage)
		r.Post("/nack", s.nackMessage)
		r.Post("/reject", s.rejectMessage)
		r.Post("/visibility", s.extendVisibility)
	})

	// Reliability Stats (M4)
	s.router.Get("/reliability/stats", s.handleReliabilityStats)

	// Delay Stats (M5)
	s.router.Get("/delay/stats", s.handleDelayStats)

	// Priority Stats (M6)
	s.router.Get("/priority/stats", s.handlePriorityStats)

	// ==========================================================================
	// TRACING API (M7)
	// ==========================================================================
	//
	// These endpoints provide message tracing and observability.
	//
	// ENDPOINTS:
	//   GET /traces              List recent traces
	//   GET /traces/{traceID}    Get specific trace by ID
	//   GET /traces/search       Search traces by topic, partition, time range
	//   GET /traces/stats        Get tracer statistics
	//
	// QUERY PARAMETERS (for /traces):
	//   limit: Max number of traces to return (default: 100)
	//
	// QUERY PARAMETERS (for /traces/search):
	//   topic: Filter by topic name
	//   partition: Filter by partition number (-1 for all)
	//   consumer_group: Filter by consumer group
	//   start: Start time (RFC3339)
	//   end: End time (RFC3339)
	//   status: Filter by status (completed, error, pending)
	//   limit: Max number of traces to return
	//
	// ==========================================================================
	s.router.Route("/traces", func(r chi.Router) {
		r.Get("/", s.listTraces)
		r.Get("/search", s.searchTraces)
		r.Get("/stats", s.handleTracerStats)
		r.Get("/{traceID}", s.getTrace)
	})

	// ==========================================================================
	// SCHEMA REGISTRY API (M8)
	// ==========================================================================
	//
	// These endpoints provide schema management and validation.
	//
	// SCHEMA MANAGEMENT:
	//   POST   /schemas/subjects/{subject}/versions     Register new schema
	//   GET    /schemas/subjects/{subject}/versions     List all versions
	//   GET    /schemas/subjects/{subject}/versions/{version}  Get specific version
	//   GET    /schemas/subjects/{subject}/versions/latest     Get latest version
	//   DELETE /schemas/subjects/{subject}/versions/{version}  Delete version
	//   DELETE /schemas/subjects/{subject}              Delete subject entirely
	//
	// COMPATIBILITY:
	//   POST   /schemas/compatibility/subjects/{subject}/versions/{version}  Test compatibility
	//   GET    /schemas/config                          Get global compatibility mode
	//   PUT    /schemas/config                          Set global compatibility mode
	//   GET    /schemas/config/{subject}                Get subject compatibility mode
	//   PUT    /schemas/config/{subject}                Set subject compatibility mode
	//
	// LOOKUP:
	//   GET    /schemas/ids/{id}                        Get schema by global ID
	//   POST   /schemas/subjects/{subject}              Check if schema exists
	//   GET    /schemas/subjects                        List all subjects
	//
	// STATS:
	//   GET    /schemas/stats                           Get registry statistics
	//
	// ==========================================================================
	s.router.Route("/schemas", func(r chi.Router) {
		// Global config
		r.Get("/config", s.getGlobalSchemaConfig)
		r.Put("/config", s.setGlobalSchemaConfig)

		// Subjects
		r.Get("/subjects", s.listSchemaSubjects)

		// Subject-specific routes
		r.Route("/subjects/{subject}", func(r chi.Router) {
			// Check if schema exists under subject
			r.Post("/", s.checkSchemaExists)

			// Subject config
			r.Delete("/", s.deleteSchemaSubject)

			// Versions
			r.Route("/versions", func(r chi.Router) {
				r.Post("/", s.registerSchema)
				r.Get("/", s.listSchemaVersions)
				r.Get("/latest", s.getLatestSchema)
				r.Get("/{version}", s.getSchemaVersion)
				r.Delete("/{version}", s.deleteSchemaVersion)
			})
		})

		// Compatibility testing
		r.Post("/compatibility/subjects/{subject}/versions/{version}", s.testSchemaCompatibility)

		// Subject config (separate route)
		r.Get("/config/{subject}", s.getSubjectSchemaConfig)
		r.Put("/config/{subject}", s.setSubjectSchemaConfig)

		// Schema by ID
		r.Get("/ids/{id}", s.getSchemaByID)

		// Stats
		r.Get("/stats", s.handleSchemaStats)
	})

	// ==========================================================================
	// TRANSACTIONS API (M9)
	// ==========================================================================
	//
	// These endpoints provide exactly-once semantics (EOS) for message production.
	//
	// PRODUCER INITIALIZATION:
	//   POST /producers/init                    Initialize producer (get PID + epoch)
	//   POST /producers/{id}/heartbeat          Send heartbeat to keep session alive
	//
	// TRANSACTION LIFECYCLE:
	//   POST /transactions/begin                Begin a new transaction
	//   POST /transactions/publish              Publish message as part of transaction
	//   POST /transactions/add-partition        Add partition to transaction scope
	//   POST /transactions/commit               Commit transaction (atomic)
	//   POST /transactions/abort                Abort transaction (rollback)
	//
	// MONITORING:
	//   GET  /transactions                      List active transactions
	//   GET  /transactions/stats                Get coordinator statistics
	//
	// FLOW:
	//   ┌─────────────────────────────────────────────────────────────────────┐
	//   │  1. POST /producers/init                                            │
	//   │     Body: {"transactional_id": "my-producer"}                       │
	//   │     Response: {"producer_id": 123, "epoch": 1}                      │
	//   │                                                                     │
	//   │  2. POST /transactions/begin                                        │
	//   │     Body: {"producer_id": 123, "epoch": 1, "transactional_id": ...} │
	//   │     Response: {"transaction_id": "txn-abc"}                         │
	//   │                                                                     │
	//   │  3. POST /transactions/publish (repeat for each message)            │
	//   │     Body: {"producer_id": 123, "epoch": 1, "topic": "orders",       │
	//   │            "key": "...", "value": "...", "sequence": 0}             │
	//   │     Response: {"partition": 0, "offset": 42}                        │
	//   │                                                                     │
	//   │  4. POST /transactions/commit                                       │
	//   │     Body: {"producer_id": 123, "epoch": 1, "transactional_id": ...} │
	//   │     Response: {"status": "committed"}                               │
	//   └─────────────────────────────────────────────────────────────────────┘
	//
	// ZOMBIE FENCING:
	//   If a producer re-initializes with the same transactional_id, the epoch
	//   increments. Any requests with the old epoch are rejected as "zombie".
	//
	// HEARTBEAT:
	//   Producers should send heartbeats every 3 seconds (configurable).
	//   If no heartbeat for 30 seconds, the session expires and any active
	//   transaction is automatically aborted.
	//
	// TIMEOUT:
	//   Transactions have a 60-second timeout (configurable). If a transaction
	//   is not committed or aborted within this time, it's automatically aborted.
	//
	// ==========================================================================
	s.router.Route("/producers", func(r chi.Router) {
		r.Post("/init", s.initProducer)
		r.Route("/{producerId}", func(r chi.Router) {
			r.Post("/heartbeat", s.producerHeartbeat)
		})
	})

	s.router.Route("/transactions", func(r chi.Router) {
		r.Get("/", s.listTransactions)
		r.Get("/stats", s.handleTransactionStats)
		r.Post("/begin", s.beginTransaction)
		r.Post("/publish", s.publishTransactional)
		r.Post("/add-partition", s.addPartitionToTransaction)
		r.Post("/commit", s.commitTransaction)
		r.Post("/abort", s.abortTransaction)
	})

	// ==========================================================================
	// COOPERATIVE REBALANCING API (M12)
	// ==========================================================================
	//
	// These endpoints provide cooperative rebalancing for consumer groups.
	// Cooperative rebalancing minimizes consumer downtime during rebalances
	// by only revoking partitions that need to move (Kafka KIP-429 style).
	//
	// COOPERATIVE ENDPOINTS:
	//   POST /groups/{groupID}/join/cooperative      Join with cooperative protocol
	//   POST /groups/{groupID}/leave/cooperative     Leave with cooperative protocol
	//   POST /groups/{groupID}/heartbeat/cooperative Heartbeat with rebalance response
	//   POST /groups/{groupID}/revoke                Acknowledge partition revocation
	//   GET  /groups/{groupID}/assignment            Get current assignment
	//   GET  /groups/{groupID}/cooperative           Get cooperative group info
	//   GET  /groups/{groupID}/rebalance/stats       Get rebalance stats for group
	//   GET  /rebalance/stats                        Get global rebalance stats
	//
	// FLOW (cooperative join):
	//   ┌─────────────────────────────────────────────────────────────────────┐
	//   │  1. POST /groups/{group}/join/cooperative                           │
	//   │     Body: {"client_id": "consumer-1", "topics": ["orders"]}         │
	//   │     Response: {"member_id": "...", "generation": 1,                 │
	//   │               "rebalance_required": true, "protocol": "cooperative"}│
	//   │                                                                     │
	//   │  2. POST /groups/{group}/heartbeat/cooperative (poll for work)      │
	//   │     Body: {"member_id": "...", "generation": 1}                     │
	//   │     Response: {"rebalance_required": true,                          │
	//   │                "partitions_to_revoke": [...],                       │
	//   │                "state": "pending_revoke"}                           │
	//   │                                                                     │
	//   │  3. Consumer: stop processing revoked partitions, commit offsets    │
	//   │                                                                     │
	//   │  4. POST /groups/{group}/revoke                                     │
	//   │     Body: {"member_id": "...", "generation": 1,                     │
	//   │            "revoked_partitions": [...]}                             │
	//   │     Response: {"status": "acknowledged"}                            │
	//   │                                                                     │
	//   │  5. POST /groups/{group}/heartbeat/cooperative (poll again)         │
	//   │     Response: {"rebalance_required": true,                          │
	//   │                "partitions_assigned": [...],                        │
	//   │                "state": "pending_assign"}                           │
	//   │                                                                     │
	//   │  6. Consumer: start processing newly assigned partitions            │
	//   └─────────────────────────────────────────────────────────────────────┘
	//
	// KEY BENEFITS:
	//   - Consumers keep processing unaffected partitions during rebalance
	//   - Sticky assignment minimizes partition movement
	//   - Two-phase protocol ensures clean handoff
	//
	// ==========================================================================
	// NOTE: Cooperative routes are registered inside the /groups/{groupID} block
	// above via RegisterCooperativeGroupRoutes() and globally via
	// RegisterCooperativeGlobalRoutes() to avoid duplicate path registration.

	// ==========================================================================
	// ADMIN API (M13)
	// ==========================================================================
	//
	// Administrative endpoints for cluster operations:
	//   - Partition scaling (add partitions to existing topics)
	//   - Partition reassignment (move replicas between nodes)
	//   - Coordinator management (view and discover group coordinators)
	//
	// SECURITY NOTE:
	// These endpoints should be protected in production environments.
	// Consider adding:
	//   - Authentication (API keys, OAuth)
	//   - Authorization (role-based access)
	//   - Audit logging
	//
	// ==========================================================================
	s.RegisterAdminRoutes(s.router)

	// ==========================================================================
	// TENANT MANAGEMENT API (M18)
	// ==========================================================================
	//
	// Administrative endpoints for multi-tenancy:
	//   - Tenant CRUD (create, read, update, delete)
	//   - Quota management (view and update per-tenant quotas)
	//   - Usage tracking (view tenant resource usage)
	//   - Lifecycle management (suspend, activate, disable)
	//
	// ENDPOINTS:
	//   POST   /admin/tenants                        Create tenant
	//   GET    /admin/tenants                        List tenants
	//   GET    /admin/tenants/{id}                   Get tenant
	//   PATCH  /admin/tenants/{id}                   Update tenant
	//   DELETE /admin/tenants/{id}                   Delete tenant
	//   POST   /admin/tenants/{id}/suspend           Suspend tenant
	//   POST   /admin/tenants/{id}/activate          Activate tenant
	//   GET    /admin/tenants/{id}/quotas            Get quotas
	//   PUT    /admin/tenants/{id}/quotas            Update quotas
	//   GET    /admin/tenants/{id}/usage             Get usage stats
	//
	// ==========================================================================
	s.RegisterTenantRoutes(s.router)
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

// handleMetrics serves Prometheus metrics.
//
// =============================================================================
// PROMETHEUS METRICS HANDLER
// =============================================================================
//
// WHY THIS ENDPOINT?
// Prometheus is a pull-based monitoring system. It periodically scrapes this
// endpoint to collect metrics. This is how goqueue exposes its operational
// metrics to monitoring systems.
//
// WHAT'S EXPOSED:
//   - Broker metrics (messages published/consumed, latencies, errors)
//   - Storage metrics (bytes written/read, fsync latency)
//   - Consumer metrics (group members, lag, rebalances)
//   - Cluster metrics (node health, leader elections, ISR changes)
//   - Go runtime metrics (goroutines, memory, GC)
//   - Process metrics (CPU, file descriptors)
//
// FORMAT: Prometheus text exposition format
//
//	# HELP goqueue_broker_messages_published_total Total messages published
//	# TYPE goqueue_broker_messages_published_total counter
//	goqueue_broker_messages_published_total{topic="orders"} 12345
//
// SCRAPE CONFIGURATION (prometheus.yaml):
//
//	scrape_configs:
//	  - job_name: 'goqueue'
//	    static_configs:
//	      - targets: ['localhost:8080']
//
// =============================================================================
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Get the metrics handler from the registry
	// If metrics are not initialized, return 503
	handler := metrics.Handler()
	if handler == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "metrics not initialized")
		return
	}
	handler.ServeHTTP(w, r)
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
		var deliverAt time.Time
		var isDelayed bool

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
		if isDelayed {
			// Delayed message with priority (M5+M6 integration)
			partition, offset, err = s.broker.PublishAtWithPriority(topicName, key, value, deliverAt, priority)
			results[i] = PublishResult{
				Partition: partition,
				Offset:    offset,
				Priority:  priority.String(),
				Delayed:   true,
				DeliverAt: deliverAt.Format(time.RFC3339),
			}
		} else if msg.Partition != nil {
			// Direct partition publish with priority
			offset, err = topic.PublishToPartitionWithPriority(*msg.Partition, key, value, priority)
			partition = *msg.Partition
			results[i] = PublishResult{
				Partition: partition,
				Offset:    offset,
				Priority:  priority.String(),
			}
		} else {
			// Normal publish with priority (key-based routing)
			partition, offset, err = topic.PublishWithPriority(key, value, priority)
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
type ConsumeMessage struct {
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
	Priority  string `json:"priority,omitempty"` // M6: Priority level (critical/high/normal/low/background)
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
// MILESTONE 4: MESSAGE ACKNOWLEDGMENT HANDLERS
// =============================================================================
//
// ENDPOINT OVERVIEW:
//
//   POST /messages/ack       - Acknowledge successful message processing
//   POST /messages/nack      - Signal retry (transient failure)
//   POST /messages/reject    - Send to DLQ (permanent failure)
//   POST /messages/visibility - Extend visibility timeout
//   GET  /reliability/stats  - Get reliability layer statistics
//
// RECEIPT HANDLE:
//   Each message returned from /groups/{group}/poll includes a receipt_handle.
//   This handle is required for all ACK/NACK/REJECT operations.
//
//   Format: {topic}:{partition}:{offset}:{deliveryCount}:{nonce}
//   Example: "orders:0:42:1:a1b2c3d4"
//
// ERROR CODES:
//   400 - Missing or invalid receipt handle
//   404 - Message not found (already acked, expired, or invalid handle)
//   410 - Message gone (visibility timeout expired)
//   429 - Too many in-flight messages (backpressure)
//
// =============================================================================

// AckRequest is the request body for POST /messages/ack
type AckRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
}

// ackMessage handles POST /messages/ack
//
// SEMANTICS:
//   - Message is fully processed and should not be redelivered
//   - May advance committed offset if this completes a contiguous range
//
// EXAMPLE:
//
//	curl -X POST http://localhost:8080/messages/ack \
//	  -H "Content-Type: application/json" \
//	  -d '{"receipt_handle": "orders:0:42:1:a1b2c3d4"}'
func (s *Server) ackMessage(w http.ResponseWriter, r *http.Request) {
	var req AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ReceiptHandle == "" {
		s.errorResponse(w, http.StatusBadRequest, "receipt_handle is required")
		return
	}

	result, err := s.broker.Ack(req.ReceiptHandle)
	if err != nil {
		// Map error to appropriate HTTP status
		switch err {
		case broker.ErrMessageNotFound:
			s.errorResponse(w, http.StatusNotFound, "message not found or already processed")
		case broker.ErrInvalidReceiptHandle:
			s.errorResponse(w, http.StatusBadRequest, "invalid receipt handle format")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":          true,
		"topic":            result.Topic,
		"partition":        result.Partition,
		"offset":           result.Offset,
		"committed_offset": result.NewCommittedOffset,
		"offset_advanced":  result.OffsetAdvanced,
		"in_flight_count":  result.RemainingInFlight,
	})
}

// NackRequest is the request body for POST /messages/nack
type NackRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
	Reason        string `json:"reason,omitempty"` // Optional: why the processing failed
}

// nackMessage handles POST /messages/nack
//
// SEMANTICS:
//   - Processing failed but should be retried (transient failure)
//   - Message will be redelivered after exponential backoff
//   - After MaxRetries (default 3), message goes to DLQ
//
// EXAMPLE:
//
//	curl -X POST http://localhost:8080/messages/nack \
//	  -H "Content-Type: application/json" \
//	  -d '{"receipt_handle": "orders:0:42:1:a1b2c3d4", "reason": "database timeout"}'
func (s *Server) nackMessage(w http.ResponseWriter, r *http.Request) {
	var req NackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ReceiptHandle == "" {
		s.errorResponse(w, http.StatusBadRequest, "receipt_handle is required")
		return
	}

	result, err := s.broker.Nack(req.ReceiptHandle, req.Reason)
	if err != nil {
		switch err {
		case broker.ErrMessageNotFound:
			s.errorResponse(w, http.StatusNotFound, "message not found or already processed")
		case broker.ErrInvalidReceiptHandle:
			s.errorResponse(w, http.StatusBadRequest, "invalid receipt handle format")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	response := map[string]interface{}{
		"success":         true,
		"action":          result.Action, // "requeued" or "dlq"
		"topic":           result.Topic,
		"partition":       result.Partition,
		"offset":          result.Offset,
		"delivery_count":  result.DeliveryCount,
		"next_visible_at": result.NextVisibleAt.Format(time.RFC3339),
	}

	if result.Action == "dlq" {
		response["dlq_topic"] = result.DLQTopic
	}

	s.writeJSON(w, http.StatusOK, response)
}

// RejectRequest is the request body for POST /messages/reject
type RejectRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
	Reason        string `json:"reason"` // Required: why this message cannot be processed
}

// rejectMessage handles POST /messages/reject
//
// SEMANTICS:
//   - Message is "poison" and cannot be processed (permanent failure)
//   - Goes directly to DLQ without retry
//
// USE CASES:
//   - Invalid message format
//   - Business logic determines message is unprocessable
//   - Schema validation failure
//
// EXAMPLE:
//
//	curl -X POST http://localhost:8080/messages/reject \
//	  -H "Content-Type: application/json" \
//	  -d '{"receipt_handle": "orders:0:42:1:a1b2c3d4", "reason": "invalid order format"}'
func (s *Server) rejectMessage(w http.ResponseWriter, r *http.Request) {
	var req RejectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ReceiptHandle == "" {
		s.errorResponse(w, http.StatusBadRequest, "receipt_handle is required")
		return
	}

	if req.Reason == "" {
		s.errorResponse(w, http.StatusBadRequest, "reason is required for reject")
		return
	}

	result, err := s.broker.Reject(req.ReceiptHandle, req.Reason)
	if err != nil {
		switch err {
		case broker.ErrMessageNotFound:
			s.errorResponse(w, http.StatusNotFound, "message not found or already processed")
		case broker.ErrInvalidReceiptHandle:
			s.errorResponse(w, http.StatusBadRequest, "invalid receipt handle format")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"action":    "rejected",
		"topic":     result.Topic,
		"partition": result.Partition,
		"offset":    result.Offset,
		"dlq_topic": result.DLQTopic,
		"reason":    req.Reason,
	})
}

// ExtendVisibilityRequest is the request body for POST /messages/visibility
type ExtendVisibilityRequest struct {
	ReceiptHandle    string `json:"receipt_handle"`
	ExtensionSeconds int    `json:"extension_seconds"` // Additional time in seconds
}

// extendVisibility handles POST /messages/visibility
//
// SEMANTICS:
//   - Extends the visibility timeout for a message currently being processed
//   - Use when processing will take longer than the default timeout
//   - Can be called multiple times
//
// USE CASE:
//   - Default timeout is 30s
//   - Processing a large file will take 60s
//   - After 20s, extend visibility by 60s more
//   - Prevents timeout-based redelivery while still processing
//
// EXAMPLE:
//
//	curl -X POST http://localhost:8080/messages/visibility \
//	  -H "Content-Type: application/json" \
//	  -d '{"receipt_handle": "orders:0:42:1:a1b2c3d4", "extension_seconds": 60}'
func (s *Server) extendVisibility(w http.ResponseWriter, r *http.Request) {
	var req ExtendVisibilityRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ReceiptHandle == "" {
		s.errorResponse(w, http.StatusBadRequest, "receipt_handle is required")
		return
	}

	if req.ExtensionSeconds <= 0 {
		s.errorResponse(w, http.StatusBadRequest, "extension_seconds must be positive")
		return
	}

	extension := time.Duration(req.ExtensionSeconds) * time.Second
	newDeadline, err := s.broker.ExtendVisibility(req.ReceiptHandle, extension)
	if err != nil {
		switch err {
		case broker.ErrMessageNotFound:
			s.errorResponse(w, http.StatusNotFound, "message not found or already processed")
		case broker.ErrInvalidReceiptHandle:
			s.errorResponse(w, http.StatusBadRequest, "invalid receipt handle format")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":      true,
		"new_deadline": newDeadline.Format(time.RFC3339),
		"extended_by":  req.ExtensionSeconds,
	})
}

// handleReliabilityStats handles GET /reliability/stats
//
// Returns statistics about the reliability layer:
//   - ACK manager: in-flight messages, acks, nacks, rejects
//   - Visibility tracker: tracked messages, timeouts, extensions
//   - DLQ router: routed messages per topic, total routed
func (s *Server) handleReliabilityStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.ReliabilityStats()

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"ack_manager": map[string]interface{}{
			"total_acks":     stats.AckManager.TotalAcks,
			"total_nacks":    stats.AckManager.TotalNacks,
			"total_rejects":  stats.AckManager.TotalRejects,
			"total_expired":  stats.AckManager.TotalExpired,
			"total_retries":  stats.AckManager.TotalRetries,
			"total_dlq":      stats.AckManager.TotalDLQ,
			"current_queued": stats.AckManager.CurrentQueued,
		},
		"visibility_tracker": map[string]interface{}{
			"total_tracked":     stats.Visibility.TotalTracked,
			"current_in_flight": stats.Visibility.CurrentInFlight,
			"total_expired":     stats.Visibility.TotalExpired,
			"total_acked":       stats.Visibility.TotalAcked,
			"total_extended":    stats.Visibility.TotalExtended,
		},
		"dlq": map[string]interface{}{
			"total_routed":     stats.DLQ.TotalRouted,
			"routes_by_reason": stats.DLQ.RoutesByReason,
			"routes_by_topic":  stats.DLQ.RoutesByTopic,
			"create_errors":    stats.DLQ.CreateErrors,
			"publish_errors":   stats.DLQ.PublishErrors,
		},
	})
}

// =============================================================================
// MILESTONE 5: DELAYED MESSAGES API
// =============================================================================
//
// These endpoints provide delayed/scheduled message delivery management.
//
// ENDPOINTS:
//   GET    /topics/{name}/delayed               List pending delayed messages
//   GET    /topics/{name}/delayed/{offset}      Get specific delayed message
//   DELETE /topics/{name}/delayed/{p}/{offset}  Cancel delayed message
//   GET    /delay/stats                         Scheduler statistics
//
// =============================================================================

// DelayedMessageResponse represents a delayed message in API responses.
type DelayedMessageResponse struct {
	Topic         string `json:"topic"`
	Partition     int    `json:"partition"`
	Offset        int64  `json:"offset"`
	DeliverAt     string `json:"deliver_at"`
	TimeRemaining string `json:"time_remaining"`
	State         string `json:"state"`
}

// listDelayedMessages handles GET /topics/{topicName}/delayed
//
// Returns all pending delayed messages for a topic.
// Supports pagination via ?limit=N&skip=N query parameters.
//
// EXAMPLE:
//
//	curl http://localhost:8080/topics/orders/delayed?limit=100&skip=0
func (s *Server) listDelayedMessages(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")

	// Parse pagination params
	limit := 100
	skip := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if sk := r.URL.Query().Get("skip"); sk != "" {
		if parsed, err := strconv.Atoi(sk); err == nil && parsed >= 0 {
			skip = parsed
		}
	}

	messages, err := s.broker.GetDelayedMessages(topicName, limit, skip)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Convert to API response format
	response := make([]DelayedMessageResponse, len(messages))
	for i, msg := range messages {
		response[i] = DelayedMessageResponse{
			Topic:         msg.Topic,
			Partition:     msg.Partition,
			Offset:        msg.Offset,
			DeliverAt:     msg.DeliverAt.Format(time.RFC3339),
			TimeRemaining: msg.TimeRemaining.String(),
			State:         msg.State,
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"topic":    topicName,
		"messages": response,
		"count":    len(response),
		"limit":    limit,
		"skip":     skip,
	})
}

// getDelayedMessage handles GET /topics/{topicName}/delayed/{offset}
//
// Returns details about a specific delayed message.
// Note: This looks up by offset only - caller must know the partition.
// For full lookup, use GET /topics/{name}/delayed to list all.
//
// EXAMPLE:
//
//	curl http://localhost:8080/topics/orders/delayed/1234
func (s *Server) getDelayedMessage(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")
	offsetStr := chi.URLParam(r, "offset")

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid offset")
		return
	}

	// Try to find the message in any partition
	// This is a simple implementation - a production system might have
	// a more efficient lookup if partition is known
	messages, err := s.broker.GetDelayedMessages(topicName, 0, 0) // Get all
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	for _, msg := range messages {
		if msg.Offset == offset {
			s.writeJSON(w, http.StatusOK, DelayedMessageResponse{
				Topic:         msg.Topic,
				Partition:     msg.Partition,
				Offset:        msg.Offset,
				DeliverAt:     msg.DeliverAt.Format(time.RFC3339),
				TimeRemaining: msg.TimeRemaining.String(),
				State:         msg.State,
			})
			return
		}
	}

	s.errorResponse(w, http.StatusNotFound, "delayed message not found")
}

// cancelDelayedMessage handles DELETE /topics/{topicName}/delayed/{partition}/{offset}
//
// Cancels a pending delayed message. The message will never be delivered.
// Returns 200 if cancelled, 404 if not found or already delivered.
//
// EXAMPLE:
//
//	curl -X DELETE http://localhost:8080/topics/orders/delayed/0/1234
func (s *Server) cancelDelayedMessage(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")
	partitionStr := chi.URLParam(r, "partition")
	offsetStr := chi.URLParam(r, "offset")

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid partition")
		return
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid offset")
		return
	}

	cancelled, err := s.broker.CancelDelayed(topicName, partition, offset)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	if !cancelled {
		s.errorResponse(w, http.StatusNotFound, "delayed message not found or already delivered")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"cancelled": true,
		"topic":     topicName,
		"partition": partition,
		"offset":    offset,
	})
}

// handleDelayStats handles GET /delay/stats
//
// Returns statistics about the delay scheduling system:
//   - Total scheduled, delivered, cancelled messages
//   - Pending messages by topic
//   - Timer wheel statistics
//
// EXAMPLE:
//
//	curl http://localhost:8080/delay/stats
func (s *Server) handleDelayStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.DelayStats()

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"total_scheduled": stats.TotalScheduled,
		"total_delivered": stats.TotalDelivered,
		"total_cancelled": stats.TotalCancelled,
		"total_pending":   stats.TotalPending,
		"by_topic":        stats.ByTopic,
		"timer_wheel": map[string]interface{}{
			"total_scheduled": stats.TimerWheel.TotalScheduled,
			"total_expired":   stats.TimerWheel.TotalExpired,
			"total_cancelled": stats.TimerWheel.TotalCancelled,
			"current_active":  stats.TimerWheel.CurrentActive,
			"current_tick":    stats.TimerWheel.CurrentTick,
		},
	})
}

// =============================================================================
// PRIORITY STATS (M6)
// =============================================================================
//
// ENDPOINT: GET /priority/stats
//
// Returns per-priority-per-partition statistics across all topics.
//
// WHY PER-PRIORITY-PER-PARTITION?
// This is the most granular view, allowing:
//   - Hot partition detection at the priority level
//   - Priority imbalance detection
//   - Starvation monitoring (oldest pending message age)
//   - Capacity planning by priority
//
// RESPONSE STRUCTURE:
//
//	{
//	  "total_by_priority": [100, 50, 25, 10, 5],  // [critical, high, normal, low, background]
//	  "topics": {
//	    "orders": {
//	      "total_by_priority": [100, 50, 25, 10, 5],
//	      "partitions": {
//	        "0": {
//	          "pending": [10, 5, 3, 1, 0],
//	          "consumed": [90, 45, 22, 9, 5],
//	          "total": [100, 50, 25, 10, 5],
//	          "oldest_pending": ["2024-01-15T10:00:00Z", ...]
//	        }
//	      }
//	    }
//	  }
//	}
//
// EXAMPLE:
//
//	curl http://localhost:8080/priority/stats
func (s *Server) handlePriorityStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.PriorityStats()

	// Convert to JSON-friendly format
	response := map[string]interface{}{
		"total_by_priority": convertPriorityArray(stats.TotalByPriority),
		"topics":            make(map[string]interface{}),
	}

	for topicName, topicStats := range stats.Topics {
		partitions := make(map[string]interface{})
		for partID, partStats := range topicStats.Partitions {
			partitions[strconv.Itoa(partID)] = map[string]interface{}{
				"pending":        convertPriorityArray(partStats.Pending),
				"consumed":       convertPriorityArray(partStats.Consumed),
				"total":          convertPriorityArray(partStats.Total),
				"oldest_pending": convertPriorityTimeArray(partStats.OldestPending),
			}
		}
		response["topics"].(map[string]interface{})[topicName] = map[string]interface{}{
			"total_by_priority": convertPriorityArray(topicStats.TotalByPriority),
			"partitions":        partitions,
		}
	}

	s.writeJSON(w, http.StatusOK, response)
}

// convertPriorityArray converts a [5]int64 to a []int64 for JSON serialization.
// Array index maps to: [critical, high, normal, low, background]
func convertPriorityArray(arr [5]int64) []int64 {
	return arr[:]
}

// convertPriorityTimeArray converts a [5]time.Time to RFC3339 strings.
// Zero times become empty strings to indicate no pending messages.
func convertPriorityTimeArray(arr [5]time.Time) []string {
	result := make([]string, 5)
	for i, t := range arr {
		if !t.IsZero() {
			result[i] = t.Format(time.RFC3339)
		}
	}
	return result
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

// =============================================================================
// TRACING HANDLERS (M7)
// =============================================================================
//
// These handlers provide HTTP endpoints for querying message traces.
// Traces capture the full lifecycle of messages: publish → consume → ack/nack/reject
//
// ENDPOINTS:
//   GET /traces          - List recent traces
//   GET /traces/{id}     - Get specific trace
//   GET /traces/search   - Search with filters
//   GET /traces/stats    - Tracer statistics
//
// =============================================================================

// listTraces returns recent traces.
//
// QUERY PARAMETERS:
//   - limit: Maximum traces to return (default: 100, max: 1000)
//
// RESPONSE FORMAT:
//
//	{
//	  "traces": [
//	    {
//	      "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
//	      "topic": "orders",
//	      "partition": 0,
//	      "offset": 100,
//	      "start_time": "2024-01-15T10:00:00Z",
//	      "end_time": "2024-01-15T10:00:01Z",
//	      "duration_ms": 1000,
//	      "status": "completed",
//	      "span_count": 3
//	    }
//	  ],
//	  "total": 100
//	}
func (s *Server) listTraces(w http.ResponseWriter, r *http.Request) {
	// Parse limit parameter
	limit := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	traces := s.broker.Tracer().GetRecentTraces(limit)

	response := map[string]interface{}{
		"traces": convertTracesToJSON(traces),
		"total":  len(traces),
	}

	s.writeJSON(w, http.StatusOK, response)
}

// getTrace returns a specific trace by ID.
//
// URL PARAMETERS:
//   - traceID: 32-character hex trace ID
//
// RESPONSE FORMAT:
//
//	{
//	  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
//	  "topic": "orders",
//	  "partition": 0,
//	  "offset": 100,
//	  "start_time": "2024-01-15T10:00:00Z",
//	  "end_time": "2024-01-15T10:00:01Z",
//	  "duration_ms": 1000,
//	  "status": "completed",
//	  "spans": [
//	    {
//	      "span_id": "00f067aa0ba902b7",
//	      "event_type": "publish.received",
//	      "timestamp": "2024-01-15T10:00:00Z",
//	      ...
//	    }
//	  ]
//	}
func (s *Server) getTrace(w http.ResponseWriter, r *http.Request) {
	traceIDStr := chi.URLParam(r, "traceID")

	// Parse trace ID from hex string
	traceID, err := broker.ParseTraceID(traceIDStr)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid trace ID format")
		return
	}

	trace := s.broker.Tracer().GetTrace(traceID)
	if trace == nil {
		s.errorResponse(w, http.StatusNotFound, "trace not found")
		return
	}

	s.writeJSON(w, http.StatusOK, convertTraceToJSON(trace))
}

// searchTraces searches traces with filters.
//
// QUERY PARAMETERS:
//   - topic: Filter by topic name
//   - partition: Filter by partition number (-1 for all)
//   - consumer_group: Filter by consumer group
//   - start: Start time (RFC3339)
//   - end: End time (RFC3339)
//   - status: Filter by status (completed, error, pending)
//   - limit: Max traces to return (default: 100)
//
// EXAMPLE:
//
//	curl "http://localhost:8080/traces/search?topic=orders&start=2024-01-15T00:00:00Z&limit=50"
func (s *Server) searchTraces(w http.ResponseWriter, r *http.Request) {
	query := broker.TraceQuery{
		Partition: -1, // Default: all partitions
	}

	// Parse query parameters
	if topic := r.URL.Query().Get("topic"); topic != "" {
		query.Topic = topic
	}
	if partitionStr := r.URL.Query().Get("partition"); partitionStr != "" {
		if p, err := strconv.Atoi(partitionStr); err == nil {
			query.Partition = p
		}
	}
	if consumerGroup := r.URL.Query().Get("consumer_group"); consumerGroup != "" {
		query.ConsumerGroup = consumerGroup
	}
	if startStr := r.URL.Query().Get("start"); startStr != "" {
		if t, err := time.Parse(time.RFC3339, startStr); err == nil {
			query.StartTime = t
		}
	}
	if endStr := r.URL.Query().Get("end"); endStr != "" {
		if t, err := time.Parse(time.RFC3339, endStr); err == nil {
			query.EndTime = t
		}
	}
	if status := r.URL.Query().Get("status"); status != "" {
		query.Status = status
	}
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			query.Limit = l
		}
	}
	if query.Limit == 0 {
		query.Limit = 100
	}

	traces := s.broker.Tracer().SearchTraces(query)

	response := map[string]interface{}{
		"traces": convertTracesToJSON(traces),
		"total":  len(traces),
		"query": map[string]interface{}{
			"topic":          query.Topic,
			"partition":      query.Partition,
			"consumer_group": query.ConsumerGroup,
			"start":          formatOptionalTime(query.StartTime),
			"end":            formatOptionalTime(query.EndTime),
			"status":         string(query.Status),
			"limit":          query.Limit,
		},
	}

	s.writeJSON(w, http.StatusOK, response)
}

// handleTracerStats returns tracer statistics.
//
// RESPONSE FORMAT:
//
//	{
//	  "enabled": true,
//	  "spans_in_buffer": 1000,
//	  "buffer_capacity": 10000,
//	  "traces_indexed": 500,
//	  "sampling_rate": 1.0,
//	  "exporters_enabled": ["file", "stdout"]
//	}
func (s *Server) handleTracerStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.Tracer().Stats()

	response := map[string]interface{}{
		"enabled":           stats.Enabled,
		"spans_in_buffer":   stats.SpansInBuffer,
		"buffer_capacity":   stats.BufferCapacity,
		"traces_indexed":    stats.TracesIndexed,
		"sampling_rate":     stats.SamplingRate,
		"exporters_enabled": stats.ExportersEnabled,
	}

	s.writeJSON(w, http.StatusOK, response)
}

// =============================================================================
// TRACE CONVERSION HELPERS
// =============================================================================

// convertTracesToJSON converts a slice of traces to JSON-friendly format.
func convertTracesToJSON(traces []*broker.Trace) []map[string]interface{} {
	result := make([]map[string]interface{}, len(traces))
	for i, trace := range traces {
		result[i] = map[string]interface{}{
			"trace_id":    trace.TraceID.String(),
			"topic":       trace.Topic,
			"partition":   trace.Partition,
			"offset":      trace.Offset,
			"start_time":  trace.StartTime.Format(time.RFC3339Nano),
			"end_time":    trace.EndTime.Format(time.RFC3339Nano),
			"duration_ms": trace.Duration.Milliseconds(),
			"status":      string(trace.Status),
			"span_count":  len(trace.Spans),
		}
	}
	return result
}

// convertTraceToJSON converts a trace with all spans to JSON-friendly format.
func convertTraceToJSON(trace *broker.Trace) map[string]interface{} {
	spans := make([]map[string]interface{}, len(trace.Spans))
	for i, span := range trace.Spans {
		spanJSON := map[string]interface{}{
			"span_id":    span.SpanID.String(),
			"event_type": string(span.EventType),
			"timestamp":  time.Unix(0, span.Timestamp).Format(time.RFC3339Nano),
			"topic":      span.Topic,
			"partition":  span.Partition,
			"offset":     span.Offset,
		}

		// Add optional fields
		if !span.ParentSpanID.IsZero() {
			spanJSON["parent_span_id"] = span.ParentSpanID.String()
		}
		if span.ConsumerGroup != "" {
			spanJSON["consumer_group"] = span.ConsumerGroup
		}
		if span.ConsumerID != "" {
			spanJSON["consumer_id"] = span.ConsumerID
		}
		if span.Duration > 0 {
			spanJSON["duration_ns"] = span.Duration
		}
		if span.Error != "" {
			spanJSON["error"] = span.Error
		}
		if len(span.Attributes) > 0 {
			spanJSON["attributes"] = span.Attributes
		}

		spans[i] = spanJSON
	}

	return map[string]interface{}{
		"trace_id":    trace.TraceID.String(),
		"topic":       trace.Topic,
		"partition":   trace.Partition,
		"offset":      trace.Offset,
		"start_time":  trace.StartTime.Format(time.RFC3339Nano),
		"end_time":    trace.EndTime.Format(time.RFC3339Nano),
		"duration_ms": trace.Duration.Milliseconds(),
		"status":      string(trace.Status),
		"spans":       spans,
	}
}

// formatOptionalTime formats a time as RFC3339 or empty string if zero.
func formatOptionalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// =============================================================================
// SCHEMA REGISTRY HANDLERS (M8)
// =============================================================================
//
// WHAT IS A SCHEMA REGISTRY?
// The schema registry provides centralized schema management for message
// validation and evolution. It ensures producers send well-formed data
// and manages safe schema changes over time.
//
// API DESIGN:
// Our API follows the Confluent Schema Registry API pattern for familiarity,
// adapted for our JSON Schema focus.
//
// KEY CONCEPTS:
//   - Subject: Schema identifier (we use topic name - TopicNameStrategy)
//   - Version: Sequential version number within a subject
//   - Schema ID: Global unique ID across all subjects
//   - Compatibility: Rules for safe schema evolution
//
// =============================================================================

// registerSchema handles POST /schemas/subjects/{subject}/versions
//
// REGISTERS a new schema version for a subject.
// Validates JSON Schema syntax, checks compatibility with previous versions,
// and assigns a global ID.
//
// REQUEST BODY:
//
//	{
//	  "schema": "{\"type\":\"object\",\"properties\":{...}}"
//	}
//
// RESPONSE:
//
//	{
//	  "id": 1,
//	  "version": 1
//	}
func (s *Server) registerSchema(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	if subject == "" {
		s.errorResponse(w, http.StatusBadRequest, "subject is required")
		return
	}

	var req struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.Schema == "" {
		s.errorResponse(w, http.StatusBadRequest, "schema is required")
		return
	}

	schema, err := s.broker.SchemaRegistry().RegisterSchema(subject, req.Schema)
	if err != nil {
		if strings.Contains(err.Error(), "incompatible") {
			s.errorResponse(w, http.StatusConflict, "schema incompatible: "+err.Error())
			return
		}
		if strings.Contains(err.Error(), "invalid") {
			s.errorResponse(w, http.StatusUnprocessableEntity, "invalid schema: "+err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to register schema: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":      schema.ID,
		"version": schema.Version,
	})
}

// listSchemaVersions handles GET /schemas/subjects/{subject}/versions
//
// LISTS all version numbers for a subject.
//
// RESPONSE:
//
//	[1, 2, 3]
func (s *Server) listSchemaVersions(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	if subject == "" {
		s.errorResponse(w, http.StatusBadRequest, "subject is required")
		return
	}

	versions, err := s.broker.SchemaRegistry().ListVersions(subject)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "subject not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, versions)
}

// getSchemaVersion handles GET /schemas/subjects/{subject}/versions/{version}
//
// GETS a specific schema version.
//
// RESPONSE:
//
//	{
//	  "subject": "orders",
//	  "version": 1,
//	  "id": 1,
//	  "schema": "{...}",
//	  "schemaType": "JSON"
//	}
func (s *Server) getSchemaVersion(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	versionStr := chi.URLParam(r, "version")

	version, err := strconv.Atoi(versionStr)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid version number")
		return
	}

	schema, err := s.broker.SchemaRegistry().GetSchemaBySubjectVersion(subject, version)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"subject":    schema.Subject,
		"version":    schema.Version,
		"id":         schema.ID,
		"schema":     schema.Schema,
		"schemaType": schema.SchemaType,
	})
}

// getLatestSchema handles GET /schemas/subjects/{subject}/versions/latest
//
// GETS the latest schema version for a subject.
func (s *Server) getLatestSchema(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	if subject == "" {
		s.errorResponse(w, http.StatusBadRequest, "subject is required")
		return
	}

	schema, err := s.broker.SchemaRegistry().GetLatestSchema(subject)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "subject not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"subject":    schema.Subject,
		"version":    schema.Version,
		"id":         schema.ID,
		"schema":     schema.Schema,
		"schemaType": schema.SchemaType,
	})
}

// deleteSchemaVersion handles DELETE /schemas/subjects/{subject}/versions/{version}
//
// DELETES a specific schema version (soft delete).
func (s *Server) deleteSchemaVersion(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	versionStr := chi.URLParam(r, "version")

	version, err := strconv.Atoi(versionStr)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid version number")
		return
	}

	if err := s.broker.SchemaRegistry().DeleteSchemaVersion(subject, version); err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"deleted": version,
	})
}

// deleteSchemaSubject handles DELETE /schemas/subjects/{subject}
//
// DELETES a subject and all its versions.
func (s *Server) deleteSchemaSubject(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	if subject == "" {
		s.errorResponse(w, http.StatusBadRequest, "subject is required")
		return
	}

	versions, err := s.broker.SchemaRegistry().DeleteSubject(subject)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "subject not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, versions)
}

// listSchemaSubjects handles GET /schemas/subjects
//
// LISTS all registered subjects.
func (s *Server) listSchemaSubjects(w http.ResponseWriter, r *http.Request) {
	subjects := s.broker.SchemaRegistry().ListSubjects()
	s.writeJSON(w, http.StatusOK, subjects)
}

// checkSchemaExists handles POST /schemas/subjects/{subject}
//
// CHECKS if a schema is already registered under the subject.
// Returns the schema if found, 404 if not.
func (s *Server) checkSchemaExists(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	if subject == "" {
		s.errorResponse(w, http.StatusBadRequest, "subject is required")
		return
	}

	var req struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	schema, err := s.broker.SchemaRegistry().CheckSchemaExists(subject, req.Schema)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	if schema == nil {
		s.errorResponse(w, http.StatusNotFound, "schema not found")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"subject": schema.Subject,
		"version": schema.Version,
		"id":      schema.ID,
	})
}

// getSchemaByID handles GET /schemas/ids/{id}
//
// GETS a schema by its global ID.
func (s *Server) getSchemaByID(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid schema ID")
		return
	}

	schema, err := s.broker.SchemaRegistry().GetSchemaByID(id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "schema not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"schema":     schema.Schema,
		"schemaType": schema.SchemaType,
		"subject":    schema.Subject,
		"version":    schema.Version,
		"id":         schema.ID,
	})
}

// testSchemaCompatibility handles POST /schemas/compatibility/subjects/{subject}/versions/{version}
//
// TESTS if a schema is compatible with a specific version (or latest if version=-1).
func (s *Server) testSchemaCompatibility(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	versionStr := chi.URLParam(r, "version")

	// Handle "latest" as -1
	var version int
	if versionStr == "latest" {
		version = -1
	} else {
		var err error
		version, err = strconv.Atoi(versionStr)
		if err != nil {
			s.errorResponse(w, http.StatusBadRequest, "invalid version number")
			return
		}
	}

	var req struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	isCompatible, err := s.broker.SchemaRegistry().TestCompatibility(subject, req.Schema, version)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "invalid") {
			s.errorResponse(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"is_compatible": isCompatible,
	})
}

// getGlobalSchemaConfig handles GET /schemas/config
//
// GETS the global compatibility mode.
func (s *Server) getGlobalSchemaConfig(w http.ResponseWriter, r *http.Request) {
	compat := s.broker.SchemaRegistry().GetGlobalCompatibility()
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"compatibility": string(compat),
	})
}

// setGlobalSchemaConfig handles PUT /schemas/config
//
// SETS the global compatibility mode.
func (s *Server) setGlobalSchemaConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Compatibility string `json:"compatibility"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	mode := broker.ParseCompatibilityMode(req.Compatibility)
	if err := s.broker.SchemaRegistry().SetGlobalCompatibility(mode); err != nil {
		s.errorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"compatibility": string(mode),
	})
}

// getSubjectSchemaConfig handles GET /schemas/config/{subject}
//
// GETS the compatibility mode for a specific subject.
func (s *Server) getSubjectSchemaConfig(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	compat := s.broker.SchemaRegistry().GetCompatibilityMode(subject)
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"compatibility": string(compat),
	})
}

// setSubjectSchemaConfig handles PUT /schemas/config/{subject}
//
// SETS the compatibility mode for a specific subject.
func (s *Server) setSubjectSchemaConfig(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")

	var req struct {
		Compatibility string `json:"compatibility"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	mode := broker.ParseCompatibilityMode(req.Compatibility)
	if err := s.broker.SchemaRegistry().SetCompatibilityMode(subject, mode); err != nil {
		s.errorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"compatibility": string(mode),
	})
}

// handleSchemaStats handles GET /schemas/stats
//
// RETURNS schema registry statistics.
func (s *Server) handleSchemaStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.SchemaStats()
	s.writeJSON(w, http.StatusOK, stats)
}

// =============================================================================
// TRANSACTION HANDLERS (M9)
// =============================================================================
//
// These handlers implement the transactional producer API for exactly-once
// semantics (EOS).
//
// KAFKA COMPARISON:
//   - Kafka clients use initTransactions(), beginTransaction(), send(),
//     commitTransaction(), abortTransaction()
//   - goqueue exposes these as HTTP endpoints for language-agnostic access
//
// THREAD SAFETY:
//   All handler methods are thread-safe. The transaction coordinator handles
//   all synchronization internally.
//

// =============================================================================
// PRODUCER INITIALIZATION
// =============================================================================

// initProducer handles POST /producers/init
//
// Initializes a producer and returns a Producer ID (PID) and epoch.
// If the producer was previously registered (via transactional_id), it will
// get the same PID with an incremented epoch (zombie fencing).
//
// REQUEST BODY:
//
//	{
//	  "transactional_id": "my-producer"  // Required for transactional producers
//	}
//
// RESPONSE:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1
//	}
//
// ERRORS:
//   - 400 Bad Request: Invalid JSON or missing transactional_id
//   - 500 Internal Server Error: Coordinator error
//
// ZOMBIE FENCING:
//
//	When a producer re-initializes with the same transactional_id:
//	  - Epoch is incremented (e.g., from 1 to 2)
//	  - Any in-progress transaction from the old epoch is aborted
//	  - Requests from the old epoch are rejected
//
//	This prevents "zombie" producers (e.g., a crashed producer that restarted)
//	from interfering with the new producer instance.
func (s *Server) initProducer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TransactionalId      string `json:"transactional_id"`
		TransactionTimeoutMs int64  `json:"transaction_timeout_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.TransactionalId == "" {
		s.errorResponse(w, http.StatusBadRequest, "transactional_id is required")
		return
	}

	// Default timeout if not specified
	timeoutMs := req.TransactionTimeoutMs
	if timeoutMs == 0 {
		timeoutMs = 60000 // 60 seconds default
	}

	// Get transaction coordinator
	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	// Initialize producer
	pid, err := coord.InitProducerId(req.TransactionalId, timeoutMs)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "failed to init producer: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"producer_id": pid.ProducerId,
		"epoch":       pid.Epoch,
	})
}

// producerHeartbeat handles POST /producers/{producerId}/heartbeat
//
// Sends a heartbeat to keep the producer session alive.
// If no heartbeat is received within the session timeout (default 30s),
// any active transaction is aborted and the producer is marked as dead.
//
// URL PARAMETERS:
//   - producerId: The producer ID (from /producers/init)
//
// REQUEST BODY:
//
//	{
//	  "transactional_id": "my-producer"
//	}
//
// RESPONSE:
//
//	{
//	  "status": "ok"
//	}
//
// ERRORS:
//   - 400 Bad Request: Invalid producer ID or missing transactional_id
//   - 404 Not Found: Producer not found
//   - 409 Conflict: Producer has been fenced (newer epoch exists)
func (s *Server) producerHeartbeat(w http.ResponseWriter, r *http.Request) {
	producerIdStr := chi.URLParam(r, "producerId")
	producerId, err := strconv.ParseInt(producerIdStr, 10, 64)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid producer_id")
		return
	}

	var req struct {
		TransactionalId string `json:"transactional_id"`
		Epoch           int16  `json:"epoch"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.TransactionalId == "" {
		s.errorResponse(w, http.StatusBadRequest, "transactional_id is required")
		return
	}

	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	pid := broker.ProducerIdAndEpoch{
		ProducerId: producerId,
		Epoch:      req.Epoch,
	}

	if err := coord.Heartbeat(req.TransactionalId, pid); err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") || strings.Contains(err.Error(), "epoch") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "ok",
	})
}

// =============================================================================
// TRANSACTION LIFECYCLE
// =============================================================================

// beginTransaction handles POST /transactions/begin
//
// Begins a new transaction for the producer. A producer can only have one
// active transaction at a time.
//
// REQUEST BODY:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1,
//	  "transactional_id": "my-producer"
//	}
//
// RESPONSE:
//
//	{
//	  "transaction_id": "txn-123-1-1234567890"
//	}
//
// ERRORS:
//   - 400 Bad Request: Invalid JSON or missing fields
//   - 404 Not Found: Producer not found
//   - 409 Conflict: Producer fenced or transaction already in progress
func (s *Server) beginTransaction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProducerId      int64  `json:"producer_id"`
		Epoch           int16  `json:"epoch"`
		TransactionalId string `json:"transactional_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.TransactionalId == "" {
		s.errorResponse(w, http.StatusBadRequest, "transactional_id is required")
		return
	}

	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	pid := broker.ProducerIdAndEpoch{
		ProducerId: req.ProducerId,
		Epoch:      req.Epoch,
	}

	txnId, err := coord.BeginTransaction(req.TransactionalId, pid)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") || strings.Contains(err.Error(), "in progress") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"transaction_id": txnId,
	})
}

// publishTransactional handles POST /transactions/publish
//
// Publishes a message as part of an active transaction. The message is written
// to the partition log but not visible to consumers until the transaction commits.
//
// REQUEST BODY:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1,
//	  "topic": "orders",
//	  "partition": -1,           // -1 for auto-partition based on key
//	  "key": "order-123",        // Base64-encoded or string
//	  "value": "{...}",          // Message payload
//	  "sequence": 0              // Sequence number for deduplication
//	}
//
// RESPONSE:
//
//	{
//	  "partition": 0,
//	  "offset": 42,
//	  "duplicate": false
//	}
//
// SEQUENCE NUMBERS:
//
//	The sequence number is per-partition and must be monotonically increasing.
//	  - Start from 0 for each partition
//	  - Increment by 1 for each message
//	  - Duplicates (same sequence) return success without re-writing
//
// ERRORS:
//   - 400 Bad Request: Invalid JSON, missing fields, or invalid sequence
//   - 404 Not Found: Topic not found
//   - 409 Conflict: Producer fenced or no active transaction
func (s *Server) publishTransactional(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProducerId int64  `json:"producer_id"`
		Epoch      int16  `json:"epoch"`
		Topic      string `json:"topic"`
		Partition  int    `json:"partition"`
		Key        string `json:"key"`
		Value      string `json:"value"`
		Sequence   int32  `json:"sequence"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.Topic == "" {
		s.errorResponse(w, http.StatusBadRequest, "topic is required")
		return
	}

	partition, offset, duplicate, err := s.broker.PublishTransactional(
		req.Topic,
		req.Partition,
		[]byte(req.Key),
		[]byte(req.Value),
		req.ProducerId,
		req.Epoch,
		req.Sequence,
	)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "sequence") {
			s.errorResponse(w, http.StatusBadRequest, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"partition": partition,
		"offset":    offset,
		"duplicate": duplicate,
	})
}

// addPartitionToTransaction handles POST /transactions/add-partition
//
// Adds a topic-partition to the transaction scope. This is typically called
// automatically by publishTransactional, but can be called explicitly for
// partitions that will receive messages later.
//
// REQUEST BODY:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1,
//	  "transactional_id": "my-producer",
//	  "topic": "orders",
//	  "partition": 0
//	}
//
// RESPONSE:
//
//	{
//	  "status": "added"
//	}
func (s *Server) addPartitionToTransaction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProducerId      int64  `json:"producer_id"`
		Epoch           int16  `json:"epoch"`
		TransactionalId string `json:"transactional_id"`
		Topic           string `json:"topic"`
		Partition       int    `json:"partition"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	pid := broker.ProducerIdAndEpoch{
		ProducerId: req.ProducerId,
		Epoch:      req.Epoch,
	}

	err := coord.AddPartitionToTransaction(req.TransactionalId, pid, req.Topic, req.Partition)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "added",
	})
}

// commitTransaction handles POST /transactions/commit
//
// Commits the active transaction, making all messages visible to consumers.
// This is a two-phase commit:
//  1. Write COMMIT control records to all partitions
//  2. Mark transaction as completed
//
// REQUEST BODY:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1,
//	  "transactional_id": "my-producer"
//	}
//
// RESPONSE:
//
//	{
//	  "status": "committed",
//	  "partitions_committed": 3
//	}
//
// ERRORS:
//   - 400 Bad Request: Invalid JSON or missing fields
//   - 404 Not Found: No active transaction
//   - 409 Conflict: Producer fenced
//   - 500 Internal Server Error: Commit failed
func (s *Server) commitTransaction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProducerId      int64  `json:"producer_id"`
		Epoch           int16  `json:"epoch"`
		TransactionalId string `json:"transactional_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.TransactionalId == "" {
		s.errorResponse(w, http.StatusBadRequest, "transactional_id is required")
		return
	}

	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	pid := broker.ProducerIdAndEpoch{
		ProducerId: req.ProducerId,
		Epoch:      req.Epoch,
	}

	err := coord.CommitTransaction(req.TransactionalId, pid)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "committed",
	})
}

// abortTransaction handles POST /transactions/abort
//
// Aborts the active transaction, discarding all messages written within it.
// This writes ABORT control records to all partitions.
//
// REQUEST BODY:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1,
//	  "transactional_id": "my-producer"
//	}
//
// RESPONSE:
//
//	{
//	  "status": "aborted",
//	  "partitions_aborted": 3
//	}
//
// ERRORS:
//   - 400 Bad Request: Invalid JSON or missing fields
//   - 404 Not Found: No active transaction
//   - 409 Conflict: Producer fenced
func (s *Server) abortTransaction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProducerId      int64  `json:"producer_id"`
		Epoch           int16  `json:"epoch"`
		TransactionalId string `json:"transactional_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.TransactionalId == "" {
		s.errorResponse(w, http.StatusBadRequest, "transactional_id is required")
		return
	}

	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	pid := broker.ProducerIdAndEpoch{
		ProducerId: req.ProducerId,
		Epoch:      req.Epoch,
	}

	err := coord.AbortTransaction(req.TransactionalId, pid)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "aborted",
	})
}

// =============================================================================
// TRANSACTION MONITORING
// =============================================================================

// listTransactions handles GET /transactions
//
// Lists all active transactions.
//
// QUERY PARAMETERS:
//   - limit: Maximum number of transactions to return (default: 100)
//
// RESPONSE:
//
//	{
//	  "transactions": [
//	    {
//	      "transaction_id": "txn-123-1-1234567890",
//	      "transactional_id": "my-producer",
//	      "producer_id": 123,
//	      "epoch": 1,
//	      "state": "Ongoing",
//	      "start_time": "2024-01-15T10:30:00Z",
//	      "partitions": [
//	        {"topic": "orders", "partition": 0}
//	      ]
//	    }
//	  ],
//	  "count": 1
//	}
func (s *Server) listTransactions(w http.ResponseWriter, r *http.Request) {
	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	transactions := coord.GetActiveTransactions()

	// Convert to JSON-friendly format
	type txnPartition struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
	}
	type txnInfo struct {
		TransactionId   string         `json:"transaction_id"`
		TransactionalId string         `json:"transactional_id"`
		ProducerId      int64          `json:"producer_id"`
		Epoch           int16          `json:"epoch"`
		State           string         `json:"state"`
		StartTime       time.Time      `json:"start_time"`
		Partitions      []txnPartition `json:"partitions"`
	}

	txnList := make([]txnInfo, 0, len(transactions))
	for _, txn := range transactions {
		// Convert map[string]map[int]struct{} to []txnPartition
		partitions := make([]txnPartition, 0)
		for topic, partMap := range txn.Partitions {
			for partition := range partMap {
				partitions = append(partitions, txnPartition{
					Topic:     topic,
					Partition: partition,
				})
			}
		}
		txnList = append(txnList, txnInfo{
			TransactionId:   txn.TransactionId,
			TransactionalId: txn.TransactionalId,
			ProducerId:      txn.ProducerId,
			Epoch:           txn.Epoch,
			State:           txn.State.String(),
			StartTime:       txn.StartTime,
			Partitions:      partitions,
		})
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"transactions": txnList,
		"count":        len(txnList),
	})
}

// handleTransactionStats handles GET /transactions/stats
//
// Returns transaction coordinator statistics.
//
// RESPONSE:
//
//	{
//	  "total_producers": 10,
//	  "transactional_producers": 5,
//	  "active_transactions": 2,
//	  "total_transactions_committed": 150,
//	  "total_transactions_aborted": 3,
//	  "total_transactions_timed_out": 1
//	}
func (s *Server) handleTransactionStats(w http.ResponseWriter, r *http.Request) {
	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	stats := coord.Stats()
	s.writeJSON(w, http.StatusOK, stats)
}
