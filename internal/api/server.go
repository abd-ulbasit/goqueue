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
	"net/http/pprof"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"goqueue/internal/broker"
	"goqueue/internal/metrics"
	"goqueue/internal/security"
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

	// Security components (M21)
	security *security.SecurityManager

	// Audit logger for security event tracking (M27)
	auditLogger *security.AuditLogger
}

// ServerConfig holds API server configuration.
type ServerConfig struct {
	Addr         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration

	// =========================================================================
	// ReadHeaderTimeout - SLOWLORIS ATTACK PREVENTION
	// =========================================================================
	//
	// WHY: Without ReadHeaderTimeout, an attacker can open a connection and
	// send headers very slowly (one byte at a time), keeping the connection
	// open indefinitely. This is a "slowloris" attack that exhausts server
	// resources by holding file descriptors and goroutines hostage.
	//
	// HOW IT WORKS:
	//   - Limits time allowed to read request headers (not body)
	//   - If client doesn't finish sending headers in time → connection closed
	//   - ReadTimeout covers headers + body, but ReadHeaderTimeout is stricter
	//     for the header phase alone
	//
	// COMPARISON:
	//   - Go stdlib: ReadHeaderTimeout added specifically for slowloris
	//   - Nginx: client_header_timeout (default 60s)
	//   - Apache: RequestReadTimeout header=20-40
	//   - CloudFlare: 15s default
	//
	// RELATIONSHIP TO ReadTimeout:
	//   ReadTimeout covers the ENTIRE request (headers + body).
	//   ReadHeaderTimeout covers ONLY headers.
	//   Both should be set:
	//     ReadHeaderTimeout = 10s (fast, headers are small)
	//     ReadTimeout = 30s (slower, bodies can be large)
	//
	// =========================================================================
	ReadHeaderTimeout time.Duration

	// =========================================================================
	// MaxRequestBodySize - OOM PREVENTION
	// =========================================================================
	//
	// WHY: Without body size limits, a single malicious request with a
	// multi-GB body can exhaust server memory. http.MaxBytesReader wraps
	// the request body with a hard limit, returning HTTP 413 on overflow.
	//
	// HOW IT WORKS:
	//   1. Middleware wraps r.Body with http.MaxBytesReader(w, r.Body, limit)
	//   2. If client sends more than limit → read returns error
	//   3. Handler sees *http.MaxBytesError → returns 413 Payload Too Large
	//   4. Connection is closed (no point reading more)
	//
	// COMPARISON:
	//   - Kafka: message.max.bytes (1MB default), replica.fetch.max.bytes
	//   - RabbitMQ: max_message_size (128MB default, was unlimited)
	//   - SQS: 256KB hard limit
	//   - Nginx: client_max_body_size (1MB default)
	//   - goqueue: 1MB default, 16MB for publish (matches our MaxValueSize)
	//
	// TRADEOFF:
	//   - Too small: Rejects legitimate large messages
	//   - Too large: Vulnerable to OOM attacks
	//   - 1MB default is safe; the publish endpoint gets 16MB (our MaxValueSize)
	//
	// =========================================================================
	MaxRequestBodySize int64

	// =========================================================================
	// RateLimitRPS - GLOBAL API RATE LIMITING
	// =========================================================================
	//
	// WHY: In single-tenant mode, there's no per-tenant rate limiting
	// (those are handled by TenantManager in multi-tenant mode). Without
	// ANY rate limiting, the API is vulnerable to abuse and overload.
	//
	// HOW IT WORKS:
	//   Token bucket algorithm:
	//   - Bucket holds up to RateLimitRPS tokens
	//   - Tokens refill at RateLimitRPS per second
	//   - Each request consumes 1 token
	//   - If bucket empty → HTTP 429 Too Many Requests
	//
	// COMPARISON:
	//   - Kafka: quota.producer.default, quota.consumer.default (bytes/sec)
	//   - RabbitMQ: Per-connection rate limiting
	//   - SQS: Account-level throttling (3000 msg/s per queue)
	//   - goqueue: Simple token bucket, configurable RPS
	//
	// WHEN APPLIED:
	//   - Only in single-tenant mode (multi-tenant has per-tenant limits)
	//   - 0 = disabled (no rate limiting)
	//   - Default: 1000 req/s (generous for single-instance)
	//
	// =========================================================================
	RateLimitRPS int

	// Security configuration (M21)
	Security security.SecurityConfig

	// Audit logging configuration (M27)
	Audit security.AuditConfig
}

// DefaultServerConfig returns sensible defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Addr:               ":8080",
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		IdleTimeout:        60 * time.Second,
		ReadHeaderTimeout:  10 * time.Second,
		MaxRequestBodySize: 1 * 1024 * 1024, // 1MB default (publish endpoint gets 16MB)
		RateLimitRPS:       1000,            // 1000 req/s default
		Security:           security.LoadSecurityConfigFromEnv(),
	}
}

// NewServer creates a new API server.
func NewServer(b *broker.Broker, config ServerConfig) *Server {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	r := chi.NewRouter()

	// Initialize security manager
	securityMgr := security.NewSecurityManagerWithConfig(config.Security)

	// Initialize audit logger (M27)
	auditLogger := security.NewAuditLogger(config.Audit)

	s := &Server{
		broker:      b,
		router:      r,
		logger:      logger,
		security:    securityMgr,
		auditLogger: auditLogger,
	}

	// Set up middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(s.loggingMiddleware)
	r.Use(middleware.Recoverer)

	// =========================================================================
	// CORS MIDDLEWARE (#10 - Cross-Origin Resource Sharing)
	// =========================================================================
	//
	// WHY: Without CORS headers, browsers block JavaScript clients from
	// calling the goqueue API from different origins (domains/ports).
	// This is critical for:
	//   - Dashboard UIs hosted on different domains
	//   - Developer tools making API calls from localhost
	//   - SPA frontends consuming the management API
	//
	// HOW CORS WORKS:
	//   1. Browser sends preflight OPTIONS request with Origin header
	//   2. Server responds with Access-Control-Allow-* headers
	//   3. Browser checks headers → allows or blocks the actual request
	//
	// COMPARISON:
	//   - Kafka REST Proxy: Configurable CORS via access.control.*
	//   - RabbitMQ Management: Built-in CORS support
	//   - AWS API Gateway: CORS enabled per route
	//   - goqueue: Global CORS middleware, permissive defaults
	//
	// SECURITY NOTE:
	//   AllowedOrigins=["*"] is permissive. In production, restrict to
	//   specific domains. API key auth still protects endpoints.
	//
	// =========================================================================
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-API-Key", "X-Request-ID"},
		ExposedHeaders:   []string{"X-Request-ID", "X-RateLimit-Limit", "X-RateLimit-Remaining", "Retry-After"},
		AllowCredentials: false,
		MaxAge:           300, // 5 minutes - browser caches preflight responses
	}))
	logger.Info("CORS middleware enabled")

	// =========================================================================
	// REQUEST CONTEXT TIMEOUT MIDDLEWARE (#7 - Context Deadlines)
	// =========================================================================
	//
	// WHY: Without context deadlines, slow handlers hold goroutines and
	// memory indefinitely. The timeout propagates through broker → storage,
	// so all operations automatically cancel if the request exceeds the limit.
	//
	// TIMEOUT: 30s (matches Kafka's request.timeout.ms default)
	// EXCLUDED: Health checks, pprof, long-poll endpoints (manage own timeouts)
	//
	// =========================================================================
	r.Use(requestTimeoutMiddleware(30 * time.Second))
	logger.Info("request context timeout enabled", "timeout", "30s")

	// =========================================================================
	// REQUEST BODY SIZE LIMITER MIDDLEWARE (M27 - Production Hardening)
	// =========================================================================
	//
	// WHY: Without body size limits, a single client can send a multi-GB
	// request and exhaust server memory (OOM kill). This middleware wraps
	// r.Body with http.MaxBytesReader, which enforces a hard limit on
	// request body size. If exceeded, the reader returns an error and the
	// handler returns HTTP 413 Payload Too Large.
	//
	// FLOW:
	//   Client ──► [MaxBytesReader] ──► Handler
	//                    │
	//                    └─ Body > limit? ──► HTTP 413 + close connection
	//
	// NOTE: The publish endpoint overrides this with a 16MB limit
	// (matching our MaxValueSize) since message payloads can be larger
	// than typical API requests.
	//
	// =========================================================================
	if config.MaxRequestBodySize > 0 {
		r.Use(maxBodySizeMiddleware(config.MaxRequestBodySize))
		logger.Info("request body size limit enabled", "max_bytes", config.MaxRequestBodySize)
	}

	// =========================================================================
	// API RATE LIMITER MIDDLEWARE (M27 - Production Hardening)
	// =========================================================================
	//
	// WHY: In single-tenant mode, there's no per-tenant quota enforcement
	// (handled by TenantManager in multi-tenant mode). Without ANY rate
	// limiting, the API is vulnerable to accidental or malicious overload.
	//
	// ALGORITHM: Token bucket
	//   - Bucket refills at RateLimitRPS tokens per second
	//   - Each request consumes 1 token
	//   - If empty → HTTP 429 Too Many Requests with Retry-After header
	//
	// COMPARISON:
	//   - Kafka: quota.producer.default (bytes/sec per client)
	//   - RabbitMQ: Per-connection rate limiting
	//   - SQS: 3000 requests/sec per queue
	//   - goqueue: Global token bucket, 1000 req/s default
	//
	// =========================================================================
	if config.RateLimitRPS > 0 {
		r.Use(NewRateLimiterMiddleware(config.RateLimitRPS))
		logger.Info("API rate limiting enabled", "rps", config.RateLimitRPS)
	}

	// ┌─────────────────────────────────────────────────────────────────────────┐
	// │ SECURITY MIDDLEWARE (M21)                                               │
	// │                                                                         │
	// │ If authentication is enabled, all requests (except health endpoints)    │
	// │ must include a valid API key in one of:                                 │
	// │   - Authorization: Bearer <key>                                         │
	// │   - X-API-Key: <key>                                                    │
	// │   - ?api_key=<key> (query param, less secure)                           │
	// │                                                                         │
	// │ The middleware validates the key and adds the APIKey object to context. │
	// │ Subsequent handlers can check permissions via security.GetAPIKeyFromContext() │
	// └─────────────────────────────────────────────────────────────────────────┘
	if securityMgr.IsAuthEnabled() {
		logger.Info("API authentication enabled")
		r.Use(securityMgr.Keys.AuthMiddleware)
	}

	// =========================================================================
	// AUDIT LOGGING MIDDLEWARE (M27 - Security Compliance)
	// =========================================================================
	//
	// WHY: Audit logs provide an immutable record of who did what and when.
	// Required for SOC 2, PCI-DSS, HIPAA, and general security posture.
	//
	// WHAT IT CAPTURES:
	//   - Request method, path, source IP
	//   - Response status code
	//   - Processing duration
	//   - Request ID (correlation)
	//
	// PLACEMENT: After auth middleware so authenticated identity is available.
	// Skips health/metrics endpoints to avoid noise.
	//
	// =========================================================================
	if auditLogger != nil {
		r.Use(security.AuditMiddleware(auditLogger))
		logger.Info("audit logging middleware enabled")
	}

	// Register routes
	s.registerRoutes()

	// =========================================================================
	// PPROF DEBUG ENDPOINTS (#8 - Runtime Profiling)
	// =========================================================================
	//
	// WHY: pprof provides runtime profiling data that's essential for
	// diagnosing production issues:
	//   - CPU hotspots (where is time spent?)
	//   - Memory leaks (what's allocating?)
	//   - Goroutine leaks (where are goroutines blocked?)
	//   - Mutex contention (which locks are hot?)
	//   - Block profiling (where are goroutines blocked on I/O?)
	//
	// HOW TO USE:
	//   go tool pprof http://localhost:8080/debug/pprof/profile?seconds=30
	//   go tool pprof http://localhost:8080/debug/pprof/heap
	//   go tool pprof http://localhost:8080/debug/pprof/goroutine
	//   curl http://localhost:8080/debug/pprof/goroutine?debug=1
	//
	// COMPARISON:
	//   - Kafka: JMX beans + jcmd/jmap for JVM profiling
	//   - RabbitMQ: Built-in Erlang observer
	//   - Redis: INFO command + MEMORY DOCTOR
	//   - goqueue: Standard Go pprof (industry standard for Go services)
	//
	// SECURITY:
	//   pprof endpoints are registered AFTER auth middleware, so they
	//   require authentication if auth is enabled. In production, consider
	//   binding pprof to a separate internal port.
	//
	// ENDPOINTS:
	//   /debug/pprof/              Index page with all profiles
	//   /debug/pprof/cmdline       Command line arguments
	//   /debug/pprof/profile       CPU profile (default 30s)
	//   /debug/pprof/symbol        Symbol lookup
	//   /debug/pprof/trace         Execution trace
	//   /debug/pprof/heap          Heap memory profile
	//   /debug/pprof/goroutine     Goroutine stacks
	//   /debug/pprof/mutex         Mutex contention
	//   /debug/pprof/block         Block (I/O wait) profile
	//
	// =========================================================================
	r.Route("/debug/pprof", func(r chi.Router) {
		r.HandleFunc("/", pprof.Index)
		r.HandleFunc("/cmdline", pprof.Cmdline)
		r.HandleFunc("/profile", pprof.Profile)
		r.HandleFunc("/symbol", pprof.Symbol)
		r.HandleFunc("/trace", pprof.Trace)
		r.Handle("/heap", pprof.Handler("heap"))
		r.Handle("/goroutine", pprof.Handler("goroutine"))
		r.Handle("/threadcreate", pprof.Handler("threadcreate"))
		r.Handle("/block", pprof.Handler("block"))
		r.Handle("/mutex", pprof.Handler("mutex"))
		r.Handle("/allocs", pprof.Handler("allocs"))
	})
	logger.Info("pprof debug endpoints enabled at /debug/pprof/")

	// =========================================================================
	// API VERSIONING (#9 - /v1/ Prefix)
	// =========================================================================
	//
	// WHY: API versioning allows non-breaking evolution of the API.
	// When v2 introduces breaking changes, v1 clients keep working.
	//
	// STRATEGY: URL path prefix versioning
	//   - /v1/topics/...     Versioned (recommended for new clients)
	//   - /topics/...        Backward-compatible (still works, same handlers)
	//
	// COMPARISON:
	//   - Kafka REST Proxy: /v3/kafka/v3/clusters (path versioning)
	//   - RabbitMQ HTTP API: No versioning (single version)
	//   - AWS SQS: Action-based versioning in query params
	//   - Stripe: /v1/ path prefix (industry gold standard)
	//   - goqueue: /v1/ prefix with backward compatibility
	//
	// HOW IT WORKS:
	//   Both /topics and /v1/topics route to the SAME handlers.
	//   The /v1/ mount uses chi.Mount to create an alias.
	//   When v2 is needed, we'll add /v2/ with new handlers.
	//
	// FLOW:
	//   GET /topics/orders           → s.getTopic("orders")  (legacy)
	//   GET /v1/topics/orders        → s.getTopic("orders")  (versioned)
	//   GET /v2/topics/orders        → (future) new handler
	//
	// =========================================================================
	v1Router := chi.NewRouter()
	s.registerAPIRoutes(v1Router)
	r.Mount("/v1", v1Router)
	logger.Info("API versioning enabled", "versions", []string{"/v1/", "/" + " (backward compat)"})

	s.httpServer = &http.Server{
		Addr:              config.Addr,
		Handler:           r,
		ReadTimeout:       config.ReadTimeout,
		WriteTimeout:      config.WriteTimeout,
		IdleTimeout:       config.IdleTimeout,
		ReadHeaderTimeout: config.ReadHeaderTimeout,
	}

	return s
}

// registerRoutes sets up all API endpoints on the main router.
// Routes are registered both at the root (backward compat) and under /v1/.
func (s *Server) registerRoutes() {
	// Health, metrics, and stats endpoints are always at root level
	// (not versioned - these are infrastructure, not API)
	s.router.Get("/health", s.handleHealth)
	s.router.Get("/healthz", s.handleHealthz)
	s.router.Get("/readyz", s.handleReadyz)
	s.router.Get("/livez", s.handleLivez)
	s.router.Get("/version", s.handleVersion)
	s.router.Get("/stats", s.handleStats)
	s.router.Get("/metrics", s.handleMetrics)

	// Register API routes at root level for backward compatibility
	s.registerAPIRoutes(s.router)
}

// registerAPIRoutes registers all versioned API routes on the given router.
// This is called for both the root router (backward compat) and /v1/ (versioned).
func (s *Server) registerAPIRoutes(r chi.Router) {

	// Topics
	r.Route("/topics", func(r chi.Router) {
		r.Post("/", s.createTopic)
		r.Get("/", s.listTopics)

		r.Route("/{topicName}", func(r chi.Router) {
			r.Get("/", s.getTopic)
			r.Delete("/", s.deleteTopic)

			// Messages
			r.Post("/messages", s.publishMessages)

			// ======================================================================
			// INTERNAL: REQUEST FORWARDING ENDPOINT
			// ======================================================================
			//
			// WHY: In cluster mode, writes must go to the partition leader.
			// When a non-leader receives a publish, it forwards here.
			//
			// FLOW:
			//   Producer ──► Non-Leader Node ──forward──► Leader Node (this endpoint)
			//
			// SECURITY NOTE:
			//   This endpoint is internal. In production, consider:
			//   - Restricting to cluster-internal IPs
			//   - Adding authentication headers
			//   - Rate limiting per source node
			//
			// ======================================================================
			r.Post("/messages/forward", s.forwardPublishHandler)

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

			// ======================================================================
			// PARTITION INFO (Cluster Mode)
			// ======================================================================
			//
			// WHY: Clients and operators need visibility into partition assignments.
			//
			// USE CASES:
			//   - Debug routing issues (which node handles which partition?)
			//   - Monitor ISR health (are replicas in sync?)
			//   - Client optimization (direct connect to leader)
			//
			// RESPONSE: Array of partition info objects with:
			//   - partition: Partition number (0-based)
			//   - leader: Node ID of current leader
			//   - replicas: All replica node IDs
			//   - isr: In-Sync Replicas (subset of replicas that are caught up)
			//
			// COMPARISON:
			//   - Kafka: AdminClient.describeTopics()
			//   - RabbitMQ: GET /api/queues shows node ownership
			//   - goqueue: GET /topics/{name}/partitions
			//
			// ======================================================================
			r.Get("/partitions", s.getTopicPartitions)

			// Partitions
			r.Route("/partitions/{partitionID}", func(r chi.Router) {
				r.Get("/messages", s.consumeMessages)
			})
		})
	})

	// Consumer Groups (M3)
	r.Route("/groups", func(r chi.Router) {
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
	s.RegisterCooperativeGlobalRoutes(r)

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
	r.Route("/messages", func(r chi.Router) {
		r.Post("/ack", s.ackMessage)
		r.Post("/nack", s.nackMessage)
		r.Post("/reject", s.rejectMessage)
		r.Post("/visibility", s.extendVisibility)
	})

	// Reliability Stats (M4)
	r.Get("/reliability/stats", s.handleReliabilityStats)

	// Delay Stats (M5)
	r.Get("/delay/stats", s.handleDelayStats)

	// Priority Stats (M6)
	r.Get("/priority/stats", s.handlePriorityStats)

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
	r.Route("/traces", func(r chi.Router) {
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
	r.Route("/schemas", func(r chi.Router) {
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
	r.Route("/producers", func(r chi.Router) {
		r.Post("/init", s.initProducer)
		r.Route("/{producerID}", func(r chi.Router) {
			r.Post("/heartbeat", s.producerHeartbeat)
		})
	})

	r.Route("/transactions", func(r chi.Router) {
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
	s.RegisterAdminRoutes(r)

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
	s.RegisterTenantRoutes(r)
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
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ TLS SUPPORT (M21)                                                           │
// │                                                                             │
// │ If TLS is enabled, the server uses ListenAndServeTLS instead.               │
// │ This encrypts all HTTP traffic using the configured certificate.            │
// │                                                                             │
// │ CLIENTS MUST:                                                               │
// │   - Use https:// instead of http://                                         │
// │   - Trust the CA that signed the certificate (or use -k/--insecure)         │
// └─────────────────────────────────────────────────────────────────────────────┘
func (s *Server) Start() error {
	if s.security.IsTLSEnabled() {
		s.logger.Info("starting HTTPS API server with TLS", "addr", s.httpServer.Addr)

		// Get TLS config
		tlsConfig, err := s.security.TLSConfig.NewTLSConfig()
		if err != nil {
			return err
		}
		s.httpServer.TLSConfig = tlsConfig

		// Start cert hot-reload watcher if enabled (M27).
		// The watcher polls cert/key files and atomically swaps on change.
		s.security.TLSConfig.StartCertReloader()

		go func() {
			// When using crypto/tls.Config, we call ListenAndServeTLS with empty strings
			// because the cert/key are already in the TLSConfig
			if err := s.httpServer.ListenAndServeTLS("", ""); err != http.ErrServerClosed {
				s.logger.Error("HTTPS server error", "error", err)
			}
		}()
	} else {
		s.logger.Info("starting HTTP API server", "addr", s.httpServer.Addr)
		go func() {
			if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
				s.logger.Error("HTTP server error", "error", err)
			}
		}()
	}
	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("shutting down HTTP API server")

	// Stop cert hot-reload watcher if running (M27)
	s.security.TLSConfig.StopCertReloader()

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
// RESPONSE HELPERS
// =============================================================================

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (s *Server) errorResponse(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]interface{}{
		"error":  message,
		"status": status,
	})
}
