// =============================================================================
// BROKER - THE CENTRAL COORDINATOR
// =============================================================================
//
// WHAT IS A BROKER?
// A broker is a server that:
//   - Manages topics (create, delete, list)
//   - Handles producer requests (publish messages)
//   - Handles consumer requests (read messages)
//   - Stores data durably on disk
//
// In a distributed setup, multiple brokers form a cluster. For M1, we have
// a single broker (no clustering yet).
//
// BROKER RESPONSIBILITIES:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                           BROKER                                        │
//   │                                                                         │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │                    Topic Management                              │  │
//   │   │   - CreateTopic("orders")                                        │  │
//   │   │   - DeleteTopic("orders")                                        │  │
//   │   │   - ListTopics()                                                 │  │
//   │   │   - GetTopic("orders")                                           │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                              │                                          │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │                    Producer Interface                            │  │
//   │   │   - Publish("orders", key, value) → (partition, offset)          │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                              │                                          │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │                    Consumer Interface                            │  │
//   │   │   - Consume("orders", partition, offset) → []messages            │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                              │                                          │
//   │   ┌──────────────────────────────────────────────────────────────────┐  │
//   │   │                    Storage Layer                                 │  │
//   │   │   - Topics → Partitions → Logs → Segments                        │  │
//   │   └──────────────────────────────────────────────────────────────────┘  │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON - How other systems structure brokers:
//   - Kafka: Broker manages partitions, ZooKeeper manages cluster metadata
//   - RabbitMQ: Broker manages queues, exchanges, bindings
//   - SQS: Completely managed (no broker concept exposed)
//
// MILESTONE 1 SCOPE:
//   - Single broker (no clustering)
//   - Topic CRUD operations
//   - Simple produce/consume API
//   - File-based storage
//
// =============================================================================

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrBrokerClosed means the broker has been shut down
	ErrBrokerClosed = errors.New("broker is closed")
)

// =============================================================================
// BROKER CONFIGURATION
// =============================================================================

// BrokerConfig holds broker configuration.
type BrokerConfig struct {
	// DataDir is the root directory for all data storage
	// Structure: DataDir/logs/{topic}/{partition}/
	DataDir string

	// NodeID identifies this broker in a cluster (future use)
	NodeID string

	// LogLevel controls logging verbosity
	LogLevel slog.Level

	// ClusterEnabled enables cluster mode (M10)
	// If false, broker runs in single-node mode
	ClusterEnabled bool

	// ClusterConfig contains cluster configuration (only used if ClusterEnabled)
	ClusterConfig *ClusterModeConfig

	// ==========================================================================
	// MULTI-TENANCY CONFIGURATION
	// ==========================================================================
	//
	// WHY OPTIONAL?
	// Multi-tenancy adds overhead (quota checks, namespace resolution) that's
	// unnecessary in single-customer deployments. GoQueue supports two modes:
	//
	// SINGLE-TENANT MODE (Default):
	//   - No namespace prefixing (topics are "orders", not "tenant1.orders")
	//   - No quota enforcement
	//   - No tenant isolation
	//   - Zero overhead from multi-tenancy features
	//   - Ideal for: Kubernetes deployments where each customer gets their own cluster
	//
	// MULTI-TENANT MODE (EnableMultiTenancy=true):
	//   - Topic prefixing: {tenantID}.{topicName}
	//   - Per-tenant quotas (rate limits, storage, message count)
	//   - Tenant isolation (one tenant can't see another's topics)
	//   - Usage tracking and statistics
	//   - Ideal for: Managed service / SaaS deployments
	//
	// COMPARISON TO OTHER SYSTEMS:
	//   - Kafka: Multi-tenancy via topic prefixes (manual) or separate clusters
	//   - RabbitMQ: Virtual hosts (vhosts) for tenant isolation
	//   - SQS: AWS accounts provide tenant isolation
	//
	// ==========================================================================

	// EnableMultiTenancy activates tenant isolation and quota enforcement.
	// When false (default), the broker runs in single-tenant mode with no
	// namespace prefixing or quota checks.
	EnableMultiTenancy bool
}

// ClusterModeConfig contains cluster-specific configuration.
// These settings are only used when ClusterEnabled is true.
type ClusterModeConfig struct {
	// ClientAddress is where clients connect (e.g., "0.0.0.0:8080")
	ClientAddress string

	// ClusterAddress is where other nodes connect (e.g., "0.0.0.0:9000")
	ClusterAddress string

	// AdvertiseAddress is the address to advertise to other nodes
	// Use this when running behind NAT or in containers
	AdvertiseAddress string

	// Peers is the list of other nodes to connect to on startup
	// Format: ["host1:port1", "host2:port2"]
	Peers []string

	// QuorumSize is the minimum number of nodes required for cluster operations
	// Default: 1 (single-node mode)
	QuorumSize int
}

// DefaultBrokerConfig returns sensible defaults.
func DefaultBrokerConfig() BrokerConfig {
	return BrokerConfig{
		DataDir:  "./data",
		NodeID:   "node-1",
		LogLevel: slog.LevelInfo,
	}
}

// =============================================================================
// BROKER STRUCT
// =============================================================================

// Broker is the main server managing topics and handling requests.
type Broker struct {
	// config holds broker configuration
	config BrokerConfig

	// topics maps topic name to Topic instance
	topics map[string]*Topic

	// logsDir is where log files are stored
	logsDir string

	// groupCoordinator manages consumer groups and offsets
	// Added in Milestone 3 for consumer group support
	groupCoordinator *GroupCoordinator

	// ackManager handles per-message acknowledgment (M4)
	// Provides ACK/NACK/REJECT semantics on top of offset-based consumption
	ackManager *AckManager

	// reliabilityConfig holds M4 reliability settings
	reliabilityConfig ReliabilityConfig

	// ==========================================================================
	// MILESTONE 5: DELAY SCHEDULER
	// ==========================================================================
	//
	// The scheduler handles delayed/scheduled message delivery. When a message
	// is published with a delay, it's:
	//   1. Written immediately to the log (durability)
	//   2. Registered with the scheduler (timer + delay index)
	//   3. Hidden from consumers until delay expires
	//
	// FLOW:
	//   ┌──────────┐  PublishWithDelay  ┌─────────────┐
	//   │ Producer │──────────────────►│ Write to Log │
	//   └──────────┘                    └──────┬──────┘
	//                                          │
	//                                          ▼
	//                                   ┌─────────────┐
	//                                   │ Register in │
	//                                   │  Scheduler  │
	//                                   └──────┬──────┘
	//                                          │
	//                                    delay expires
	//                                          │
	//                                          ▼
	//                                   ┌─────────────┐
	//                                   │ Make Visible │
	//                                   │ to Consumers │
	//                                   └─────────────┘
	//
	// ==========================================================================
	scheduler *Scheduler

	// ==========================================================================
	// MILESTONE 7: MESSAGE TRACER
	// ==========================================================================
	//
	// The tracer records spans for each message operation, enabling end-to-end
	// visibility into message lifecycle:
	//   - publish.received → publish.partitioned → publish.persisted
	//   - consume.fetched → consume.acked / consume.nacked / consume.rejected
	//   - delay.scheduled → delay.ready
	//
	// Trace context is propagated via message headers using W3C Trace Context
	// format (traceparent header).
	//
	// QUERY CAPABILITIES:
	//   - GetTrace(traceID) - Get all spans for a message
	//   - GetRecentTraces(limit) - Recent traces
	//   - SearchTraces(query) - Filter by topic, partition, consumer
	//
	// STORAGE:
	//   - Ring buffer for fast in-memory access
	//   - Optional file export for persistence
	//   - Optional OTLP/Jaeger export for external systems
	//
	// ==========================================================================
	tracer *Tracer

	// ==========================================================================
	// MILESTONE 8: SCHEMA REGISTRY
	// ==========================================================================
	//
	// The schema registry manages message schemas for validation and evolution.
	// It provides:
	//   - Schema storage and versioning
	//   - Compatibility checking (BACKWARD, FORWARD, FULL, NONE)
	//   - Message validation against registered schemas
	//   - Schema ID in message headers for consumer awareness
	//
	// FLOW:
	//   ┌──────────┐  register   ┌─────────────────┐
	//   │ Producer │────────────►│ Schema Registry │
	//   └──────────┘             └────────┬────────┘
	//        │                            │
	//        │ publish                    │ validate
	//        ▼                            ▼
	//   ┌──────────┐             ┌─────────────────┐
	//   │  Broker  │◄────────────│ JSON Schema     │
	//   └──────────┘  reject if  │ Validator       │
	//                 invalid    └─────────────────┘
	//
	// SUBJECT NAMING: TopicNameStrategy (subject = topic name)
	// SCHEMA FORMAT: JSON Schema (Draft 7)
	// SCHEMA ID: Stored in message header "schema-id"
	//
	// FUTURE: Protobuf support (noted for later implementation)
	//
	// ==========================================================================
	schemaRegistry *SchemaRegistry

	// ==========================================================================
	// MILESTONE 9: TRANSACTION COORDINATOR
	// ==========================================================================
	//
	// The transaction coordinator provides exactly-once semantics (EOS) through:
	//   - Idempotent producers: Deduplication via sequence numbers
	//   - Transactions: Atomic writes across multiple partitions/topics
	//   - Read committed isolation: Consumers only see committed messages
	//
	// KAFKA COMPARISON:
	//   - Kafka uses internal __transaction_state topic for persistence
	//   - goqueue uses file-based WAL + snapshots (simpler, same guarantees)
	//
	// FLOW:
	//   ┌──────────────┐  initProducerId  ┌─────────────────────────┐
	//   │   Producer   │─────────────────►│ Transaction Coordinator │
	//   │              │◄─────────────────│  - Assigns PID+Epoch    │
	//   └──────────────┘  PID=123,Epoch=1 │  - Tracks sequences     │
	//         │                           │  - Manages transactions │
	//         │ beginTransaction          └─────────────────────────┘
	//         │                                      │
	//         │ publish(msg1, seq=0)                 │ WAL: begin_txn
	//         │ publish(msg2, seq=1)                 │ WAL: add_partition
	//         │                                      │
	//         │ commitTransaction                    │
	//         ▼                                      ▼
	//   ┌──────────────────────────────────────────────────────────┐
	//   │ Partition Logs                                           │
	//   │ [msg1] [msg2] [COMMIT marker]                            │
	//   └──────────────────────────────────────────────────────────┘
	//
	// ZOMBIE FENCING:
	//   When a producer re-initializes with same transactional.id, epoch bumps.
	//   Old producers with stale epochs are rejected ("zombie fencing").
	//
	// HEARTBEAT:
	//   Producers send heartbeats to keep transactions alive.
	//   If transaction times out (60s) → automatic abort.
	//
	// CONTROL RECORDS:
	//   COMMIT/ABORT markers written to partition logs using FlagControlRecord.
	//   Consumers use these for read_committed filtering.
	//
	// ==========================================================================
	transactionCoordinator *TransactionCoordinator

	// ==========================================================================
	// UNCOMMITTED OFFSET TRACKER (read_committed isolation)
	// ==========================================================================
	//
	// Tracks offsets that belong to uncommitted transactions. During consume,
	// these offsets are filtered out to provide read_committed isolation.
	//
	// FLOW:
	//   PublishTransactional → Track(offset) → [offset hidden from consumers]
	//   CommitTransaction    → ClearTransaction() → [offset visible]
	//   AbortTransaction     → ClearTransaction() → [offsets moved to abortedTracker]
	//
	// ==========================================================================
	uncommittedTracker *UncommittedTracker

	// ABORTED OFFSET TRACKER (read_committed isolation)
	// ==========================================================================
	//
	// Tracks offsets that belong to ABORTED transactions. These messages were
	// written to the log but should remain invisible to consumers forever.
	//
	// WHY SEPARATE FROM UNCOMMITTED?
	//   - Uncommitted: temporary state, cleared on commit (offsets become visible)
	//   - Aborted: permanent state, messages never become visible
	//
	// FLOW:
	//   AbortTransaction → ClearTransaction() + MarkAborted() → [offsets hidden forever]
	//
	// ==========================================================================
	abortedTracker *AbortedTracker

	// ==========================================================================
	// MILESTONE 10: CLUSTER COORDINATOR
	// ==========================================================================
	//
	// The cluster coordinator manages distributed mode:
	//   - Node discovery and membership
	//   - Controller election (single controller per cluster)
	//   - Cluster metadata (topics, partition assignments)
	//   - Heartbeats and failure detection
	//
	// In cluster mode:
	//   - Multiple brokers form a cluster
	//   - Partitions are distributed across nodes
	//   - Controller manages metadata changes
	//   - Nodes communicate via HTTP for cluster ops
	//
	// In single-node mode:
	//   - clusterCoordinator is nil
	//   - All partitions are local
	//   - No inter-node communication
	//
	// ==========================================================================
	clusterCoordinator *clusterCoordinator

	// ==========================================================================
	// MILESTONE 12: COOPERATIVE REBALANCING
	// ==========================================================================
	//
	// The cooperative group coordinator extends consumer group functionality
	// with cooperative rebalancing (Kafka KIP-429 style incremental rebalance).
	//
	// EAGER REBALANCE (before M12):
	//   ┌──────────┐    join    ┌────────────┐
	//   │ Consumer │───────────►│ REVOKE ALL │ ← Stop-the-world!
	//   │ joins    │            │ partitions │
	//   └──────────┘            └─────┬──────┘
	//                                 │
	//                                 ▼
	//                          ┌────────────┐
	//                          │ Reassign   │
	//                          │ all        │
	//                          └─────┬──────┘
	//                                │
	//                                ▼
	//                          ┌────────────┐
	//                          │ Resume     │
	//                          └────────────┘
	//
	// COOPERATIVE REBALANCE (M12):
	//   ┌──────────┐    join    ┌────────────┐
	//   │ Consumer │───────────►│ Revoke     │ ← Only affected!
	//   │ joins    │            │ partitions │
	//   └──────────┘            │ that MOVE  │
	//                           └─────┬──────┘
	//                                 │
	//                                 ▼
	//                          ┌────────────┐
	//                          │ Reassign   │
	//                          │ + new ones │
	//                          └─────┬──────┘
	//                                │
	//                                ▼
	//                          ┌────────────┐
	//                          │ Consumers  │
	//                          │ keep other │ ← No downtime for unchanged!
	//                          │ partitions │
	//                          └────────────┘
	//
	// KEY CONCEPTS:
	//   - Two-phase protocol: revoke affected → assign new
	//   - Sticky assignment: minimize partition moves
	//   - Incremental: consumers keep unaffected partitions
	//   - Heartbeat-based: rebalance info in heartbeat response
	//
	// ==========================================================================
	cooperativeGroupCoordinator *CooperativeGroupCoordinator

	// ==========================================================================
	// MILESTONE 18: MULTI-TENANCY AND QUOTAS
	// ==========================================================================
	//
	// The tenant manager provides multi-tenant isolation and resource control:
	//   - Namespace isolation: Topics prefixed with tenant ID (e.g., "acme.orders")
	//   - Quota enforcement: Rate limits, storage limits, topic count limits
	//   - Usage tracking: Messages published/consumed, bytes transferred
	//   - Lifecycle management: Suspend, disable, delete tenants
	//
	// COMPARISON:
	//   - Kafka: Client quotas + topic prefixes (convention-based)
	//   - RabbitMQ: Virtual hosts for isolation
	//   - SQS: AWS account-level isolation
	//   - goqueue: Namespace isolation + token bucket rate limiting
	//
	// QUOTA TYPES:
	//   - Rate limits: Token bucket algorithm (publish rate, consume rate)
	//   - Storage limits: Max bytes, max topics, max partitions
	//   - Size limits: Max message size, max retention
	//
	// ==========================================================================
	tenantManager *TenantManager

	// quotaEnforcer handles quota checks - strategy pattern:
	//   - NoOpEnforcer: Single-tenant mode (always allows, zero overhead)
	//   - TenantQuotaEnforcer: Multi-tenant mode (actual quota checks)
	// This eliminates scattered `if tenantManager != nil` checks.
	quotaEnforcer QuotaEnforcer

	// mu protects topics map
	mu sync.RWMutex

	// logger for broker operations
	logger *slog.Logger

	// startedAt is when broker started
	startedAt time.Time

	// closed tracks if broker is shut down
	closed bool
}

// =============================================================================
// BROKER LIFECYCLE
// =============================================================================

// NewBroker creates and starts a new broker.
//
// STARTUP PROCESS:
//  1. Create data directories if needed
//  2. Discover existing topics
//  3. Load all topics (recovers from crash if needed)
//  4. Ready to accept requests
func NewBroker(config BrokerConfig) (*Broker, error) {
	// Set up logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: config.LogLevel,
	}))

	// Create data directories
	logsDir := filepath.Join(config.DataDir, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %w", err)
	}

	// Create group coordinator for consumer group management (M3)
	coordinatorConfig := DefaultCoordinatorConfig(config.DataDir)
	groupCoordinator, err := NewGroupCoordinator(coordinatorConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create group coordinator: %w", err)
	}

	// Initialize reliability configuration (M4)
	reliabilityConfig := DefaultReliabilityConfig()

	broker := &Broker{
		config:             config,
		topics:             make(map[string]*Topic),
		logsDir:            logsDir,
		groupCoordinator:   groupCoordinator,
		reliabilityConfig:  reliabilityConfig,
		uncommittedTracker: NewUncommittedTracker(), // LSO support for read_committed
		abortedTracker:     NewAbortedTracker(),     // Abort filtering for read_committed
		logger:             logger,
		startedAt:          time.Now(),
	}

	// Create ACK manager for per-message acknowledgment (M4)
	// Must be created after broker struct exists (circular dependency)
	broker.ackManager = NewAckManager(broker, reliabilityConfig)

	// ==========================================================================
	// MILESTONE 5: INITIALIZE DELAY SCHEDULER
	// ==========================================================================
	//
	// The scheduler manages delayed messages using a hierarchical timer wheel
	// for O(1) timer operations and a persistent delay index for crash recovery.
	//
	// STARTUP FLOW:
	//   1. Create scheduler with delay index directory
	//   2. Scheduler loads pending delays from disk
	//   3. Re-registers timers for pending delayed messages
	//   4. Starts timer wheel processing
	//
	// ==========================================================================
	delayDir := filepath.Join(config.DataDir, "delay")
	schedulerConfig := DefaultSchedulerConfig(delayDir)

	scheduler, err := NewScheduler(schedulerConfig)
	if err != nil {
		// Clean up already-created components
		groupCoordinator.Close()
		return nil, fmt.Errorf("failed to create scheduler: %w", err)
	}
	broker.scheduler = scheduler

	// Set broker reference for message operations
	scheduler.SetBroker(broker)

	// Set callback for when delayed messages become ready
	scheduler.SetDeliveryCallback(broker.handleDelayedMessageReady)

	// Start the scheduler (loads pending delays, starts timer processing)
	if err := scheduler.Start(); err != nil {
		scheduler.Close()
		groupCoordinator.Close()
		return nil, fmt.Errorf("failed to start scheduler: %w", err)
	}

	// ==========================================================================
	// MILESTONE 7: INITIALIZE MESSAGE TRACER
	// ==========================================================================
	//
	// The tracer provides end-to-end visibility into message lifecycle.
	// It records spans for all operations (publish, consume, ack, etc.)
	// and supports querying by trace ID, time range, or custom criteria.
	//
	// STORAGE:
	//   - In-memory ring buffer for fast access to recent traces
	//   - File-based JSON export for persistence (optional)
	//   - OTLP/Jaeger export for external systems (optional)
	//
	// STARTUP FLOW:
	//   1. Create tracer with data directory for files
	//   2. Configure exporters (ring buffer always enabled)
	//   3. Ready to record spans
	//
	// ==========================================================================
	traceDir := filepath.Join(config.DataDir, "traces")
	tracerConfig := DefaultTracerConfig(traceDir)
	tracerConfig.NodeID = config.NodeID

	tracer, err := NewTracer(tracerConfig)
	if err != nil {
		scheduler.Close()
		groupCoordinator.Close()
		return nil, fmt.Errorf("failed to create tracer: %w", err)
	}
	broker.tracer = tracer

	// ==========================================================================
	// MILESTONE 8: INITIALIZE SCHEMA REGISTRY
	// ==========================================================================
	//
	// The schema registry manages message schemas for validation and evolution.
	// It stores schemas on disk and validates messages against registered schemas
	// during publish.
	//
	// STARTUP FLOW:
	//   1. Create registry with schema storage directory
	//   2. Load existing schemas from disk into memory cache
	//   3. Compile validators for each schema
	//   4. Ready to validate messages
	//
	// ==========================================================================
	schemaRegistryConfig := DefaultSchemaRegistryConfig(config.DataDir)
	schemaRegistry, err := NewSchemaRegistry(schemaRegistryConfig)
	if err != nil {
		tracer.Shutdown()
		scheduler.Close()
		groupCoordinator.Close()
		return nil, fmt.Errorf("failed to create schema registry: %w", err)
	}
	broker.schemaRegistry = schemaRegistry

	// ==========================================================================
	// MILESTONE 9: INITIALIZE TRANSACTION COORDINATOR
	// ==========================================================================
	//
	// The transaction coordinator provides exactly-once semantics through:
	//   - Idempotent producers: Sequence-based deduplication per partition
	//   - Transactions: Atomic writes across multiple partitions/topics
	//   - Zombie fencing: Epoch-based rejection of stale producers
	//
	// STARTUP FLOW:
	//   1. Create coordinator with transaction log directory
	//   2. Load producer state snapshots from disk
	//   3. Replay WAL to recover recent changes
	//   4. Re-initialize in-progress transaction tracking
	//   5. Start heartbeat timeout checker goroutine
	//   6. Start periodic snapshot writer goroutine
	//
	// PERSISTENCE:
	//   - Snapshot: data/transactions/producer_state.json
	//   - WAL: data/transactions/transactions.log
	//
	// ==========================================================================
	txnCoordinatorConfig := DefaultTransactionCoordinatorConfig(config.DataDir)
	transactionCoordinator, err := NewTransactionCoordinator(txnCoordinatorConfig, broker)
	if err != nil {
		schemaRegistry.Close()
		tracer.Shutdown()
		scheduler.Close()
		groupCoordinator.Close()
		return nil, fmt.Errorf("failed to create transaction coordinator: %w", err)
	}
	broker.transactionCoordinator = transactionCoordinator

	// ==========================================================================
	// MILESTONE 10: INITIALIZE CLUSTER COORDINATOR (OPTIONAL)
	// ==========================================================================
	//
	// When ClusterEnabled is true, the broker joins a cluster of nodes.
	// The cluster coordinator handles:
	//   - Node discovery via configured peer list
	//   - Heartbeating and failure detection
	//   - Controller election (single leader per cluster)
	//   - Cluster metadata (topic/partition assignments)
	//
	// In cluster mode, partitions can be distributed across nodes.
	// The controller assigns partitions and manages metadata.
	//
	// If ClusterEnabled is false, broker runs in single-node mode
	// with all partitions local.
	//
	// ==========================================================================
	if config.ClusterEnabled && config.ClusterConfig != nil {
		cc, err := newClusterCoordinator(broker, config.ClusterConfig, logger)
		if err != nil {
			transactionCoordinator.Close()
			schemaRegistry.Close()
			tracer.Shutdown()
			scheduler.Close()
			groupCoordinator.Close()
			return nil, fmt.Errorf("failed to create cluster coordinator: %w", err)
		}
		broker.clusterCoordinator = cc

		// Start cluster operations (bootstrap, join cluster)
		startCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		if err := cc.Start(startCtx); err != nil {
			cancel()
			transactionCoordinator.Close()
			schemaRegistry.Close()
			tracer.Shutdown()
			scheduler.Close()
			groupCoordinator.Close()
			return nil, fmt.Errorf("failed to start cluster coordinator: %w", err)
		}
		cancel()
	}

	// ==========================================================================
	// MILESTONE 12: INITIALIZE COOPERATIVE GROUP COORDINATOR
	// ==========================================================================
	//
	// The cooperative group coordinator provides incremental rebalancing
	// (Kafka KIP-429 style) that minimizes consumer downtime.
	//
	// KEY BENEFITS:
	//   - Consumers keep processing unaffected partitions during rebalance
	//   - Only partitions that need to move are revoked
	//   - Sticky assignment minimizes unnecessary partition movement
	//   - Two-phase protocol (revoke → assign) ensures clean handoff
	//
	// CONFIGURATION:
	//   - Default assignment strategy: Sticky (minimize moves)
	//   - Revocation timeout: 60 seconds
	//   - Supported protocols: Cooperative (incremental), Eager (legacy)
	//
	// This wraps the existing GroupCoordinator and adds cooperative features.
	// Groups can choose their protocol (cooperative or eager) at creation time.
	//
	// ==========================================================================
	coopConfig := DefaultCooperativeGroupConfig()
	cooperativeGroupCoordinator := NewCooperativeGroupCoordinator(groupCoordinator, coopConfig)
	broker.cooperativeGroupCoordinator = cooperativeGroupCoordinator

	// ==========================================================================
	// MILESTONE 18: INITIALIZE TENANT MANAGER (OPTIONAL)
	// ==========================================================================
	//
	// Multi-tenancy is OPTIONAL and disabled by default. When disabled:
	//   - No TenantManager is created
	//   - NoOpEnforcer is used (zero overhead quota checks)
	//   - No namespace prefixing
	//   - Topics are accessed directly by name
	//
	// When enabled (config.EnableMultiTenancy = true):
	//   - TenantManager handles tenant CRUD and quota enforcement
	//   - TenantQuotaEnforcer provides actual quota checks
	//   - Topics are prefixed with tenant ID: {tenantID}.{topicName}
	//   - Usage tracking and statistics are maintained
	//
	// QUOTA ENFORCER STRATEGY:
	//   - NoOpEnforcer: All checks return nil (single-tenant, zero cost)
	//   - TenantQuotaEnforcer: Actual quota checks via QuotaManager
	//   This eliminates scattered `if tenantManager != nil` checks.
	//
	// USE CASES:
	//   - Single-tenant (default): K8s deployments where each customer gets own cluster
	//   - Multi-tenant: Managed service / SaaS deployments
	//
	// ==========================================================================
	if config.EnableMultiTenancy {
		tenantManagerConfig := DefaultTenantManagerConfig(config.DataDir)
		tenantManager, err := NewTenantManager(tenantManagerConfig)
		if err != nil {
			if broker.clusterCoordinator != nil {
				broker.clusterCoordinator.Stop(context.Background())
			}
			transactionCoordinator.Close()
			schemaRegistry.Close()
			tracer.Shutdown()
			scheduler.Close()
			groupCoordinator.Close()
			return nil, fmt.Errorf("failed to create tenant manager: %w", err)
		}
		broker.tenantManager = tenantManager
		broker.quotaEnforcer = NewTenantQuotaEnforcer(tenantManager)
		logger.Info("multi-tenancy enabled")
	} else {
		// Single-tenant mode: use no-op enforcer (zero overhead)
		broker.quotaEnforcer = NewNoOpEnforcer()
	}

	// Discover and load existing topics
	if err := broker.loadExistingTopics(); err != nil {
		if broker.clusterCoordinator != nil {
			broker.clusterCoordinator.Stop(context.Background())
		}
		transactionCoordinator.Close()
		schemaRegistry.Close()
		tracer.Shutdown()
		scheduler.Close()
		groupCoordinator.Close()
		return nil, fmt.Errorf("failed to load existing topics: %w", err)
	}

	// Log startup info
	clusterMode := "single-node"
	if config.ClusterEnabled {
		clusterMode = "cluster"
	}

	// Build multi-tenancy status
	multiTenancyStatus := "disabled"
	if config.EnableMultiTenancy {
		multiTenancyStatus = "enabled"
	}

	logger.Info("broker started",
		"mode", clusterMode,
		"nodeID", config.NodeID,
		"dataDir", config.DataDir,
		"topics", len(broker.topics),
		"visibility_timeout_ms", reliabilityConfig.VisibilityTimeoutMs,
		"max_retries", reliabilityConfig.MaxRetries,
		"dlq_enabled", reliabilityConfig.DLQEnabled,
		"delay_scheduler", "enabled",
		"max_delay", schedulerConfig.MaxDelay.String(),
		"tracing", tracerConfig.Enabled,
		"schema_registry", "enabled",
		"transactions", "enabled",
		"txn_timeout_ms", txnCoordinatorConfig.TransactionTimeoutMs,
		"heartbeat_interval_ms", txnCoordinatorConfig.HeartbeatIntervalMs,
		"multi_tenancy", multiTenancyStatus)

	return broker, nil
}

// loadExistingTopics discovers and loads topics from disk.
func (b *Broker) loadExistingTopics() error {
	entries, err := os.ReadDir(b.logsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No topics yet
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		topicName := entry.Name()
		b.logger.Info("loading topic", "topic", topicName)

		topic, err := LoadTopic(b.logsDir, topicName)
		if err != nil {
			b.logger.Error("failed to load topic",
				"topic", topicName,
				"error", err)
			// Continue loading other topics
			continue
		}

		b.topics[topicName] = topic

		// Register topic with group coordinator for partition assignment
		b.groupCoordinator.RegisterTopic(topicName, topic.NumPartitions())

		b.logger.Info("loaded topic",
			"topic", topicName,
			"partitions", topic.NumPartitions(),
			"messages", topic.TotalMessages())
	}

	return nil
}

// Close shuts down the broker gracefully.
//
// SHUTDOWN PROCESS:
//  1. Stop accepting new requests
//  2. Close group coordinator (flushes offsets)
//  3. Sync all topics to disk
//  4. Close all topics
//  5. Release resources
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	b.logger.Info("shutting down broker")

	var errs []error

	// ==========================================================================
	// CLUSTER SHUTDOWN (M10)
	// ==========================================================================
	// Leave cluster gracefully FIRST so other nodes know we're departing.
	// This allows the cluster to:
	//   1. Transfer partition leadership
	//   2. Update membership state
	//   3. Potentially trigger controller election
	// ==========================================================================
	if b.clusterCoordinator != nil {
		b.logger.Info("leaving cluster")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := b.clusterCoordinator.Stop(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("cluster coordinator: %w", err))
		}
		cancel()
	}

	// Close scheduler first (stops timer processing, flushes delay indices)
	if b.scheduler != nil {
		if err := b.scheduler.Close(); err != nil {
			errs = append(errs, fmt.Errorf("scheduler: %w", err))
		}
	}

	// Close tracer (flushes pending spans to file)
	if b.tracer != nil {
		if err := b.tracer.Shutdown(); err != nil {
			errs = append(errs, fmt.Errorf("tracer: %w", err))
		}
	}

	// Close schema registry
	if b.schemaRegistry != nil {
		if err := b.schemaRegistry.Close(); err != nil {
			errs = append(errs, fmt.Errorf("schema registry: %w", err))
		}
	}

	// Close transaction coordinator (flushes snapshots, completes pending transactions)
	// Must happen before topics close since it may need to write control records
	if b.transactionCoordinator != nil {
		if err := b.transactionCoordinator.Close(); err != nil {
			errs = append(errs, fmt.Errorf("transaction coordinator: %w", err))
		}
	}

	// Close ACK manager (stops visibility tracking, flushes retry queue)
	if b.ackManager != nil {
		if err := b.ackManager.Close(); err != nil {
			errs = append(errs, fmt.Errorf("ack manager: %w", err))
		}
	}

	// Close group coordinator (flushes pending offset commits)
	if b.groupCoordinator != nil {
		if err := b.groupCoordinator.Close(); err != nil {
			errs = append(errs, fmt.Errorf("group coordinator: %w", err))
		}
	}

	// Close tenant manager (flushes tenant configs and usage data)
	if b.tenantManager != nil {
		if err := b.tenantManager.Close(); err != nil {
			errs = append(errs, fmt.Errorf("tenant manager: %w", err))
		}
	}

	for name, topic := range b.topics {
		if err := topic.Close(); err != nil {
			errs = append(errs, fmt.Errorf("topic %s: %w", name, err))
		}
	}

	b.closed = true
	b.logger.Info("broker shutdown complete")

	if len(errs) > 0 {
		return fmt.Errorf("errors during shutdown: %v", errs)
	}
	return nil
}

// IsClosed returns true if the broker has been closed.
func (b *Broker) IsClosed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.closed
}

// =============================================================================
// TOPIC MANAGEMENT
// =============================================================================

// CreateTopic creates a new topic with the given configuration.
//
// PARAMETERS:
//   - config: Topic configuration (name, partitions, retention)
//
// RETURNS:
//   - Error if topic already exists or creation fails
//
// NOTE: Topic creation is idempotent in behavior but returns error if exists.
// This matches Kafka's behavior.
func (b *Broker) CreateTopic(config TopicConfig) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBrokerClosed
	}

	// Check if topic already exists
	if _, exists := b.topics[config.Name]; exists {
		return fmt.Errorf("%w: %s", ErrTopicExists, config.Name)
	}

	// Create topic
	topic, err := NewTopic(b.logsDir, config)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	b.topics[config.Name] = topic

	// Register with group coordinator for consumer group partition assignment
	b.groupCoordinator.RegisterTopic(config.Name, config.NumPartitions)

	b.logger.Info("created topic",
		"topic", config.Name,
		"partitions", config.NumPartitions)

	return nil
}

// DeleteTopic removes a topic and all its data.
//
// WARNING: This permanently deletes all messages in the topic!
func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBrokerClosed
	}

	topic, exists := b.topics[name]
	if !exists {
		return fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}

	// Delete topic (closes and removes files)
	if err := topic.Delete(); err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	delete(b.topics, name)

	// Unregister from group coordinator
	b.groupCoordinator.UnregisterTopic(name)

	b.logger.Info("deleted topic", "topic", name)

	return nil
}

// GetTopic returns a topic by name.
func (b *Broker) GetTopic(name string) (*Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, ErrBrokerClosed
	}

	topic, exists := b.topics[name]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}

	return topic, nil
}

// ListTopics returns names of all topics.
func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	return names
}

// TopicExists checks if a topic exists.
func (b *Broker) TopicExists(name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	_, exists := b.topics[name]
	return exists
}

// =============================================================================
// PRODUCER INTERFACE
// =============================================================================

// Publish writes a message to a topic.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//
// RETURNS:
//   - Partition the message was written to
//   - Offset within that partition
//   - Error if publish fails
//
// This is the main producer API. It:
//  1. Looks up the topic
//  2. Routes to appropriate partition (by key hash or round-robin)
//  3. Appends message to partition's log
//  4. Returns offset for producer acknowledgment
func (b *Broker) Publish(topic string, key, value []byte) (partition int, offset int64, err error) {
	return b.PublishWithTrace(topic, key, value, TraceContext{})
}

// PublishWithTrace writes a message with trace context propagation.
// If traceCtx is empty (zero TraceID), a new trace is started.
// This enables end-to-end tracing across services.
func (b *Broker) PublishWithTrace(topic string, key, value []byte, traceCtx TraceContext) (partition int, offset int64, err error) {
	// =========================================================================
	// METRICS: Start timing for latency measurement
	// =========================================================================
	publishStart := time.Now()

	// Start or continue trace
	var ctx TraceContext
	if traceCtx.TraceID.IsZero() {
		ctx = b.tracer.StartTrace(topic, 0, 0) // offset unknown yet
	} else {
		ctx = traceCtx
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, 0, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	// =========================================================================
	// MILESTONE 8: SCHEMA VALIDATION
	// =========================================================================
	//
	// If schema validation is enabled for this topic (subject), validate the
	// message value against the registered schema before persisting.
	//
	// VALIDATION FLOW:
	//   1. Check if validation is enabled for topic
	//   2. Get latest schema for topic (subject = topic name)
	//   3. Validate message JSON against schema
	//   4. Reject with 400 error if invalid
	//
	// This ensures data quality at publish time - invalid messages never enter
	// the log, preventing downstream consumer failures.
	//
	// =========================================================================
	schemaStart := time.Now()
	if err := b.schemaRegistry.ValidateMessage(topic, value); err != nil {
		// Record validation failure span
		if !ctx.TraceID.IsZero() {
			span := NewSpan(ctx.TraceID, SpanEventValidationFailed, topic, 0, 0)
			span.WithError(err)
			span.WithAttribute("error_type", "schema_validation")
			b.tracer.RecordSpan(span)
		}
		// METRICS: Record schema validation failure
		InstrumentSchemaValidation(topic, false, schemaStart)
		InstrumentPublishError(topic, "validation")
		return 0, 0, fmt.Errorf("schema validation failed: %w", err)
	}
	// METRICS: Record schema validation success (if validation was performed)
	InstrumentSchemaValidation(topic, true, schemaStart)

	// Record publish.received span
	if !ctx.TraceID.IsZero() {
		span := NewSpan(ctx.TraceID, SpanEventPublishReceived, topic, 0, 0)
		span.WithAttribute("key_size", fmt.Sprintf("%d", len(key)))
		span.WithAttribute("value_size", fmt.Sprintf("%d", len(value)))
		b.tracer.RecordSpan(span)
	}

	// Perform publish (includes partitioning and persistence)
	partition, offset, err = t.Publish(key, value)
	if err != nil {
		// Record error span
		if !ctx.TraceID.IsZero() {
			span := NewSpan(ctx.TraceID, SpanEventPublishReceived, topic, partition, offset)
			span.WithError(err)
			b.tracer.RecordSpan(span)
		}
		// METRICS: Record publish error
		InstrumentPublishError(topic, "storage")
		return 0, 0, err
	}

	// Record publish.persisted span
	if !ctx.TraceID.IsZero() {
		span := NewSpan(ctx.TraceID, SpanEventPublishPersisted, topic, partition, offset)
		b.tracer.RecordSpan(span)
	}

	// =========================================================================
	// METRICS: Record successful publish
	// =========================================================================
	InstrumentPublish(topic, len(value), publishStart)

	return partition, offset, nil
}

// PublishBatch writes multiple messages to a topic.
// All messages are written to appropriate partitions based on their keys.
//
// RETURNS:
//   - Slice of results (partition, offset) for each message
//   - Error if any publish fails (partial writes may have occurred)
func (b *Broker) PublishBatch(topic string, messages []struct {
	Key   []byte
	Value []byte
}) ([]struct {
	Partition int
	Offset    int64
}, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	results := make([]struct {
		Partition int
		Offset    int64
	}, len(messages))

	for i, msg := range messages {
		partition, offset, err := t.Publish(msg.Key, msg.Value)
		if err != nil {
			return results[:i], fmt.Errorf("failed at message %d: %w", i, err)
		}
		results[i] = struct {
			Partition int
			Offset    int64
		}{partition, offset}
	}

	return results, nil
}

// =============================================================================
// PRIORITY-AWARE PRODUCER INTERFACE (M6)
// =============================================================================

// PublishWithPriority writes a message with specified priority to a topic.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//   - priority: Message priority (Critical, High, Normal, Low, Background)
//
// Priority determines the order of delivery to consumers when using
// ConsumeByPriority or ConsumeByPriorityWFQ.
//
// EXAMPLE:
//
//	// High priority payment message
//	p, o, err := broker.PublishWithPriority("orders", orderID, data, storage.PriorityHigh)
//
//	// Background analytics event
//	p, o, err := broker.PublishWithPriority("events", nil, data, storage.PriorityBackground)
func (b *Broker) PublishWithPriority(topic string, key, value []byte, priority storage.Priority) (partition int, offset int64, err error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, 0, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	return t.PublishWithPriority(key, value, priority)
}

// PublishBatchWithPriority writes multiple messages with priorities to a topic.
//
// Each message can have its own priority, enabling mixed-priority batch writes.
//
// RETURNS:
//   - Slice of results (partition, offset) for each message
//   - Error if any publish fails (partial writes may have occurred)
func (b *Broker) PublishBatchWithPriority(topic string, messages []struct {
	Key      []byte
	Value    []byte
	Priority storage.Priority
}) ([]struct {
	Partition int
	Offset    int64
}, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	results := make([]struct {
		Partition int
		Offset    int64
	}, len(messages))

	for i, msg := range messages {
		partition, offset, err := t.PublishWithPriority(msg.Key, msg.Value, msg.Priority)
		if err != nil {
			return results[:i], fmt.Errorf("failed at message %d: %w", i, err)
		}
		results[i] = struct {
			Partition int
			Offset    int64
		}{partition, offset}
	}

	return results, nil
}

// =============================================================================
// TRANSACTIONAL PRODUCER INTERFACE (M9)
// =============================================================================
//
// These methods implement the TransactionBroker interface required by the
// TransactionCoordinator for exactly-once semantics.
//
// KAFKA COMPARISON:
//   - Kafka: initTransactions(), beginTransaction(), send(), commitTransaction()
//   - goqueue: InitProducerId(), BeginTransaction(), PublishTransactional(), CommitTransaction()
//
// FLOW FOR TRANSACTIONAL PUBLISH:
//
//   ┌──────────────────────────────────────────────────────────────────────────┐
//   │  1. InitProducerId(txn.id)                                               │
//   │     └── Returns PID=123, Epoch=1                                         │
//   │                                                                          │
//   │  2. BeginTransaction(PID, Epoch)                                         │
//   │     └── Transaction state: Empty → Ongoing                               │
//   │                                                                          │
//   │  3. PublishTransactional(topic, key, value, PID, Epoch, seq)             │
//   │     └── Validates sequence number (deduplication)                        │
//   │     └── Writes message to partition log                                  │
//   │     └── Records partition in transaction                                 │
//   │                                                                          │
//   │  4. CommitTransaction(PID, Epoch)                                        │
//   │     └── Writes COMMIT control record to all partitions                   │
//   │     └── Transaction state: Ongoing → PrepareCommit → CompleteCommit      │
//   └──────────────────────────────────────────────────────────────────────────┘
//

// WriteControlRecord implements TransactionBroker interface.
// Writes a control record (COMMIT or ABORT marker) to a specific partition.
//
// Control records are special messages that mark transaction boundaries.
// They're used by consumers with read_committed isolation to filter out
// messages from aborted transactions.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - isCommit: true for COMMIT marker, false for ABORT marker
//   - producerId: Producer's unique identifier
//   - epoch: Producer's current epoch
//   - transactionalId: Transaction's string identifier
//
// WIRE FORMAT:
// The control record is written as a regular message with:
//   - FlagControlRecord set in Flags byte
//   - FlagTransactionCommit set if isCommit is true
//   - Value contains serialized ControlRecordPayload (PID, Epoch, TxnId)
//
// COMPARISON:
//   - Kafka: Uses ControlRecordType in special batch format
//   - goqueue: Uses flags byte in standard message format (simpler)
func (b *Broker) WriteControlRecord(topic string, partition int, isCommit bool, producerId int64, epoch int16, transactionalId string) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	// Create the control record message
	// Note: storage package uses uint64/uint16 for PID/Epoch, so we convert here
	var controlMsg *storage.Message
	if isCommit {
		controlMsg = storage.NewCommitControlRecord(0, uint64(producerId), uint16(epoch), transactionalId)
	} else {
		controlMsg = storage.NewAbortControlRecord(0, uint64(producerId), uint16(epoch), transactionalId)
	}

	// Write to the partition using PublishMessageToPartition to preserve Flags
	// The old approach (PublishToPartition(key, value)) lost the FlagControlRecord flag!
	offset, err := t.PublishMessageToPartition(partition, controlMsg)
	if err != nil {
		return fmt.Errorf("failed to write control record: %w", err)
	}

	// Log the control record for debugging
	recordType := "COMMIT"
	if !isCommit {
		recordType = "ABORT"
	}
	b.logger.Debug("wrote control record",
		"type", recordType,
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"producer_id", producerId,
		"epoch", epoch,
		"txn_id", transactionalId)

	return nil
}

// ClearUncommittedTransaction implements TransactionBroker interface.
// Clears tracked uncommitted offsets when a transaction commits or aborts.
//
// PARAMETERS:
//   - txnId: The transaction ID to clear
//
// RETURNS:
//   - List of offsets that were cleared (for abort filtering)
//
// WHY BOTH COMMIT AND ABORT CALL THIS:
//
//	COMMIT CASE:
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│  Transaction commits → offsets should become visible to consumers       │
//	│  1. Clear from uncommittedTracker (returns offsets, but we don't need   │
//	│     them since messages should be visible)                              │
//	│  2. COMMIT control record already written to log                        │
//	│  3. Consumers can now read these offsets                                │
//	└─────────────────────────────────────────────────────────────────────────┘
//
//	ABORT CASE:
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│  Transaction aborts → offsets remain invisible forever                  │
//	│  1. Clear from uncommittedTracker (returns offsets)                     │
//	│  2. MarkTransactionAborted() moves offsets to abortedTracker            │
//	│  3. Consumers filter aborted offsets via abortedTracker                 │
//	└─────────────────────────────────────────────────────────────────────────┘
func (b *Broker) ClearUncommittedTransaction(txnId string) []partitionOffset {
	if b.uncommittedTracker == nil {
		return nil
	}

	cleared := b.uncommittedTracker.ClearTransaction(txnId)
	b.logger.Debug("cleared uncommitted offsets for transaction",
		"txn_id", txnId,
		"offsets_cleared", len(cleared))
	return cleared
}

// MarkTransactionAborted implements TransactionBroker interface.
// Marks offsets from an aborted transaction as permanently invisible.
//
// PARAMETERS:
//   - offsets: List of offsets returned from ClearUncommittedTransaction
//
// WHEN CALLED:
//
//	Only when a transaction aborts, after ClearUncommittedTransaction.
//
// FLOW:
//
//	AbortTransaction → ClearUncommittedTransaction → MarkTransactionAborted
//
// The offsets will be filtered during consume operations forever.
func (b *Broker) MarkTransactionAborted(offsets []partitionOffset) {
	if b.abortedTracker == nil || len(offsets) == 0 {
		return
	}

	b.abortedTracker.MarkAborted(offsets)
	b.logger.Debug("marked offsets as aborted",
		"count", len(offsets))
}

// PublishTransactional writes a message as part of an active transaction.
// This is the primary method for transactional producers to publish messages.
//
// SEQUENCE VALIDATION:
// The sequence number is validated against the expected sequence for this
// partition. If the sequence is lower than expected, it's a duplicate and
// will be silently ignored. If the sequence is higher than expected, an
// error is returned (missing sequence).
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Target partition (or -1 for automatic routing)
//   - key: Routing key (used for partition selection if partition=-1)
//   - value: Message payload
//   - producerId: Producer's unique identifier
//   - epoch: Producer's current epoch (for zombie fencing)
//   - sequence: Sequence number for this partition (for deduplication)
//
// RETURNS:
//   - partition: Actual partition the message was written to
//   - offset: Offset assigned to the message
//   - duplicate: true if this was a duplicate (already seen sequence)
//   - error: If validation fails or write fails
//
// FLOW:
//
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│  1. Validate producer epoch (zombie fencing)                            │
//	│     └── Reject if epoch < current epoch for this PID                    │
//	│                                                                         │
//	│  2. Check sequence number (deduplication)                               │
//	│     └── If seq < expected: duplicate, return (true, nil)                │
//	│     └── If seq > expected: error (missing sequence)                     │
//	│     └── If seq == expected: proceed                                     │
//	│                                                                         │
//	│  3. Determine partition                                                 │
//	│     └── If partition >= 0: use specified partition                      │
//	│     └── If partition == -1: use key-based routing                       │
//	│                                                                         │
//	│  4. Write message to partition log                                      │
//	│                                                                         │
//	│  5. Update sequence tracking (expected = seq + 1)                       │
//	│                                                                         │
//	│  6. Register partition with transaction coordinator                     │
//	│     └── So coordinator knows where to write control records             │
//	└─────────────────────────────────────────────────────────────────────────┘
func (b *Broker) PublishTransactional(
	topic string,
	partition int,
	key, value []byte,
	producerId int64,
	epoch int16,
	sequence int32,
) (actualPartition int, offset int64, duplicate bool, err error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0, false, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, 0, false, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	numPartitions := t.NumPartitions()
	b.mu.RUnlock()

	// Step 1: Determine actual partition (if not specified)
	actualPartition = partition
	if partition < 0 {
		// Use key-based routing
		if len(key) > 0 {
			actualPartition = int(murmur3Hash(key)) % numPartitions
		} else {
			// Round-robin (simplified - in production, use per-producer counter)
			actualPartition = int(time.Now().UnixNano()) % numPartitions
		}
	}

	if actualPartition < 0 || actualPartition >= numPartitions {
		return 0, 0, false, fmt.Errorf("invalid partition %d (topic has %d partitions)", partition, numPartitions)
	}

	// Step 2: Check sequence number with transaction coordinator
	// Build ProducerIdAndEpoch for the check
	pid := ProducerIdAndEpoch{
		ProducerId: producerId,
		Epoch:      epoch,
	}

	// CheckSequence returns (existingOffset, isDuplicate, error)
	// We pass 0 for offset since we don't have it yet - it will be assigned after write
	existingOffset, isDuplicate, err := b.transactionCoordinator.CheckSequence(pid, topic, actualPartition, sequence, 0)
	if err != nil {
		return 0, 0, false, fmt.Errorf("sequence check failed: %w", err)
	}

	if isDuplicate {
		// Duplicate message - return success without writing
		// This is idempotent behavior: same sequence returns success
		b.logger.Debug("duplicate message detected",
			"producer_id", producerId,
			"topic", topic,
			"partition", actualPartition,
			"sequence", sequence,
			"existing_offset", existingOffset)
		// METRICS: Record duplicate rejection for observability
		InstrumentDuplicateRejected()
		return actualPartition, existingOffset, true, nil
	}

	// Step 3: Write message to partition
	offset, err = t.PublishToPartition(actualPartition, key, value)
	if err != nil {
		return actualPartition, 0, false, fmt.Errorf("failed to publish: %w", err)
	}

	// =========================================================================
	// STEP 4: TRACK UNCOMMITTED OFFSET FOR read_committed ISOLATION
	// =========================================================================
	//
	// The message is now written to the log but the transaction is not committed.
	// Consumers with read_committed isolation should NOT see this message until
	// the transaction commits.
	//
	// We track the offset in uncommittedTracker so consume operations can filter
	// it out. When the transaction commits/aborts, we clear the tracking.
	//
	// NOTE: We lookup the transaction ID by producerId+epoch. If the producer
	// is in an active transaction, we track the offset. If not (non-transactional
	// publish), we don't track it.
	//
	// =========================================================================
	state := b.transactionCoordinator.GetProducerStateByProducerId(producerId, epoch)
	b.logger.Debug("looked up producer state by ID",
		"producer_id", producerId,
		"epoch", epoch,
		"state_found", state != nil)
	if state != nil {
		b.logger.Debug("producer state details",
			"state", state.State,
			"current_txn_id", state.CurrentTransactionId)
		if state.State == TransactionStateOngoing && state.CurrentTransactionId != "" {
			b.uncommittedTracker.Track(
				topic,
				actualPartition,
				offset,
				state.CurrentTransactionId,
				producerId,
				epoch,
			)
			b.logger.Debug("tracked uncommitted offset",
				"topic", topic,
				"partition", actualPartition,
				"offset", offset,
				"txn_id", state.CurrentTransactionId)
		}
	}

	b.logger.Debug("published transactional message",
		"producer_id", producerId,
		"epoch", epoch,
		"topic", topic,
		"partition", actualPartition,
		"offset", offset,
		"sequence", sequence)

	return actualPartition, offset, false, nil
}

// GetTransactionCoordinator returns the transaction coordinator for external access.
// Used by HTTP handlers to expose transaction APIs.
func (b *Broker) GetTransactionCoordinator() *TransactionCoordinator {
	return b.transactionCoordinator
}

// GetAckManager returns the acknowledgment manager for external access.
// Used by gRPC handlers to process ACK/NACK/REJECT operations.
//
// RETURNS:
//   - *AckManager if acknowledgment is enabled (M4)
//   - nil if not configured
func (b *Broker) GetAckManager() *AckManager {
	return b.ackManager
}

// GetGroupCoordinator returns the consumer group coordinator for external access.
// Used by gRPC handlers to manage consumer group membership and offset commits.
//
// RETURNS:
//   - *GroupCoordinator if consumer groups are enabled (M3)
//   - nil if not configured
func (b *Broker) GetGroupCoordinator() *GroupCoordinator {
	return b.groupCoordinator
}

// =============================================================================
// CONSUMER INTERFACE
// =============================================================================

// Consume reads messages from a topic partition in priority order.
//
// DEFAULT BEHAVIOR: Messages are returned highest-priority-first.
//   - Priority 0 (Critical) before Priority 1 (High)
//   - Within same priority, FIFO order is maintained
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset (inclusive) - message at this offset can be returned
//   - maxMessages: Max messages to return (0 = no limit)
//
// OFFSET SEMANTICS: All consume methods use INCLUSIVE offsets.
//
//	fromOffset = 0 → returns message at offset 0 (if exists)
//	fromOffset = 5 → returns messages starting from offset 5
//
// RETURNS:
//   - Slice of messages (may be empty if no new messages)
//   - Error if read fails
//
// For offset-sequential consumption (Kafka-like FIFO), use ConsumeByOffset().
//
// FILTERING (read_committed isolation):
// This method filters out messages that should not be visible to consumers:
//  1. Control records (transaction markers) - handled in Partition.Consume
//  2. Delayed messages - messages scheduled for future delivery
//  3. Uncommitted transactions - messages from in-progress transactions
//
// This provides read_committed semantics: consumers only see messages from
// committed transactions. The uncommittedTracker maintains offsets belonging
// to active transactions and filters them during consume.
func (b *Broker) Consume(topic string, partition int, fromOffset int64, maxMessages int) ([]Message, error) {
	// =========================================================================
	// METRICS: Start timing for latency measurement
	// =========================================================================
	consumeStart := time.Now()

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	// =========================================================================
	// LOOP-BASED FETCH WITH FILTERING
	// =========================================================================
	//
	// PROBLEM: After filtering (control records, delayed, uncommitted, aborted),
	//          we might return fewer messages than requested.
	//
	// SOLUTION: Fetch in a loop with multiplier until we have enough messages.
	//           - fetchMultiplier = 3 (fetch 3x more than needed)
	//           - maxAttempts = 5 (avoid infinite loops on sparse partitions)
	//
	// EXAMPLE:
	//   Consumer requests 10 messages
	//   → Fetch 30 from storage
	//   → After filtering: 7 messages remain
	//   → Fetch another 9 messages (3x the shortfall)
	//   → After filtering: 10 total messages
	//   → Return 10 to consumer
	//
	// FILTERS APPLIED:
	//   1. Control records (COMMIT/ABORT markers)
	//   2. Delayed messages (deliverAt > now)
	//   3. Uncommitted transactions (read_committed isolation)
	//   4. Aborted transactions (permanently hidden)
	//
	// =========================================================================

	const maxAttempts = 5
	const fetchMultiplier = 2

	var allFiltered []*storage.Message
	currentOffset := fromOffset

	for attempt := 0; attempt < maxAttempts && len(allFiltered) < maxMessages; attempt++ {
		// Calculate how many more messages we need
		needed := maxMessages - len(allFiltered)
		fetchSize := needed * fetchMultiplier

		// Fetch from storage
		storageMessages, err := t.Consume(partition, currentOffset, fetchSize)
		if err != nil {
			return nil, err
		}

		// No more messages available
		if len(storageMessages) == 0 {
			break
		}

		// Apply filters to this batch
		for _, sm := range storageMessages {
			// Filter out control records (commit/abort markers)
			if sm.IsControlRecord() {
				continue
			}
			// Check if delayed
			if b.IsDelayed(topic, partition, sm.Offset) {
				continue
			}
			// Check uncommitted transaction
			if b.uncommittedTracker != nil && b.uncommittedTracker.IsUncommitted(topic, partition, sm.Offset) {
				continue
			}
			// Check aborted transaction
			if b.abortedTracker != nil && b.abortedTracker.IsAborted(topic, partition, sm.Offset) {
				continue
			}
			allFiltered = append(allFiltered, sm)

			// Stop if we have enough
			if len(allFiltered) >= maxMessages {
				break
			}
		}

		// Update offset for next iteration
		if len(storageMessages) > 0 {
			currentOffset = storageMessages[len(storageMessages)-1].Offset + 1
		}
	}

	// Limit to requested amount (in case we got more)
	filteredMessages := allFiltered
	if len(filteredMessages) > maxMessages {
		filteredMessages = filteredMessages[:maxMessages]
	}

	// Convert storage.Message to broker.Message for API
	messages := make([]Message, len(filteredMessages))
	for i, sm := range filteredMessages {
		messages[i] = Message{
			Topic:     topic,
			Partition: partition,
			Offset:    sm.Offset,
			Timestamp: time.Unix(0, sm.Timestamp),
			Key:       sm.Key,
			Value:     sm.Value,
			Priority:  sm.Priority,
		}

		// Record consume.fetched span for each message
		// Note: We don't have trace context from message headers yet (M7 enhancement)
		// For now, create a new trace for each consumed message
		ctx := b.tracer.StartTrace(topic, partition, sm.Offset)
		if !ctx.TraceID.IsZero() {
			span := NewSpan(ctx.TraceID, SpanEventConsumeFetched, topic, partition, sm.Offset)
			span.WithAttribute("priority", fmt.Sprintf("%d", sm.Priority))
			b.tracer.RecordSpan(span)
		}
	}

	// =========================================================================
	// METRICS: Record successful consume operation
	// =========================================================================
	// Calculate total bytes consumed for metrics
	totalBytes := 0
	for _, msg := range messages {
		totalBytes += len(msg.Value)
	}
	// consumerGroup is empty here since this is direct partition consumption
	// Consumer group consumption goes through ConsumerGroup.Consume() which has group context
	InstrumentConsume("", topic, len(messages), totalBytes, consumeStart)

	return messages, nil
}

// ConsumeByOffset reads messages sequentially by offset (FIFO, ignoring priority).
// This is Kafka-like behavior where you consume the log in strict offset order.
//
// Use this when:
//   - You need strict offset ordering (e.g., replication, replay)
//   - You want to consume ALL messages regardless of priority
//   - You're treating the partition as an append-only log
//
// FILTERING: Same as Consume() - delayed messages are filtered out.
func (b *Broker) ConsumeByOffset(topic string, partition int, fromOffset int64, maxMessages int) ([]Message, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	// Loop-based fetch with multiplier to handle filtering
	const maxAttempts = 5
	const fetchMultiplier = 2

	var allFiltered []*storage.Message
	currentOffset := fromOffset

	for attempt := 0; attempt < maxAttempts && len(allFiltered) < maxMessages; attempt++ {
		needed := maxMessages - len(allFiltered)
		fetchSize := needed * fetchMultiplier

		storageMessages, err := t.ConsumeByOffset(partition, currentOffset, fetchSize)
		if err != nil {
			return nil, err
		}

		if len(storageMessages) == 0 {
			break
		}

		// Apply filters to this batch
		for _, sm := range storageMessages {
			if sm.IsControlRecord() {
				continue
			}
			if b.IsDelayed(topic, partition, sm.Offset) {
				continue
			}
			if b.uncommittedTracker != nil && b.uncommittedTracker.IsUncommitted(topic, partition, sm.Offset) {
				continue
			}
			if b.abortedTracker != nil && b.abortedTracker.IsAborted(topic, partition, sm.Offset) {
				continue
			}
			allFiltered = append(allFiltered, sm)

			if len(allFiltered) >= maxMessages {
				break
			}
		}

		if len(storageMessages) > 0 {
			currentOffset = storageMessages[len(storageMessages)-1].Offset + 1
		}
	}

	// Limit to requested amount
	filteredMessages := allFiltered
	if len(filteredMessages) > maxMessages {
		filteredMessages = filteredMessages[:maxMessages]
	}

	// Convert storage.Message to broker.Message for API
	messages := make([]Message, len(filteredMessages))
	for i, sm := range filteredMessages {
		messages[i] = Message{
			Topic:     topic,
			Partition: partition,
			Offset:    sm.Offset,
			Timestamp: time.Unix(0, sm.Timestamp),
			Key:       sm.Key,
			Value:     sm.Value,
			Priority:  sm.Priority,
		}
	}

	return messages, nil
}

// =============================================================================
// PRIORITY-AWARE CONSUMER INTERFACE (M6)
// =============================================================================

// ConsumeByPriority reads messages from a partition respecting strict priority order.
// Higher priority messages are returned before lower priority ones.
//
// NOTE: This is now the same as default Consume(). Kept for explicit API clarity.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset (inclusive) - message at this offset can be returned
//   - maxMessages: Maximum messages to return
//
// This uses strict priority (Critical first, then High, etc.)
// For Weighted Fair Queuing, use ConsumeByPriorityWFQ.
//
// FILTERING: Same as Consume() - delayed messages are filtered out.
func (b *Broker) ConsumeByPriority(topic string, partition int, fromOffset int64, maxMessages int) ([]Message, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	p, err := t.Partition(partition)
	if err != nil {
		return nil, err
	}

	// Loop-based fetch with multiplier to handle filtering
	const maxAttempts = 5
	const fetchMultiplier = 2

	var allFiltered []*storage.Message
	currentOffset := fromOffset

	for attempt := 0; attempt < maxAttempts && len(allFiltered) < maxMessages; attempt++ {
		needed := maxMessages - len(allFiltered)
		fetchSize := needed * fetchMultiplier

		storageMessages, err := p.ConsumeByPriority(currentOffset, fetchSize)
		if err != nil {
			return nil, err
		}

		if len(storageMessages) == 0 {
			break
		}

		// Apply filters to this batch
		for _, sm := range storageMessages {
			if sm.IsControlRecord() {
				continue
			}
			if b.IsDelayed(topic, partition, sm.Offset) {
				continue
			}
			if b.uncommittedTracker != nil && b.uncommittedTracker.IsUncommitted(topic, partition, sm.Offset) {
				continue
			}
			if b.abortedTracker != nil && b.abortedTracker.IsAborted(topic, partition, sm.Offset) {
				continue
			}
			allFiltered = append(allFiltered, sm)

			if len(allFiltered) >= maxMessages {
				break
			}
		}

		if len(storageMessages) > 0 {
			currentOffset = storageMessages[len(storageMessages)-1].Offset + 1
		}
	}

	// Limit to requested amount
	filteredMessages := allFiltered
	if len(filteredMessages) > maxMessages {
		filteredMessages = filteredMessages[:maxMessages]
	}

	// Convert to API messages
	messages := make([]Message, len(filteredMessages))
	for i, sm := range filteredMessages {
		messages[i] = Message{
			Topic:     topic,
			Partition: partition,
			Offset:    sm.Offset,
			Timestamp: time.Unix(0, sm.Timestamp),
			Key:       sm.Key,
			Value:     sm.Value,
			Priority:  sm.Priority,
		}
	}

	return messages, nil
}

// ConsumeByPriorityWFQ reads messages using Weighted Fair Queuing.
// This provides fair distribution across priorities based on configurable weights.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset (inclusive) - messages below this are filtered
//   - maxMessages: Maximum messages to return
//   - scheduler: The WFQ scheduler to use (maintains fairness state)
//
// NOTE: The scheduler should be maintained across calls for proper WFQ behavior.
// Create one scheduler per consumer for best results.
//
// FILTERING: Same as Consume() - delayed messages are filtered out.
func (b *Broker) ConsumeByPriorityWFQ(topic string, partition int, fromOffset int64, maxMessages int, scheduler *PriorityScheduler) ([]Message, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	p, err := t.Partition(partition)
	if err != nil {
		return nil, err
	}

	// Loop-based fetch with multiplier to handle filtering
	const maxAttempts = 5
	const fetchMultiplier = 2

	var allFiltered []*storage.Message
	currentOffset := fromOffset

	for attempt := 0; attempt < maxAttempts && len(allFiltered) < maxMessages; attempt++ {
		needed := maxMessages - len(allFiltered)
		fetchSize := needed * fetchMultiplier

		storageMessages, err := p.ConsumeByPriorityWFQ(currentOffset, fetchSize, scheduler)
		if err != nil {
			return nil, err
		}

		if len(storageMessages) == 0 {
			break
		}

		// Apply filters to this batch
		for _, sm := range storageMessages {
			if sm.IsControlRecord() {
				continue
			}
			if b.IsDelayed(topic, partition, sm.Offset) {
				continue
			}
			if b.uncommittedTracker != nil && b.uncommittedTracker.IsUncommitted(topic, partition, sm.Offset) {
				continue
			}
			if b.abortedTracker != nil && b.abortedTracker.IsAborted(topic, partition, sm.Offset) {
				continue
			}
			allFiltered = append(allFiltered, sm)

			if len(allFiltered) >= maxMessages {
				break
			}
		}

		if len(storageMessages) > 0 {
			currentOffset = storageMessages[len(storageMessages)-1].Offset + 1
		}
	}

	// Limit to requested amount
	filteredMessages := allFiltered
	if len(filteredMessages) > maxMessages {
		filteredMessages = filteredMessages[:maxMessages]
	}

	// Convert to API messages
	messages := make([]Message, len(filteredMessages))
	for i, sm := range filteredMessages {
		messages[i] = Message{
			Topic:     topic,
			Partition: partition,
			Offset:    sm.Offset,
			Timestamp: time.Unix(0, sm.Timestamp),
			Key:       sm.Key,
			Value:     sm.Value,
			Priority:  sm.Priority,
		}
	}

	return messages, nil
}

// MarkConsumed marks a message as consumed in the priority index.
// Call this after processing to filter out the message from future priority queries.
func (b *Broker) MarkConsumed(topic string, partition int, offset int64) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	p, err := t.Partition(partition)
	if err != nil {
		return err
	}

	p.MarkConsumed(offset)
	return nil
}

// GetOffsetBounds returns the earliest and latest offsets for a partition.
// Useful for consumers to know the valid offset range.
func (b *Broker) GetOffsetBounds(topic string, partition int) (earliest, latest int64, err error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, 0, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	p, err := t.Partition(partition)
	if err != nil {
		return 0, 0, err
	}

	return p.EarliestOffset(), p.LatestOffset(), nil
}

// =============================================================================
// MESSAGE TYPE (API representation)
// =============================================================================

// Message is the API representation of a message.
// This is what consumers receive (includes topic/partition context).
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Priority  storage.Priority // Priority level (0=Critical to 4=Background)
}

// =============================================================================
// BROKER METADATA
// =============================================================================

// Stats returns broker statistics.
type BrokerStats struct {
	NodeID     string
	Uptime     time.Duration
	TopicCount int
	TotalSize  int64
	TopicStats map[string]TopicStats
}

type TopicStats struct {
	Name          string
	Partitions    int
	TotalMessages int64
	TotalSize     int64
}

// =============================================================================
// PRIORITY STATISTICS (MILESTONE 6)
// =============================================================================
//
// Per-priority-per-partition metrics provide maximum granularity for:
//   - Understanding message distribution across priorities
//   - Monitoring queue health at the priority level
//   - Detecting priority imbalances or starvation
//   - Capacity planning based on priority patterns
//
// STRUCTURE:
//   BrokerPriorityStats
//     └── TopicPriorityStats (per topic)
//           └── PartitionPriorityStats (per partition)
//                 └── PriorityLevelStats (per priority level)
//
// COMPARISON WITH OTHER SYSTEMS:
//   - RabbitMQ: Queue-level priority stats only (no partition concept)
//   - Kafka: No native priority support
//   - SQS: Per-queue stats, no priority breakdown
//   - goqueue: Full per-priority-per-partition granularity
//
// =============================================================================

// BrokerPriorityStats provides aggregated priority statistics across all topics.
type BrokerPriorityStats struct {
	// TotalByPriority aggregates message counts across all topics/partitions
	TotalByPriority [5]int64

	// Topics maps topic name to its priority stats
	Topics map[string]*TopicPriorityStats
}

// TopicPriorityStats provides priority statistics for a single topic.
type TopicPriorityStats struct {
	Name string

	// TotalByPriority aggregates message counts across all partitions
	TotalByPriority [5]int64

	// Partitions maps partition ID to its priority stats
	Partitions map[int]*PartitionPriorityStats
}

// PartitionPriorityStats provides detailed priority metrics for a partition.
type PartitionPriorityStats struct {
	PartitionID int

	// Pending counts unconsumed messages at each priority level
	Pending [5]int64

	// Consumed counts messages marked as consumed at each priority
	Consumed [5]int64

	// Total is Pending + Consumed for each priority
	Total [5]int64

	// OldestPending tracks the timestamp of oldest pending message per priority
	// Zero time means no pending messages at that priority
	OldestPending [5]time.Time

	// AvgWaitTime tracks average wait time for consumed messages per priority
	// This helps identify if lower priorities are experiencing starvation
	AvgWaitTime [5]time.Duration
}

func (b *Broker) Stats() BrokerStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := BrokerStats{
		NodeID:     b.config.NodeID,
		Uptime:     time.Since(b.startedAt),
		TopicCount: len(b.topics),
		TopicStats: make(map[string]TopicStats),
	}

	for name, topic := range b.topics {
		ts := TopicStats{
			Name:          name,
			Partitions:    topic.NumPartitions(),
			TotalMessages: topic.TotalMessages(),
			TotalSize:     topic.TotalSize(),
		}
		stats.TopicStats[name] = ts
		stats.TotalSize += ts.TotalSize
	}

	return stats
}

// =============================================================================
// PriorityStats - Per-Priority-Per-Partition Statistics
// =============================================================================
//
// WHAT: Collects detailed priority metrics from all topics and partitions.
//
// WHY: The user requested "per priority per partition" metrics - the most
// granular option. This enables:
//   - Monitoring priority distribution across the cluster
//   - Detecting priority starvation before it impacts SLAs
//   - Understanding message flow patterns by priority
//   - Capacity planning based on priority usage
//
// HOW IT WORKS:
//  1. Iterates through all topics
//  2. For each topic, iterates through all partitions
//  3. Collects PriorityMetrics from each partition's priority index
//  4. Aggregates up to topic and broker levels
//
// PERFORMANCE NOTE:
// This method acquires read locks and iterates the entire broker state.
// For large deployments, consider caching or sampling strategies.
//
// =============================================================================
func (b *Broker) PriorityStats() BrokerPriorityStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := BrokerPriorityStats{
		Topics: make(map[string]*TopicPriorityStats),
	}

	for name, topic := range b.topics {
		topicStats := &TopicPriorityStats{
			Name:       name,
			Partitions: make(map[int]*PartitionPriorityStats),
		}

		// Collect stats from each partition
		numPartitions := topic.NumPartitions()
		for i := 0; i < numPartitions; i++ {
			partition, err := topic.Partition(i)
			if err != nil || partition == nil {
				continue
			}

			// Get priority metrics from the partition's priority index
			// Returns a slice with one snapshot per priority level
			metricsSlice := partition.PriorityMetrics()

			partitionStats := &PartitionPriorityStats{
				PartitionID: i,
			}

			// Process each priority level's metrics
			for _, m := range metricsSlice {
				p := int(m.Priority)
				if p < 0 || p >= 5 {
					continue
				}

				// Pending = unconsumed messages at this priority
				partitionStats.Pending[p] = int64(m.PendingMessages)

				// Total = all-time message count at this priority
				partitionStats.Total[p] = m.TotalMessages

				// Consumed = Total - Pending
				partitionStats.Consumed[p] = m.TotalMessages - int64(m.PendingMessages)

				// OldestPending timestamp conversion
				if m.OldestPendingTimestamp > 0 {
					partitionStats.OldestPending[p] = time.Unix(0, m.OldestPendingTimestamp)
				}

				// Aggregate to topic level
				topicStats.TotalByPriority[p] += m.TotalMessages
			}

			topicStats.Partitions[i] = partitionStats
		}

		// Aggregate to broker level
		for p := 0; p < 5; p++ {
			stats.TotalByPriority[p] += topicStats.TotalByPriority[p]
		}

		stats.Topics[name] = topicStats
	}

	return stats
}

// NodeID returns the broker's node identifier.
func (b *Broker) NodeID() string {
	return b.config.NodeID
}

// DataDir returns the data directory path.
func (b *Broker) DataDir() string {
	return b.config.DataDir
}

// GroupCoordinator returns the broker's consumer group coordinator.
// Used by the API layer for consumer group operations.
func (b *Broker) GroupCoordinator() *GroupCoordinator {
	return b.groupCoordinator
}

// CooperativeGroupCoordinator returns the broker's cooperative group coordinator.
// Used by the API layer for cooperative rebalancing operations (M12).
// Returns nil if cooperative rebalancing is not enabled.
func (b *Broker) CooperativeGroupCoordinator() *CooperativeGroupCoordinator {
	return b.cooperativeGroupCoordinator
}

// Uptime returns how long the broker has been running.
func (b *Broker) Uptime() time.Duration {
	return time.Since(b.startedAt)
}

// =============================================================================
// MILESTONE 4: RELIABILITY LAYER API
// =============================================================================
//
// These methods provide per-message acknowledgment (ACK/NACK/REJECT) on top
// of the Kafka-style offset-based consumption model.
//
// FLOW COMPARISON:
//
//   KAFKA (offset-only):
//   ┌────────┐  poll  ┌────────┐ process ┌────────┐ commit ┌────────┐
//   │Consumer│───────►│Receives│────────►│Process │───────►│Commit  │
//   │        │        │batch   │         │all     │        │offset  │
//   └────────┘        └────────┘         └────────┘        └────────┘
//
//   GOQUEUE (per-message ACK):
//   ┌────────┐  poll  ┌────────┐ process ┌────────┐ ack    ┌────────┐
//   │Consumer│───────►│Receives│────────►│Process │───────►│ACK each│
//   │        │        │+receipt│         │one msg │        │message │
//   └────────┘        └────────┘         └────────┘        └────────┘
//                                              │                │
//                                              │ fail           │ offset
//                                              ▼                │ advances
//                                         ┌────────┐            │
//                                         │NACK/   │────────────┘
//                                         │Reject  │
//                                         └────────┘
//
// =============================================================================

// AckManager returns the broker's ACK manager for per-message acknowledgment.
// Used by the API layer for ACK/NACK/REJECT operations.
func (b *Broker) AckManager() *AckManager {
	return b.ackManager
}

// Tracer returns the broker's message tracer for observability.
// Used by the API layer for trace query operations.
func (b *Broker) Tracer() *Tracer {
	return b.tracer
}

// ReliabilityConfig returns the current reliability configuration.
func (b *Broker) ReliabilityConfig() ReliabilityConfig {
	return b.reliabilityConfig
}

// ConsumeWithReceipts reads messages and tracks them for per-message ACK.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset
//   - maxMessages: Max messages to return
//   - consumerID, groupID: Consumer identification for tracking
//
// RETURNS:
//   - Messages with receipt handles attached
//   - Error if read fails
//
// IMPORTANT:
// Each returned message has a ReceiptHandle that MUST be used for ACK/NACK/REJECT.
// Messages not ACKed within VisibilityTimeout will be redelivered.
func (b *Broker) ConsumeWithReceipts(
	topic string,
	partition int,
	fromOffset int64,
	maxMessages int,
	consumerID, groupID string,
) ([]MessageWithReceipt, error) {
	// First, get the raw messages using existing Consume method
	messages, err := b.Consume(topic, partition, fromOffset, maxMessages)
	if err != nil {
		return nil, err
	}

	// Track each message for per-message ACK and generate receipt handles
	results := make([]MessageWithReceipt, 0, len(messages))
	visibilityTimeout := time.Duration(b.reliabilityConfig.VisibilityTimeoutMs) * time.Millisecond

	for _, msg := range messages {
		// Track delivery and get receipt handle
		receiptHandle, err := b.ackManager.TrackDelivery(&msg, consumerID, groupID, visibilityTimeout)
		if err != nil {
			// Backpressure or tracking error - stop here
			b.logger.Warn("failed to track delivery",
				"topic", topic,
				"partition", partition,
				"offset", msg.Offset,
				"error", err)
			break
		}

		results = append(results, MessageWithReceipt{
			Message:       msg,
			ReceiptHandle: receiptHandle,
		})
	}

	return results, nil
}

// MessageWithReceipt extends Message with a receipt handle for ACK/NACK/REJECT.
type MessageWithReceipt struct {
	Message
	ReceiptHandle string
}

// Ack acknowledges successful processing of a message.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//
// SEMANTICS:
//   - Message is considered fully processed
//   - Will not be redelivered
//   - Committed offset may advance (if contiguous)
func (b *Broker) Ack(receiptHandle string) (*AckResult, error) {
	ackStart := time.Now()
	result, err := b.ackManager.Ack(receiptHandle)
	if err != nil {
		return nil, err
	}

	// Record consume.acked span
	ctx := b.tracer.StartTrace(result.Topic, result.Partition, result.Offset)
	if !ctx.TraceID.IsZero() {
		span := NewSpan(ctx.TraceID, SpanEventConsumeAcked, result.Topic, result.Partition, result.Offset)
		span.WithAttribute("new_committed_offset", fmt.Sprintf("%d", result.NewCommittedOffset))
		span.WithAttribute("offset_advanced", fmt.Sprintf("%t", result.OffsetAdvanced))
		b.tracer.RecordSpan(span)
	}

	// METRICS: Record acknowledgment
	// consumerGroup is empty here - group context comes from ConsumerGroup
	InstrumentAck(result.Topic, "", ackStart)

	return result, nil
}

// Nack indicates processing failed and message should be retried.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//   - reason: Why the message failed (for logging/debugging)
//
// SEMANTICS:
//   - Message will be redelivered after exponential backoff
//   - Each NACK increments delivery count
//   - After MaxRetries, message goes to DLQ
func (b *Broker) Nack(receiptHandle, reason string) (*AckResult, error) {
	nackStart := time.Now()
	result, err := b.ackManager.Nack(receiptHandle, reason)
	if err != nil {
		return nil, err
	}

	// Record consume.nacked span
	ctx := b.tracer.StartTrace(result.Topic, result.Partition, result.Offset)
	if !ctx.TraceID.IsZero() {
		span := NewSpan(ctx.TraceID, SpanEventConsumeNacked, result.Topic, result.Partition, result.Offset)
		span.WithAttribute("reason", reason)
		span.WithAttribute("delivery_count", fmt.Sprintf("%d", result.DeliveryCount))
		if !result.NextVisibleAt.IsZero() {
			span.WithAttribute("next_visible_at", result.NextVisibleAt.Format(time.RFC3339))
		}
		b.tracer.RecordSpan(span)
	}

	// METRICS: Record negative acknowledgment
	InstrumentNack(result.Topic, "", nackStart)

	return result, nil
}

// Reject sends a message directly to the dead letter queue.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//   - reason: Why the message was rejected
//
// SEMANTICS:
//   - Message is considered "poison" (can never succeed)
//   - Immediately routed to DLQ (no retry)
//
// USE CASES:
//   - Message format is invalid
//   - Business logic determines message is unprocessable
func (b *Broker) Reject(receiptHandle, reason string) (*AckResult, error) {
	rejectStart := time.Now()
	result, err := b.ackManager.Reject(receiptHandle, reason)
	if err != nil {
		return nil, err
	}

	// Record consume.rejected span
	ctx := b.tracer.StartTrace(result.Topic, result.Partition, result.Offset)
	if !ctx.TraceID.IsZero() {
		span := NewSpan(ctx.TraceID, SpanEventConsumeRejected, result.Topic, result.Partition, result.Offset)
		span.WithAttribute("reason", reason)
		if result.DLQTopic != "" {
			span.WithAttribute("dlq_topic", result.DLQTopic)
		}
		b.tracer.RecordSpan(span)
	}

	// METRICS: Record message rejection (sent to DLQ)
	InstrumentReject(result.Topic, "", rejectStart)

	return result, nil
}

// ExtendVisibility extends the visibility timeout for a message.
//
// PARAMETERS:
//   - receiptHandle: The receipt handle from ConsumeWithReceipts
//   - extension: Additional time to add
//
// USE CASE:
//   - Processing takes longer than expected
//   - Prevents timeout while still working on message
//
// EXAMPLE:
//
//	// Processing will take longer than 30s visibility timeout
//	if estimatedTime > 25*time.Second {
//	    broker.ExtendVisibility(receipt, 30*time.Second)
//	}
func (b *Broker) ExtendVisibility(receiptHandle string, extension time.Duration) (time.Time, error) {
	return b.ackManager.ExtendVisibility(receiptHandle, extension)
}

// GetConsumerLag returns lag information for a consumer.
func (b *Broker) GetConsumerLag(consumerID, groupID, topic string, partition int) (*ConsumerLag, error) {
	return b.ackManager.GetConsumerLag(consumerID, groupID, topic, partition)
}

// ReliabilityStats returns combined reliability layer statistics.
type ReliabilityStats struct {
	AckManager AckManagerStats
	Visibility VisibilityStats
	DLQ        DLQStats
}

func (b *Broker) ReliabilityStats() ReliabilityStats {
	return ReliabilityStats{
		AckManager: b.ackManager.Stats(),
		Visibility: b.ackManager.visibilityTracker.Stats(),
		DLQ:        b.ackManager.dlqRouter.Stats(),
	}
}

// =============================================================================
// MILESTONE 5: DELAYED MESSAGES API
// =============================================================================
//
// These methods provide delayed and scheduled message delivery capabilities.
// Messages can be published with a delay (relative) or deliverAt (absolute time).
//
// WHY DELAYED MESSAGES?
//
//   Many use cases require messages to be delivered at a future time:
//   - Scheduled tasks: "Send reminder email in 24 hours"
//   - Rate limiting: "Retry this API call in 30 seconds"
//   - Business logic: "Execute trade at market open"
//   - Debouncing: "Wait 5 seconds for more updates before processing"
//
// COMPARISON:
//
//   - Kafka: No native delay support - requires external schedulers or hacky
//     solutions like long-lived consumers that hold messages
//   - RabbitMQ: Plugin required (rabbitmq_delayed_message_exchange) with limits
//   - SQS: DelaySeconds (0-900s = 15 min max) and message timers (up to 15 min)
//   - Redis: ZADD with score as timestamp, poll for ready messages
//   - goqueue: Native support up to 7 days with millisecond precision
//
// FLOW:
//
//   PublishWithDelay(topic, key, value, delay=30s)
//   ┌────────────┐
//   │ Producer   │
//   └─────┬──────┘
//         │
//         ▼
//   ┌────────────────────────────────────────────────────────────────────┐
//   │ 1. Write to Log (immediate, ensures durability)                    │
//   │    → Returns: partition=2, offset=1234                             │
//   └─────┬──────────────────────────────────────────────────────────────┘
//         │
//         ▼
//   ┌────────────────────────────────────────────────────────────────────┐
//   │ 2. Register with Scheduler                                         │
//   │    → Timer wheel entry (in-memory, O(1))                           │
//   │    → Delay index entry (on-disk, crash-safe)                       │
//   └─────┬──────────────────────────────────────────────────────────────┘
//         │
//         │ ... time passes (30 seconds) ...
//         │
//         ▼
//   ┌────────────────────────────────────────────────────────────────────┐
//   │ 3. Timer Fires → handleDelayedMessageReady()                       │
//   │    → Message marked as visible in delay index                      │
//   │    → Normal consumers now see this message                         │
//   └────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

// PublishWithDelay publishes a message that becomes visible after the specified delay.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//   - delay: Duration before message becomes visible (max 7 days)
//
// RETURNS:
//   - Partition the message was written to
//   - Offset within that partition
//   - Error if publish fails or delay is invalid
//
// SEMANTICS:
//   - Message is written immediately to the log (durable from time of call)
//   - Message is hidden from consumers until delay expires
//   - If broker crashes and restarts, pending delays are recovered
//   - Delay resolution is ~10ms (timer wheel tick interval)
//
// EXAMPLE:
//
//	// Send reminder email in 24 hours
//	partition, offset, err := broker.PublishWithDelay(
//	    "email-reminders",
//	    []byte("user-123"),
//	    []byte(`{"type":"reminder","message":"Don't forget!"}`),
//	    24 * time.Hour,
//	)
func (b *Broker) PublishWithDelay(topic string, key, value []byte, delay time.Duration) (partition int, offset int64, err error) {
	// Calculate absolute delivery time from relative delay
	deliverAt := time.Now().Add(delay)
	return b.PublishAt(topic, key, value, deliverAt)
}

// PublishAt publishes a message that becomes visible at the specified time.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//   - deliverAt: Absolute time when message becomes visible
//
// RETURNS:
//   - Partition the message was written to
//   - Offset within that partition
//   - Error if publish fails or deliverAt is invalid
//
// SEMANTICS:
//   - If deliverAt is in the past, message is delivered immediately
//   - If deliverAt is more than MaxDelay in the future, error returned
//
// USE CASES:
//   - Scheduled jobs: "Run this at 9am Monday"
//   - Market operations: "Execute at market open"
//   - Time-zone aware: "Send at 10am user's local time"
//
// EXAMPLE:
//
//	// Execute trade at market open (9:30 AM ET)
//	marketOpen := time.Date(2024, 1, 15, 9, 30, 0, 0, nyLocation)
//	partition, offset, err := broker.PublishAt(
//	    "trades",
//	    []byte("AAPL"),
//	    []byte(`{"action":"buy","shares":100}`),
//	    marketOpen,
//	)
func (b *Broker) PublishAt(topic string, key, value []byte, deliverAt time.Time) (partition int, offset int64, err error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, 0, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	// Calculate delay duration for validation
	delay := time.Until(deliverAt)

	// If deliverAt is in the past or very near future, publish normally
	if delay <= 0 {
		return t.Publish(key, value)
	}

	// Check max delay limit
	if delay > b.scheduler.config.MaxDelay {
		return 0, 0, fmt.Errorf("delay %v exceeds maximum %v", delay, b.scheduler.config.MaxDelay)
	}

	// Step 1: Write message to log immediately (ensures durability)
	partition, offset, err = t.Publish(key, value)
	if err != nil {
		return 0, 0, err
	}

	// Step 2: Register with scheduler (timer + delay index)
	err = b.scheduler.ScheduleAt(topic, partition, offset, deliverAt)
	if err != nil {
		// Message is written but not scheduled - it will be visible immediately
		// This is acceptable as it's "at least once" delivery
		b.logger.Warn("failed to schedule delayed message, will be visible immediately",
			"topic", topic,
			"partition", partition,
			"offset", offset,
			"error", err)
		// Don't return error - message is published, just not delayed
	}

	b.logger.Debug("published delayed message",
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"deliver_at", deliverAt.Format(time.RFC3339),
		"delay", delay.String())

	return partition, offset, nil
}

// PublishWithDelayAndPriority publishes a delayed message with a specified priority.
//
// MILESTONE 5+6 INTEGRATION:
// This combines delayed delivery (M5) with priority queuing (M6). The message
// is written immediately with its priority, but remains hidden until the delay
// expires. When it becomes visible, it enters the priority-aware consumption.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//   - delay: Duration before message becomes visible (max 7 days)
//   - priority: Message priority level (Critical/High/Normal/Low/Background)
//
// RETURNS:
//   - Partition the message was written to
//   - Offset within that partition
//   - Error if publish fails, delay is invalid, or priority is invalid
//
// FLOW:
//
//	┌──────────┐                      ┌──────────────────┐
//	│ Producer │ PublishWithDelay.  ..│ Write with       │
//	│          │ ──────────────────►  │ Priority to Log  │
//	└──────────┘                      └────────┬─────────┘
//	                                           │
//	                                           ▼
//	                                  ┌──────────────────┐
//	                                  │ Register with    │
//	                                  │ Scheduler        │
//	                                  └────────┬─────────┘
//	                                           │
//	                                     delay expires
//	                                           │
//	                                           ▼
//	                                  ┌──────────────────┐
//	                                  │ Visible with     │
//	                                  │ Priority         │
//	                                  └──────────────────┘
//
// EXAMPLE:
//
//	// High-priority payment retry in 30 seconds
//	partition, offset, err := broker.PublishWithDelayAndPriority(
//	    "payments",
//	    []byte("order-123"),
//	    []byte(`{"action":"retry","amount":99.99}`),
//	    30 * time.Second,
//	    storage.PriorityHigh,
//	)
func (b *Broker) PublishWithDelayAndPriority(topic string, key, value []byte, delay time.Duration, priority storage.Priority) (partition int, offset int64, err error) {
	deliverAt := time.Now().Add(delay)
	return b.PublishAtWithPriority(topic, key, value, deliverAt, priority)
}

// PublishAtWithPriority publishes a message with priority that becomes visible at a specific time.
//
// PARAMETERS:
//   - topic: Topic name
//   - key: Routing key (for partition selection). nil = round-robin.
//   - value: Message payload
//   - deliverAt: Absolute time when message becomes visible
//   - priority: Message priority level
//
// RETURNS:
//   - Partition the message was written to
//   - Offset within that partition
//   - Error if publish fails, deliverAt is invalid, or priority is invalid
//
// SEMANTICS:
//   - If deliverAt is in the past, message is delivered immediately with priority
//   - Message priority is stored in the log at write time
//   - When delay expires, message enters priority-aware consumption
//
// USE CASES:
//   - Critical alerts scheduled for specific times
//   - High-priority batch jobs at off-peak hours
//   - Priority-based retry with specific retry times
func (b *Broker) PublishAtWithPriority(topic string, key, value []byte, deliverAt time.Time, priority storage.Priority) (partition int, offset int64, err error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0, ErrBrokerClosed
	}

	t, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return 0, 0, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	b.mu.RUnlock()

	// Validate priority
	if priority > storage.PriorityBackground {
		return 0, 0, fmt.Errorf("invalid priority: %d", priority)
	}

	// Calculate delay duration for validation
	delay := time.Until(deliverAt)

	// If deliverAt is in the past or very near future, publish with priority immediately
	if delay <= 0 {
		return t.PublishWithPriority(key, value, priority)
	}

	// Check max delay limit
	if delay > b.scheduler.config.MaxDelay {
		return 0, 0, fmt.Errorf("delay %v exceeds maximum %v", delay, b.scheduler.config.MaxDelay)
	}

	// Step 1: Write message to log immediately WITH PRIORITY (ensures durability)
	partition, offset, err = t.PublishWithPriority(key, value, priority)
	if err != nil {
		return 0, 0, err
	}

	// Step 2: Register with scheduler (timer + delay index)
	err = b.scheduler.ScheduleAt(topic, partition, offset, deliverAt)
	if err != nil {
		// Message is written but not scheduled - it will be visible immediately
		b.logger.Warn("failed to schedule delayed message with priority, will be visible immediately",
			"topic", topic,
			"partition", partition,
			"offset", offset,
			"priority", priority,
			"error", err)
	}

	b.logger.Debug("published delayed message with priority",
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"priority", priority,
		"deliver_at", deliverAt.Format(time.RFC3339),
		"delay", delay.String())

	return partition, offset, nil
}

// handleDelayedMessageReady is called when a delayed message timer expires.
// This makes the message visible to consumers.
//
// INTERNAL CALLBACK:
// Called by the scheduler when timer fires. The message is already in the log;
// this callback updates the delay index to mark it as delivered.
//
// FILTERING: Consumer methods (Consume, ConsumeByOffset) check IsDelayed()
// which uses the delay index to filter out pending delayed messages.
func (b *Broker) handleDelayedMessageReady(topic string, partition int, offset int64) error {
	b.logger.Debug("delayed message ready",
		"topic", topic,
		"partition", partition,
		"offset", offset)

	// The message is already in the log. The delay index tracks its state.
	// The scheduler has already marked this entry as DELIVERED in the delay index.
	// Consumer filtering is implemented in Consume() and ConsumeByOffset() methods
	// which call b.IsDelayed() to skip pending delayed messages.

	return nil
}

// CancelDelayed cancels a pending delayed message.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - offset: Message offset
//
// RETURNS:
//   - true if message was cancelled
//   - false if message was already delivered, cancelled, or not found
//
// SEMANTICS:
//   - Cancelled messages are never delivered to consumers
//   - Cancellation is permanent (cannot be un-cancelled)
//   - The message data still exists in the log but is marked cancelled
//
// USE CASES:
//   - User cancels a scheduled email
//   - Order is modified before scheduled processing
//   - Duplicate prevention (cancel earlier version)
func (b *Broker) CancelDelayed(topic string, partition int, offset int64) (bool, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return false, ErrBrokerClosed
	}
	b.mu.RUnlock()

	err := b.scheduler.Cancel(topic, partition, offset)
	if err != nil {
		if errors.Is(err, ErrDelayedMessageNotFound) {
			return false, nil
		}
		if errors.Is(err, ErrDelayedMessageNotPending) {
			// Idempotency: already delivered/cancelled/expired isn't an error.
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// IsDelayed checks if a message is currently delayed (not yet delivered).
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - offset: Message offset
//
// RETURNS:
//   - true if message is pending delivery
//   - false if delivered, cancelled, expired, or not a delayed message
func (b *Broker) IsDelayed(topic string, partition int, offset int64) bool {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return false
	}
	b.mu.RUnlock()

	return b.scheduler.IsDelayed(topic, partition, offset)
}

// GetDelayedMessages returns pending delayed messages for a topic.
//
// PARAMETERS:
//   - topic: Topic name
//   - limit: Maximum messages to return (0 = default 100)
//   - skip: Number of messages to skip (for pagination)
//
// RETURNS:
//   - Slice of scheduled messages with their delivery times
func (b *Broker) GetDelayedMessages(topic string, limit, skip int) ([]*ScheduledMessage, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}
	b.mu.RUnlock()

	return b.scheduler.GetDelayedMessages(topic, limit, skip), nil
}

// GetDelayedMessage returns details about a specific delayed message.
func (b *Broker) GetDelayedMessage(topic string, partition int, offset int64) (*ScheduledMessage, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBrokerClosed
	}
	b.mu.RUnlock()

	return b.scheduler.GetDelayedMessage(topic, partition, offset)
}

// Scheduler returns the broker's delay scheduler.
// Used by the API layer for delay-related operations.
func (b *Broker) Scheduler() *Scheduler {
	return b.scheduler
}

// DelayStats holds statistics for the delay/scheduling system.
type DelayStats struct {
	TotalScheduled uint64           `json:"total_scheduled"`
	TotalDelivered uint64           `json:"total_delivered"`
	TotalCancelled uint64           `json:"total_cancelled"`
	TotalPending   int64            `json:"total_pending"`
	ByTopic        map[string]int64 `json:"by_topic"`
	TimerWheel     TimerWheelStats  `json:"timer_wheel"`
}

// DelayStats returns statistics about the delay scheduling system.
func (b *Broker) DelayStats() DelayStats {
	if b.scheduler == nil {
		return DelayStats{}
	}

	stats := b.scheduler.Stats()
	return DelayStats{
		TotalScheduled: stats.TotalScheduled,
		TotalDelivered: stats.TotalDelivered,
		TotalCancelled: stats.TotalCancelled,
		TotalPending:   stats.TotalPending,
		ByTopic:        stats.ByTopic,
		TimerWheel:     b.scheduler.timerWheel.Stats(),
	}
}

// =============================================================================
// SCHEMA REGISTRY INTERFACE (M8)
// =============================================================================

// SchemaRegistry returns the broker's schema registry.
// Used by the API layer for schema operations.
func (b *Broker) SchemaRegistry() *SchemaRegistry {
	return b.schemaRegistry
}

// SchemaStats returns statistics about the schema registry.
func (b *Broker) SchemaStats() SchemaRegistryStats {
	return b.schemaRegistry.Stats()
}

// =============================================================================
// TIME-BASED OFFSET LOOKUP (M14)
// =============================================================================
//
// GetOffsetByTimestamp finds the offset for a message at or after a given timestamp.
//
// This is used by the offset reset service to implement timestamp-based resets:
//   - "Reset to messages from 2 hours ago"
//   - "Reprocess from 9am this morning"
//
// DESIGN PATTERN:
// Instead of having the gRPC service layer handle topic/partition lookups,
// we encapsulate all orchestration in the broker. This follows the same
// pattern as PublishMessage():
//
//	gRPC Service              Broker                    Topic/Partition
//	─────────────────────────────────────────────────────────────────
//	Publish(topic, msg) ──► ProduceMessage()
//	                          └─► GetTopic()
//	                          └─► Partition()
//	                          └─► ProduceMessage()
//
//	GetOffsetByTimestamp() ──► GetOffsetByTimestamp()
//	(topic, partition,         └─► GetTopic()
//	 timestamp)                └─► Partition()
//	                           └─► GetOffsetByTimestamp()
//
// BENEFITS:
//   - Single point of call from gRPC layer
//   - Consistent error handling
//   - Easier to test
//   - Reduces coupling between layers
//
// RETURNS:
//   - The offset of the first message with timestamp >= given timestamp
//   - Error if topic/partition not found or timestamp lookup fails
func (b *Broker) GetOffsetByTimestamp(topic string, partition int, timestamp int64) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return 0, ErrBrokerClosed
	}

	// Get topic
	t, ok := b.topics[topic]
	if !ok {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	// Get partition
	p, err := t.Partition(partition)
	if err != nil {
		return 0, fmt.Errorf("partition not found: %w", err)
	}

	// Get offset by timestamp
	offset, err := p.GetOffsetByTimestamp(timestamp)
	if err != nil {
		return 0, fmt.Errorf("timestamp lookup failed: %w", err)
	}

	return offset, nil
}
