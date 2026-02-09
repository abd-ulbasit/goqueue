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
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrBrokerClosed means the broker has been shut down
	ErrBrokerClosed = errors.New("broker is closed")

	// ErrNotController means the operation requires the controller node
	// In cluster mode, topic creation and deletion must go through the controller
	// to ensure proper metadata replication.
	ErrNotController = errors.New("not the controller: retry request to reach controller node")
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
	//   ┌──────────────┐  initProducerID  ┌─────────────────────────┐
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
	// MILESTONE 11: REPLICATION COORDINATOR
	// ==========================================================================
	//
	// The replication coordinator manages synchronous replication in cluster mode.
	// It ensures durability by waiting for ISR (In-Sync Replicas) before ACK.
	//
	// SYNCHRONOUS REPLICATION FLOW:
	//   ┌──────────┐  publish  ┌─────────┐  replicate  ┌──────────────┐
	//   │ Producer │─────────►│ Leader  │────────────►│ ISR Followers │
	//   └──────────┘          │ Broker  │◄────────────│ (wait for ACK)│
	//        ▲                └────┬────┘             └──────────────┘
	//        │                     │
	//        │    ACK after ISR    │
	//        │◄────────────────────┘
	//
	// COMPARISON:
	//   - Kafka: acks=all waits for all ISR replicas before ACK
	//   - RabbitMQ: Publisher confirms wait for mirrored queue sync
	//   - goqueue: WaitForReplication() blocks until ISR ack
	//
	// WHY ISR (not all replicas)?
	//   - If a replica is slow/dead, writes would block indefinitely
	//   - ISR = replicas that are "caught up" (within lag threshold)
	//   - If replica falls behind, it's removed from ISR
	//   - Writes only wait for ISR members (fast, alive replicas)
	//
	// ==========================================================================
	replicationCoordinator *replicationCoordinator

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

	// ==========================================================================
	// MILESTONE 27: PRODUCTION READINESS - RETENTION & DISK SAFETY
	// ==========================================================================
	//
	// retentionRunner periodically enforces time-based message retention.
	// Without it, old segments accumulate forever, eventually exhausting disk.
	//
	// COMPARISON:
	//   - Kafka: log.retention.hours + log.retention.check.interval.ms
	//   - RabbitMQ: Queue TTL or per-message TTL
	//   - SQS: MessageRetentionPeriod (default 4 days, max 14 days)
	//   - goqueue: Background goroutine, configurable interval
	//
	// diskMonitor periodically checks free disk space and sets an atomic flag
	// when usage exceeds the threshold. The Publish path checks this flag
	// (single atomic load, ~1ns) to reject writes before disk fills.
	//
	// COMPARISON:
	//   - Kafka: Logs warning but doesn't reject (can fill disk)
	//   - RabbitMQ: disk_free_limit → blocks ALL publishers (flow control)
	//   - goqueue: Rejects new publishes with 503 (client can retry later)
	//
	// ==========================================================================
	retentionRunner *RetentionRunner
	diskMonitor     *DiskMonitor

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
	if err := os.MkdirAll(logsDir, 0o750); err != nil {
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

	// =========================================================================
	// RESTORE ABORTED TRACKER FROM DISK (M26 - Persistence)
	// =========================================================================
	//
	// Load previously persisted aborted offsets. Without this, consumers
	// would see messages from aborted transactions after a broker restart.
	//
	// The aborted tracker is saved to disk on every abort operation
	// (see MarkTransactionAborted). Here we restore that state.
	// =========================================================================
	abortedFilePath := AbortedTrackerFilePath(config.DataDir)
	if err := broker.abortedTracker.LoadFromFile(abortedFilePath); err != nil {
		logger.Error("failed to load aborted tracker from disk",
			"path", abortedFilePath,
			"error", err)
		// Non-fatal: start with empty tracker. Previously aborted messages
		// may briefly be visible until the next abort persists the state.
	} else {
		stats := broker.abortedTracker.Stats()
		if stats.TotalAbortedOffsets > 0 {
			logger.Info("restored aborted tracker from disk",
				"path", abortedFilePath,
				"aborted_offsets", stats.TotalAbortedOffsets,
				"topics", len(stats.ByTopic))
		}
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
		_ = tracer.Shutdown()
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
		_ = tracer.Shutdown()
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
			_ = tracer.Shutdown()
			scheduler.Close()
			groupCoordinator.Close()
			return nil, fmt.Errorf("failed to create cluster coordinator: %w", err)
		}
		broker.clusterCoordinator = cc

		// ======================================================================
		// MILESTONE 11: CREATE REPLICATION COORDINATOR
		// ======================================================================
		//
		// The replication coordinator manages synchronous replication:
		//   - Tracks replica state (leader/follower, ISR)
		//   - Handles WaitForReplication() for synchronous writes
		//   - Manages snapshot creation/recovery
		//
		// Created here but started in StartCluster() after HTTP server is up.
		//
		// ======================================================================
		rc, err := newReplicationCoordinator(broker, logger)
		if err != nil {
			_ = cc.Stop(context.Background())
			transactionCoordinator.Close()
			schemaRegistry.Close()
			_ = tracer.Shutdown()
			scheduler.Close()
			groupCoordinator.Close()
			return nil, fmt.Errorf("failed to create replication coordinator: %w", err)
		}
		broker.replicationCoordinator = rc

		// ======================================================================
		// IMPORTANT: Cluster coordinator created but NOT started yet!
		// ======================================================================
		// The cluster coordinator uses HTTP for peer communication. However,
		// the HTTP server hasn't been created yet at this point in broker init.
		//
		// STARTUP ORDER PROBLEM (why we delay Start):
		//   1. NewBroker() creates broker components
		//   2. Coordinator.Start() tries to join peers via HTTP
		//   3. But HTTP server doesn't exist yet!
		//   4. All pods fail to connect → quorum never forms
		//
		// SOLUTION:
		//   - Create coordinator here (to register routes later)
		//   - Caller must call broker.StartCluster() AFTER HTTP server starts
		//
		// FLOW:
		//   broker = NewBroker()         // Create coordinator, don't start
		//   server = NewServer(broker)   // Create HTTP server
		//   broker.RegisterClusterRoutes(server.Mux())  // Wire cluster HTTP endpoints
		//   server.Start()               // HTTP now listening
		//   broker.StartCluster()        // NOW coordinator can join peers
		//
		// ======================================================================
		logger.Info("cluster coordinator created (call StartCluster() after HTTP server starts)")
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
				_ = broker.clusterCoordinator.Stop(context.Background())
			}
			transactionCoordinator.Close()
			schemaRegistry.Close()
			_ = tracer.Shutdown()
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
			_ = broker.clusterCoordinator.Stop(context.Background())
		}
		transactionCoordinator.Close()
		schemaRegistry.Close()
		_ = tracer.Shutdown()
		scheduler.Close()
		groupCoordinator.Close()
		return nil, fmt.Errorf("failed to load existing topics: %w", err)
	}

	// ==========================================================================
	// MILESTONE 27: START RETENTION RUNNER & DISK MONITOR
	// ==========================================================================
	//
	// These run AFTER topics are loaded so the retention runner can iterate
	// them, and the disk monitor knows the data directory to check.
	//
	// STARTUP ORDER:
	//   1. Topics loaded (above)
	//   2. Retention runner starts (scans topics periodically)
	//   3. Disk monitor starts (polls free space periodically)
	//   4. Broker is ready to accept requests
	//
	// ==========================================================================
	retentionConfig := DefaultRetentionConfig()
	retentionRunner := NewRetentionRunner(broker, retentionConfig)
	retentionRunner.Start()
	broker.retentionRunner = retentionRunner

	diskMonitorConfig := DefaultDiskMonitorConfig(config.DataDir)
	diskMonitor := NewDiskMonitor(diskMonitorConfig)
	if err := diskMonitor.Start(); err != nil {
		logger.Warn("failed to start disk monitor", "error", err)
	}
	broker.diskMonitor = diskMonitor

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

// =============================================================================
// CLUSTER LIFECYCLE
// =============================================================================

// StartCluster starts the cluster coordinator to join or form a cluster.
//
// WHEN TO CALL:
//
//	This must be called AFTER the HTTP server is listening, because the cluster
//	coordinator uses HTTP to communicate with peers during bootstrap.
//
// STARTUP SEQUENCE:
//
//	broker := NewBroker(config)                   // Creates coordinator
//	server := api.NewServer(broker, serverConfig) // Creates HTTP server
//	broker.RegisterClusterRoutes(server.Mux())    // Wire cluster endpoints
//	server.Start()                                // HTTP now listening
//	broker.StartCluster()                         // NOW safe to join cluster
//
// WHY THIS ORDER:
//
//	When a pod starts, it tries to contact peer pods via HTTP to join the cluster.
//	If the HTTP server isn't running yet, all join attempts fail. With all pods
//	starting simultaneously (Kubernetes Parallel podManagementPolicy), this creates
//	a deadlock where no pod can join because no pod is listening.
//
// COMPARISON:
//   - Kafka: ZooKeeper handles coordination (separate service, always running)
//   - Consul: Serf gossip layer starts before RPC
//   - etcd: Raft peer connections on separate port, started first
//   - goqueue: HTTP-based clustering on main port, so HTTP must start first
func (b *Broker) StartCluster() error {
	if b.clusterCoordinator == nil {
		// Not in cluster mode, nothing to do
		return nil
	}

	b.logger.Info("starting cluster coordinator")

	// Bootstrap timeout: give pods time to discover each other
	startCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := b.clusterCoordinator.Start(startCtx); err != nil {
		return fmt.Errorf("failed to start cluster coordinator: %w", err)
	}

	b.logger.Info("cluster coordinator started",
		"node_id", b.clusterCoordinator.NodeID(),
		"is_controller", b.clusterCoordinator.IsController())

	// =========================================================================
	// MILESTONE 11: START REPLICATION COORDINATOR
	// =========================================================================
	//
	// The replication coordinator provides synchronous replication.
	// It must be started AFTER the cluster coordinator is ready because:
	//   - It uses cluster membership info to find peers
	//   - It registers replicas based on partition assignments
	//   - It needs the cluster client for fetching from leaders
	//
	// SYNCHRONOUS REPLICATION FLOW:
	//   1. Producer publishes message to leader
	//   2. Leader writes to local log
	//   3. Leader calls WaitForReplication() → blocks
	//   4. Followers fetch and ACK
	//   5. Leader returns ACK to producer
	//
	// =========================================================================
	if b.replicationCoordinator != nil {
		b.logger.Info("starting replication coordinator")
		if err := b.replicationCoordinator.Start(startCtx); err != nil {
			return fmt.Errorf("failed to start replication coordinator: %w", err)
		}
		b.logger.Info("replication coordinator started")
	}

	return nil
}

// RegisterClusterRoutes registers cluster HTTP endpoints on the given mux.
//
// These endpoints handle inter-node communication:
//   - POST /cluster/heartbeat  - Periodic health check from peers
//   - POST /cluster/join       - Node requesting to join cluster
//   - POST /cluster/leave      - Node requesting graceful departure
//   - GET  /cluster/state      - Get current cluster state
//   - POST /cluster/vote       - Controller election vote request
//   - POST /cluster/metadata   - Sync metadata from controller
//   - GET  /cluster/health     - Cluster health status
//
// Must be called BEFORE StartCluster() but AFTER HTTP server is created.
func (b *Broker) RegisterClusterRoutes(mux *http.ServeMux) {
	if b.clusterCoordinator == nil {
		// Not in cluster mode, nothing to register
		return
	}
	b.clusterCoordinator.RegisterRoutes(mux)

	// =========================================================================
	// REPLICATION ENDPOINTS (M11)
	// =========================================================================
	//
	// These endpoints handle data replication between nodes:
	//   - POST /replication/fetch    - Followers fetch messages from leader
	//   - GET  /replication/leo      - Get log end offset for partition
	//   - POST /replication/ack      - Follower acknowledges replication
	//   - GET  /replication/snapshot - Request snapshot for recovery
	//
	// Used by:
	//   - ReplicaManager on followers to pull data
	//   - ISRManager to track replication lag
	//
	// =========================================================================
	if b.replicationCoordinator != nil {
		b.replicationCoordinator.RegisterRoutes(mux)
	}
}

// IsClusterEnabled returns true if this broker is running in cluster mode.
func (b *Broker) IsClusterEnabled() bool {
	return b.clusterCoordinator != nil
}

// IsController returns true if this broker is the cluster controller.
// Returns false if not in cluster mode.
//
// WHY THIS MATTERS:
//   - Topic creation requires controller (cluster metadata)
//   - Partition scaling requires controller
//   - Client requests should be forwarded to controller for metadata ops
func (b *Broker) IsController() bool {
	if b.clusterCoordinator == nil {
		return false
	}
	return b.clusterCoordinator.IsController()
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

	// ==========================================================================
	// REPLICATION SHUTDOWN (M11)
	// ==========================================================================
	// Stop replication AFTER cluster departure but BEFORE closing topics.
	// This ensures:
	//   1. No new replication requests come in
	//   2. Pending replication ACKs are handled
	//   3. Topics still accessible for final replication
	// ==========================================================================
	if b.replicationCoordinator != nil {
		b.logger.Info("stopping replication coordinator")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := b.replicationCoordinator.Stop(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("replication coordinator: %w", err))
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
		// ======================================================================
		// IN-FLIGHT MESSAGE DRAINING (#11 - Graceful Shutdown)
		// ======================================================================
		//
		// WHY: When the broker shuts down, some messages may be "in-flight" -
		// meaning a consumer has received them but hasn't ACK'd yet. If we
		// close immediately, those messages will be re-delivered after restart
		// (at-least-once semantics), wasting consumer processing work.
		//
		// HOW IT WORKS:
		//   1. Log current in-flight count
		//   2. Poll every 500ms for up to 30s
		//   3. If count reaches 0 → proceed with clean shutdown
		//   4. If timeout → proceed anyway (don't block shutdown forever)
		//
		// COMPARISON:
		//   - Kafka: controlled.shutdown.enable (waits for ISR leadership transfer)
		//   - RabbitMQ: SIGTERM → drain mode → wait for pending acks
		//   - SQS: No draining; messages return after visibility timeout
		//   - goqueue: Poll in-flight count with 30s timeout
		//
		// TRADEOFF:
		//   - Long drain timeout: More messages processed cleanly, slower shutdown
		//   - Short drain timeout: Faster shutdown, more re-deliveries
		//   - 30s default: Matches our visibility timeout, reasonable for most workloads
		//
		// FLOW:
		//   ┌──────────────┐  in-flight > 0?  ┌──────────────┐  timeout?
		//   │ Drain Start  │─────────────────►│ Poll 500ms   │──────────► Force Close
		//   └──────────────┘                  └──────┬───────┘
		//                                            │ in-flight == 0
		//                                            ▼
		//                                     Clean Close
		//
		// ======================================================================
		inFlight := b.ackManager.TotalInFlightCount()
		if inFlight > 0 {
			b.logger.Info("draining in-flight messages before shutdown",
				"in_flight", inFlight)

			drainTimeout := 30 * time.Second
			drainDeadline := time.Now().Add(drainTimeout)
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()

			for time.Now().Before(drainDeadline) {
				<-ticker.C
				remaining := b.ackManager.TotalInFlightCount()
				if remaining == 0 {
					b.logger.Info("all in-flight messages drained successfully")
					break
				}
				b.logger.Info("waiting for in-flight messages to drain",
					"remaining", remaining,
					"deadline_in", time.Until(drainDeadline).Round(time.Second))
			}

			finalCount := b.ackManager.TotalInFlightCount()
			if finalCount > 0 {
				b.logger.Warn("drain timeout reached, proceeding with shutdown",
					"remaining_in_flight", finalCount,
					"note", "these messages will be re-delivered after restart")
			}
		}

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

	// ==========================================================================
	// MILESTONE 27: STOP RETENTION RUNNER & DISK MONITOR
	// ==========================================================================
	// Stop background goroutines before closing topics.
	// Retention runner must stop first to avoid accessing closed topics.
	// ==========================================================================
	if b.retentionRunner != nil {
		b.retentionRunner.Stop()
	}
	if b.diskMonitor != nil {
		b.diskMonitor.Stop()
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

	// =========================================================================
	// CLUSTER MODE: REGISTER WITH CLUSTER METADATA
	// =========================================================================
	//
	// In cluster mode, topic metadata must be registered with the controller
	// so that partition assignments are created and synced to all nodes.
	//
	// WHO CAN CREATE TOPICS?
	//   - Controller: Creates metadata directly and syncs to followers
	//   - Follower: Returns error (client should retry to hit controller via LB)
	//
	// DURABILITY FIX:
	//   Previously, non-controllers created topics locally but didn't register
	//   with cluster metadata. This caused topic loss on pod restart because
	//   the cluster metadata was never updated. Now we return an error instead.
	//
	// PARTITION ASSIGNMENT FLOW:
	//   1. Controller creates topic metadata
	//   2. Controller assigns partitions using round-robin
	//   3. Controller syncs metadata to all followers
	//   4. Followers receive metadata via /cluster/metadata endpoint
	//   5. All nodes now know about partition assignments
	//
	// =========================================================================
	if b.clusterCoordinator != nil {
		// Check if we're the controller - only controller can create topic metadata
		// Non-controllers should return error so client can retry (may hit controller)
		if !b.clusterCoordinator.IsController() {
			return ErrNotController
		}

		// Default replication factor: min(cluster size, 3)
		replicationFactor := 3
		clusterSize := b.clusterCoordinator.ClusterSize()
		if clusterSize < replicationFactor {
			replicationFactor = clusterSize
		}

		if err := b.clusterCoordinator.CreateTopicMeta(config.Name, config.NumPartitions, replicationFactor); err != nil {
			return fmt.Errorf("failed to register topic with cluster metadata: %w", err)
		}
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

// CreateTopicLocal creates a topic locally without registering with cluster metadata.
//
// WHY THIS METHOD EXISTS:
//
//	When followers receive metadata sync from controller, they need to
//	create topics locally to:
//	1. Accept replication requests from leaders
//	2. Store replicated messages
//	3. Be ready to serve as leader if elected
//
// DIFFERENCE FROM CreateTopic:
//   - CreateTopic: Creates locally + registers with cluster metadata
//   - CreateTopicLocal: Creates locally ONLY (cluster metadata already exists)
//
// WHEN TO USE:
//   - Cluster metadata sync: Follower creating topic from synced metadata
//   - Topic recovery: Recreating topic from stored metadata
//
// IDEMPOTENT: Returns nil if topic already exists (unlike CreateTopic which errors)
func (b *Broker) CreateTopicLocal(config TopicConfig) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBrokerClosed
	}

	// Skip if already exists (idempotent for cluster sync)
	if _, exists := b.topics[config.Name]; exists {
		return nil
	}

	// Create topic locally - NO cluster metadata registration
	topic, err := NewTopic(b.logsDir, config)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	b.topics[config.Name] = topic

	// Register with group coordinator for consumer group partition assignment
	b.groupCoordinator.RegisterTopic(config.Name, config.NumPartitions)

	b.logger.Info("created local topic (from metadata sync)",
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
// PARTITION INFO API
// =============================================================================
//
// WHY: Clients and operators need visibility into partition assignments.
// This enables:
//   - Debugging routing issues (which node handles which partition?)
//   - Monitoring ISR health (are replicas in sync?)
//   - Client optimization (direct connect to leader)
//
// COMPARISON:
//   - Kafka: AdminClient.describeTopics() returns PartitionInfo
//   - RabbitMQ: Management API /api/queues shows node ownership
//   - goqueue: GET /topics/{name}/partitions
//
// =============================================================================

// GetPartitionInfo returns partition assignment info for a specific partition.
// Returns nil if topic doesn't exist or cluster mode is disabled.
func (b *Broker) GetPartitionInfo(topic string, partition int) *PartitionInfo {
	if b.clusterCoordinator == nil {
		// Single-node mode - no cluster metadata
		return &PartitionInfo{
			Topic:     topic,
			Partition: partition,
			Leader:    b.config.NodeID,
			Replicas:  []string{b.config.NodeID},
			ISR:       []string{b.config.NodeID},
			Version:   0,
		}
	}
	return b.clusterCoordinator.GetPartitionInfo(topic, partition)
}

// GetTopicPartitions returns partition info for all partitions of a topic.
// Returns empty slice if topic doesn't exist in cluster metadata.
func (b *Broker) GetTopicPartitions(topic string) []*PartitionInfo {
	if b.clusterCoordinator == nil {
		// Single-node mode - check if topic exists locally
		b.mu.RLock()
		t, exists := b.topics[topic]
		b.mu.RUnlock()

		if !exists {
			return nil
		}

		// Return single partition for single-node mode
		infos := make([]*PartitionInfo, t.config.NumPartitions)
		for i := 0; i < t.config.NumPartitions; i++ {
			infos[i] = &PartitionInfo{
				Topic:     topic,
				Partition: i,
				Leader:    b.config.NodeID,
				Replicas:  []string{b.config.NodeID},
				ISR:       []string{b.config.NodeID},
				Version:   0,
			}
		}
		return infos
	}
	return b.clusterCoordinator.GetTopicPartitions(topic)
}
