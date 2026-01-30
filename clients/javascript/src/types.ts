// =============================================================================
// GOQUEUE TYPESCRIPT CLIENT - TYPE DEFINITIONS
// =============================================================================
//
// This file contains all TypeScript type definitions for the GoQueue client.
// These types mirror the server's API schemas and provide full type safety.
//
// ORGANIZATION:
//   - Request/Response types for each API operation
//   - Grouped by feature (Topics, Messages, Consumer Groups, etc.)
//   - Enums for constants (Priority, Compatibility, etc.)
//
// =============================================================================

// =============================================================================
// COMMON TYPES
// =============================================================================

/**
 * Error response returned by the GoQueue API when an operation fails.
 *
 * @example
 * ```typescript
 * try {
 *   await client.topics.create({ name: 'existing-topic' });
 * } catch (error) {
 *   if (error instanceof GoQueueError) {
 *     console.error('API Error:', error.message);
 *   }
 * }
 * ```
 */
export interface ErrorResponse {
  /** Human-readable error message */
  error: string;
}

// =============================================================================
// HEALTH & STATUS TYPES
// =============================================================================

/**
 * Health status values returned by health check endpoints.
 *
 * - `ok`: All systems operational
 * - `degraded`: Some features may be unavailable
 * - `fail`: Service is not operational
 */
export type HealthStatus = 'ok' | 'degraded' | 'fail';

/**
 * Probe status for Kubernetes health checks.
 *
 * - `pass`: Check passed, service is healthy
 * - `fail`: Check failed, service needs attention
 */
export type ProbeStatus = 'pass' | 'fail';

/**
 * Individual health check result for a subsystem.
 *
 * @example
 * ```typescript
 * const readiness = await client.health.readiness({ verbose: true });
 * if (readiness.checks?.storage?.status === 'fail') {
 *   console.error('Storage subsystem is unhealthy');
 * }
 * ```
 */
export interface HealthCheckResult {
  /** Status of this specific check */
  status: 'pass' | 'warn' | 'fail';
  /** Human-readable message about the check */
  message?: string;
  /** How long the check took */
  latency?: string;
}

/**
 * Basic health check response.
 *
 * Used by `/health` endpoint for simple health checks.
 */
export interface HealthResponse {
  /** Overall health status */
  status: HealthStatus;
  /** When the check was performed */
  timestamp: string;
}

/**
 * Liveness probe response for Kubernetes.
 *
 * Used by `/healthz` and `/livez` endpoints.
 *
 * @remarks
 * If this check fails, Kubernetes will restart the container.
 * Only fails if the process is deadlocked or unresponsive.
 */
export interface LivenessResponse {
  /** Probe status */
  status: ProbeStatus;
  /** When the check was performed */
  timestamp: string;
  /** How long the service has been running */
  uptime?: string;
  /** Additional status message */
  message?: string;
}

/**
 * Readiness probe response for Kubernetes.
 *
 * Used by `/readyz` endpoint.
 *
 * @remarks
 * If this check fails, the pod is removed from service endpoints.
 * The pod won't receive traffic but won't be restarted.
 */
export interface ReadinessResponse {
  /** Probe status */
  status: ProbeStatus;
  /** When the check was performed */
  timestamp: string;
  /** How long the service has been running */
  uptime?: string;
  /** Additional status message */
  message?: string;
  /** Detailed results for each subsystem (when verbose=true) */
  checks?: Record<string, HealthCheckResult>;
  /** Broker information */
  broker?: {
    node_id: string;
    topic_count: number;
    total_size: number;
  };
}

/**
 * Version information about the GoQueue server.
 */
export interface VersionResponse {
  /** Semantic version (e.g., "1.0.0") */
  version: string;
  /** Git commit hash */
  git_commit?: string;
  /** When the binary was built */
  build_time?: string;
  /** Go version used to build */
  go_version?: string;
}

/**
 * Operational statistics about the broker.
 */
export interface StatsResponse {
  /** Unique identifier for this node */
  node_id: string;
  /** How long the broker has been running */
  uptime: string;
  /** Total number of topics */
  topics: number;
  /** Total data size across all topics */
  total_size_bytes: number;
  /** Per-topic statistics */
  topic_stats?: Record<string, unknown>;
}

// =============================================================================
// TOPIC TYPES
// =============================================================================

/**
 * Request to create a new topic.
 *
 * @example
 * ```typescript
 * await client.topics.create({
 *   name: 'orders',
 *   num_partitions: 6,  // For higher parallelism
 *   retention_hours: 168  // 7 days
 * });
 * ```
 */
export interface CreateTopicRequest {
  /**
   * Topic name.
   *
   * Must match pattern: `^[a-zA-Z0-9_-]+$`
   * Length: 1-255 characters
   *
   * @example "orders", "user-events", "notifications_v2"
   */
  name: string;

  /**
   * Number of partitions for the topic.
   *
   * More partitions = more parallelism but more resources.
   *
   * **Guidelines:**
   * - 1 partition: Single consumer, strict ordering
   * - 3-6 partitions: Typical workloads
   * - 10+ partitions: High-throughput scenarios
   *
   * @default 3
   */
  num_partitions?: number;

  /**
   * How long to retain messages, in hours.
   *
   * Messages older than this are eligible for deletion.
   *
   * @default 168 (7 days)
   */
  retention_hours?: number;
}

/**
 * Response after creating a topic.
 */
export interface CreateTopicResponse {
  /** Name of the created topic */
  name: string;
  /** Number of partitions created */
  partitions: number;
  /** Whether the topic was created (false if it already existed) */
  created: boolean;
}

/**
 * Detailed information about a topic.
 *
 * Returned by `client.topics.get()`.
 */
export interface TopicDetails {
  /** Topic name */
  name: string;
  /** Number of partitions */
  partitions: number;
  /** Total messages across all partitions */
  total_messages: number;
  /** Total data size in bytes */
  total_size_bytes: number;
  /**
   * Offset ranges per partition.
   *
   * Key: partition number as string
   * Value: earliest and latest offsets
   */
  partition_offsets?: Record<string, { earliest: number; latest: number }>;
}

/**
 * Response containing list of topic names.
 */
export interface TopicListResponse {
  /** Array of topic names */
  topics: string[];
}

// =============================================================================
// MESSAGE TYPES
// =============================================================================

/**
 * Message priority levels.
 *
 * GoQueue supports 5 priority levels for message routing:
 *
 * - `critical`: System emergencies, circuit breakers. Processed first.
 * - `high`: Paid users, real-time updates. High priority.
 * - `normal`: Default for most messages.
 * - `low`: Batch jobs, reports. Processed when queue is light.
 * - `background`: Analytics, cleanup. Lowest priority.
 *
 * @comparison
 * - **RabbitMQ**: Uses 0-9 scale (higher = higher priority)
 * - **GoQueue**: Uses semantic names for clarity
 * - **Kafka**: No native priority support
 * - **SQS**: No priority support in standard queues
 */
export type MessagePriority = 'critical' | 'high' | 'normal' | 'low' | 'background';

/**
 * A single message to publish.
 *
 * @example
 * ```typescript
 * // Simple message
 * { value: '{"orderId": "12345"}' }
 *
 * // Message with key (for ordering)
 * { key: 'user-123', value: '{"event": "purchase"}' }
 *
 * // High priority message
 * { value: '{"alert": "critical"}', priority: 'critical' }
 *
 * // Delayed message (1 hour)
 * { value: '{"reminder": "follow up"}', delay: '1h' }
 * ```
 */
export interface PublishMessage {
  /**
   * Message key for partition routing.
   *
   * Messages with the same key always go to the same partition,
   * guaranteeing ordering for that key.
   *
   * @example "user-123", "order-456"
   */
  key?: string;

  /**
   * Message payload.
   *
   * Can be any string (JSON, plain text, base64-encoded binary).
   */
  value: string;

  /**
   * Explicit partition number.
   *
   * Overrides key-based routing. Use when you need direct control.
   */
  partition?: number;

  /**
   * Relative delay before delivery.
   *
   * Format: Duration string like "30s", "5m", "1h", "24h"
   *
   * @example "30s" for 30 seconds, "1h" for 1 hour
   */
  delay?: string;

  /**
   * Absolute delivery time (RFC3339 timestamp).
   *
   * Use when you need delivery at a specific time.
   *
   * @example "2025-01-31T09:00:00Z"
   */
  deliverAt?: string;

  /**
   * Message priority.
   *
   * Higher priority messages are delivered before lower priority ones.
   *
   * @default "normal"
   */
  priority?: MessagePriority;
}

/**
 * Request to publish messages to a topic.
 */
export interface PublishRequest {
  /** Array of messages to publish (batch publishing) */
  messages: PublishMessage[];
}

/**
 * Result for a single published message.
 */
export interface PublishResult {
  /** Partition the message was written to */
  partition: number;
  /** Offset within the partition */
  offset: number;
  /** Priority assigned to the message */
  priority?: string;
  /** Whether the message is delayed */
  delayed?: boolean;
  /** When the message will be delivered (for delayed messages) */
  deliverAt?: string;
  /** Error message if this specific message failed */
  error?: string;
}

/**
 * Response after publishing messages.
 */
export interface PublishResponse {
  /** Results for each message (same order as request) */
  results: PublishResult[];
}

/**
 * A consumed message.
 */
export interface ConsumeMessage {
  /** Offset within the partition */
  offset: number;
  /** When the message was published */
  timestamp: string;
  /** Message key (if provided during publish) */
  key?: string;
  /** Message payload */
  value: string;
  /** Message priority */
  priority?: string;
}

/**
 * Response from consuming messages.
 */
export interface ConsumeResponse {
  /** Array of consumed messages */
  messages: ConsumeMessage[];
  /** Offset to use for next consume call */
  next_offset: number;
}

// =============================================================================
// DELAYED MESSAGE TYPES
// =============================================================================

/**
 * Information about a delayed message.
 */
export interface DelayedMessage {
  /** Topic the message belongs to */
  topic: string;
  /** Partition number */
  partition: number;
  /** Message offset */
  offset: number;
  /** When the message will be delivered */
  deliver_at: string;
  /** Message priority */
  priority?: string;
}

/**
 * Response containing delayed messages.
 */
export interface DelayedMessagesResponse {
  /** Array of delayed messages */
  messages: DelayedMessage[];
  /** Total count of delayed messages */
  count: number;
}

/**
 * Statistics about delayed message processing.
 */
export interface DelayStats {
  /** Number of messages waiting for delivery */
  pending_count: number;
  /** Total messages delivered after delay */
  delivered_count: number;
  /** Total messages cancelled before delivery */
  cancelled_count: number;
  /** Average delay duration */
  avg_delay?: string;
}

// =============================================================================
// CONSUMER GROUP TYPES
// =============================================================================

/**
 * Consumer group state.
 *
 * - `stable`: All members have been assigned partitions
 * - `rebalancing`: Partition assignment is in progress
 * - `dead`: Group has no active members
 */
export type GroupState = 'stable' | 'rebalancing' | 'dead';

/**
 * Information about a consumer group member.
 */
export interface GroupMember {
  /** Unique member identifier */
  member_id: string;
  /** Client-provided identifier */
  client_id?: string;
  /** Partitions assigned to this member */
  assigned_partitions: string[];
}

/**
 * Detailed information about a consumer group.
 *
 * @example
 * ```typescript
 * const group = await client.groups.get('order-processors');
 * console.log(`Group state: ${group.state}`);
 * console.log(`Members: ${group.members?.length}`);
 * for (const member of group.members ?? []) {
 *   console.log(`  ${member.member_id}: ${member.assigned_partitions.join(', ')}`);
 * }
 * ```
 */
export interface GroupDetails {
  /** Group identifier */
  group_id: string;
  /** Current group state */
  state: GroupState;
  /** List of group members */
  members?: GroupMember[];
  /** Current generation (increments on rebalance) */
  generation?: number;
  /** Assignment protocol in use */
  protocol?: string;
}

/**
 * Response containing list of consumer groups.
 */
export interface GroupListResponse {
  /** Array of group IDs */
  groups: string[];
}

/**
 * Request to join a consumer group.
 */
export interface JoinGroupRequest {
  /**
   * Client identifier.
   *
   * Used for logging and debugging. Should identify your application.
   *
   * @example "order-service-pod-1", "analytics-worker-a"
   */
  client_id: string;

  /**
   * Topics to subscribe to.
   *
   * Partitions from these topics will be distributed among group members.
   */
  topics: string[];

  /**
   * Session timeout duration.
   *
   * If no heartbeat is received within this time, the member is removed.
   *
   * @default "30s"
   */
  session_timeout?: string;
}

/**
 * Response after joining a consumer group.
 */
export interface JoinGroupResponse {
  /** Assigned member identifier */
  member_id: string;
  /** Current group generation */
  generation: number;
  /** Whether this member is the group leader */
  leader: boolean;
  /** Partitions assigned to this member */
  assigned_partitions: string[];
  /** Whether a rebalance is needed */
  rebalance_required?: boolean;
}

/**
 * Request to send a heartbeat to keep session alive.
 */
export interface HeartbeatRequest {
  /** Member identifier (from JoinGroupResponse) */
  member_id: string;
  /** Current generation (from JoinGroupResponse) */
  generation: number;
}

/**
 * Response to a heartbeat.
 */
export interface HeartbeatResponse {
  /** Whether a rebalance has been triggered */
  rebalance_required: boolean;
  /** Current group state */
  state?: string;
}

/**
 * Request to leave a consumer group.
 */
export interface LeaveGroupRequest {
  /** Member identifier to remove */
  member_id: string;
}

/**
 * A message from polling a consumer group.
 */
export interface PollMessage {
  /** Topic the message came from */
  topic: string;
  /** Partition number */
  partition: number;
  /** Message offset */
  offset: number;
  /** Message key */
  key?: string;
  /** Message payload */
  value: string;
  /** When the message was published */
  timestamp: string;
  /**
   * Receipt handle for ACK/NACK operations.
   *
   * Must be used to acknowledge or reject the message.
   */
  receipt_handle: string;
}

/**
 * Response from polling for messages.
 */
export interface PollResponse {
  /** Array of messages */
  messages: PollMessage[];
}

// =============================================================================
// OFFSET TYPES
// =============================================================================

/**
 * Committed offsets response.
 *
 * Structure: `{ [topic]: { [partition]: offset } }`
 */
export interface OffsetsResponse {
  offsets: Record<string, Record<string, number>>;
}

/**
 * Request to commit offsets.
 */
export interface CommitOffsetsRequest {
  /** Member identifier */
  member_id: string;
  /**
   * Offsets to commit.
   *
   * Structure: `{ [topic]: { [partition]: offset } }`
   */
  offsets: Record<string, Record<string, number>>;
}

// =============================================================================
// RELIABILITY TYPES (ACK/NACK/DLQ)
// =============================================================================

/**
 * Request to acknowledge a message.
 *
 * Acknowledging tells the broker the message was successfully processed.
 */
export interface AckRequest {
  /**
   * Receipt handle from the consumed message.
   *
   * This uniquely identifies the message being acknowledged.
   */
  receipt_handle: string;
  /** Consumer identifier (optional) */
  consumer_id?: string;
}

/**
 * Request to negative-acknowledge a message.
 *
 * NACKing tells the broker the message processing failed temporarily.
 * The message will be redelivered after the delay.
 */
export interface NackRequest {
  /** Receipt handle from the consumed message */
  receipt_handle: string;
  /**
   * Delay before redelivery.
   *
   * @default Uses visibility timeout if not specified
   */
  delay?: string;
}

/**
 * Request to reject a message permanently.
 *
 * Rejecting moves the message to the dead letter queue (DLQ).
 * Use for messages that can never be processed (invalid format, etc).
 */
export interface RejectRequest {
  /** Receipt handle from the consumed message */
  receipt_handle: string;
  /** Reason for rejection (stored in DLQ) */
  reason?: string;
}

/**
 * Request to extend message visibility timeout.
 *
 * Use when processing takes longer than expected.
 */
export interface ExtendVisibilityRequest {
  /** Receipt handle from the consumed message */
  receipt_handle: string;
  /**
   * New visibility timeout.
   *
   * @example "60s" for 60 seconds
   */
  timeout: string;
}

/**
 * Statistics about message reliability operations.
 */
export interface ReliabilityStats {
  /** Total messages acknowledged */
  acks_total: number;
  /** Total messages NACKed (temporary failures) */
  nacks_total: number;
  /** Total messages rejected (moved to DLQ) */
  rejects_total: number;
  /** Messages currently in dead letter queues */
  dlq_messages: number;
  /** Total visibility timeout extensions */
  visibility_extensions: number;
}

// =============================================================================
// PRIORITY TYPES
// =============================================================================

/**
 * Statistics about priority queue processing.
 */
export interface PriorityStats {
  /** Message counts by priority level */
  messages_by_priority: Record<string, number>;
  /** Average wait time by priority level */
  avg_wait_time_by_priority: Record<string, string>;
}

// =============================================================================
// SCHEMA REGISTRY TYPES
// =============================================================================

/**
 * Schema compatibility levels.
 *
 * Controls what changes are allowed when registering new schema versions:
 *
 * - `NONE`: No compatibility checking
 * - `BACKWARD`: New schema can read old data
 * - `FORWARD`: Old schema can read new data
 * - `FULL`: Both backward and forward compatible
 * - `*_TRANSITIVE`: Check against all previous versions, not just the latest
 */
export type CompatibilityLevel =
  | 'NONE'
  | 'BACKWARD'
  | 'FORWARD'
  | 'FULL'
  | 'BACKWARD_TRANSITIVE'
  | 'FORWARD_TRANSITIVE'
  | 'FULL_TRANSITIVE';

/**
 * Request to register a schema.
 */
export interface RegisterSchemaRequest {
  /** JSON Schema definition as a string */
  schema: string;
  /**
   * Schema type.
   *
   * @default "JSON"
   */
  schemaType?: string;
}

/**
 * Response after registering a schema.
 */
export interface RegisterSchemaResponse {
  /** Global schema ID */
  id: number;
}

/**
 * A specific version of a schema.
 */
export interface SchemaVersion {
  /** Subject name */
  subject: string;
  /** Version number */
  version: number;
  /** Global schema ID */
  id: number;
  /** Schema definition */
  schema: string;
}

/**
 * Schema without version information.
 */
export interface Schema {
  /** Schema definition */
  schema: string;
}

/**
 * Compatibility configuration.
 */
export interface CompatibilityConfig {
  /** Compatibility level */
  compatibilityLevel: CompatibilityLevel;
}

/**
 * Schema registry statistics.
 */
export interface SchemaStats {
  /** Number of subjects */
  subjects: number;
  /** Number of unique schemas */
  schemas: number;
  /** Total schema versions */
  versions: number;
}

// =============================================================================
// TRANSACTION TYPES
// =============================================================================

/**
 * Transaction state values.
 */
export type TransactionState =
  | 'ongoing'
  | 'preparing_commit'
  | 'preparing_abort'
  | 'completed_commit'
  | 'completed_abort';

/**
 * Request to initialize a producer.
 */
export interface InitProducerRequest {
  /**
   * Transactional ID for exactly-once semantics.
   *
   * If provided, the producer can use transactions.
   */
  transactional_id?: string;
}

/**
 * Response after initializing a producer.
 */
export interface InitProducerResponse {
  /** Producer ID assigned by the broker */
  producer_id: number;
  /** Producer epoch (for zombie fencing) */
  epoch: number;
}

/**
 * Request to send producer heartbeat.
 */
export interface ProducerHeartbeatRequest {
  /** Current producer epoch */
  epoch: number;
}

/**
 * Request to begin a transaction.
 */
export interface BeginTransactionRequest {
  /** Producer ID */
  producer_id: number;
  /** Producer epoch */
  epoch: number;
  /** Transactional ID */
  transactional_id: string;
}

/**
 * Response after beginning a transaction.
 */
export interface BeginTransactionResponse {
  /** Transaction ID */
  transaction_id: string;
}

/**
 * Request to publish a message within a transaction.
 */
export interface TransactionalPublishRequest {
  /** Producer ID */
  producer_id: number;
  /** Producer epoch */
  epoch: number;
  /** Topic to publish to */
  topic: string;
  /** Message key */
  key?: string;
  /** Message value */
  value: string;
  /** Sequence number for idempotency */
  sequence?: number;
}

/**
 * Response after transactional publish.
 */
export interface TransactionalPublishResponse {
  /** Partition the message was written to */
  partition: number;
  /** Offset within the partition */
  offset: number;
}

/**
 * Request to commit a transaction.
 */
export interface CommitTransactionRequest {
  /** Producer ID */
  producer_id: number;
  /** Producer epoch */
  epoch: number;
  /** Transactional ID */
  transactional_id: string;
}

/**
 * Request to abort a transaction.
 */
export interface AbortTransactionRequest {
  /** Producer ID */
  producer_id: number;
  /** Producer epoch */
  epoch: number;
  /** Transactional ID */
  transactional_id: string;
}

/**
 * Information about an active transaction.
 */
export interface TransactionInfo {
  /** Transaction ID */
  transaction_id: string;
  /** Producer ID */
  producer_id: number;
  /** Current transaction state */
  state: TransactionState;
  /** When the transaction started */
  started_at: string;
}

/**
 * Response containing active transactions.
 */
export interface TransactionListResponse {
  /** Array of active transactions */
  transactions: TransactionInfo[];
}

/**
 * Transaction processing statistics.
 */
export interface TransactionStats {
  /** Currently active transactions */
  active_transactions: number;
  /** Total committed transactions */
  committed_total: number;
  /** Total aborted transactions */
  aborted_total: number;
  /** Average transaction duration */
  avg_duration?: string;
}

// =============================================================================
// TRACING TYPES
// =============================================================================

/**
 * Trace status values.
 */
export type TraceStatus = 'completed' | 'error' | 'pending';

/**
 * A single event in a message trace.
 */
export interface TraceEvent {
  /** When the event occurred */
  timestamp: string;
  /** Type of event (e.g., "published", "consumed", "acked") */
  event_type: string;
  /** Additional event details */
  details?: Record<string, unknown>;
}

/**
 * Complete trace for a message.
 *
 * Traces the journey of a message through the system.
 */
export interface Trace {
  /** Unique trace identifier */
  trace_id: string;
  /** Topic the message belongs to */
  topic: string;
  /** Partition number */
  partition: number;
  /** Message offset */
  offset: number;
  /** Current trace status */
  status: TraceStatus;
  /** Sequence of events for this message */
  events?: TraceEvent[];
}

/**
 * Response containing traces.
 */
export interface TraceListResponse {
  /** Array of traces */
  traces: Trace[];
}

/**
 * Trace search parameters.
 */
export interface TraceSearchParams {
  /** Filter by topic */
  topic?: string;
  /** Filter by partition */
  partition?: number;
  /** Filter by consumer group */
  consumer_group?: string;
  /** Start time (RFC3339) */
  start?: string;
  /** End time (RFC3339) */
  end?: string;
  /** Filter by status */
  status?: TraceStatus;
  /** Maximum traces to return */
  limit?: number;
}

/**
 * Tracer statistics.
 */
export interface TracerStats {
  /** Total traces recorded */
  traces_total: number;
  /** Traces that completed successfully */
  traces_completed: number;
  /** Traces that ended with errors */
  traces_error: number;
  /** Average message latency */
  avg_latency?: string;
}

// =============================================================================
// ADMIN TYPES
// =============================================================================

/**
 * Request to add partitions to a topic.
 *
 * @warning This can affect message ordering for keyed messages.
 */
export interface AddPartitionsRequest {
  /**
   * New total partition count.
   *
   * Must be greater than current partition count.
   */
  count: number;
  /** Optional replica assignments for new partitions */
  replica_assignments?: Record<string, string[]>;
}

/**
 * Response after adding partitions.
 */
export interface AddPartitionsResponse {
  /** Whether the operation succeeded */
  success: boolean;
  /** Topic name */
  topic_name: string;
  /** Partition count before */
  old_partition_count: number;
  /** Partition count after */
  new_partition_count: number;
  /** IDs of partitions that were added */
  partitions_added?: number[];
  /** Error message if operation failed */
  error?: string;
}

/**
 * Tenant quotas for multi-tenant deployments.
 */
export interface TenantQuotas {
  /** Maximum number of topics */
  max_topics?: number;
  /** Maximum partitions per topic */
  max_partitions_per_topic?: number;
  /** Maximum message size in bytes */
  max_message_size_bytes?: number;
  /** Maximum messages per second */
  max_messages_per_second?: number;
  /** Maximum bytes per second */
  max_bytes_per_second?: number;
  /** Maximum retention in hours */
  max_retention_hours?: number;
}

/**
 * Tenant status values.
 */
export type TenantStatus = 'active' | 'suspended' | 'disabled';

/**
 * Tenant information.
 */
export interface Tenant {
  /** Tenant ID */
  id: string;
  /** Tenant name */
  name: string;
  /** Display name */
  display_name?: string;
  /** Current status */
  status: TenantStatus;
  /** When the tenant was created */
  created_at: string;
  /** Tenant quotas */
  quotas?: TenantQuotas;
}

/**
 * Request to create a tenant.
 */
export interface CreateTenantRequest {
  /** Tenant name */
  name: string;
  /** Display name */
  display_name?: string;
  /** Tenant quotas */
  quotas?: TenantQuotas;
}

/**
 * Response containing tenants.
 */
export interface TenantListResponse {
  /** Array of tenants */
  tenants: Tenant[];
}

// =============================================================================
// CLIENT CONFIGURATION TYPES
// =============================================================================

/**
 * Configuration options for the GoQueue client.
 *
 * @example
 * ```typescript
 * const client = new GoQueueClient({
 *   baseUrl: 'http://localhost:8080',
 *   timeout: 30000,
 *   headers: {
 *     'X-Tenant-ID': 'my-tenant'
 *   }
 * });
 * ```
 */
export interface GoQueueClientConfig {
  /**
   * Base URL of the GoQueue server.
   *
   * @example "http://localhost:8080", "https://goqueue.example.com"
   */
  baseUrl: string;

  /**
   * Request timeout in milliseconds.
   *
   * @default 30000 (30 seconds)
   */
  timeout?: number;

  /**
   * Additional headers to include with every request.
   *
   * Useful for authentication, tenant ID, correlation ID, etc.
   */
  headers?: Record<string, string>;

  /**
   * Custom fetch implementation.
   *
   * Use for testing or to provide a custom HTTP client.
   */
  fetch?: typeof fetch;

  /**
   * Retry configuration.
   */
  retry?: {
    /**
     * Maximum number of retries.
     *
     * @default 3
     */
    maxRetries?: number;

    /**
     * Initial retry delay in milliseconds.
     *
     * Uses exponential backoff.
     *
     * @default 100
     */
    initialDelay?: number;

    /**
     * Maximum retry delay in milliseconds.
     *
     * @default 5000
     */
    maxDelay?: number;
  };
}
