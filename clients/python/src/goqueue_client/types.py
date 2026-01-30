"""
GoQueue Python Client - Type Definitions
=========================================

This module contains all type definitions for the GoQueue Python client.
These types provide full type safety and IDE support through Python type hints.

All types are compatible with Python 3.9+ and use TypedDict for structured data.

Example:
    >>> from goqueue_client import GoQueueClient, PublishMessage
    >>> message: PublishMessage = {
    ...     "key": "user-123",
    ...     "value": '{"event": "purchase"}',
    ...     "priority": "high"
    ... }
"""

from __future__ import annotations

from typing import Literal, TypedDict

# =============================================================================
# COMMON TYPES
# =============================================================================


class ErrorResponse(TypedDict):
    """
    Error response returned by the GoQueue API when an operation fails.

    Attributes:
        error: Human-readable error message describing what went wrong.

    Example:
        >>> try:
        ...     await client.topics.get("non-existent")
        ... except GoQueueError as e:
        ...     print(f"Error: {e.message}")
    """

    error: str


# =============================================================================
# HEALTH & STATUS TYPES
# =============================================================================

HealthStatus = Literal["ok", "degraded", "fail"]
"""
Health status values returned by health check endpoints.

- ``ok``: All systems operational
- ``degraded``: Some features may be unavailable  
- ``fail``: Service is not operational
"""

ProbeStatus = Literal["pass", "fail"]
"""
Probe status for Kubernetes health checks.

- ``pass``: Check passed, service is healthy
- ``fail``: Check failed, service needs attention
"""


class HealthCheckResult(TypedDict, total=False):
    """
    Individual health check result for a subsystem.

    Attributes:
        status: Status of this specific check (pass/warn/fail).
        message: Human-readable message about the check.
        latency: How long the check took to complete.
    """

    status: Literal["pass", "warn", "fail"]
    message: str
    latency: str


class HealthResponse(TypedDict):
    """
    Basic health check response from ``/health`` endpoint.

    Attributes:
        status: Overall health status of the server.
        timestamp: ISO 8601 timestamp when the check was performed.

    Example:
        >>> health = await client.health.check()
        >>> print(health["status"])  # "ok"
    """

    status: HealthStatus
    timestamp: str


class LivenessResponse(TypedDict, total=False):
    """
    Liveness probe response for Kubernetes ``/healthz`` and ``/livez`` endpoints.

    Used for Kubernetes livenessProbe configuration. If this check fails,
    Kubernetes will restart the container.

    Attributes:
        status: Probe status (pass/fail).
        timestamp: ISO 8601 timestamp when the check was performed.
        uptime: How long the service has been running.
        message: Additional status message.
    """

    status: ProbeStatus
    timestamp: str
    uptime: str
    message: str


class BrokerInfo(TypedDict, total=False):
    """Broker information included in readiness response."""

    node_id: str
    topic_count: int
    total_size: int


class ReadinessResponse(TypedDict, total=False):
    """
    Readiness probe response for Kubernetes ``/readyz`` endpoint.

    Used for Kubernetes readinessProbe configuration. If this check fails,
    the pod is removed from service endpoints (no traffic) but not restarted.

    Attributes:
        status: Probe status (pass/fail).
        timestamp: ISO 8601 timestamp when the check was performed.
        uptime: How long the service has been running.
        message: Additional status message.
        checks: Detailed results for each subsystem (when verbose=true).
        broker: Broker information.
    """

    status: ProbeStatus
    timestamp: str
    uptime: str
    message: str
    checks: dict[str, HealthCheckResult]
    broker: BrokerInfo


class VersionResponse(TypedDict, total=False):
    """
    Version information about the GoQueue server.

    Attributes:
        version: Semantic version (e.g., "1.0.0").
        git_commit: Git commit hash of the build.
        build_time: When the binary was built.
        go_version: Go version used to build the server.
    """

    version: str
    git_commit: str
    build_time: str
    go_version: str


class StatsResponse(TypedDict, total=False):
    """
    Operational statistics about the broker.

    Attributes:
        node_id: Unique identifier for this node.
        uptime: How long the broker has been running.
        topics: Total number of topics.
        total_size_bytes: Total data size across all topics.
        topic_stats: Per-topic statistics.
    """

    node_id: str
    uptime: str
    topics: int
    total_size_bytes: int
    topic_stats: dict[str, object]


# =============================================================================
# TOPIC TYPES
# =============================================================================


class CreateTopicRequest(TypedDict, total=False):
    """
    Request to create a new topic.

    Attributes:
        name: Topic name. Must match pattern ``^[a-zA-Z0-9_-]+$``.
            Length: 1-255 characters.
        num_partitions: Number of partitions (default 3).
            More partitions = more parallelism but more resources.
        retention_hours: Message retention in hours (default 168 = 7 days).

    Example:
        >>> await client.topics.create({
        ...     "name": "orders",
        ...     "num_partitions": 6,
        ...     "retention_hours": 168
        ... })
    """

    name: str  # Required but TypedDict doesn't support mixed required/optional well
    num_partitions: int
    retention_hours: int


class CreateTopicResponse(TypedDict):
    """
    Response after creating a topic.

    Attributes:
        name: Name of the created topic.
        partitions: Number of partitions created.
        created: Whether the topic was created (false if already existed).
    """

    name: str
    partitions: int
    created: bool


class PartitionOffsets(TypedDict):
    """Offset information for a partition."""

    earliest: int
    latest: int


class TopicDetails(TypedDict, total=False):
    """
    Detailed information about a topic.

    Attributes:
        name: Topic name.
        partitions: Number of partitions.
        total_messages: Total messages across all partitions.
        total_size_bytes: Total data size in bytes.
        partition_offsets: Offset ranges per partition.

    Example:
        >>> details = await client.topics.get("orders")
        >>> print(f"Total messages: {details['total_messages']}")
    """

    name: str
    partitions: int
    total_messages: int
    total_size_bytes: int
    partition_offsets: dict[str, PartitionOffsets]


class TopicListResponse(TypedDict):
    """Response containing list of topic names."""

    topics: list[str]


# =============================================================================
# MESSAGE TYPES
# =============================================================================

MessagePriority = Literal["critical", "high", "normal", "low", "background"]
"""
Message priority levels supported by GoQueue.

- ``critical``: System emergencies, circuit breakers. Processed first.
- ``high``: Paid users, real-time updates. High priority.
- ``normal``: Default for most messages.
- ``low``: Batch jobs, reports. Processed when queue is light.
- ``background``: Analytics, cleanup. Lowest priority.

Comparison with other systems:

- **RabbitMQ**: Uses 0-9 scale (higher = higher priority)
- **GoQueue**: Uses semantic names for clarity
- **Kafka**: No native priority support
- **SQS**: No priority support in standard queues
"""


class PublishMessage(TypedDict, total=False):
    """
    A single message to publish.

    Attributes:
        key: Message key for partition routing. Messages with the same key
            always go to the same partition, guaranteeing ordering for that key.
        value: Message payload. Can be any string (JSON, plain text, base64).
        partition: Explicit partition number. Overrides key-based routing.
        delay: Relative delay before delivery (e.g., "30s", "1h", "24h").
        deliverAt: Absolute delivery time (RFC3339 timestamp).
        priority: Message priority level.

    Example:
        >>> # Simple message
        >>> message: PublishMessage = {"value": '{"orderId": "12345"}'}
        >>> 
        >>> # Message with key (for ordering)
        >>> message: PublishMessage = {
        ...     "key": "user-123",
        ...     "value": '{"event": "purchase"}'
        ... }
        >>> 
        >>> # High priority delayed message
        >>> message: PublishMessage = {
        ...     "value": '{"alert": "critical"}',
        ...     "priority": "critical",
        ...     "delay": "30s"
        ... }
    """

    key: str
    value: str  # Required
    partition: int
    delay: str
    deliverAt: str
    priority: MessagePriority


class PublishResult(TypedDict, total=False):
    """
    Result for a single published message.

    Attributes:
        partition: Partition the message was written to.
        offset: Offset within the partition.
        priority: Priority assigned to the message.
        delayed: Whether the message is delayed.
        deliverAt: When the message will be delivered (for delayed messages).
        error: Error message if this specific message failed.
    """

    partition: int
    offset: int
    priority: str
    delayed: bool
    deliverAt: str
    error: str


class PublishResponse(TypedDict):
    """Response after publishing messages."""

    results: list[PublishResult]


class ConsumeMessage(TypedDict, total=False):
    """
    A consumed message.

    Attributes:
        offset: Offset within the partition.
        timestamp: ISO 8601 timestamp when the message was published.
        key: Message key (if provided during publish).
        value: Message payload.
        priority: Message priority.
    """

    offset: int
    timestamp: str
    key: str
    value: str
    priority: str


class ConsumeResponse(TypedDict):
    """Response from consuming messages."""

    messages: list[ConsumeMessage]
    next_offset: int


# =============================================================================
# DELAYED MESSAGE TYPES
# =============================================================================


class DelayedMessage(TypedDict, total=False):
    """
    Information about a delayed message.

    Attributes:
        topic: Topic the message belongs to.
        partition: Partition number.
        offset: Message offset.
        deliver_at: ISO 8601 timestamp when the message will be delivered.
        priority: Message priority.
    """

    topic: str
    partition: int
    offset: int
    deliver_at: str
    priority: str


class DelayedMessagesResponse(TypedDict):
    """Response containing delayed messages."""

    messages: list[DelayedMessage]
    count: int


class DelayStats(TypedDict, total=False):
    """
    Statistics about delayed message processing.

    Attributes:
        pending_count: Number of messages waiting for delivery.
        delivered_count: Total messages delivered after delay.
        cancelled_count: Total messages cancelled before delivery.
        avg_delay: Average delay duration.
    """

    pending_count: int
    delivered_count: int
    cancelled_count: int
    avg_delay: str


# =============================================================================
# CONSUMER GROUP TYPES
# =============================================================================

GroupState = Literal["stable", "rebalancing", "dead"]
"""
Consumer group state.

- ``stable``: All members have been assigned partitions.
- ``rebalancing``: Partition assignment is in progress.
- ``dead``: Group has no active members.
"""


class GroupMember(TypedDict, total=False):
    """
    Information about a consumer group member.

    Attributes:
        member_id: Unique member identifier.
        client_id: Client-provided identifier.
        assigned_partitions: Partitions assigned to this member.
    """

    member_id: str
    client_id: str
    assigned_partitions: list[str]


class GroupDetails(TypedDict, total=False):
    """
    Detailed information about a consumer group.

    Attributes:
        group_id: Group identifier.
        state: Current group state.
        members: List of group members.
        generation: Current generation (increments on rebalance).
        protocol: Assignment protocol in use.

    Example:
        >>> group = await client.groups.get("order-processors")
        >>> print(f"Group state: {group['state']}")
        >>> for member in group.get("members", []):
        ...     print(f"  {member['member_id']}: {member['assigned_partitions']}")
    """

    group_id: str
    state: GroupState
    members: list[GroupMember]
    generation: int
    protocol: str


class GroupListResponse(TypedDict):
    """Response containing list of consumer groups."""

    groups: list[str]


class JoinGroupRequest(TypedDict, total=False):
    """
    Request to join a consumer group.

    Attributes:
        client_id: Client identifier for logging/debugging.
        topics: Topics to subscribe to.
        session_timeout: Session timeout duration (default "30s").

    Example:
        >>> await client.groups.join("processors", {
        ...     "client_id": "worker-1",
        ...     "topics": ["orders", "payments"],
        ...     "session_timeout": "30s"
        ... })
    """

    client_id: str  # Required
    topics: list[str]  # Required
    session_timeout: str


class JoinGroupResponse(TypedDict, total=False):
    """
    Response after joining a consumer group.

    Attributes:
        member_id: Assigned member identifier.
        generation: Current group generation.
        leader: Whether this member is the group leader.
        assigned_partitions: Partitions assigned to this member.
        rebalance_required: Whether a rebalance is needed.
    """

    member_id: str
    generation: int
    leader: bool
    assigned_partitions: list[str]
    rebalance_required: bool


class HeartbeatRequest(TypedDict):
    """
    Request to send a heartbeat to keep session alive.

    Must be sent within the session timeout (default: 30s).

    Attributes:
        member_id: Member identifier (from JoinGroupResponse).
        generation: Current generation (from JoinGroupResponse).
    """

    member_id: str
    generation: int


class HeartbeatResponse(TypedDict, total=False):
    """
    Response to a heartbeat.

    Attributes:
        rebalance_required: Whether a rebalance has been triggered.
        state: Current group state.
    """

    rebalance_required: bool
    state: str


class LeaveGroupRequest(TypedDict):
    """
    Request to leave a consumer group.

    Attributes:
        member_id: Member identifier to remove.
    """

    member_id: str


class PollMessage(TypedDict, total=False):
    """
    A message from polling a consumer group.

    Attributes:
        topic: Topic the message came from.
        partition: Partition number.
        offset: Message offset.
        key: Message key.
        value: Message payload.
        timestamp: ISO 8601 timestamp when published.
        receipt_handle: Handle for ACK/NACK operations.
            Must be used to acknowledge or reject the message.
    """

    topic: str
    partition: int
    offset: int
    key: str
    value: str
    timestamp: str
    receipt_handle: str


class PollResponse(TypedDict):
    """Response from polling for messages."""

    messages: list[PollMessage]


# =============================================================================
# OFFSET TYPES
# =============================================================================


class OffsetsResponse(TypedDict):
    """
    Committed offsets response.

    Structure: ``{topic: {partition: offset}}``

    Attributes:
        offsets: Nested dict of topic -> partition -> offset.
    """

    offsets: dict[str, dict[str, int]]


class CommitOffsetsRequest(TypedDict):
    """
    Request to commit offsets.

    Attributes:
        member_id: Member identifier.
        offsets: Offsets to commit. Structure: ``{topic: {partition: offset}}``.
    """

    member_id: str
    offsets: dict[str, dict[str, int]]


# =============================================================================
# RELIABILITY TYPES (ACK/NACK/DLQ)
# =============================================================================


class AckRequest(TypedDict, total=False):
    """
    Request to acknowledge a message.

    Acknowledging tells the broker the message was successfully processed.
    The message will not be redelivered.

    Attributes:
        receipt_handle: Receipt handle from the consumed message.
        consumer_id: Consumer identifier (optional).
    """

    receipt_handle: str  # Required
    consumer_id: str


class NackRequest(TypedDict, total=False):
    """
    Request to negative-acknowledge a message.

    NACKing tells the broker the message processing failed temporarily.
    The message will be redelivered after the delay.

    Attributes:
        receipt_handle: Receipt handle from the consumed message.
        delay: Delay before redelivery (defaults to visibility timeout).
    """

    receipt_handle: str  # Required
    delay: str


class RejectRequest(TypedDict, total=False):
    """
    Request to reject a message permanently.

    Rejecting moves the message to the dead letter queue (DLQ).
    Use for messages that can never be processed.

    Attributes:
        receipt_handle: Receipt handle from the consumed message.
        reason: Reason for rejection (stored in DLQ).
    """

    receipt_handle: str  # Required
    reason: str


class ExtendVisibilityRequest(TypedDict):
    """
    Request to extend message visibility timeout.

    Use when processing takes longer than expected.

    Attributes:
        receipt_handle: Receipt handle from the consumed message.
        timeout: New visibility timeout (e.g., "60s").
    """

    receipt_handle: str
    timeout: str


class ReliabilityStats(TypedDict, total=False):
    """
    Statistics about message reliability operations.

    Attributes:
        acks_total: Total messages acknowledged.
        nacks_total: Total messages NACKed (temporary failures).
        rejects_total: Total messages rejected (moved to DLQ).
        dlq_messages: Messages currently in dead letter queues.
        visibility_extensions: Total visibility timeout extensions.
    """

    acks_total: int
    nacks_total: int
    rejects_total: int
    dlq_messages: int
    visibility_extensions: int


# =============================================================================
# PRIORITY TYPES
# =============================================================================


class PriorityStats(TypedDict, total=False):
    """
    Statistics about priority queue processing.

    Attributes:
        messages_by_priority: Message counts by priority level.
        avg_wait_time_by_priority: Average wait time by priority level.
    """

    messages_by_priority: dict[str, int]
    avg_wait_time_by_priority: dict[str, str]


# =============================================================================
# SCHEMA REGISTRY TYPES
# =============================================================================

CompatibilityLevel = Literal[
    "NONE",
    "BACKWARD",
    "FORWARD",
    "FULL",
    "BACKWARD_TRANSITIVE",
    "FORWARD_TRANSITIVE",
    "FULL_TRANSITIVE",
]
"""
Schema compatibility levels.

Controls what changes are allowed when registering new schema versions:

- ``NONE``: No compatibility checking.
- ``BACKWARD``: New schema can read old data.
- ``FORWARD``: Old schema can read new data.
- ``FULL``: Both backward and forward compatible.
- ``*_TRANSITIVE``: Check against all versions, not just latest.
"""


class RegisterSchemaRequest(TypedDict, total=False):
    """
    Request to register a schema.

    Attributes:
        schema: JSON Schema definition as a string.
        schemaType: Schema type (default "JSON").
    """

    schema: str  # Required
    schemaType: str


class RegisterSchemaResponse(TypedDict):
    """Response after registering a schema."""

    id: int


class SchemaVersion(TypedDict, total=False):
    """
    A specific version of a schema.

    Attributes:
        subject: Subject name.
        version: Version number.
        id: Global schema ID.
        schema: Schema definition.
    """

    subject: str
    version: int
    id: int
    schema: str


class Schema(TypedDict):
    """Schema without version information."""

    schema: str


class CompatibilityConfig(TypedDict):
    """Compatibility configuration."""

    compatibilityLevel: CompatibilityLevel


class SchemaStats(TypedDict, total=False):
    """
    Schema registry statistics.

    Attributes:
        subjects: Number of subjects.
        schemas: Number of unique schemas.
        versions: Total schema versions.
    """

    subjects: int
    schemas: int
    versions: int


# =============================================================================
# TRANSACTION TYPES
# =============================================================================

TransactionState = Literal[
    "ongoing",
    "preparing_commit",
    "preparing_abort",
    "completed_commit",
    "completed_abort",
]
"""Transaction state values."""


class InitProducerRequest(TypedDict, total=False):
    """
    Request to initialize a producer.

    Attributes:
        transactional_id: Transactional ID for exactly-once semantics.
    """

    transactional_id: str


class InitProducerResponse(TypedDict):
    """
    Response after initializing a producer.

    Attributes:
        producer_id: Producer ID assigned by the broker.
        epoch: Producer epoch (for zombie fencing).
    """

    producer_id: int
    epoch: int


class ProducerHeartbeatRequest(TypedDict):
    """Request to send producer heartbeat."""

    epoch: int


class BeginTransactionRequest(TypedDict):
    """
    Request to begin a transaction.

    Attributes:
        producer_id: Producer ID.
        epoch: Producer epoch.
        transactional_id: Transactional ID.
    """

    producer_id: int
    epoch: int
    transactional_id: str


class BeginTransactionResponse(TypedDict):
    """Response after beginning a transaction."""

    transaction_id: str


class TransactionalPublishRequest(TypedDict, total=False):
    """
    Request to publish a message within a transaction.

    Attributes:
        producer_id: Producer ID.
        epoch: Producer epoch.
        topic: Topic to publish to.
        key: Message key.
        value: Message value.
        sequence: Sequence number for idempotency.
    """

    producer_id: int  # Required
    epoch: int  # Required
    topic: str  # Required
    key: str
    value: str  # Required
    sequence: int


class TransactionalPublishResponse(TypedDict):
    """Response after transactional publish."""

    partition: int
    offset: int


class CommitTransactionRequest(TypedDict):
    """Request to commit a transaction."""

    producer_id: int
    epoch: int
    transactional_id: str


class AbortTransactionRequest(TypedDict):
    """Request to abort a transaction."""

    producer_id: int
    epoch: int
    transactional_id: str


class TransactionInfo(TypedDict, total=False):
    """
    Information about an active transaction.

    Attributes:
        transaction_id: Transaction ID.
        producer_id: Producer ID.
        state: Current transaction state.
        started_at: ISO 8601 timestamp when started.
    """

    transaction_id: str
    producer_id: int
    state: TransactionState
    started_at: str


class TransactionListResponse(TypedDict):
    """Response containing active transactions."""

    transactions: list[TransactionInfo]


class TransactionStats(TypedDict, total=False):
    """
    Transaction processing statistics.

    Attributes:
        active_transactions: Currently active transactions.
        committed_total: Total committed transactions.
        aborted_total: Total aborted transactions.
        avg_duration: Average transaction duration.
    """

    active_transactions: int
    committed_total: int
    aborted_total: int
    avg_duration: str


# =============================================================================
# TRACING TYPES
# =============================================================================

TraceStatus = Literal["completed", "error", "pending"]
"""Trace status values."""


class TraceEvent(TypedDict, total=False):
    """
    A single event in a message trace.

    Attributes:
        timestamp: ISO 8601 timestamp when the event occurred.
        event_type: Type of event (e.g., "published", "consumed", "acked").
        details: Additional event details.
    """

    timestamp: str
    event_type: str
    details: dict[str, object]


class Trace(TypedDict, total=False):
    """
    Complete trace for a message.

    Traces the journey of a message through the system.

    Attributes:
        trace_id: Unique trace identifier.
        topic: Topic the message belongs to.
        partition: Partition number.
        offset: Message offset.
        status: Current trace status.
        events: Sequence of events for this message.
    """

    trace_id: str
    topic: str
    partition: int
    offset: int
    status: TraceStatus
    events: list[TraceEvent]


class TraceListResponse(TypedDict):
    """Response containing traces."""

    traces: list[Trace]


class TraceSearchParams(TypedDict, total=False):
    """
    Trace search parameters.

    All parameters are optional filters.

    Attributes:
        topic: Filter by topic.
        partition: Filter by partition.
        consumer_group: Filter by consumer group.
        start: Start time (RFC3339).
        end: End time (RFC3339).
        status: Filter by status.
        limit: Maximum traces to return.
    """

    topic: str
    partition: int
    consumer_group: str
    start: str
    end: str
    status: TraceStatus
    limit: int


class TracerStats(TypedDict, total=False):
    """
    Tracer statistics.

    Attributes:
        traces_total: Total traces recorded.
        traces_completed: Traces that completed successfully.
        traces_error: Traces that ended with errors.
        avg_latency: Average message latency.
    """

    traces_total: int
    traces_completed: int
    traces_error: int
    avg_latency: str


# =============================================================================
# ADMIN TYPES
# =============================================================================


class AddPartitionsRequest(TypedDict, total=False):
    """
    Request to add partitions to a topic.

    Warning:
        This can affect message ordering for keyed messages.

    Attributes:
        count: New total partition count (must be > current).
        replica_assignments: Optional replica assignments for new partitions.
    """

    count: int  # Required
    replica_assignments: dict[str, list[str]]


class AddPartitionsResponse(TypedDict, total=False):
    """
    Response after adding partitions.

    Attributes:
        success: Whether the operation succeeded.
        topic_name: Topic name.
        old_partition_count: Partition count before.
        new_partition_count: Partition count after.
        partitions_added: IDs of partitions that were added.
        error: Error message if operation failed.
    """

    success: bool
    topic_name: str
    old_partition_count: int
    new_partition_count: int
    partitions_added: list[int]
    error: str


class TenantQuotas(TypedDict, total=False):
    """
    Tenant quotas for multi-tenant deployments.

    Attributes:
        max_topics: Maximum number of topics.
        max_partitions_per_topic: Maximum partitions per topic.
        max_message_size_bytes: Maximum message size in bytes.
        max_messages_per_second: Maximum messages per second.
        max_bytes_per_second: Maximum bytes per second.
        max_retention_hours: Maximum retention in hours.
    """

    max_topics: int
    max_partitions_per_topic: int
    max_message_size_bytes: int
    max_messages_per_second: int
    max_bytes_per_second: int
    max_retention_hours: int


TenantStatus = Literal["active", "suspended", "disabled"]
"""Tenant status values."""


class Tenant(TypedDict, total=False):
    """
    Tenant information.

    Attributes:
        id: Tenant ID.
        name: Tenant name.
        display_name: Display name.
        status: Current status.
        created_at: ISO 8601 timestamp when created.
        quotas: Tenant quotas.
    """

    id: str
    name: str
    display_name: str
    status: TenantStatus
    created_at: str
    quotas: TenantQuotas


class CreateTenantRequest(TypedDict, total=False):
    """
    Request to create a tenant.

    Attributes:
        name: Tenant name.
        display_name: Display name.
        quotas: Tenant quotas.
    """

    name: str  # Required
    display_name: str
    quotas: TenantQuotas


class TenantListResponse(TypedDict):
    """Response containing tenants."""

    tenants: list[Tenant]
