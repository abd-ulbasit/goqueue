"""
GoQueue Python Client SDK
=========================

A Python client for interacting with GoQueue, a high-performance
distributed message queue that combines the best features of Kafka,
SQS, and RabbitMQ.

Installation:
    pip install goqueue-client

Quick Start:
    >>> import asyncio
    >>> from goqueue_client import GoQueueClient
    >>>
    >>> async def main():
    ...     async with GoQueueClient("http://localhost:8080") as client:
    ...         # Create a topic
    ...         await client.topics.create({
    ...             "name": "orders",
    ...             "num_partitions": 3
    ...         })
    ...         
    ...         # Publish a message
    ...         await client.messages.publish("orders", [
    ...             {"value": '{"orderId": "12345"}'}
    ...         ])
    ...         
    ...         # Consume (simple consumer)
    ...         response = await client.messages.consume("orders", 0)
    ...         for msg in response["messages"]:
    ...             print(msg["value"])
    >>>
    >>> asyncio.run(main())

Features:
    - **Topics**: Create, list, delete, and inspect topics
    - **Messages**: Publish and consume messages with ACK/NACK
    - **Consumer Groups**: Parallel processing with automatic rebalancing
    - **Delayed Messages**: Schedule messages for future delivery
    - **Priority Queues**: Priority-based message delivery
    - **Schema Registry**: JSON schema validation
    - **Transactions**: Exactly-once delivery semantics
    - **Tracing**: Message lifecycle tracking

See Also:
    - GitHub: https://github.com/abd-ulbasit/goqueue
    - Documentation: https://goqueue.dev/docs/python
"""

from .client import (
    GoQueueClient,
    GoQueueError,
    HealthService,
    TopicsService,
    MessagesService,
    DelayedService,
    GroupsService,
    PriorityService,
    SchemasService,
    TransactionsService,
    TracingService,
    AdminService,
)

from .types import (
    # Health
    HealthResponse,
    LivenessResponse,
    ReadinessResponse,
    VersionResponse,
    StatsResponse,
    # Topics
    CreateTopicRequest,
    CreateTopicResponse,
    TopicDetails,
    TopicListResponse,
    PartitionInfo,
    # Messages
    PublishMessage,
    PublishResponse,
    PublishResult,
    ConsumeResponse,
    Message,
    # Delayed
    DelayedMessagesResponse,
    DelayedMessage,
    DelayStats,
    # Consumer Groups
    GroupListResponse,
    GroupDetails,
    GroupMember,
    JoinGroupRequest,
    JoinGroupResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    LeaveGroupRequest,
    PollResponse,
    # Offsets
    OffsetsResponse,
    CommitOffsetsRequest,
    OffsetCommit,
    # Reliability
    ReliabilityStats,
    # Priority
    PriorityStats,
    # Schemas
    RegisterSchemaRequest,
    RegisterSchemaResponse,
    SchemaVersion,
    Schema,
    CompatibilityConfig,
    SchemaStats,
    # Transactions
    InitProducerRequest,
    InitProducerResponse,
    ProducerHeartbeatRequest,
    BeginTransactionRequest,
    BeginTransactionResponse,
    TransactionalPublishRequest,
    TransactionalPublishResponse,
    CommitTransactionRequest,
    AbortTransactionRequest,
    TransactionListResponse,
    TransactionInfo,
    TransactionStats,
    # Tracing
    TraceListResponse,
    Trace,
    TraceEvent,
    TraceSearchParams,
    TracerStats,
    # Admin
    AddPartitionsRequest,
    AddPartitionsResponse,
    TenantListResponse,
    CreateTenantRequest,
    Tenant,
    TenantQuotas,
)

__version__ = "1.0.0"

__all__ = [
    # Main client
    "GoQueueClient",
    "GoQueueError",
    # Service classes
    "HealthService",
    "TopicsService",
    "MessagesService",
    "DelayedService",
    "GroupsService",
    "PriorityService",
    "SchemasService",
    "TransactionsService",
    "TracingService",
    "AdminService",
    # Health types
    "HealthResponse",
    "LivenessResponse",
    "ReadinessResponse",
    "VersionResponse",
    "StatsResponse",
    # Topic types
    "CreateTopicRequest",
    "CreateTopicResponse",
    "TopicDetails",
    "TopicListResponse",
    "PartitionInfo",
    # Message types
    "PublishMessage",
    "PublishResponse",
    "PublishResult",
    "ConsumeResponse",
    "Message",
    # Delayed types
    "DelayedMessagesResponse",
    "DelayedMessage",
    "DelayStats",
    # Consumer group types
    "GroupListResponse",
    "GroupDetails",
    "GroupMember",
    "JoinGroupRequest",
    "JoinGroupResponse",
    "HeartbeatRequest",
    "HeartbeatResponse",
    "LeaveGroupRequest",
    "PollResponse",
    # Offset types
    "OffsetsResponse",
    "CommitOffsetsRequest",
    "OffsetCommit",
    # Reliability types
    "ReliabilityStats",
    # Priority types
    "PriorityStats",
    # Schema types
    "RegisterSchemaRequest",
    "RegisterSchemaResponse",
    "SchemaVersion",
    "Schema",
    "CompatibilityConfig",
    "SchemaStats",
    # Transaction types
    "InitProducerRequest",
    "InitProducerResponse",
    "ProducerHeartbeatRequest",
    "BeginTransactionRequest",
    "BeginTransactionResponse",
    "TransactionalPublishRequest",
    "TransactionalPublishResponse",
    "CommitTransactionRequest",
    "AbortTransactionRequest",
    "TransactionListResponse",
    "TransactionInfo",
    "TransactionStats",
    # Tracing types
    "TraceListResponse",
    "Trace",
    "TraceEvent",
    "TraceSearchParams",
    "TracerStats",
    # Admin types
    "AddPartitionsRequest",
    "AddPartitionsResponse",
    "TenantListResponse",
    "CreateTenantRequest",
    "Tenant",
    "TenantQuotas",
    # Version
    "__version__",
]
