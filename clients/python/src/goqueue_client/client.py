"""
GoQueue Python Client - HTTP Client Implementation
===================================================

This module provides the main client implementation for GoQueue's HTTP/REST API.

Architecture::

    ┌─────────────────────────────────────────────────────────────────────────┐
    │                          Application Code                               │
    │                                                                         │
    │   client = GoQueueClient("http://localhost:8080")                       │
    │                                                                         │
    │   # Publish messages                                                    │
    │   await client.messages.publish("orders", [{"value": '{"id": 1}'}])     │
    │                                                                         │
    │   # Consume with consumer groups                                        │
    │   messages = await client.groups.poll("processors", member_id)          │
    │   await client.messages.ack(messages[0]["receipt_handle"])              │
    │                                                                         │
    └─────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                        GoQueueClient                                    │
    │                                                                         │
    │   ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐     │
    │   │  health  │ │  topics  │ │ messages │ │  groups  │ │  schemas │     │
    │   └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘     │
    │                              │                                         │
    │                        httpx.AsyncClient                               │
    │                     (async HTTP + retries)                             │
    └─────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                              GoQueue Server

Comparison with other Python clients:

- **confluent-kafka-python**: Wraps librdkafka C library, complex callbacks
- **boto3 SQS**: Synchronous by default, pagination handling
- **pika (RabbitMQ)**: Channel/connection model, blocking consumers
- **goqueue-client**: Simple async HTTP, works anywhere

Example:
    >>> import asyncio
    >>> from goqueue_client import GoQueueClient
    >>>
    >>> async def main():
    ...     client = GoQueueClient("http://localhost:8080")
    ...     
    ...     # Create topic
    ...     await client.topics.create({"name": "orders", "num_partitions": 3})
    ...     
    ...     # Publish
    ...     await client.messages.publish("orders", [{"value": '{"id": 1}'}])
    ...     
    ...     await client.close()
    >>>
    >>> asyncio.run(main())
"""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import quote, urljoin

import httpx

from .types import (
    # Health types
    HealthResponse,
    LivenessResponse,
    ReadinessResponse,
    VersionResponse,
    StatsResponse,
    # Topic types
    CreateTopicRequest,
    CreateTopicResponse,
    TopicDetails,
    TopicListResponse,
    # Message types
    PublishMessage,
    PublishResponse,
    ConsumeResponse,
    # Delayed types
    DelayedMessagesResponse,
    DelayedMessage,
    DelayStats,
    # Consumer group types
    GroupListResponse,
    GroupDetails,
    JoinGroupRequest,
    JoinGroupResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    LeaveGroupRequest,
    PollResponse,
    # Offset types
    OffsetsResponse,
    CommitOffsetsRequest,
    # Reliability types
    ReliabilityStats,
    # Priority types
    PriorityStats,
    # Schema types
    RegisterSchemaRequest,
    RegisterSchemaResponse,
    SchemaVersion,
    Schema,
    CompatibilityConfig,
    SchemaStats,
    # Transaction types
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
    TransactionStats,
    # Tracing types
    TraceListResponse,
    Trace,
    TraceSearchParams,
    TracerStats,
    # Admin types
    AddPartitionsRequest,
    AddPartitionsResponse,
    TenantListResponse,
    CreateTenantRequest,
    Tenant,
)


# =============================================================================
# CUSTOM EXCEPTION
# =============================================================================


class GoQueueError(Exception):
    """
    Exception raised when a GoQueue API request fails.

    Provides additional context about the failed request including
    the HTTP status code and response body.

    Attributes:
        message: Human-readable error message.
        status: HTTP status code from the response.
        body: Original response body if available.

    Example:
        >>> try:
        ...     await client.topics.get("non-existent")
        ... except GoQueueError as e:
        ...     print(f"Status: {e.status}")  # 404
        ...     print(f"Message: {e.message}")  # "Topic not found"
    """

    def __init__(self, message: str, status: int, body: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.status = status
        self.body = body

    def __str__(self) -> str:
        return f"GoQueueError({self.status}): {self.message}"


# =============================================================================
# HTTP CLIENT (INTERNAL)
# =============================================================================


class _HttpClient:
    """
    Internal HTTP client that handles all API requests.

    Provides:
    - Consistent error handling
    - Automatic JSON serialization
    - Timeout support
    - Retry with exponential backoff
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
        max_retries: int = 3,
        initial_delay: float = 0.1,
        max_delay: float = 5.0,
    ) -> None:
        # Remove trailing slash from base URL
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.headers = headers or {}
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    **self.headers,
                },
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        params: dict[str, Any] | None = None,
        retry: bool = True,
    ) -> Any:
        """
        Make an HTTP request with retry and timeout support.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE).
            path: URL path (relative to base_url).
            body: Request body (will be JSON-encoded).
            params: Query parameters.
            retry: Whether to retry on failure.

        Returns:
            Parsed JSON response.

        Raises:
            GoQueueError: If the request fails.
        """
        # Filter out None values from params
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        last_error: Exception | None = None
        max_attempts = (self.max_retries + 1) if retry else 1

        for attempt in range(max_attempts):
            try:
                return await self._do_request(method, path, body, params)
            except GoQueueError as e:
                # Don't retry client errors (4xx)
                if 400 <= e.status < 500:
                    raise
                last_error = e
            except Exception as e:
                last_error = e

            # Wait before retrying (exponential backoff)
            if attempt < max_attempts - 1:
                delay = min(
                    self.initial_delay * (2 ** attempt),
                    self.max_delay,
                )
                await asyncio.sleep(delay)

        if last_error is not None:
            if isinstance(last_error, GoQueueError):
                raise last_error
            raise GoQueueError(str(last_error), 0)
        
        raise GoQueueError("Request failed with no error details", 0)

    async def _do_request(
        self,
        method: str,
        path: str,
        body: Any,
        params: dict[str, Any] | None,
    ) -> Any:
        """Execute a single HTTP request."""
        client = await self._get_client()

        try:
            response = await client.request(
                method,
                path,
                json=body if body is not None else None,
                params=params,
            )
        except httpx.TimeoutException:
            raise GoQueueError(f"Request timeout after {self.timeout}s", 408)
        except httpx.RequestError as e:
            raise GoQueueError(f"Network error: {e}", 0)

        # Handle non-JSON responses (like /metrics)
        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type:
            if not response.is_success:
                raise GoQueueError(
                    f"Request failed: {response.status_code} {response.reason_phrase}",
                    response.status_code,
                )
            return response.text

        # Parse JSON response
        try:
            data = response.json()
        except Exception:
            data = {}

        # Handle errors
        if not response.is_success:
            error_message = (
                data.get("error")
                if isinstance(data, dict)
                else f"Request failed: {response.status_code}"
            )
            raise GoQueueError(error_message, response.status_code, data)

        return data


# =============================================================================
# SERVICE CLASSES
# =============================================================================


class HealthService:
    """
    Health check operations.

    Provides endpoints for health monitoring and Kubernetes probes.

    Example:
        >>> # Simple health check
        >>> health = await client.health.check()
        >>> print(health["status"])  # "ok"
        >>>
        >>> # Kubernetes readiness with verbose output
        >>> readiness = await client.health.readiness(verbose=True)
        >>> for name, result in readiness.get("checks", {}).items():
        ...     print(f"{name}: {result['status']}")
    """

    def __init__(self, http: _HttpClient) -> None:
        self._http = http

    async def check(self) -> HealthResponse:
        """
        Basic health check.

        Returns the overall health status of the server.

        Returns:
            Health response with status and timestamp.
        """
        return await self._http.request("GET", "/health")

    async def liveness(self) -> LivenessResponse:
        """
        Kubernetes liveness probe.

        Use for ``livenessProbe`` in Kubernetes deployments.
        Returns 200 if the process is alive and not deadlocked.

        Note:
            Failing this probe causes the container to be killed and restarted.

        Returns:
            Liveness response with status.
        """
        return await self._http.request("GET", "/healthz")

    async def readiness(self, *, verbose: bool = False) -> ReadinessResponse:
        """
        Kubernetes readiness probe.

        Use for ``readinessProbe`` in Kubernetes deployments.
        Returns 200 if the server is ready to handle requests.

        Note:
            Failing this probe removes the pod from service endpoints.

        Args:
            verbose: Include detailed check results for each subsystem.

        Returns:
            Readiness response with status and optional checks.
        """
        return await self._http.request("GET", "/readyz", params={"verbose": verbose})

    async def startup(self) -> LivenessResponse:
        """
        Kubernetes startup probe.

        Use for ``startupProbe`` in Kubernetes deployments.
        Returns 200 when initialization is complete.

        Returns:
            Liveness response with status.
        """
        return await self._http.request("GET", "/livez")

    async def version(self) -> VersionResponse:
        """
        Get version information.

        Returns build and version information about the server.

        Returns:
            Version response with version, git_commit, build_time, go_version.
        """
        return await self._http.request("GET", "/version")

    async def stats(self) -> StatsResponse:
        """
        Get broker statistics.

        Returns operational statistics about the broker.

        Returns:
            Stats response with node_id, uptime, topics, total_size_bytes.
        """
        return await self._http.request("GET", "/stats")

    async def metrics(self) -> str:
        """
        Get Prometheus metrics.

        Returns metrics in Prometheus text exposition format.
        Configure Prometheus to scrape this endpoint.

        Returns:
            Prometheus metrics as plain text.
        """
        return await self._http.request("GET", "/metrics")


class TopicsService:
    """
    Topic management operations.

    Example:
        >>> # Create a topic with 6 partitions
        >>> await client.topics.create({
        ...     "name": "orders",
        ...     "num_partitions": 6,
        ...     "retention_hours": 168
        ... })
        >>>
        >>> # List all topics
        >>> response = await client.topics.list()
        >>> print(response["topics"])
        >>>
        >>> # Get topic details
        >>> details = await client.topics.get("orders")
        >>> print(f"Total messages: {details['total_messages']}")
        >>>
        >>> # Delete a topic
        >>> await client.topics.delete("old-topic")
    """

    def __init__(self, http: _HttpClient) -> None:
        self._http = http

    async def list(self) -> TopicListResponse:
        """
        List all topics.

        Returns the names of all topics in the broker.

        Returns:
            Response with list of topic names.
        """
        return await self._http.request("GET", "/topics")

    async def create(self, request: CreateTopicRequest) -> CreateTopicResponse:
        """
        Create a new topic.

        Topics are the primary unit of organization in GoQueue.
        Messages are published to topics and consumed from topics.

        Args:
            request: Topic creation parameters including name, num_partitions,
                and retention_hours.

        Returns:
            Response with name, partitions, and created status.

        Raises:
            GoQueueError: 409 if topic already exists.
        """
        return await self._http.request("POST", "/topics", body=request)

    async def get(self, name: str) -> TopicDetails:
        """
        Get topic details.

        Returns detailed information about a specific topic including
        partition offsets and size information.

        Args:
            name: Topic name.

        Returns:
            Topic details with partitions, messages, and offsets.

        Raises:
            GoQueueError: 404 if topic not found.
        """
        return await self._http.request("GET", f"/topics/{quote(name)}")

    async def delete(self, name: str) -> dict[str, Any]:
        """
        Delete a topic.

        Permanently deletes a topic and all its data.

        Warning:
            This operation is irreversible!

        Args:
            name: Topic name to delete.

        Returns:
            Response with deleted status and name.

        Raises:
            GoQueueError: 404 if topic not found.
        """
        return await self._http.request("DELETE", f"/topics/{quote(name)}")


class MessagesService:
    """
    Message publishing and consumption operations.

    Example:
        >>> # Publish a simple message
        >>> result = await client.messages.publish("orders", [
        ...     {"value": json.dumps({"orderId": "12345"})}
        ... ])
        >>>
        >>> # Publish with key (for ordering)
        >>> await client.messages.publish("orders", [
        ...     {"key": "user-123", "value": json.dumps({"event": "purchase"})}
        ... ])
        >>>
        >>> # Publish high priority message
        >>> await client.messages.publish("alerts", [
        ...     {"value": "Critical alert!", "priority": "critical"}
        ... ])
        >>>
        >>> # Publish delayed message (1 hour)
        >>> await client.messages.publish("reminders", [
        ...     {"value": "Follow up", "delay": "1h"}
        ... ])
    """

    def __init__(self, http: _HttpClient) -> None:
        self._http = http

    async def publish(
        self, topic: str, messages: list[PublishMessage]
    ) -> PublishResponse:
        """
        Publish messages to a topic.

        Messages can include keys for ordering, priorities, and delays.

        Args:
            topic: Topic name to publish to.
            messages: Array of messages to publish.

        Returns:
            Results for each message (partition, offset, or error).

        Raises:
            GoQueueError: 404 if topic not found.
        """
        return await self._http.request(
            "POST",
            f"/topics/{quote(topic)}/messages",
            body={"messages": messages},
        )

    async def consume(
        self,
        topic: str,
        partition: int,
        *,
        offset: int = 0,
        limit: int = 100,
    ) -> ConsumeResponse:
        """
        Consume messages from a partition (simple consumer).

        For production use, prefer consumer groups with ``client.groups.poll()``.
        This method is useful for debugging and simple use cases.

        Args:
            topic: Topic name.
            partition: Partition ID (0-based).
            offset: Starting offset (default: 0).
            limit: Maximum messages to return (default: 100, max: 1000).

        Returns:
            Response with messages and next_offset.
        """
        return await self._http.request(
            "GET",
            f"/topics/{quote(topic)}/partitions/{partition}/messages",
            params={"offset": offset, "limit": limit},
        )

    async def ack(
        self, receipt_handle: str, *, consumer_id: str | None = None
    ) -> dict[str, Any]:
        """
        Acknowledge a message.

        Tells the broker the message was successfully processed.
        The message will not be redelivered.

        Args:
            receipt_handle: Receipt handle from the consumed message.
            consumer_id: Optional consumer identifier.

        Returns:
            Response with acknowledged status.
        """
        return await self._http.request(
            "POST",
            "/messages/ack",
            body={"receipt_handle": receipt_handle, "consumer_id": consumer_id},
        )

    async def nack(
        self, receipt_handle: str, *, delay: str | None = None
    ) -> dict[str, Any]:
        """
        Negative acknowledge a message.

        Tells the broker the message processing failed temporarily.
        The message will be redelivered after the delay.

        Args:
            receipt_handle: Receipt handle from the consumed message.
            delay: Optional delay before redelivery (e.g., "30s", "1m").

        Returns:
            Response with requeued status.
        """
        return await self._http.request(
            "POST",
            "/messages/nack",
            body={"receipt_handle": receipt_handle, "delay": delay},
        )

    async def reject(
        self, receipt_handle: str, *, reason: str | None = None
    ) -> dict[str, Any]:
        """
        Reject a message (send to DLQ).

        Use for messages that can never be processed (invalid format, etc).
        The message will be moved to the dead letter queue.

        Args:
            receipt_handle: Receipt handle from the consumed message.
            reason: Reason for rejection (stored in DLQ).

        Returns:
            Response with rejected status and dlq_topic.
        """
        return await self._http.request(
            "POST",
            "/messages/reject",
            body={"receipt_handle": receipt_handle, "reason": reason},
        )

    async def extend_visibility(
        self, receipt_handle: str, timeout: str
    ) -> dict[str, Any]:
        """
        Extend message visibility timeout.

        Use when processing takes longer than expected.
        Prevents the message from being redelivered while still processing.

        Args:
            receipt_handle: Receipt handle from the consumed message.
            timeout: New visibility timeout (e.g., "60s", "5m").

        Returns:
            Response with extended status and new_deadline.
        """
        return await self._http.request(
            "POST",
            "/messages/visibility",
            body={"receipt_handle": receipt_handle, "timeout": timeout},
        )

    async def reliability_stats(self) -> ReliabilityStats:
        """
        Get reliability statistics.

        Returns ACK/NACK/DLQ statistics.

        Returns:
            Stats with acks_total, nacks_total, rejects_total, dlq_messages.
        """
        return await self._http.request("GET", "/reliability/stats")


class DelayedService:
    """
    Delayed message operations.

    Example:
        >>> # List delayed messages
        >>> delayed = await client.delayed.list("orders", limit=50)
        >>> for msg in delayed["messages"]:
        ...     print(f"Delivering at: {msg['deliver_at']}")
        >>>
        >>> # Cancel a delayed message
        >>> await client.delayed.cancel("orders", 0, 12345)
        >>>
        >>> # Get delay statistics
        >>> stats = await client.delayed.stats()
        >>> print(f"Pending: {stats['pending_count']}")
    """

    def __init__(self, http: _HttpClient) -> None:
        self._http = http

    async def list(
        self, topic: str, *, limit: int = 100
    ) -> DelayedMessagesResponse:
        """
        List delayed messages for a topic.

        Args:
            topic: Topic name.
            limit: Maximum messages to return (default: 100).

        Returns:
            Response with messages and count.
        """
        return await self._http.request(
            "GET",
            f"/topics/{quote(topic)}/delayed",
            params={"limit": limit},
        )

    async def get(self, topic: str, offset: int) -> DelayedMessage:
        """
        Get a specific delayed message.

        Args:
            topic: Topic name.
            offset: Message offset.

        Returns:
            Delayed message details.
        """
        return await self._http.request(
            "GET",
            f"/topics/{quote(topic)}/delayed/{offset}",
        )

    async def cancel(
        self, topic: str, partition: int, offset: int
    ) -> dict[str, Any]:
        """
        Cancel a delayed message.

        Cancels a pending delayed message before it's delivered.

        Args:
            topic: Topic name.
            partition: Partition ID.
            offset: Message offset.

        Returns:
            Response with cancelled status.
        """
        return await self._http.request(
            "DELETE",
            f"/topics/{quote(topic)}/delayed/{partition}/{offset}",
        )

    async def stats(self) -> DelayStats:
        """
        Get delay queue statistics.

        Returns:
            Stats with pending_count, delivered_count, cancelled_count.
        """
        return await self._http.request("GET", "/delay/stats")


class GroupsService:
    """
    Consumer group operations.

    Consumer groups enable parallel processing of messages by distributing
    partitions among group members.

    Example:
        >>> # Join a consumer group
        >>> join_response = await client.groups.join(
        ...     "order-processors",
        ...     {
        ...         "client_id": "worker-1",
        ...         "topics": ["orders"],
        ...         "session_timeout": "30s"
        ...     }
        ... )
        >>> member_id = join_response["member_id"]
        >>> generation = join_response["generation"]
        >>>
        >>> # Poll for messages
        >>> while running:
        ...     response = await client.groups.poll(
        ...         "order-processors",
        ...         member_id,
        ...         max_messages=10,
        ...         timeout="30s"
        ...     )
        ...     
        ...     for msg in response["messages"]:
        ...         # Process message
        ...         await process_order(msg["value"])
        ...         
        ...         # Acknowledge
        ...         await client.messages.ack(msg["receipt_handle"])
        ...     
        ...     # Send heartbeat
        ...     heartbeat = await client.groups.heartbeat(
        ...         "order-processors",
        ...         {"member_id": member_id, "generation": generation}
        ...     )
        ...     
        ...     if heartbeat["rebalance_required"]:
        ...         # Rejoin to get new assignments
        ...         pass
        >>>
        >>> # Leave the group
        >>> await client.groups.leave("order-processors", {"member_id": member_id})
    """

    def __init__(self, http: _HttpClient) -> None:
        self._http = http

    async def list(self) -> GroupListResponse:
        """
        List all consumer groups.

        Returns:
            Response with list of group IDs.
        """
        return await self._http.request("GET", "/groups")

    async def get(self, group_id: str) -> GroupDetails:
        """
        Get consumer group details.

        Args:
            group_id: Consumer group ID.

        Returns:
            Group details with state, members, and generation.
        """
        return await self._http.request("GET", f"/groups/{quote(group_id)}")

    async def delete(self, group_id: str) -> dict[str, Any]:
        """
        Delete a consumer group.

        Deletes the group and its offset data.

        Args:
            group_id: Consumer group ID.

        Returns:
            Response with deleted status.
        """
        return await self._http.request("DELETE", f"/groups/{quote(group_id)}")

    async def join(
        self, group_id: str, request: JoinGroupRequest
    ) -> JoinGroupResponse:
        """
        Join a consumer group.

        The broker will assign partitions based on the number of members
        and available partitions.

        Args:
            group_id: Consumer group ID.
            request: Join request with client_id, topics, and session_timeout.

        Returns:
            Response with member_id, generation, leader, assigned_partitions.
        """
        return await self._http.request(
            "POST",
            f"/groups/{quote(group_id)}/join",
            body=request,
        )

    async def heartbeat(
        self, group_id: str, request: HeartbeatRequest
    ) -> HeartbeatResponse:
        """
        Send a heartbeat to keep the session alive.

        Must be sent within the session timeout (default: 30s).

        Args:
            group_id: Consumer group ID.
            request: Heartbeat request with member_id and generation.

        Returns:
            Response with rebalance_required status.
        """
        return await self._http.request(
            "POST",
            f"/groups/{quote(group_id)}/heartbeat",
            body=request,
        )

    async def leave(
        self, group_id: str, request: LeaveGroupRequest
    ) -> dict[str, Any]:
        """
        Leave a consumer group.

        Triggers a rebalance of the remaining members.

        Args:
            group_id: Consumer group ID.
            request: Leave request with member_id.

        Returns:
            Response with left status.
        """
        return await self._http.request(
            "POST",
            f"/groups/{quote(group_id)}/leave",
            body=request,
        )

    async def poll(
        self,
        group_id: str,
        member_id: str,
        *,
        max_messages: int = 100,
        timeout: str = "30s",
    ) -> PollResponse:
        """
        Poll for messages from assigned partitions.

        Long-polls for messages. Returns immediately if messages are available,
        or waits up to the timeout.

        Args:
            group_id: Consumer group ID.
            member_id: Member ID (from join response).
            max_messages: Maximum messages to return (default: 100).
            timeout: Long-poll timeout (default: "30s").

        Returns:
            Response with list of messages.
        """
        return await self._http.request(
            "GET",
            f"/groups/{quote(group_id)}/poll",
            params={
                "member_id": member_id,
                "max_messages": max_messages,
                "timeout": timeout,
            },
        )

    async def get_offsets(
        self, group_id: str, *, topic: str | None = None
    ) -> OffsetsResponse:
        """
        Get committed offsets for a consumer group.

        Args:
            group_id: Consumer group ID.
            topic: Optional topic filter.

        Returns:
            Response with offsets dict.
        """
        return await self._http.request(
            "GET",
            f"/groups/{quote(group_id)}/offsets",
            params={"topic": topic},
        )

    async def commit_offsets(
        self, group_id: str, request: CommitOffsetsRequest
    ) -> dict[str, Any]:
        """
        Commit offsets for a consumer group.

        Only offsets for assigned partitions can be committed.

        Args:
            group_id: Consumer group ID.
            request: Commit request with member_id and offsets.

        Returns:
            Response with committed status.
        """
        return await self._http.request(
            "POST",
            f"/groups/{quote(group_id)}/offsets",
            body=request,
        )


class PriorityService:
    """
    Priority queue operations.

    Example:
        >>> stats = await client.priority.stats()
        >>> print("Messages by priority:", stats["messages_by_priority"])
        >>> print("Avg wait times:", stats["avg_wait_time_by_priority"])
    """

    def __init__(self, http: _HttpClient) -> None:
        self._http = http

    async def stats(self) -> PriorityStats:
        """
        Get priority queue statistics.

        Returns:
            Stats with messages_by_priority and avg_wait_time_by_priority.
        """
        return await self._http.request("GET", "/priority/stats")


class SchemasService:
    """
    Schema registry operations.

    The schema registry validates message payloads against JSON schemas
    and ensures compatibility between schema versions.

    Example:
        >>> # Register a schema
        >>> response = await client.schemas.register(
        ...     "orders-value",
        ...     {
        ...         "schema": json.dumps({
        ...             "type": "object",
        ...             "properties": {
        ...                 "orderId": {"type": "string"},
        ...                 "amount": {"type": "number"}
        ...             },
        ...             "required": ["orderId", "amount"]
        ...         })
        ...     }
        ... )
        >>> schema_id = response["id"]
        >>>
        >>> # Get the latest schema
        >>> schema = await client.schemas.get_version("orders-value", "latest")
        >>>
        >>> # Set compatibility mode
        >>> await client.schemas.set_config({"compatibilityLevel": "BACKWARD"})
    """

    def __init__(self, http: _HttpClient) -> None:
        self._http = http

    async def list_subjects(self) -> list[str]:
        """
        List all schema subjects.

        Returns:
            List of subject names.
        """
        return await self._http.request("GET", "/schemas/subjects")

    async def list_versions(self, subject: str) -> list[int]:
        """
        List versions for a subject.

        Args:
            subject: Subject name.

        Returns:
            List of version numbers.
        """
        return await self._http.request(
            "GET",
            f"/schemas/subjects/{quote(subject)}/versions",
        )

    async def register(
        self, subject: str, request: RegisterSchemaRequest
    ) -> RegisterSchemaResponse:
        """
        Register a new schema version.

        The schema is validated for compatibility with existing versions.

        Args:
            subject: Subject name.
            request: Schema registration request with schema and schemaType.

        Returns:
            Response with global schema ID.

        Raises:
            GoQueueError: 409 if schema is incompatible.
        """
        return await self._http.request(
            "POST",
            f"/schemas/subjects/{quote(subject)}/versions",
            body=request,
        )

    async def get_version(
        self, subject: str, version: int | str
    ) -> SchemaVersion:
        """
        Get a specific schema version.

        Args:
            subject: Subject name.
            version: Version number or "latest".

        Returns:
            Schema version with subject, version, id, schema.
        """
        return await self._http.request(
            "GET",
            f"/schemas/subjects/{quote(subject)}/versions/{version}",
        )

    async def delete_version(self, subject: str, version: int) -> int:
        """
        Delete a schema version.

        Args:
            subject: Subject name.
            version: Version number.

        Returns:
            The deleted version number.
        """
        return await self._http.request(
            "DELETE",
            f"/schemas/subjects/{quote(subject)}/versions/{version}",
        )

    async def get_by_id(self, schema_id: int) -> Schema:
        """
        Get schema by global ID.

        Args:
            schema_id: Global schema ID.

        Returns:
            Schema with schema definition.
        """
        return await self._http.request("GET", f"/schemas/ids/{schema_id}")

    async def get_config(self) -> CompatibilityConfig:
        """
        Get global compatibility configuration.

        Returns:
            Config with compatibilityLevel.
        """
        return await self._http.request("GET", "/schemas/config")

    async def set_config(self, config: CompatibilityConfig) -> CompatibilityConfig:
        """
        Set global compatibility configuration.

        Args:
            config: Compatibility configuration.

        Returns:
            Updated config.
        """
        return await self._http.request("PUT", "/schemas/config", body=config)

    async def stats(self) -> SchemaStats:
        """
        Get schema registry statistics.

        Returns:
            Stats with subjects, schemas, versions.
        """
        return await self._http.request("GET", "/schemas/stats")


class TransactionsService:
    """
    Transaction operations for exactly-once semantics.

    Example:
        >>> # Initialize producer
        >>> producer = await client.transactions.init_producer({
        ...     "transactional_id": "order-processor-1"
        ... })
        >>> producer_id = producer["producer_id"]
        >>> epoch = producer["epoch"]
        >>>
        >>> # Begin transaction
        >>> await client.transactions.begin({
        ...     "producer_id": producer_id,
        ...     "epoch": epoch,
        ...     "transactional_id": "order-processor-1"
        ... })
        >>>
        >>> # Publish within transaction
        >>> await client.transactions.publish({
        ...     "producer_id": producer_id,
        ...     "epoch": epoch,
        ...     "topic": "orders",
        ...     "value": json.dumps({"orderId": "123"}),
        ...     "sequence": 1
        ... })
        >>>
        >>> # Commit transaction
        >>> await client.transactions.commit({
        ...     "producer_id": producer_id,
        ...     "epoch": epoch,
        ...     "transactional_id": "order-processor-1"
        ... })
    """

    def __init__(self, http: _HttpClient) -> None:
        self._http = http

    async def init_producer(
        self, request: InitProducerRequest | None = None
    ) -> InitProducerResponse:
        """
        Initialize an idempotent/transactional producer.

        Returns a producer ID and epoch for subsequent operations.

        Args:
            request: Initialization request with optional transactional_id.

        Returns:
            Response with producer_id and epoch.
        """
        return await self._http.request(
            "POST",
            "/producers/init",
            body=request or {},
        )

    async def producer_heartbeat(
        self, producer_id: int, request: ProducerHeartbeatRequest
    ) -> dict[str, Any]:
        """
        Send producer heartbeat.

        Keeps the producer session alive.

        Args:
            producer_id: Producer ID.
            request: Heartbeat request with epoch.

        Returns:
            Response with success status.
        """
        return await self._http.request(
            "POST",
            f"/producers/{producer_id}/heartbeat",
            body=request,
        )

    async def list(self) -> TransactionListResponse:
        """
        List active transactions.

        Returns:
            Response with list of transactions.
        """
        return await self._http.request("GET", "/transactions")

    async def begin(
        self, request: BeginTransactionRequest
    ) -> BeginTransactionResponse:
        """
        Begin a new transaction.

        Args:
            request: Transaction begin request.

        Returns:
            Response with transaction_id.
        """
        return await self._http.request(
            "POST",
            "/transactions/begin",
            body=request,
        )

    async def publish(
        self, request: TransactionalPublishRequest
    ) -> TransactionalPublishResponse:
        """
        Publish a message within a transaction.

        Args:
            request: Transactional publish request.

        Returns:
            Response with partition and offset.
        """
        return await self._http.request(
            "POST",
            "/transactions/publish",
            body=request,
        )

    async def commit(
        self, request: CommitTransactionRequest
    ) -> dict[str, Any]:
        """
        Commit a transaction.

        Atomically commits all messages in the transaction.

        Args:
            request: Commit request.

        Returns:
            Response with status "committed".
        """
        return await self._http.request(
            "POST",
            "/transactions/commit",
            body=request,
        )

    async def abort(self, request: AbortTransactionRequest) -> dict[str, Any]:
        """
        Abort a transaction.

        Rolls back all messages in the transaction.

        Args:
            request: Abort request.

        Returns:
            Response with status "aborted".
        """
        return await self._http.request(
            "POST",
            "/transactions/abort",
            body=request,
        )

    async def stats(self) -> TransactionStats:
        """
        Get transaction statistics.

        Returns:
            Stats with active_transactions, committed_total, aborted_total.
        """
        return await self._http.request("GET", "/transactions/stats")


class TracingService:
    """
    Message tracing operations.

    Traces allow you to follow a message's journey through the system.

    Example:
        >>> # List recent traces
        >>> response = await client.tracing.list(limit=100)
        >>> for trace in response["traces"]:
        ...     print(f"{trace['trace_id']}: {trace['status']}")
        >>>
        >>> # Search for error traces
        >>> results = await client.tracing.search({
        ...     "topic": "orders",
        ...     "status": "error",
        ...     "start": "2025-01-30T00:00:00Z"
        ... })
        >>>
        >>> # Get trace details
        >>> trace = await client.tracing.get("trace-123")
        >>> for event in trace.get("events", []):
        ...     print(f"{event['timestamp']}: {event['event_type']}")
    """

    def __init__(self, http: _HttpClient) -> None:
        self._http = http

    async def list(self, *, limit: int = 100) -> TraceListResponse:
        """
        List recent traces.

        Args:
            limit: Maximum traces to return (default: 100).

        Returns:
            Response with list of traces.
        """
        return await self._http.request(
            "GET",
            "/traces",
            params={"limit": limit},
        )

    async def search(
        self, params: TraceSearchParams | None = None
    ) -> TraceListResponse:
        """
        Search traces by criteria.

        Args:
            params: Search parameters (topic, partition, status, etc).

        Returns:
            Response with list of matching traces.
        """
        return await self._http.request(
            "GET",
            "/traces/search",
            params=params,
        )

    async def get(self, trace_id: str) -> Trace:
        """
        Get a specific trace by ID.

        Args:
            trace_id: Trace ID.

        Returns:
            Trace with trace_id, topic, partition, offset, status, events.
        """
        return await self._http.request("GET", f"/traces/{quote(trace_id)}")

    async def stats(self) -> TracerStats:
        """
        Get tracer statistics.

        Returns:
            Stats with traces_total, traces_completed, traces_error.
        """
        return await self._http.request("GET", "/traces/stats")


class AdminService:
    """
    Administrative operations.

    Example:
        >>> # Add partitions to a topic
        >>> await client.admin.add_partitions("orders", {"count": 12})
        >>>
        >>> # Create a tenant
        >>> await client.admin.create_tenant({
        ...     "name": "acme-corp",
        ...     "quotas": {
        ...         "max_topics": 100,
        ...         "max_messages_per_second": 10000
        ...     }
        ... })
    """

    def __init__(self, http: _HttpClient) -> None:
        self._http = http

    async def add_partitions(
        self, topic: str, request: AddPartitionsRequest
    ) -> AddPartitionsResponse:
        """
        Add partitions to a topic.

        Warning:
            This can affect message ordering for keyed messages.

        Args:
            topic: Topic name.
            request: Request with new partition count.

        Returns:
            Response with success, old/new counts, partitions_added.
        """
        return await self._http.request(
            "POST",
            f"/admin/topics/{quote(topic)}/partitions",
            body=request,
        )

    async def list_tenants(self) -> TenantListResponse:
        """
        List all tenants.

        Returns:
            Response with list of tenants.
        """
        return await self._http.request("GET", "/admin/tenants")

    async def create_tenant(self, request: CreateTenantRequest) -> Tenant:
        """
        Create a new tenant.

        Args:
            request: Tenant creation request.

        Returns:
            Created tenant.
        """
        return await self._http.request(
            "POST",
            "/admin/tenants",
            body=request,
        )


# =============================================================================
# MAIN CLIENT CLASS
# =============================================================================


class GoQueueClient:
    """
    GoQueue client for interacting with a GoQueue server.

    This is the main entry point for the GoQueue Python SDK.
    It provides access to all GoQueue features through service-specific APIs.

    Example:
        >>> import asyncio
        >>> from goqueue_client import GoQueueClient
        >>>
        >>> async def main():
        ...     # Create a client
        ...     client = GoQueueClient(
        ...         "http://localhost:8080",
        ...         timeout=30.0,
        ...         headers={"X-Tenant-ID": "my-tenant"}
        ...     )
        ...     
        ...     try:
        ...         # Check health
        ...         health = await client.health.check()
        ...         print(f"Status: {health['status']}")
        ...         
        ...         # Create a topic
        ...         await client.topics.create({
        ...             "name": "orders",
        ...             "num_partitions": 6
        ...         })
        ...         
        ...         # Publish messages
        ...         await client.messages.publish("orders", [
        ...             {"key": "user-123", "value": '{"orderId": "abc"}'}
        ...         ])
        ...         
        ...         # Join a consumer group
        ...         join = await client.groups.join("order-processors", {
        ...             "client_id": "worker-1",
        ...             "topics": ["orders"]
        ...         })
        ...         member_id = join["member_id"]
        ...         
        ...         # Poll for messages
        ...         response = await client.groups.poll("order-processors", member_id)
        ...         for msg in response["messages"]:
        ...             print(f"Received: {msg['value']}")
        ...             await client.messages.ack(msg["receipt_handle"])
        ...     finally:
        ...         await client.close()
        >>>
        >>> asyncio.run(main())

    Attributes:
        health: Health check operations (probes, stats, version).
        topics: Topic management (create, list, delete).
        messages: Message operations (publish, consume, ack/nack).
        delayed: Delayed message operations (list, cancel).
        groups: Consumer group operations (join, poll, leave).
        priority: Priority queue statistics.
        schemas: Schema registry operations.
        transactions: Transaction operations (exactly-once).
        tracing: Message tracing operations.
        admin: Administrative operations (partitions, tenants).

    See Also:
        - https://github.com/abd-ulbasit/goqueue
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
        max_retries: int = 3,
        initial_delay: float = 0.1,
        max_delay: float = 5.0,
    ) -> None:
        """
        Create a new GoQueue client.

        Args:
            base_url: Base URL of the GoQueue server.
            timeout: Request timeout in seconds (default: 30.0).
            headers: Additional headers for all requests.
            max_retries: Maximum retry attempts (default: 3).
            initial_delay: Initial retry delay in seconds (default: 0.1).
            max_delay: Maximum retry delay in seconds (default: 5.0).
        """
        self._http = _HttpClient(
            base_url,
            timeout=timeout,
            headers=headers,
            max_retries=max_retries,
            initial_delay=initial_delay,
            max_delay=max_delay,
        )

        self.health = HealthService(self._http)
        self.topics = TopicsService(self._http)
        self.messages = MessagesService(self._http)
        self.delayed = DelayedService(self._http)
        self.groups = GroupsService(self._http)
        self.priority = PriorityService(self._http)
        self.schemas = SchemasService(self._http)
        self.transactions = TransactionsService(self._http)
        self.tracing = TracingService(self._http)
        self.admin = AdminService(self._http)

    async def close(self) -> None:
        """
        Close the client and release resources.

        Should be called when done using the client.

        Example:
            >>> client = GoQueueClient("http://localhost:8080")
            >>> try:
            ...     # Use client
            ...     pass
            ... finally:
            ...     await client.close()
        """
        await self._http.close()

    async def __aenter__(self) -> "GoQueueClient":
        """Support async context manager protocol."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Close client when exiting context."""
        await self.close()
