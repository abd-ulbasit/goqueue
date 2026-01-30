// =============================================================================
// GOQUEUE TYPESCRIPT CLIENT - HTTP CLIENT IMPLEMENTATION
// =============================================================================
//
// This is the main client implementation for GoQueue's HTTP/REST API.
//
// ARCHITECTURE:
//
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │                          Application Code                              │
//   │                                                                        │
//   │   const client = new GoQueueClient({ baseUrl: 'http://localhost:8080' });
//   │                                                                        │
//   │   // Publish messages                                                  │
//   │   await client.messages.publish('orders', [{ value: '{"id": 1}' }]);  │
//   │                                                                        │
//   │   // Consume with consumer groups                                      │
//   │   const messages = await client.groups.poll('order-processors', ...); │
//   │   await client.messages.ack(messages[0].receipt_handle);              │
//   │                                                                        │
//   └────────────────────────────────────────────────────────────────────────┘
//                                     │
//                                     ▼
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │                        GoQueueClient                                   │
//   │                                                                        │
//   │   ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │
//   │   │  health  │ │  topics  │ │ messages │ │  groups  │ │  schemas │   │
//   │   └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘   │
//   │        │            │            │            │            │          │
//   │        └────────────┴────────────┴────────────┴────────────┘          │
//   │                                  │                                    │
//   │                           HttpClient                                  │
//   │                      (fetch + retry + timeout)                        │
//   └──────────────────────────────────┼────────────────────────────────────┘
//                                      │
//                                      ▼
//                              GoQueue Server
//
// COMPARISON WITH OTHER CLIENTS:
//
//   - Kafka JS: Complex consumer lifecycle, rebalancing callbacks
//   - SQS SDK: Simple but missing consumer groups
//   - RabbitMQ: Channel-based, AMQP protocol
//   - GoQueue: REST API, simple HTTP, works anywhere
//
// =============================================================================

import type {
  GoQueueClientConfig,
  // Health types
  HealthResponse,
  LivenessResponse,
  ReadinessResponse,
  VersionResponse,
  StatsResponse,
  // Topic types
  CreateTopicRequest,
  CreateTopicResponse,
  TopicDetails,
  TopicListResponse,
  // Message types
  PublishMessage,
  PublishResponse,
  ConsumeResponse,
  // Delayed message types
  DelayedMessagesResponse,
  DelayedMessage,
  DelayStats,
  // Consumer group types
  GroupListResponse,
  GroupDetails,
  JoinGroupRequest,
  JoinGroupResponse,
  HeartbeatRequest,
  HeartbeatResponse,
  LeaveGroupRequest,
  PollResponse,
  // Offset types
  OffsetsResponse,
  CommitOffsetsRequest,
  // Reliability types
  AckRequest,
  NackRequest,
  RejectRequest,
  ExtendVisibilityRequest,
  ReliabilityStats,
  // Priority types
  PriorityStats,
  // Schema types
  RegisterSchemaRequest,
  RegisterSchemaResponse,
  SchemaVersion,
  Schema,
  CompatibilityConfig,
  SchemaStats,
  // Transaction types
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
  // Tracing types
  TraceListResponse,
  Trace,
  TraceSearchParams,
  TracerStats,
  // Admin types
  AddPartitionsRequest,
  AddPartitionsResponse,
  TenantListResponse,
  CreateTenantRequest,
  Tenant,
} from './types.js';

// =============================================================================
// CUSTOM ERROR CLASS
// =============================================================================

/**
 * Error thrown by the GoQueue client.
 *
 * Contains additional context about the failed request.
 *
 * @example
 * ```typescript
 * try {
 *   await client.topics.get('non-existent');
 * } catch (error) {
 *   if (error instanceof GoQueueError) {
 *     console.log('Status:', error.status);  // 404
 *     console.log('Message:', error.message);  // "Topic not found"
 *   }
 * }
 * ```
 */
export class GoQueueError extends Error {
  /**
   * HTTP status code from the response.
   */
  readonly status: number;

  /**
   * Original response body if available.
   */
  readonly body?: unknown;

  constructor(message: string, status: number, body?: unknown) {
    super(message);
    this.name = 'GoQueueError';
    this.status = status;
    this.body = body;

    // Maintains proper stack trace for where our error was thrown (only in V8)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, GoQueueError);
    }
  }
}

// =============================================================================
// HTTP CLIENT (INTERNAL)
// =============================================================================

/**
 * Internal HTTP client that handles all API requests.
 *
 * Provides:
 * - Consistent error handling
 * - Automatic JSON serialization
 * - Timeout support
 * - Retry with exponential backoff
 */
class HttpClient {
  private baseUrl: string;
  private timeout: number;
  private headers: Record<string, string>;
  private fetchImpl: typeof fetch;
  private maxRetries: number;
  private initialDelay: number;
  private maxDelay: number;

  constructor(config: GoQueueClientConfig) {
    // Remove trailing slash from base URL
    this.baseUrl = config.baseUrl.replace(/\/$/, '');
    this.timeout = config.timeout ?? 30000;
    this.headers = config.headers ?? {};
    this.fetchImpl = config.fetch ?? fetch;
    this.maxRetries = config.retry?.maxRetries ?? 3;
    this.initialDelay = config.retry?.initialDelay ?? 100;
    this.maxDelay = config.retry?.maxDelay ?? 5000;
  }

  /**
   * Makes an HTTP request with retry and timeout support.
   */
  async request<T>(
    method: string,
    path: string,
    options?: {
      body?: unknown;
      params?: Record<string, string | number | boolean | undefined>;
      retry?: boolean;
    }
  ): Promise<T> {
    const url = this.buildUrl(path, options?.params);
    const shouldRetry = options?.retry ?? true;

    let lastError: Error | undefined;

    for (let attempt = 0; attempt <= (shouldRetry ? this.maxRetries : 0); attempt++) {
      try {
        const response = await this.doRequest(method, url, options?.body);
        return response as T;
      } catch (error) {
        lastError = error as Error;

        // Don't retry client errors (4xx)
        if (error instanceof GoQueueError && error.status >= 400 && error.status < 500) {
          throw error;
        }

        // Wait before retrying (exponential backoff)
        if (attempt < this.maxRetries && shouldRetry) {
          const delay = Math.min(
            this.initialDelay * Math.pow(2, attempt),
            this.maxDelay
          );
          await this.sleep(delay);
        }
      }
    }

    throw lastError;
  }

  /**
   * Executes a single HTTP request.
   */
  private async doRequest(method: string, url: string, body?: unknown): Promise<unknown> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    try {
      const response = await this.fetchImpl(url, {
        method,
        headers: {
          'Content-Type': 'application/json',
          Accept: 'application/json',
          ...this.headers,
        },
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });

      // Handle non-JSON responses (like /metrics)
      const contentType = response.headers.get('content-type');
      if (contentType && !contentType.includes('application/json')) {
        if (!response.ok) {
          throw new GoQueueError(
            `Request failed: ${response.status} ${response.statusText}`,
            response.status
          );
        }
        return await response.text();
      }

      // Parse JSON response
      let data: unknown;
      try {
        data = await response.json();
      } catch {
        // Empty response is OK for some endpoints
        data = {};
      }

      // Handle errors
      if (!response.ok) {
        const errorMessage =
          typeof data === 'object' && data !== null && 'error' in data
            ? (data as { error: string }).error
            : `Request failed: ${response.status}`;
        throw new GoQueueError(errorMessage, response.status, data);
      }

      return data;
    } catch (error) {
      if (error instanceof GoQueueError) {
        throw error;
      }

      // Handle timeout
      if (error instanceof Error && error.name === 'AbortError') {
        throw new GoQueueError(`Request timeout after ${this.timeout}ms`, 408);
      }

      // Handle network errors
      throw new GoQueueError(
        `Network error: ${error instanceof Error ? error.message : 'Unknown error'}`,
        0
      );
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Builds a URL with query parameters.
   */
  private buildUrl(
    path: string,
    params?: Record<string, string | number | boolean | undefined>
  ): string {
    const url = new URL(path, this.baseUrl);

    if (params) {
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined) {
          url.searchParams.set(key, String(value));
        }
      }
    }

    return url.toString();
  }

  /**
   * Sleep for the specified number of milliseconds.
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// =============================================================================
// SERVICE CLASSES
// =============================================================================

/**
 * Health check operations.
 *
 * Provides endpoints for health monitoring and Kubernetes probes.
 *
 * @example
 * ```typescript
 * // Simple health check
 * const health = await client.health.check();
 * console.log(health.status);  // 'ok'
 *
 * // Kubernetes readiness with verbose output
 * const readiness = await client.health.readiness({ verbose: true });
 * for (const [name, result] of Object.entries(readiness.checks ?? {})) {
 *   console.log(`${name}: ${result.status}`);
 * }
 * ```
 */
class HealthService {
  constructor(private http: HttpClient) {}

  /**
   * Basic health check.
   *
   * Returns the overall health status of the server.
   */
  async check(): Promise<HealthResponse> {
    return this.http.request('GET', '/health');
  }

  /**
   * Kubernetes liveness probe.
   *
   * Use for `livenessProbe` in Kubernetes deployments.
   * Returns 200 if the process is alive and not deadlocked.
   *
   * @remarks
   * Failing this probe causes the container to be killed and restarted.
   */
  async liveness(): Promise<LivenessResponse> {
    return this.http.request('GET', '/healthz');
  }

  /**
   * Kubernetes readiness probe.
   *
   * Use for `readinessProbe` in Kubernetes deployments.
   * Returns 200 if the server is ready to handle requests.
   *
   * @param options - Probe options
   * @param options.verbose - Include detailed check results
   *
   * @remarks
   * Failing this probe removes the pod from service endpoints.
   */
  async readiness(options?: { verbose?: boolean }): Promise<ReadinessResponse> {
    return this.http.request('GET', '/readyz', {
      params: { verbose: options?.verbose },
    });
  }

  /**
   * Kubernetes startup probe.
   *
   * Use for `startupProbe` in Kubernetes deployments.
   * Returns 200 when initialization is complete.
   */
  async startup(): Promise<LivenessResponse> {
    return this.http.request('GET', '/livez');
  }

  /**
   * Get version information.
   *
   * Returns build and version information about the server.
   */
  async version(): Promise<VersionResponse> {
    return this.http.request('GET', '/version');
  }

  /**
   * Get broker statistics.
   *
   * Returns operational statistics about the broker.
   */
  async stats(): Promise<StatsResponse> {
    return this.http.request('GET', '/stats');
  }

  /**
   * Get Prometheus metrics.
   *
   * Returns metrics in Prometheus text exposition format.
   * Configure Prometheus to scrape this endpoint.
   */
  async metrics(): Promise<string> {
    return this.http.request('GET', '/metrics');
  }
}

/**
 * Topic management operations.
 *
 * @example
 * ```typescript
 * // Create a topic with 6 partitions
 * await client.topics.create({
 *   name: 'orders',
 *   num_partitions: 6,
 *   retention_hours: 168
 * });
 *
 * // List all topics
 * const { topics } = await client.topics.list();
 *
 * // Get topic details
 * const details = await client.topics.get('orders');
 * console.log(`Total messages: ${details.total_messages}`);
 *
 * // Delete a topic
 * await client.topics.delete('old-topic');
 * ```
 */
class TopicsService {
  constructor(private http: HttpClient) {}

  /**
   * List all topics.
   *
   * Returns the names of all topics in the broker.
   */
  async list(): Promise<TopicListResponse> {
    return this.http.request('GET', '/topics');
  }

  /**
   * Create a new topic.
   *
   * Topics are the primary unit of organization in GoQueue.
   * Messages are published to topics and consumed from topics.
   *
   * @param request - Topic creation parameters
   * @param request.name - Topic name (alphanumeric, hyphens, underscores)
   * @param request.num_partitions - Number of partitions (default: 3)
   * @param request.retention_hours - Message retention in hours (default: 168)
   *
   * @throws {GoQueueError} 409 if topic already exists
   */
  async create(request: CreateTopicRequest): Promise<CreateTopicResponse> {
    return this.http.request('POST', '/topics', { body: request });
  }

  /**
   * Get topic details.
   *
   * Returns detailed information about a specific topic including
   * partition offsets and size information.
   *
   * @param name - Topic name
   *
   * @throws {GoQueueError} 404 if topic not found
   */
  async get(name: string): Promise<TopicDetails> {
    return this.http.request('GET', `/topics/${encodeURIComponent(name)}`);
  }

  /**
   * Delete a topic.
   *
   * Permanently deletes a topic and all its data.
   *
   * @warning This operation is irreversible!
   *
   * @param name - Topic name to delete
   *
   * @throws {GoQueueError} 404 if topic not found
   */
  async delete(name: string): Promise<{ deleted: boolean; name: string }> {
    return this.http.request('DELETE', `/topics/${encodeURIComponent(name)}`);
  }
}

/**
 * Message publishing and consumption operations.
 *
 * @example
 * ```typescript
 * // Publish a simple message
 * const result = await client.messages.publish('orders', [
 *   { value: JSON.stringify({ orderId: '12345' }) }
 * ]);
 *
 * // Publish with key (for ordering)
 * await client.messages.publish('orders', [
 *   { key: 'user-123', value: JSON.stringify({ event: 'purchase' }) }
 * ]);
 *
 * // Publish high priority message
 * await client.messages.publish('alerts', [
 *   { value: 'Critical alert!', priority: 'critical' }
 * ]);
 *
 * // Publish delayed message (1 hour)
 * await client.messages.publish('reminders', [
 *   { value: 'Follow up', delay: '1h' }
 * ]);
 *
 * // Simple consume (without consumer groups)
 * const messages = await client.messages.consume('orders', 0, { limit: 10 });
 * ```
 */
class MessagesService {
  constructor(private http: HttpClient) {}

  /**
   * Publish messages to a topic.
   *
   * Messages can include keys for ordering, priorities, and delays.
   *
   * @param topic - Topic name to publish to
   * @param messages - Array of messages to publish
   *
   * @returns Results for each message (partition, offset, or error)
   *
   * @throws {GoQueueError} 404 if topic not found
   */
  async publish(topic: string, messages: PublishMessage[]): Promise<PublishResponse> {
    return this.http.request('POST', `/topics/${encodeURIComponent(topic)}/messages`, {
      body: { messages },
    });
  }

  /**
   * Consume messages from a partition (simple consumer).
   *
   * For production use, prefer consumer groups with `client.groups.poll()`.
   * This method is useful for debugging and simple use cases.
   *
   * @param topic - Topic name
   * @param partition - Partition ID (0-based)
   * @param options - Consumption options
   * @param options.offset - Starting offset (default: 0)
   * @param options.limit - Maximum messages to return (default: 100, max: 1000)
   */
  async consume(
    topic: string,
    partition: number,
    options?: { offset?: number; limit?: number }
  ): Promise<ConsumeResponse> {
    return this.http.request(
      'GET',
      `/topics/${encodeURIComponent(topic)}/partitions/${partition}/messages`,
      { params: options }
    );
  }

  /**
   * Acknowledge a message.
   *
   * Tells the broker the message was successfully processed.
   * The message will not be redelivered.
   *
   * @param receiptHandle - Receipt handle from the consumed message
   * @param consumerId - Optional consumer identifier
   */
  async ack(receiptHandle: string, consumerId?: string): Promise<{ acknowledged: boolean }> {
    const request: AckRequest = { receipt_handle: receiptHandle, consumer_id: consumerId };
    return this.http.request('POST', '/messages/ack', { body: request });
  }

  /**
   * Negative acknowledge a message.
   *
   * Tells the broker the message processing failed temporarily.
   * The message will be redelivered after the delay.
   *
   * @param receiptHandle - Receipt handle from the consumed message
   * @param delay - Optional delay before redelivery (e.g., "30s", "1m")
   */
  async nack(receiptHandle: string, delay?: string): Promise<{ requeued: boolean }> {
    const request: NackRequest = { receipt_handle: receiptHandle, delay };
    return this.http.request('POST', '/messages/nack', { body: request });
  }

  /**
   * Reject a message (send to DLQ).
   *
   * Use for messages that can never be processed (invalid format, etc).
   * The message will be moved to the dead letter queue.
   *
   * @param receiptHandle - Receipt handle from the consumed message
   * @param reason - Reason for rejection (stored in DLQ)
   */
  async reject(
    receiptHandle: string,
    reason?: string
  ): Promise<{ rejected: boolean; dlq_topic?: string }> {
    const request: RejectRequest = { receipt_handle: receiptHandle, reason };
    return this.http.request('POST', '/messages/reject', { body: request });
  }

  /**
   * Extend message visibility timeout.
   *
   * Use when processing takes longer than expected.
   * Prevents the message from being redelivered while still processing.
   *
   * @param receiptHandle - Receipt handle from the consumed message
   * @param timeout - New visibility timeout (e.g., "60s", "5m")
   */
  async extendVisibility(
    receiptHandle: string,
    timeout: string
  ): Promise<{ extended: boolean; new_deadline?: string }> {
    const request: ExtendVisibilityRequest = { receipt_handle: receiptHandle, timeout };
    return this.http.request('POST', '/messages/visibility', { body: request });
  }

  /**
   * Get reliability statistics.
   *
   * Returns ACK/NACK/DLQ statistics.
   */
  async reliabilityStats(): Promise<ReliabilityStats> {
    return this.http.request('GET', '/reliability/stats');
  }
}

/**
 * Delayed message operations.
 *
 * @example
 * ```typescript
 * // List delayed messages
 * const delayed = await client.delayed.list('orders', { limit: 50 });
 * for (const msg of delayed.messages) {
 *   console.log(`Delivering at: ${msg.deliver_at}`);
 * }
 *
 * // Cancel a delayed message
 * await client.delayed.cancel('orders', 0, 12345);
 *
 * // Get delay statistics
 * const stats = await client.delayed.stats();
 * console.log(`Pending: ${stats.pending_count}`);
 * ```
 */
class DelayedService {
  constructor(private http: HttpClient) {}

  /**
   * List delayed messages for a topic.
   *
   * @param topic - Topic name
   * @param options - List options
   * @param options.limit - Maximum messages to return (default: 100)
   */
  async list(topic: string, options?: { limit?: number }): Promise<DelayedMessagesResponse> {
    return this.http.request('GET', `/topics/${encodeURIComponent(topic)}/delayed`, {
      params: options,
    });
  }

  /**
   * Get a specific delayed message.
   *
   * @param topic - Topic name
   * @param offset - Message offset
   */
  async get(topic: string, offset: number): Promise<DelayedMessage> {
    return this.http.request(
      'GET',
      `/topics/${encodeURIComponent(topic)}/delayed/${offset}`
    );
  }

  /**
   * Cancel a delayed message.
   *
   * Cancels a pending delayed message before it's delivered.
   *
   * @param topic - Topic name
   * @param partition - Partition ID
   * @param offset - Message offset
   */
  async cancel(
    topic: string,
    partition: number,
    offset: number
  ): Promise<{ cancelled: boolean }> {
    return this.http.request(
      'DELETE',
      `/topics/${encodeURIComponent(topic)}/delayed/${partition}/${offset}`
    );
  }

  /**
   * Get delay queue statistics.
   */
  async stats(): Promise<DelayStats> {
    return this.http.request('GET', '/delay/stats');
  }
}

/**
 * Consumer group operations.
 *
 * Consumer groups enable parallel processing of messages by distributing
 * partitions among group members.
 *
 * @example
 * ```typescript
 * // Join a consumer group
 * const { member_id, generation, assigned_partitions } = await client.groups.join(
 *   'order-processors',
 *   {
 *     client_id: 'worker-1',
 *     topics: ['orders'],
 *     session_timeout: '30s'
 *   }
 * );
 *
 * // Poll for messages
 * while (running) {
 *   const response = await client.groups.poll('order-processors', member_id, {
 *     max_messages: 10,
 *     timeout: '30s'
 *   });
 *
 *   for (const msg of response.messages) {
 *     // Process message
 *     await processOrder(msg.value);
 *
 *     // Acknowledge
 *     await client.messages.ack(msg.receipt_handle);
 *   }
 *
 *   // Send heartbeat
 *   const heartbeat = await client.groups.heartbeat('order-processors', {
 *     member_id,
 *     generation
 *   });
 *
 *   if (heartbeat.rebalance_required) {
 *     // Rejoin to get new partition assignments
 *   }
 * }
 *
 * // Leave the group
 * await client.groups.leave('order-processors', { member_id });
 * ```
 */
class GroupsService {
  constructor(private http: HttpClient) {}

  /**
   * List all consumer groups.
   */
  async list(): Promise<GroupListResponse> {
    return this.http.request('GET', '/groups');
  }

  /**
   * Get consumer group details.
   *
   * @param groupId - Consumer group ID
   */
  async get(groupId: string): Promise<GroupDetails> {
    return this.http.request('GET', `/groups/${encodeURIComponent(groupId)}`);
  }

  /**
   * Delete a consumer group.
   *
   * Deletes the group and its offset data.
   *
   * @param groupId - Consumer group ID
   */
  async delete(groupId: string): Promise<{ deleted: boolean }> {
    return this.http.request('DELETE', `/groups/${encodeURIComponent(groupId)}`);
  }

  /**
   * Join a consumer group.
   *
   * The broker will assign partitions based on the number of members
   * and available partitions.
   *
   * @param groupId - Consumer group ID
   * @param request - Join request parameters
   */
  async join(groupId: string, request: JoinGroupRequest): Promise<JoinGroupResponse> {
    return this.http.request('POST', `/groups/${encodeURIComponent(groupId)}/join`, {
      body: request,
    });
  }

  /**
   * Send a heartbeat to keep the session alive.
   *
   * Must be sent within the session timeout (default: 30s).
   *
   * @param groupId - Consumer group ID
   * @param request - Heartbeat request with member_id and generation
   */
  async heartbeat(groupId: string, request: HeartbeatRequest): Promise<HeartbeatResponse> {
    return this.http.request('POST', `/groups/${encodeURIComponent(groupId)}/heartbeat`, {
      body: request,
    });
  }

  /**
   * Leave a consumer group.
   *
   * Triggers a rebalance of the remaining members.
   *
   * @param groupId - Consumer group ID
   * @param request - Leave request with member_id
   */
  async leave(groupId: string, request: LeaveGroupRequest): Promise<{ left: boolean }> {
    return this.http.request('POST', `/groups/${encodeURIComponent(groupId)}/leave`, {
      body: request,
    });
  }

  /**
   * Poll for messages from assigned partitions.
   *
   * Long-polls for messages. Returns immediately if messages are available,
   * or waits up to the timeout.
   *
   * @param groupId - Consumer group ID
   * @param memberId - Member ID (from join response)
   * @param options - Poll options
   * @param options.max_messages - Maximum messages to return (default: 100)
   * @param options.timeout - Long-poll timeout (default: "30s")
   */
  async poll(
    groupId: string,
    memberId: string,
    options?: { max_messages?: number; timeout?: string }
  ): Promise<PollResponse> {
    return this.http.request('GET', `/groups/${encodeURIComponent(groupId)}/poll`, {
      params: {
        member_id: memberId,
        ...options,
      },
    });
  }

  /**
   * Get committed offsets for a consumer group.
   *
   * @param groupId - Consumer group ID
   * @param topic - Optional topic filter
   */
  async getOffsets(groupId: string, topic?: string): Promise<OffsetsResponse> {
    return this.http.request('GET', `/groups/${encodeURIComponent(groupId)}/offsets`, {
      params: { topic },
    });
  }

  /**
   * Commit offsets for a consumer group.
   *
   * Only offsets for assigned partitions can be committed.
   *
   * @param groupId - Consumer group ID
   * @param request - Commit request with member_id and offsets
   */
  async commitOffsets(
    groupId: string,
    request: CommitOffsetsRequest
  ): Promise<{ committed: boolean }> {
    return this.http.request('POST', `/groups/${encodeURIComponent(groupId)}/offsets`, {
      body: request,
    });
  }
}

/**
 * Priority queue operations.
 *
 * @example
 * ```typescript
 * const stats = await client.priority.stats();
 * console.log('Messages by priority:', stats.messages_by_priority);
 * console.log('Avg wait times:', stats.avg_wait_time_by_priority);
 * ```
 */
class PriorityService {
  constructor(private http: HttpClient) {}

  /**
   * Get priority queue statistics.
   */
  async stats(): Promise<PriorityStats> {
    return this.http.request('GET', '/priority/stats');
  }
}

/**
 * Schema registry operations.
 *
 * The schema registry validates message payloads against JSON schemas
 * and ensures compatibility between schema versions.
 *
 * @example
 * ```typescript
 * // Register a schema
 * const { id } = await client.schemas.register('orders-value', {
 *   schema: JSON.stringify({
 *     type: 'object',
 *     properties: {
 *       orderId: { type: 'string' },
 *       amount: { type: 'number' }
 *     },
 *     required: ['orderId', 'amount']
 *   })
 * });
 *
 * // Get the latest schema
 * const schema = await client.schemas.getVersion('orders-value', 'latest');
 *
 * // Set compatibility mode
 * await client.schemas.setConfig({ compatibilityLevel: 'BACKWARD' });
 * ```
 */
class SchemasService {
  constructor(private http: HttpClient) {}

  /**
   * List all schema subjects.
   */
  async listSubjects(): Promise<string[]> {
    return this.http.request('GET', '/schemas/subjects');
  }

  /**
   * List versions for a subject.
   *
   * @param subject - Subject name
   */
  async listVersions(subject: string): Promise<number[]> {
    return this.http.request(
      'GET',
      `/schemas/subjects/${encodeURIComponent(subject)}/versions`
    );
  }

  /**
   * Register a new schema version.
   *
   * The schema is validated for compatibility with existing versions.
   *
   * @param subject - Subject name
   * @param request - Schema registration request
   *
   * @throws {GoQueueError} 409 if schema is incompatible
   */
  async register(subject: string, request: RegisterSchemaRequest): Promise<RegisterSchemaResponse> {
    return this.http.request(
      'POST',
      `/schemas/subjects/${encodeURIComponent(subject)}/versions`,
      { body: request }
    );
  }

  /**
   * Get a specific schema version.
   *
   * @param subject - Subject name
   * @param version - Version number or "latest"
   */
  async getVersion(subject: string, version: number | 'latest'): Promise<SchemaVersion> {
    return this.http.request(
      'GET',
      `/schemas/subjects/${encodeURIComponent(subject)}/versions/${version}`
    );
  }

  /**
   * Delete a schema version.
   *
   * @param subject - Subject name
   * @param version - Version number
   *
   * @returns The deleted version number
   */
  async deleteVersion(subject: string, version: number): Promise<number> {
    return this.http.request(
      'DELETE',
      `/schemas/subjects/${encodeURIComponent(subject)}/versions/${version}`
    );
  }

  /**
   * Get schema by global ID.
   *
   * @param id - Global schema ID
   */
  async getById(id: number): Promise<Schema> {
    return this.http.request('GET', `/schemas/ids/${id}`);
  }

  /**
   * Get global compatibility configuration.
   */
  async getConfig(): Promise<CompatibilityConfig> {
    return this.http.request('GET', '/schemas/config');
  }

  /**
   * Set global compatibility configuration.
   *
   * @param config - Compatibility configuration
   */
  async setConfig(config: CompatibilityConfig): Promise<CompatibilityConfig> {
    return this.http.request('PUT', '/schemas/config', { body: config });
  }

  /**
   * Get schema registry statistics.
   */
  async stats(): Promise<SchemaStats> {
    return this.http.request('GET', '/schemas/stats');
  }
}

/**
 * Transaction operations for exactly-once semantics.
 *
 * @example
 * ```typescript
 * // Initialize producer
 * const { producer_id, epoch } = await client.transactions.initProducer({
 *   transactional_id: 'order-processor-1'
 * });
 *
 * // Begin transaction
 * const { transaction_id } = await client.transactions.begin({
 *   producer_id,
 *   epoch,
 *   transactional_id: 'order-processor-1'
 * });
 *
 * // Publish messages within transaction
 * await client.transactions.publish({
 *   producer_id,
 *   epoch,
 *   topic: 'orders',
 *   value: JSON.stringify({ orderId: '123' }),
 *   sequence: 1
 * });
 *
 * // Commit transaction
 * await client.transactions.commit({
 *   producer_id,
 *   epoch,
 *   transactional_id: 'order-processor-1'
 * });
 * ```
 */
class TransactionsService {
  constructor(private http: HttpClient) {}

  /**
   * Initialize an idempotent/transactional producer.
   *
   * Returns a producer ID and epoch for subsequent operations.
   *
   * @param request - Initialization request
   */
  async initProducer(request?: InitProducerRequest): Promise<InitProducerResponse> {
    return this.http.request('POST', '/producers/init', { body: request ?? {} });
  }

  /**
   * Send producer heartbeat.
   *
   * Keeps the producer session alive.
   *
   * @param producerId - Producer ID
   * @param request - Heartbeat request with epoch
   */
  async producerHeartbeat(
    producerId: number,
    request: ProducerHeartbeatRequest
  ): Promise<{ success: boolean }> {
    return this.http.request('POST', `/producers/${producerId}/heartbeat`, {
      body: request,
    });
  }

  /**
   * List active transactions.
   */
  async list(): Promise<TransactionListResponse> {
    return this.http.request('GET', '/transactions');
  }

  /**
   * Begin a new transaction.
   *
   * @param request - Transaction begin request
   */
  async begin(request: BeginTransactionRequest): Promise<BeginTransactionResponse> {
    return this.http.request('POST', '/transactions/begin', { body: request });
  }

  /**
   * Publish a message within a transaction.
   *
   * @param request - Transactional publish request
   */
  async publish(request: TransactionalPublishRequest): Promise<TransactionalPublishResponse> {
    return this.http.request('POST', '/transactions/publish', { body: request });
  }

  /**
   * Commit a transaction.
   *
   * Atomically commits all messages in the transaction.
   *
   * @param request - Commit request
   */
  async commit(request: CommitTransactionRequest): Promise<{ status: 'committed' }> {
    return this.http.request('POST', '/transactions/commit', { body: request });
  }

  /**
   * Abort a transaction.
   *
   * Rolls back all messages in the transaction.
   *
   * @param request - Abort request
   */
  async abort(request: AbortTransactionRequest): Promise<{ status: 'aborted' }> {
    return this.http.request('POST', '/transactions/abort', { body: request });
  }

  /**
   * Get transaction statistics.
   */
  async stats(): Promise<TransactionStats> {
    return this.http.request('GET', '/transactions/stats');
  }
}

/**
 * Message tracing operations.
 *
 * Traces allow you to follow a message's journey through the system.
 *
 * @example
 * ```typescript
 * // List recent traces
 * const { traces } = await client.tracing.list({ limit: 100 });
 *
 * // Search for specific traces
 * const results = await client.tracing.search({
 *   topic: 'orders',
 *   status: 'error',
 *   start: '2025-01-30T00:00:00Z'
 * });
 *
 * // Get trace details
 * const trace = await client.tracing.get('trace-123');
 * for (const event of trace.events ?? []) {
 *   console.log(`${event.timestamp}: ${event.event_type}`);
 * }
 * ```
 */
class TracingService {
  constructor(private http: HttpClient) {}

  /**
   * List recent traces.
   *
   * @param options - List options
   * @param options.limit - Maximum traces to return (default: 100)
   */
  async list(options?: { limit?: number }): Promise<TraceListResponse> {
    return this.http.request('GET', '/traces', { params: options });
  }

  /**
   * Search traces by criteria.
   *
   * @param params - Search parameters
   */
  async search(params?: TraceSearchParams): Promise<TraceListResponse> {
    const queryParams: Record<string, string | number | boolean | undefined> = {};
    if (params) {
      if (params.topic) queryParams.topic = params.topic;
      if (params.partition !== undefined) queryParams.partition = params.partition;
      if (params.status) queryParams.status = params.status;
      if (params.start) queryParams.start = params.start;
      if (params.end) queryParams.end = params.end;
      if (params.limit !== undefined) queryParams.limit = params.limit;
    }
    return this.http.request('GET', '/traces/search', { params: queryParams });
  }

  /**
   * Get a specific trace by ID.
   *
   * @param traceId - Trace ID
   */
  async get(traceId: string): Promise<Trace> {
    return this.http.request('GET', `/traces/${encodeURIComponent(traceId)}`);
  }

  /**
   * Get tracer statistics.
   */
  async stats(): Promise<TracerStats> {
    return this.http.request('GET', '/traces/stats');
  }
}

/**
 * Administrative operations.
 *
 * @example
 * ```typescript
 * // Add partitions to a topic
 * await client.admin.addPartitions('orders', { count: 12 });
 *
 * // Create a tenant
 * await client.admin.createTenant({
 *   name: 'acme-corp',
 *   quotas: {
 *     max_topics: 100,
 *     max_messages_per_second: 10000
 *   }
 * });
 * ```
 */
class AdminService {
  constructor(private http: HttpClient) {}

  /**
   * Add partitions to a topic.
   *
   * @warning This can affect message ordering for keyed messages.
   *
   * @param topic - Topic name
   * @param request - Request with new partition count
   */
  async addPartitions(topic: string, request: AddPartitionsRequest): Promise<AddPartitionsResponse> {
    return this.http.request(
      'POST',
      `/admin/topics/${encodeURIComponent(topic)}/partitions`,
      { body: request }
    );
  }

  /**
   * List all tenants.
   */
  async listTenants(): Promise<TenantListResponse> {
    return this.http.request('GET', '/admin/tenants');
  }

  /**
   * Create a new tenant.
   *
   * @param request - Tenant creation request
   */
  async createTenant(request: CreateTenantRequest): Promise<Tenant> {
    return this.http.request('POST', '/admin/tenants', { body: request });
  }
}

// =============================================================================
// MAIN CLIENT CLASS
// =============================================================================

/**
 * GoQueue client for interacting with a GoQueue server.
 *
 * This is the main entry point for the GoQueue JavaScript/TypeScript SDK.
 * It provides access to all GoQueue features through service-specific APIs.
 *
 * @example
 * ```typescript
 * import { GoQueueClient } from 'goqueue-client';
 *
 * // Create a client
 * const client = new GoQueueClient({
 *   baseUrl: 'http://localhost:8080',
 *   timeout: 30000,
 *   headers: {
 *     'X-Tenant-ID': 'my-tenant'
 *   }
 * });
 *
 * // Check health
 * const health = await client.health.check();
 * console.log('Status:', health.status);
 *
 * // Create a topic
 * await client.topics.create({
 *   name: 'orders',
 *   num_partitions: 6
 * });
 *
 * // Publish messages
 * await client.messages.publish('orders', [
 *   { key: 'user-123', value: JSON.stringify({ orderId: 'abc' }) }
 * ]);
 *
 * // Join a consumer group
 * const { member_id, generation } = await client.groups.join('order-processors', {
 *   client_id: 'worker-1',
 *   topics: ['orders']
 * });
 *
 * // Poll for messages
 * const { messages } = await client.groups.poll('order-processors', member_id);
 * for (const msg of messages) {
 *   console.log('Received:', msg.value);
 *   await client.messages.ack(msg.receipt_handle);
 * }
 * ```
 *
 * @see {@link https://github.com/abd-ulbasit/goqueue} GoQueue Documentation
 */
export class GoQueueClient {
  /**
   * Health check operations.
   *
   * Provides endpoints for health monitoring and Kubernetes probes.
   */
  readonly health: HealthService;

  /**
   * Topic management operations.
   *
   * Create, list, describe, and delete topics.
   */
  readonly topics: TopicsService;

  /**
   * Message operations.
   *
   * Publish and consume messages, ACK/NACK handling.
   */
  readonly messages: MessagesService;

  /**
   * Delayed message operations.
   *
   * List, get, and cancel delayed messages.
   */
  readonly delayed: DelayedService;

  /**
   * Consumer group operations.
   *
   * Join, leave, poll, and manage consumer groups.
   */
  readonly groups: GroupsService;

  /**
   * Priority queue operations.
   *
   * Get priority queue statistics.
   */
  readonly priority: PriorityService;

  /**
   * Schema registry operations.
   *
   * Register, retrieve, and manage schemas.
   */
  readonly schemas: SchemasService;

  /**
   * Transaction operations.
   *
   * Exactly-once semantics with transactional producers.
   */
  readonly transactions: TransactionsService;

  /**
   * Tracing operations.
   *
   * Message tracing and observability.
   */
  readonly tracing: TracingService;

  /**
   * Administrative operations.
   *
   * Add partitions, manage tenants.
   */
  readonly admin: AdminService;

  /**
   * Creates a new GoQueue client.
   *
   * @param config - Client configuration
   * @param config.baseUrl - Base URL of the GoQueue server
   * @param config.timeout - Request timeout in milliseconds (default: 30000)
   * @param config.headers - Additional headers for all requests
   * @param config.fetch - Custom fetch implementation (optional)
   * @param config.retry - Retry configuration (optional)
   */
  constructor(config: GoQueueClientConfig) {
    const http = new HttpClient(config);

    this.health = new HealthService(http);
    this.topics = new TopicsService(http);
    this.messages = new MessagesService(http);
    this.delayed = new DelayedService(http);
    this.groups = new GroupsService(http);
    this.priority = new PriorityService(http);
    this.schemas = new SchemasService(http);
    this.transactions = new TransactionsService(http);
    this.tracing = new TracingService(http);
    this.admin = new AdminService(http);
  }
}

// =============================================================================
// CONVENIENCE FUNCTIONS
// =============================================================================

/**
 * Creates a GoQueue client with default configuration.
 *
 * This is a convenience function for creating a client with sensible defaults.
 *
 * @param baseUrl - Base URL of the GoQueue server
 * @returns A configured GoQueue client
 *
 * @example
 * ```typescript
 * import { createClient } from 'goqueue-client';
 *
 * const client = createClient('http://localhost:8080');
 * ```
 */
export function createClient(baseUrl: string): GoQueueClient {
  return new GoQueueClient({ baseUrl });
}
