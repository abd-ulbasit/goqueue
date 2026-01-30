// =============================================================================
// GOQUEUE JAVASCRIPT/TYPESCRIPT CLIENT
// =============================================================================
//
// Official client library for GoQueue - a high-performance distributed message queue.
//
// INSTALLATION:
//
//   npm install goqueue-client
//   # or
//   yarn add goqueue-client
//   # or
//   pnpm add goqueue-client
//
// QUICK START:
//
//   import { GoQueueClient } from 'goqueue-client';
//
//   const client = new GoQueueClient({
//     baseUrl: 'http://localhost:8080'
//   });
//
//   // Publish messages
//   await client.messages.publish('orders', [
//     { value: JSON.stringify({ orderId: '123' }) }
//   ]);
//
//   // Consume with consumer groups
//   const { member_id } = await client.groups.join('processors', {
//     client_id: 'worker-1',
//     topics: ['orders']
//   });
//
//   const { messages } = await client.groups.poll('processors', member_id);
//   for (const msg of messages) {
//     console.log(msg.value);
//     await client.messages.ack(msg.receipt_handle);
//   }
//
// =============================================================================

// Re-export everything from client
export { GoQueueClient, GoQueueError, createClient } from './client.js';

// Re-export all types
export type {
  // Config
  GoQueueClientConfig,
  // Common
  ErrorResponse,
  // Health
  HealthStatus,
  ProbeStatus,
  HealthCheckResult,
  HealthResponse,
  LivenessResponse,
  ReadinessResponse,
  VersionResponse,
  StatsResponse,
  // Topics
  CreateTopicRequest,
  CreateTopicResponse,
  TopicDetails,
  TopicListResponse,
  // Messages
  MessagePriority,
  PublishMessage,
  PublishRequest,
  PublishResult,
  PublishResponse,
  ConsumeMessage,
  ConsumeResponse,
  // Delayed
  DelayedMessage,
  DelayedMessagesResponse,
  DelayStats,
  // Consumer Groups
  GroupState,
  GroupMember,
  GroupDetails,
  GroupListResponse,
  JoinGroupRequest,
  JoinGroupResponse,
  HeartbeatRequest,
  HeartbeatResponse,
  LeaveGroupRequest,
  PollMessage,
  PollResponse,
  // Offsets
  OffsetsResponse,
  CommitOffsetsRequest,
  // Reliability
  AckRequest,
  NackRequest,
  RejectRequest,
  ExtendVisibilityRequest,
  ReliabilityStats,
  // Priority
  PriorityStats,
  // Schemas
  CompatibilityLevel,
  RegisterSchemaRequest,
  RegisterSchemaResponse,
  SchemaVersion,
  Schema,
  CompatibilityConfig,
  SchemaStats,
  // Transactions
  TransactionState,
  InitProducerRequest,
  InitProducerResponse,
  ProducerHeartbeatRequest,
  BeginTransactionRequest,
  BeginTransactionResponse,
  TransactionalPublishRequest,
  TransactionalPublishResponse,
  CommitTransactionRequest,
  AbortTransactionRequest,
  TransactionInfo,
  TransactionListResponse,
  TransactionStats,
  // Tracing
  TraceStatus,
  TraceEvent,
  Trace,
  TraceListResponse,
  TraceSearchParams,
  TracerStats,
  // Admin
  AddPartitionsRequest,
  AddPartitionsResponse,
  TenantQuotas,
  TenantStatus,
  Tenant,
  CreateTenantRequest,
  TenantListResponse,
} from './types.js';
