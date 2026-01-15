// =============================================================================
// MULTI-TENANCY BROKER INTEGRATION
// =============================================================================
//
// WHY MULTI-TENANCY?
// A single goqueue cluster can serve multiple independent customers (tenants).
// Each tenant gets:
//   - Isolated namespace (topics prefixed with tenant ID)
//   - Resource quotas (rate limits, storage limits, topic limits)
//   - Usage tracking (bytes published, messages consumed)
//   - Independent lifecycle (can be suspended/disabled)
//
// COMPARISON - How other systems do this:
//
//   KAFKA:
//     - Client quotas: Per-client-id rate limits (bytes/sec, request rate)
//     - Topic prefix conventions (tenants create topics with their prefix)
//     - No built-in tenant management (use external systems)
//     - Quotas stored in ZooKeeper/KRaft metadata
//
//   RABBITMQ:
//     - Virtual hosts (vhosts): Complete isolation, separate namespaces
//     - Per-vhost permissions and policies
//     - Resource limits via policies
//     - Memory and queue size limits
//
//   AWS SQS:
//     - Account-level isolation (AWS accounts are tenants)
//     - Service quotas: Messages/sec, queues per account
//     - No cross-account access by default
//
//   GOQUEUE APPROACH:
//     - Namespace isolation via topic prefix (like Kafka conventions)
//     - Token bucket rate limiting (industry standard algorithm)
//     - Quota enforcement at broker layer (catches all paths)
//     - File-based persistence (simple, reliable)
//
// ARCHITECTURE:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                            BROKER                                       │
//   │                                                                         │
//   │   ┌─────────────────────────────────────────────────────────────────┐   │
//   │   │                    Tenant Manager                               │   │
//   │   │   - CRUD for tenants                                            │   │
//   │   │   - Quota configuration per tenant                              │   │
//   │   │   - Usage tracking per tenant                                   │   │
//   │   └─────────────────────────────────────────────────────────────────┘   │
//   │                              │                                          │
//   │                              │ delegates quota checks                   │
//   │                              ▼                                          │
//   │   ┌─────────────────────────────────────────────────────────────────┐   │
//   │   │                    Quota Manager                                │   │
//   │   │   - Token buckets for rate limits                               │   │
//   │   │   - Threshold checks for storage/count limits                   │   │
//   │   │   - Violation tracking                                          │   │
//   │   └─────────────────────────────────────────────────────────────────┘   │
//   │                              │                                          │
//   │                              │ enforced in                              │
//   │                              ▼                                          │
//   │   ┌─────────────────────────────────────────────────────────────────┐   │
//   │   │                    Broker Operations                            │   │
//   │   │   - PublishForTenant() → checks rate + size quotas              │   │
//   │   │   - ConsumeForTenant() → checks consume rate quota              │   │
//   │   │   - CreateTopicForTenant() → checks topic count quota           │   │
//   │   └─────────────────────────────────────────────────────────────────┘   │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// ENFORCEMENT FLOW:
//
//   Client Request (tenant=acme, topic=orders)
//          │
//          ▼
//   ┌──────────────────┐
//   │ Tenant Lookup    │ → Is tenant active?
//   └────────┬─────────┘   No → 403 Tenant suspended
//            │ Yes
//            ▼
//   ┌──────────────────┐
//   │ Qualify Topic    │ → "acme.orders" (namespace isolation)
//   └────────┬─────────┘
//            │
//            ▼
//   ┌──────────────────┐
//   │ Check Quotas     │ → Rate? Storage? Size?
//   └────────┬─────────┘   Exceeded → 429 Quota exceeded
//            │ OK
//            ▼
//   ┌──────────────────┐
//   │ Execute          │ → Publish/Consume/CreateTopic
//   └────────┬─────────┘
//            │
//            ▼
//   ┌──────────────────┐
//   │ Track Usage      │ → Update counters
//   └──────────────────┘
//
// =============================================================================

package broker

import (
	"errors"
	"fmt"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// TENANT-AWARE ERROR DEFINITIONS
// =============================================================================
//
// WHY SPECIFIC ERRORS?
// Different errors require different client responses:
//   - ErrTenantSuspended: Wait for admin action, don't retry
//   - ErrQuotaExceeded: Retry after backoff (429 with Retry-After header)
//   - ErrTenantNotFound: Configuration error, fix client
//
// HTTP STATUS MAPPING:
//   - ErrTenantSuspended: 403 Forbidden
//   - ErrTenantNotFound: 404 Not Found
//   - ErrQuotaExceeded: 429 Too Many Requests
//
// =============================================================================

var (
	// ErrTenantSuspended indicates the tenant is suspended and cannot perform operations
	ErrTenantSuspended = errors.New("tenant is suspended")
)

// =============================================================================
// MULTI-TENANCY STATUS AND ACCESSORS
// =============================================================================

// IsMultiTenantEnabled returns whether multi-tenancy is enabled.
// When false, the broker operates in single-tenant mode:
//   - No namespace prefixing (topics accessed directly by name)
//   - No quota enforcement
//   - No tenant isolation
//
// This is useful for:
//   - Kubernetes deployments where each customer gets their own cluster
//   - Development/testing environments
//   - Single-customer deployments
func (b *Broker) IsMultiTenantEnabled() bool {
	return b.tenantManager != nil
}

// TenantManager returns the broker's tenant manager for tenant CRUD operations.
// Returns nil if multi-tenancy is disabled.
// This is used by the admin API.
func (b *Broker) TenantManager() *TenantManager {
	return b.tenantManager
}

// =============================================================================
// TENANT STATUS VALIDATION
// =============================================================================

// checkTenantActive validates that a tenant exists and is active.
// This is the first check in all tenant-aware operations.
//
// RETURNS:
//   - nil: Tenant is active and can perform operations
//   - ErrTenantNotFound: Tenant doesn't exist
//   - ErrTenantSuspended: Tenant is temporarily suspended
//   - ErrTenantDisabled: Tenant is permanently disabled
func (b *Broker) checkTenantActive(tenantID string) error {
	if b.tenantManager == nil {
		// Multi-tenancy not enabled, allow all
		return nil
	}

	tenant, err := b.tenantManager.GetTenant(tenantID)
	if err != nil {
		if errors.Is(err, ErrTenantNotFound) {
			return ErrTenantNotFound
		}
		return fmt.Errorf("failed to get tenant: %w", err)
	}

	switch tenant.Status {
	case TenantStatusActive:
		return nil
	case TenantStatusSuspended:
		return ErrTenantSuspended
	case TenantStatusDisabled:
		return ErrTenantDisabled
	default:
		return fmt.Errorf("unknown tenant status: %s", tenant.Status)
	}
}

// =============================================================================
// TENANT-AWARE PUBLISH
// =============================================================================
//
// PublishForTenant adds quota checks and namespace isolation to publish:
//   1. Validate tenant is active
//   2. Qualify topic name with tenant prefix
//   3. Check publish rate quota (token bucket)
//   4. Check message size quota
//   5. Check publish bytes rate quota
//   6. Execute publish
//   7. Track usage
//
// WHY NOT MODIFY Publish()?
// Keeping the original Publish() allows:
//   - Backward compatibility with existing code
//   - System internal topics without tenant overhead
//   - Gradual migration to multi-tenant mode
//
// =============================================================================

// PublishForTenant publishes a message with tenant quota enforcement.
//
// PARAMETERS:
//   - tenantID: Tenant identifier (e.g., "acme")
//   - topic: Unqualified topic name (e.g., "orders")
//   - key: Routing key for partitioning
//   - value: Message payload
//
// RETURNS:
//   - partition: Partition the message was written to
//   - offset: Offset within partition
//   - error: nil on success, or quota/tenant error
//
// The topic is automatically qualified with tenant prefix: "acme.orders"
func (b *Broker) PublishForTenant(tenantID, topic string, key, value []byte) (partition int, offset int64, err error) {
	return b.PublishForTenantWithTrace(tenantID, topic, key, value, TraceContext{})
}

// PublishForTenantWithTrace publishes with quota enforcement and trace context.
func (b *Broker) PublishForTenantWithTrace(tenantID, topic string, key, value []byte, traceCtx TraceContext) (partition int, offset int64, err error) {
	// =========================================================================
	// STEP 1: Validate tenant is active
	// =========================================================================
	if err := b.checkTenantActive(tenantID); err != nil {
		return 0, 0, err
	}

	// =========================================================================
	// STEP 2: Qualify topic name with tenant prefix
	// =========================================================================
	qualifiedTopic := QualifyTopicName(tenantID, topic)

	// =========================================================================
	// STEP 3: Check quotas via QuotaEnforcer
	// =========================================================================
	// Uses QuotaEnforcer interface - NoOpEnforcer in single-tenant mode (zero cost),
	// TenantQuotaEnforcer in multi-tenant mode (actual checks)
	if err := b.quotaEnforcer.CheckPublish(tenantID, len(value)); err != nil {
		b.logger.Warn("quota exceeded",
			"tenant", tenantID,
			"topic", topic,
			"error", err)
		return 0, 0, err
	}

	// =========================================================================
	// STEP 4: Execute publish on qualified topic
	// =========================================================================
	partition, offset, err = b.PublishWithTrace(qualifiedTopic, key, value, traceCtx)
	if err != nil {
		return 0, 0, err
	}

	// =========================================================================
	// STEP 5: Track usage for billing/monitoring
	// =========================================================================
	b.quotaEnforcer.TrackUsage(tenantID, 1, int64(len(value)), 0, 0)

	return partition, offset, nil
}

// PublishBatchForTenant publishes multiple messages with quota enforcement.
func (b *Broker) PublishBatchForTenant(tenantID, topic string, messages []struct {
	Key   []byte
	Value []byte
}) ([]struct {
	Partition int
	Offset    int64
}, error) {
	// Validate tenant
	if err := b.checkTenantActive(tenantID); err != nil {
		return nil, err
	}

	// Calculate total bytes for batch quota check
	totalBytes := int64(0)
	for _, msg := range messages {
		totalBytes += int64(len(msg.Value))
	}

	// Check quotas via QuotaEnforcer
	if err := b.quotaEnforcer.CheckPublishBatch(tenantID, len(messages), totalBytes); err != nil {
		return nil, err
	}

	// Qualify topic and publish
	qualifiedTopic := QualifyTopicName(tenantID, topic)
	results, err := b.PublishBatch(qualifiedTopic, messages)
	if err != nil {
		return nil, err
	}

	// Track usage
	b.quotaEnforcer.TrackUsage(tenantID, int64(len(messages)), totalBytes, 0, 0)

	return results, nil
}

// =============================================================================
// TENANT-AWARE CONSUME
// =============================================================================
//
// Consume operations are simpler since they don't modify data.
// We still enforce:
//   - Tenant is active
//   - Consume rate quota (prevent DoS by aggressive polling)
//
// =============================================================================

// ConsumeForTenant consumes messages with tenant quota enforcement.
func (b *Broker) ConsumeForTenant(tenantID, topic string, partition int, offset int64, maxMessages int) ([]Message, error) {
	// Validate tenant
	if err := b.checkTenantActive(tenantID); err != nil {
		return nil, err
	}

	// Check consume rate quota via QuotaEnforcer
	if err := b.quotaEnforcer.CheckConsume(tenantID, maxMessages); err != nil {
		return nil, err
	}

	// Qualify topic and consume
	qualifiedTopic := QualifyTopicName(tenantID, topic)
	messages, err := b.Consume(qualifiedTopic, partition, offset, maxMessages)
	if err != nil {
		return nil, err
	}

	// Track usage
	bytesConsumed := int64(0)
	for _, msg := range messages {
		bytesConsumed += int64(len(msg.Value))
	}
	b.quotaEnforcer.TrackUsage(tenantID, 0, 0, int64(len(messages)), bytesConsumed)

	return messages, nil
}

// =============================================================================
// TENANT-AWARE TOPIC MANAGEMENT
// =============================================================================

// CreateTopicForTenant creates a topic with tenant quota enforcement.
//
// QUOTAS CHECKED:
//   - Maximum topics per tenant
//   - Maximum partitions per tenant
//   - Maximum retention period (if configured)
func (b *Broker) CreateTopicForTenant(tenantID string, config TopicConfig) error {
	// Validate tenant
	if err := b.checkTenantActive(tenantID); err != nil {
		return err
	}

	// Check quotas via QuotaEnforcer
	currentTopics := b.countTenantTopics(tenantID)
	currentPartitions := b.countTenantPartitions(tenantID)
	if err := b.quotaEnforcer.CheckTopicCreation(tenantID, config.NumPartitions, currentTopics, currentPartitions); err != nil {
		return err
	}

	// Qualify topic name
	config.Name = QualifyTopicName(tenantID, config.Name)

	// Create topic
	if err := b.CreateTopic(config); err != nil {
		return err
	}

	// Update tenant storage usage (if multi-tenant)
	if b.tenantManager != nil {
		currentTopics := b.countTenantTopics(tenantID)
		currentPartitions := b.countTenantPartitions(tenantID)
		b.tenantManager.UpdateStorageUsage(tenantID, 0, currentTopics, currentPartitions, 0)
	}

	return nil
}

// DeleteTopicForTenant deletes a tenant's topic.
func (b *Broker) DeleteTopicForTenant(tenantID, topic string) error {
	// Validate tenant
	if err := b.checkTenantActive(tenantID); err != nil {
		return err
	}

	// Qualify topic name
	qualifiedTopic := QualifyTopicName(tenantID, topic)

	// Verify topic belongs to tenant (namespace check)
	if !IsTenantTopic(tenantID, qualifiedTopic) {
		return fmt.Errorf("topic %s does not belong to tenant %s", topic, tenantID)
	}

	// Delete topic
	if err := b.DeleteTopic(qualifiedTopic); err != nil {
		return err
	}

	// Update tenant storage usage (if multi-tenant)
	if b.tenantManager != nil {
		currentTopics := b.countTenantTopics(tenantID)
		currentPartitions := b.countTenantPartitions(tenantID)
		b.tenantManager.UpdateStorageUsage(tenantID, 0, currentTopics, currentPartitions, 0)
	}

	return nil
}

// ListTopicsForTenant returns all topics belonging to a tenant.
func (b *Broker) ListTopicsForTenant(tenantID string) ([]string, error) {
	// Validate tenant
	if err := b.checkTenantActive(tenantID); err != nil {
		return nil, err
	}

	allTopics := b.ListTopics()
	tenantTopics := make([]string, 0)

	for _, topic := range allTopics {
		if IsTenantTopic(tenantID, topic) {
			// Return unqualified name (strip tenant prefix)
			_, unqualified, _ := ParseQualifiedTopicName(topic)
			tenantTopics = append(tenantTopics, unqualified)
		}
	}

	return tenantTopics, nil
}

// =============================================================================
// TENANT-AWARE DELAY OPERATIONS
// =============================================================================

// PublishWithDelayForTenant publishes a delayed message with quota enforcement.
func (b *Broker) PublishWithDelayForTenant(tenantID, topic string, key, value []byte, delay time.Duration) (partition int, offset int64, err error) {
	// Validate tenant
	if err := b.checkTenantActive(tenantID); err != nil {
		return 0, 0, err
	}

	// Check quotas via QuotaEnforcer
	if err := b.quotaEnforcer.CheckPublish(tenantID, len(value)); err != nil {
		return 0, 0, err
	}
	if err := b.quotaEnforcer.CheckDelay(tenantID, delay); err != nil {
		return 0, 0, err
	}

	// Qualify topic and publish with delay
	qualifiedTopic := QualifyTopicName(tenantID, topic)
	partition, offset, err = b.PublishWithDelay(qualifiedTopic, key, value, delay)
	if err != nil {
		return 0, 0, err
	}

	// Track usage
	b.quotaEnforcer.TrackUsage(tenantID, 1, int64(len(value)), 0, 0)

	return partition, offset, nil
}

// =============================================================================
// TENANT-AWARE PRIORITY OPERATIONS
// =============================================================================

// PublishWithPriorityForTenant publishes a priority message with quota enforcement.
func (b *Broker) PublishWithPriorityForTenant(tenantID, topic string, key, value []byte, priority storage.Priority) (partition int, offset int64, err error) {
	// Validate tenant
	if err := b.checkTenantActive(tenantID); err != nil {
		return 0, 0, err
	}

	// Check quotas via QuotaEnforcer
	if err := b.quotaEnforcer.CheckPublish(tenantID, len(value)); err != nil {
		return 0, 0, err
	}

	// Qualify topic and publish with priority
	qualifiedTopic := QualifyTopicName(tenantID, topic)
	partition, offset, err = b.PublishWithPriority(qualifiedTopic, key, value, priority)
	if err != nil {
		return 0, 0, err
	}

	// Track usage
	b.quotaEnforcer.TrackUsage(tenantID, 1, int64(len(value)), 0, 0)

	return partition, offset, nil
}

// =============================================================================
// TENANT HELPER METHODS
// =============================================================================

// countTenantTopics counts topics belonging to a tenant.
func (b *Broker) countTenantTopics(tenantID string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0
	for topic := range b.topics {
		if IsTenantTopic(tenantID, topic) {
			count++
		}
	}
	return count
}

// countTenantPartitions counts total partitions for a tenant's topics.
func (b *Broker) countTenantPartitions(tenantID string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0
	for topic, t := range b.topics {
		if IsTenantTopic(tenantID, topic) {
			count += t.NumPartitions()
		}
	}
	return count
}

// GetTenantStorageBytes returns total storage used by a tenant.
// This is an expensive operation that should be called periodically, not per-request.
func (b *Broker) GetTenantStorageBytes(tenantID string) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var totalBytes int64
	for topic, t := range b.topics {
		if IsTenantTopic(tenantID, topic) {
			// Sum storage across all partitions
			for i := 0; i < t.NumPartitions(); i++ {
				partition, err := t.Partition(i)
				if err == nil && partition != nil {
					// Get partition size in bytes
					totalBytes += partition.Size()
				}
			}
		}
	}
	return totalBytes, nil
}

// =============================================================================
// TENANT-AWARE CONSUMER GROUP OPERATIONS
// =============================================================================

// JoinGroupForTenant joins a consumer group with tenant namespace isolation.
func (b *Broker) JoinGroupForTenant(tenantID, groupID, clientID string, topics []string) (*JoinResult, error) {
	// Validate tenant
	if err := b.checkTenantActive(tenantID); err != nil {
		return nil, err
	}

	// Check consumer group quota via QuotaEnforcer
	currentGroups := b.countTenantConsumerGroups(tenantID)
	if err := b.quotaEnforcer.CheckConsumerGroup(tenantID, currentGroups); err != nil {
		return nil, err
	}

	// Qualify group ID with tenant prefix
	qualifiedGroupID := QualifyTopicName(tenantID, groupID)

	// Qualify all topic names
	qualifiedTopics := make([]string, len(topics))
	for i, topic := range topics {
		qualifiedTopics[i] = QualifyTopicName(tenantID, topic)
	}

	// Join group
	return b.groupCoordinator.JoinGroup(qualifiedGroupID, clientID, qualifiedTopics)
}

// countTenantConsumerGroups counts consumer groups belonging to a tenant.
func (b *Broker) countTenantConsumerGroups(tenantID string) int {
	groups := b.groupCoordinator.ListGroups()
	count := 0
	prefix := tenantID + "."
	for _, group := range groups {
		if len(group) > len(prefix) && group[:len(prefix)] == prefix {
			count++
		}
	}
	return count
}
