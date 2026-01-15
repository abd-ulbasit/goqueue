// =============================================================================
// QUOTA ENFORCER INTERFACE
// =============================================================================
//
// WHY AN INTERFACE?
// The QuotaEnforcer interface abstracts quota checking, allowing the broker
// to operate in two modes without scattered nil checks:
//
//   SINGLE-TENANT MODE (NoOpEnforcer):
//     - Zero overhead - all checks immediately return success
//     - No quota tracking, no rate limiting
//     - Used when EnableMultiTenancy = false (default)
//
//   MULTI-TENANT MODE (TenantQuotaEnforcer):
//     - Full quota enforcement via token buckets
//     - Rate limits, storage limits, connection limits
//     - Used when EnableMultiTenancy = true
//
// PATTERN: Strategy Pattern
// Instead of checking `if tenantManager != nil` throughout the code, we inject
// the appropriate enforcer at startup. This is:
//   - Cleaner (no scattered nil checks)
//   - Faster (no nil check on hot path for single-tenant)
//   - More testable (can inject mock enforcers)
//
// COMPARISON TO OTHER SYSTEMS:
//   - Kafka: Quotas always enforced, but can be set to unlimited
//   - RabbitMQ: Per-vhost limits, no option to disable
//   - goqueue: Clean separation - no enforcement in single-tenant mode
//
// ARCHITECTURE:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                        BROKER STARTUP                                   │
//   │                                                                         │
//   │   ┌──────────────────────┐     ┌──────────────────────┐                 │
//   │   │ EnableMultiTenancy   │     │ EnableMultiTenancy   │                 │
//   │   │      = false         │     │      = true          │                 │
//   │   └──────────┬───────────┘     └──────────┬───────────┘                 │
//   │              │                            │                             │
//   │              ▼                            ▼                             │
//   │   ┌──────────────────────┐     ┌──────────────────────┐                 │
//   │   │   NoOpEnforcer       │     │ TenantQuotaEnforcer  │                 │
//   │   │   (always allows)    │     │ (actual checks)      │                 │
//   │   └──────────────────────┘     └──────────────────────┘                 │
//   │              │                            │                             │
//   │              └────────────┬───────────────┘                             │
//   │                           │                                             │
//   │                           ▼                                             │
//   │              ┌──────────────────────────┐                               │
//   │              │  broker.quotaEnforcer    │                               │
//   │              │  (interface - same API)  │                               │
//   │              └──────────────────────────┘                               │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package broker

import (
	"fmt"
	"time"
)

// =============================================================================
// QUOTA ENFORCER INTERFACE
// =============================================================================

// QuotaEnforcer abstracts quota checking for the broker.
// Two implementations exist:
//   - NoOpEnforcer: Always allows (single-tenant mode)
//   - TenantQuotaEnforcer: Actual quota enforcement (multi-tenant mode)
type QuotaEnforcer interface {
	// CheckPublish validates a publish operation against quotas.
	// Returns nil if allowed, error if quota exceeded.
	CheckPublish(tenantID string, messageSize int) error

	// CheckPublishBatch validates a batch publish operation.
	// Returns nil if allowed, error if quota exceeded.
	CheckPublishBatch(tenantID string, messageCount int, totalBytes int64) error

	// CheckConsume validates a consume operation against quotas.
	// Returns nil if allowed, error if quota exceeded.
	CheckConsume(tenantID string, messageCount int) error

	// CheckTopicCreation validates creating a topic with given partitions.
	// Parameters include current counts for proper limit checking.
	CheckTopicCreation(tenantID string, partitionCount int, currentTopics int, currentPartitions int) error

	// CheckConsumerGroup validates creating/joining a consumer group.
	// Parameter includes current count for proper limit checking.
	CheckConsumerGroup(tenantID string, currentGroups int) error

	// CheckDelay validates a delayed message against max delay quota.
	CheckDelay(tenantID string, delay time.Duration) error

	// TrackUsage records usage after a successful operation.
	// This is a no-op for NoOpEnforcer but tracks in multi-tenant mode.
	TrackUsage(tenantID string, messagesPublished, bytesPublished, messagesConsumed, bytesConsumed int64)

	// IsEnabled returns whether quota enforcement is active.
	IsEnabled() bool
}

// =============================================================================
// NO-OP ENFORCER (SINGLE-TENANT MODE)
// =============================================================================
//
// NoOpEnforcer is used when multi-tenancy is disabled.
// All checks immediately return success with zero overhead.
// This is the default enforcer for single-tenant deployments.
//
// WHY NOT JUST USE NIL?
// Using a NoOpEnforcer instead of nil checks provides:
//   - Cleaner code (no `if enforcer != nil` everywhere)
//   - Type safety (interface always present)
//   - Easier testing (can verify calls without nil guards)
//   - Zero cost (empty methods inline to nothing)
//
// =============================================================================

// NoOpEnforcer always allows all operations (single-tenant mode).
type NoOpEnforcer struct{}

// NewNoOpEnforcer creates a new no-op quota enforcer.
func NewNoOpEnforcer() *NoOpEnforcer {
	return &NoOpEnforcer{}
}

// CheckPublish always returns nil (allowed).
func (e *NoOpEnforcer) CheckPublish(tenantID string, messageSize int) error {
	return nil
}

// CheckPublishBatch always returns nil (allowed).
func (e *NoOpEnforcer) CheckPublishBatch(tenantID string, messageCount int, totalBytes int64) error {
	return nil
}

// CheckConsume always returns nil (allowed).
func (e *NoOpEnforcer) CheckConsume(tenantID string, messageCount int) error {
	return nil
}

// CheckTopicCreation always returns nil (allowed).
func (e *NoOpEnforcer) CheckTopicCreation(tenantID string, partitionCount int, currentTopics int, currentPartitions int) error {
	return nil
}

// CheckConsumerGroup always returns nil (allowed).
func (e *NoOpEnforcer) CheckConsumerGroup(tenantID string, currentGroups int) error {
	return nil
}

// CheckDelay always returns nil (allowed).
func (e *NoOpEnforcer) CheckDelay(tenantID string, delay time.Duration) error {
	return nil
}

// TrackUsage is a no-op in single-tenant mode.
func (e *NoOpEnforcer) TrackUsage(tenantID string, messagesPublished, bytesPublished, messagesConsumed, bytesConsumed int64) {
	// No-op: single-tenant mode doesn't track per-tenant usage
}

// IsEnabled returns false for single-tenant mode.
func (e *NoOpEnforcer) IsEnabled() bool {
	return false
}

// =============================================================================
// TENANT QUOTA ENFORCER (MULTI-TENANT MODE)
// =============================================================================
//
// TenantQuotaEnforcer wraps TenantManager and QuotaManager to provide
// quota enforcement in multi-tenant mode.
//
// RESPONSIBILITIES:
//   - Delegate quota checks to QuotaManager
//   - Delegate usage tracking to TenantManager
//   - Format quota errors consistently
//
// =============================================================================

// TenantQuotaEnforcer enforces quotas in multi-tenant mode.
type TenantQuotaEnforcer struct {
	tenantManager *TenantManager
}

// NewTenantQuotaEnforcer creates a quota enforcer for multi-tenant mode.
func NewTenantQuotaEnforcer(tm *TenantManager) *TenantQuotaEnforcer {
	return &TenantQuotaEnforcer{
		tenantManager: tm,
	}
}

// CheckPublish validates publish against rate and size quotas.
func (e *TenantQuotaEnforcer) CheckPublish(tenantID string, messageSize int) error {
	qm := e.tenantManager.quotaManager
	result := qm.CheckPublish(tenantID, messageSize)
	if !result.Allowed {
		return formatQuotaError(result)
	}
	return nil
}

// CheckPublishBatch validates batch publish against quotas.
func (e *TenantQuotaEnforcer) CheckPublishBatch(tenantID string, messageCount int, totalBytes int64) error {
	qm := e.tenantManager.quotaManager

	// Check message count rate
	result := qm.CheckPublishRate(tenantID, messageCount)
	if !result.Allowed {
		return formatQuotaError(result)
	}

	// Check bytes rate
	result = qm.CheckPublishBytesRate(tenantID, totalBytes)
	if !result.Allowed {
		return formatQuotaError(result)
	}

	return nil
}

// CheckConsume validates consume against rate quota.
func (e *TenantQuotaEnforcer) CheckConsume(tenantID string, messageCount int) error {
	qm := e.tenantManager.quotaManager
	result := qm.CheckConsumeRate(tenantID, messageCount)
	if !result.Allowed {
		return formatQuotaError(result)
	}
	return nil
}

// CheckTopicCreation validates against topic and partition limits.
func (e *TenantQuotaEnforcer) CheckTopicCreation(tenantID string, partitionCount int, currentTopics int, currentPartitions int) error {
	qm := e.tenantManager.quotaManager
	result := qm.CheckTopicCreation(tenantID, partitionCount, currentTopics, currentPartitions)
	if !result.Allowed {
		return formatQuotaError(result)
	}
	return nil
}

// CheckConsumerGroup validates against consumer group limit.
func (e *TenantQuotaEnforcer) CheckConsumerGroup(tenantID string, currentGroups int) error {
	qm := e.tenantManager.quotaManager
	result := qm.CheckConsumerGroupCount(tenantID, currentGroups+1)
	if !result.Allowed {
		return formatQuotaError(result)
	}
	return nil
}

// CheckDelay validates against max delay quota.
func (e *TenantQuotaEnforcer) CheckDelay(tenantID string, delay time.Duration) error {
	qm := e.tenantManager.quotaManager
	result := qm.CheckDelay(tenantID, delay)
	if !result.Allowed {
		return formatQuotaError(result)
	}
	return nil
}

// TrackUsage records usage in TenantManager.
func (e *TenantQuotaEnforcer) TrackUsage(tenantID string, messagesPublished, bytesPublished, messagesConsumed, bytesConsumed int64) {
	e.tenantManager.IncrementUsage(tenantID, messagesPublished, bytesPublished, messagesConsumed, bytesConsumed)
}

// IsEnabled returns true for multi-tenant mode.
func (e *TenantQuotaEnforcer) IsEnabled() bool {
	return true
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// formatQuotaError creates a consistent error from QuotaCheckResult.
// This centralizes quota error formatting, eliminating duplication.
func formatQuotaError(result QuotaCheckResult) error {
	if result.WaitTime > 0 {
		return fmt.Errorf("%w: %v (retry after %v)",
			ErrQuotaExceeded, result.Error, result.WaitTime.Round(time.Millisecond))
	}
	return fmt.Errorf("%w: %v", ErrQuotaExceeded, result.Error)
}
