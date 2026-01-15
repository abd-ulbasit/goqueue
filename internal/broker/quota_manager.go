// =============================================================================
// QUOTA MANAGER - CENTRALIZED QUOTA ENFORCEMENT
// =============================================================================
//
// WHAT IS THE QUOTA MANAGER?
// The QuotaManager is the central authority for enforcing resource quotas
// across all tenants. It coordinates:
//   - Rate limit checks (via token buckets)
//   - Storage quota checks (via usage tracking)
//   - Connection quota checks (via counters)
//
// ENFORCEMENT FLOW:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                        QUOTA ENFORCEMENT FLOW                           │
//   │                                                                         │
//   │   ┌──────────────┐                                                      │
//   │   │   Request    │  e.g., Publish("acme.orders", msg)                   │
//   │   └──────┬───────┘                                                      │
//   │          │                                                              │
//   │          ▼                                                              │
//   │   ┌───────────────────────────────────────────────────────────────┐     │
//   │   │               QUOTA MANAGER                                   │     │
//   │   │                                                               │     │
//   │   │   1. Extract tenant ID from topic ("acme")                    │     │
//   │   │   2. Check tenant status (active?)                            │     │
//   │   │   3. Check message size quota                                 │     │
//   │   │   4. Check storage quota                                      │     │
//   │   │   5. Check publish rate quota (token bucket)                  │     │
//   │   │   6. Check publish bytes rate quota                           │     │
//   │   │                                                               │     │
//   │   └──────────────┬────────────────────────────────────────────────┘     │
//   │                  │                                                      │
//   │          ┌───────┴───────┐                                              │
//   │          │               │                                              │
//   │          ▼               ▼                                              │
//   │   ┌──────────────┐ ┌──────────────┐                                     │
//   │   │    ALLOW     │ │    REJECT    │                                     │
//   │   │  (proceed)   │ │  (quota err) │                                     │
//   │   └──────────────┘ └──────────────┘                                     │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// INTEGRATION POINTS:
//   - Broker.Publish() → CheckPublish()
//   - Broker.Consume() → CheckConsume()
//   - Broker.CreateTopic() → CheckTopicCreation()
//   - ConnectionManager → CheckConnection()
//   - GroupCoordinator → CheckConsumerGroup()
//
// =============================================================================

package broker

import (
	"fmt"
	"sync"
	"time"
)

// =============================================================================
// QUOTA MANAGER
// =============================================================================

// QuotaManager enforces quotas across all tenants.
type QuotaManager struct {
	// buckets holds token buckets per tenant
	buckets map[string]*TenantQuotaBuckets

	// violations tracks quota violations per tenant
	violations map[string]*TenantViolationStats

	// enabled controls whether enforcement is active
	enabled bool

	// mu protects all maps
	mu sync.RWMutex
}

// NewQuotaManager creates a new quota manager.
func NewQuotaManager() *QuotaManager {
	return &QuotaManager{
		buckets:    make(map[string]*TenantQuotaBuckets),
		violations: make(map[string]*TenantViolationStats),
		enabled:    true,
	}
}

// SetEnabled turns quota enforcement on/off.
func (qm *QuotaManager) SetEnabled(enabled bool) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.enabled = enabled
}

// IsEnabled returns whether quota enforcement is active.
func (qm *QuotaManager) IsEnabled() bool {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	return qm.enabled
}

// =============================================================================
// TENANT LIFECYCLE
// =============================================================================

// InitializeTenant creates quota buckets for a tenant.
func (qm *QuotaManager) InitializeTenant(tenantID string, quotas TenantQuotas) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	qm.buckets[tenantID] = NewTenantQuotaBuckets(quotas)
	qm.violations[tenantID] = &TenantViolationStats{}
}

// UpdateTenantQuotas updates quotas for a tenant.
func (qm *QuotaManager) UpdateTenantQuotas(tenantID string, quotas TenantQuotas) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if buckets, exists := qm.buckets[tenantID]; exists {
		buckets.UpdateQuotas(quotas)
	} else {
		qm.buckets[tenantID] = NewTenantQuotaBuckets(quotas)
		qm.violations[tenantID] = &TenantViolationStats{}
	}
}

// RemoveTenant removes a tenant from the quota manager.
func (qm *QuotaManager) RemoveTenant(tenantID string) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	delete(qm.buckets, tenantID)
	delete(qm.violations, tenantID)
}

// GetTenantBuckets returns the quota buckets for a tenant.
func (qm *QuotaManager) GetTenantBuckets(tenantID string) (*TenantQuotaBuckets, bool) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	buckets, exists := qm.buckets[tenantID]
	return buckets, exists
}

// =============================================================================
// RATE QUOTA CHECKS
// =============================================================================

// CheckPublishRate checks if a tenant can publish n messages.
//
// FLOW:
//  1. Check if quota enforcement is enabled
//  2. Get tenant's token bucket
//  3. Try to consume n tokens
//  4. If not enough tokens → record violation, return error
func (qm *QuotaManager) CheckPublishRate(tenantID string, n int) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypePublishRate,
		Requested: int64(n),
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true // No quotas configured
		return result
	}

	// No rate limit configured
	if buckets.PublishRate == nil {
		result.Allowed = true
		result.Limit = 0 // Unlimited
		return result
	}

	result.Limit = int64(buckets.Quotas.PublishRateLimit)
	result.Current = int64(buckets.PublishRate.AvailableTokens())

	allowed, waitTime := buckets.PublishRate.AllowWithWait(float64(n))
	if allowed {
		result.Allowed = true
		return result
	}

	// Record violation
	if violations, ok := qm.violations[tenantID]; ok {
		violations.IncrementViolation(QuotaTypePublishRate)
	}

	result.Allowed = false
	result.WaitTime = waitTime
	result.Error = fmt.Errorf("%w: tenant %s, limit %d msg/sec, retry after %v",
		ErrPublishRateExceeded, tenantID, result.Limit, waitTime)

	return result
}

// CheckConsumeRate checks if a tenant can consume n messages.
func (qm *QuotaManager) CheckConsumeRate(tenantID string, n int) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypeConsumeRate,
		Requested: int64(n),
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	if buckets.ConsumeRate == nil {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = int64(buckets.Quotas.ConsumeRateLimit)
	result.Current = int64(buckets.ConsumeRate.AvailableTokens())

	allowed, waitTime := buckets.ConsumeRate.AllowWithWait(float64(n))
	if allowed {
		result.Allowed = true
		return result
	}

	if violations, ok := qm.violations[tenantID]; ok {
		violations.IncrementViolation(QuotaTypeConsumeRate)
	}

	result.Allowed = false
	result.WaitTime = waitTime
	result.Error = fmt.Errorf("%w: tenant %s, limit %d msg/sec, retry after %v",
		ErrConsumeRateExceeded, tenantID, result.Limit, waitTime)

	return result
}

// CheckPublishBytesRate checks if a tenant can publish n bytes.
func (qm *QuotaManager) CheckPublishBytesRate(tenantID string, bytes int64) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypePublishBytesRate,
		Requested: bytes,
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	if buckets.PublishBytesRate == nil {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = buckets.Quotas.PublishBytesRateLimit
	result.Current = int64(buckets.PublishBytesRate.AvailableTokens())

	allowed, waitTime := buckets.PublishBytesRate.AllowWithWait(float64(bytes))
	if allowed {
		result.Allowed = true
		return result
	}

	if violations, ok := qm.violations[tenantID]; ok {
		violations.IncrementViolation(QuotaTypePublishBytesRate)
	}

	result.Allowed = false
	result.WaitTime = waitTime
	result.Error = fmt.Errorf("%w: tenant %s, limit %d bytes/sec",
		ErrPublishRateExceeded, tenantID, result.Limit)

	return result
}

// CheckConsumeBytesRate checks if a tenant can consume n bytes.
func (qm *QuotaManager) CheckConsumeBytesRate(tenantID string, bytes int64) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypeConsumeBytesRate,
		Requested: bytes,
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	if buckets.ConsumeBytesRate == nil {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = buckets.Quotas.ConsumeBytesRateLimit
	result.Current = int64(buckets.ConsumeBytesRate.AvailableTokens())

	allowed, waitTime := buckets.ConsumeBytesRate.AllowWithWait(float64(bytes))
	if allowed {
		result.Allowed = true
		return result
	}

	if violations, ok := qm.violations[tenantID]; ok {
		violations.IncrementViolation(QuotaTypeConsumeBytesRate)
	}

	result.Allowed = false
	result.WaitTime = waitTime
	result.Error = fmt.Errorf("%w: tenant %s, limit %d bytes/sec",
		ErrConsumeRateExceeded, tenantID, result.Limit)

	return result
}

// =============================================================================
// STORAGE QUOTA CHECKS
// =============================================================================

// CheckMessageSize checks if a message size is within quota.
func (qm *QuotaManager) CheckMessageSize(tenantID string, sizeBytes int) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypeMessageSize,
		Requested: int64(sizeBytes),
		Current:   int64(sizeBytes),
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	maxSize := buckets.Quotas.MaxMessageSizeBytes
	if maxSize == 0 {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = int64(maxSize)

	if sizeBytes <= maxSize {
		result.Allowed = true
		return result
	}

	if violations, ok := qm.violations[tenantID]; ok {
		violations.IncrementViolation(QuotaTypeMessageSize)
	}

	result.Allowed = false
	result.Error = fmt.Errorf("%w: tenant %s, message size %d bytes, limit %d bytes",
		ErrMessageSizeExceeded, tenantID, sizeBytes, maxSize)

	return result
}

// CheckStorageQuota checks if a tenant has storage capacity.
func (qm *QuotaManager) CheckStorageQuota(tenantID string, currentBytes, additionalBytes int64) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypeStorage,
		Requested: additionalBytes,
		Current:   currentBytes,
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	maxBytes := buckets.Quotas.MaxStorageBytes
	if maxBytes == 0 {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = maxBytes

	if currentBytes+additionalBytes <= maxBytes {
		result.Allowed = true
		return result
	}

	if violations, ok := qm.violations[tenantID]; ok {
		violations.IncrementViolation(QuotaTypeStorage)
	}

	result.Allowed = false
	result.Error = fmt.Errorf("%w: tenant %s, current %d bytes, limit %d bytes",
		ErrStorageQuotaExceeded, tenantID, currentBytes, maxBytes)

	return result
}

// CheckTopicCount checks if a tenant can create more topics.
func (qm *QuotaManager) CheckTopicCount(tenantID string, currentCount, additionalCount int) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypeTopics,
		Requested: int64(additionalCount),
		Current:   int64(currentCount),
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	maxTopics := buckets.Quotas.MaxTopics
	if maxTopics == 0 {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = int64(maxTopics)

	if currentCount+additionalCount <= maxTopics {
		result.Allowed = true
		return result
	}

	if violations, ok := qm.violations[tenantID]; ok {
		violations.IncrementViolation(QuotaTypeTopics)
	}

	result.Allowed = false
	result.Error = fmt.Errorf("%w: tenant %s, current %d, limit %d",
		ErrTopicCountExceeded, tenantID, currentCount, maxTopics)

	return result
}

// CheckPartitionCount checks if a tenant can create more partitions.
func (qm *QuotaManager) CheckPartitionCount(tenantID string, currentCount, additionalCount int) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypePartitions,
		Requested: int64(additionalCount),
		Current:   int64(currentCount),
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	maxPartitions := buckets.Quotas.MaxTotalPartitions
	if maxPartitions == 0 {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = int64(maxPartitions)

	if currentCount+additionalCount <= maxPartitions {
		result.Allowed = true
		return result
	}

	if violations, ok := qm.violations[tenantID]; ok {
		violations.IncrementViolation(QuotaTypePartitions)
	}

	result.Allowed = false
	result.Error = fmt.Errorf("%w: tenant %s, current %d, limit %d",
		ErrPartitionCountExceeded, tenantID, currentCount, maxPartitions)

	return result
}

// CheckPartitionsPerTopic checks if a topic can have the specified number of partitions.
func (qm *QuotaManager) CheckPartitionsPerTopic(tenantID string, partitionCount int) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypePartitions,
		Requested: int64(partitionCount),
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	maxPerTopic := buckets.Quotas.MaxPartitionsPerTopic
	if maxPerTopic == 0 {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = int64(maxPerTopic)

	if partitionCount <= maxPerTopic {
		result.Allowed = true
		return result
	}

	if violations, ok := qm.violations[tenantID]; ok {
		violations.IncrementViolation(QuotaTypePartitions)
	}

	result.Allowed = false
	result.Error = fmt.Errorf("%w: tenant %s, requested %d partitions, max %d per topic",
		ErrPartitionCountExceeded, tenantID, partitionCount, maxPerTopic)

	return result
}

// CheckRetention checks if a retention period is within quota.
func (qm *QuotaManager) CheckRetention(tenantID string, retentionHours int) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypeRetention,
		Requested: int64(retentionHours),
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	maxRetention := buckets.Quotas.MaxRetentionHours
	if maxRetention == 0 {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = int64(maxRetention)

	if retentionHours <= maxRetention {
		result.Allowed = true
		return result
	}

	result.Allowed = false
	result.Error = fmt.Errorf("%w: tenant %s, requested %d hours, max %d hours",
		ErrRetentionExceeded, tenantID, retentionHours, maxRetention)

	return result
}

// CheckDelay checks if a delay duration is within quota.
func (qm *QuotaManager) CheckDelay(tenantID string, delay time.Duration) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	delayHours := int(delay.Hours())

	result := QuotaCheckResult{
		QuotaType: QuotaTypeDelay,
		Requested: int64(delayHours),
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	maxDelay := buckets.Quotas.MaxDelayHours
	if maxDelay == 0 {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = int64(maxDelay)

	if delayHours <= maxDelay {
		result.Allowed = true
		return result
	}

	result.Allowed = false
	result.Error = fmt.Errorf("%w: tenant %s, requested %d hours, max %d hours",
		ErrDelayExceeded, tenantID, delayHours, maxDelay)

	return result
}

// =============================================================================
// CONNECTION QUOTA CHECKS
// =============================================================================

// CheckConnectionCount checks if a tenant can create more connections.
func (qm *QuotaManager) CheckConnectionCount(tenantID string, currentCount int) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypeConnections,
		Requested: 1,
		Current:   int64(currentCount),
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	maxConns := buckets.Quotas.MaxConnections
	if maxConns == 0 {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = int64(maxConns)

	if currentCount < maxConns {
		result.Allowed = true
		return result
	}

	if violations, ok := qm.violations[tenantID]; ok {
		violations.IncrementViolation(QuotaTypeConnections)
	}

	result.Allowed = false
	result.Error = fmt.Errorf("%w: tenant %s, current %d, limit %d",
		ErrConnectionCountExceeded, tenantID, currentCount, maxConns)

	return result
}

// CheckConsumerGroupCount checks if a tenant can create more consumer groups.
func (qm *QuotaManager) CheckConsumerGroupCount(tenantID string, currentCount int) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypeConsumerGroups,
		Requested: 1,
		Current:   int64(currentCount),
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	maxGroups := buckets.Quotas.MaxConsumerGroups
	if maxGroups == 0 {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = int64(maxGroups)

	if currentCount < maxGroups {
		result.Allowed = true
		return result
	}

	if violations, ok := qm.violations[tenantID]; ok {
		violations.IncrementViolation(QuotaTypeConsumerGroups)
	}

	result.Allowed = false
	result.Error = fmt.Errorf("%w: tenant %s, current %d, limit %d",
		ErrConsumerGroupCountExceeded, tenantID, currentCount, maxGroups)

	return result
}

// CheckConsumersPerGroup checks if a consumer group can have more consumers.
func (qm *QuotaManager) CheckConsumersPerGroup(tenantID string, currentCount int) QuotaCheckResult {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := QuotaCheckResult{
		QuotaType: QuotaTypeConsumersPerGroup,
		Requested: 1,
		Current:   int64(currentCount),
	}

	if !qm.enabled {
		result.Allowed = true
		return result
	}

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		result.Allowed = true
		return result
	}

	maxPerGroup := buckets.Quotas.MaxConsumersPerGroup
	if maxPerGroup == 0 {
		result.Allowed = true
		result.Limit = 0
		return result
	}

	result.Limit = int64(maxPerGroup)

	if currentCount < maxPerGroup {
		result.Allowed = true
		return result
	}

	result.Allowed = false
	result.Error = fmt.Errorf("consumer group consumer limit exceeded: tenant %s, current %d, limit %d",
		tenantID, currentCount, maxPerGroup)

	return result
}

// =============================================================================
// COMPOSITE CHECKS
// =============================================================================

// CheckPublish performs all checks needed before publishing a message.
//
// CHECKS (in order):
//  1. Message size
//  2. Publish rate (messages/sec)
//  3. Publish bytes rate
//
// Returns first failing check, or success if all pass.
func (qm *QuotaManager) CheckPublish(tenantID string, messageSize int) QuotaCheckResult {
	// Check message size first (cheapest check)
	if result := qm.CheckMessageSize(tenantID, messageSize); !result.Allowed {
		return result
	}

	// Check message rate
	if result := qm.CheckPublishRate(tenantID, 1); !result.Allowed {
		return result
	}

	// Check bytes rate
	if result := qm.CheckPublishBytesRate(tenantID, int64(messageSize)); !result.Allowed {
		return result
	}

	return QuotaCheckResult{Allowed: true}
}

// CheckConsume performs all checks needed before consuming messages.
func (qm *QuotaManager) CheckConsume(tenantID string, messageCount int, totalBytes int64) QuotaCheckResult {
	// Check message rate
	if result := qm.CheckConsumeRate(tenantID, messageCount); !result.Allowed {
		return result
	}

	// Check bytes rate
	if result := qm.CheckConsumeBytesRate(tenantID, totalBytes); !result.Allowed {
		return result
	}

	return QuotaCheckResult{Allowed: true}
}

// CheckTopicCreation performs all checks needed before creating a topic.
func (qm *QuotaManager) CheckTopicCreation(tenantID string, currentTopics, currentPartitions, newPartitions int) QuotaCheckResult {
	// Check topic count
	if result := qm.CheckTopicCount(tenantID, currentTopics, 1); !result.Allowed {
		return result
	}

	// Check partitions per topic
	if result := qm.CheckPartitionsPerTopic(tenantID, newPartitions); !result.Allowed {
		return result
	}

	// Check total partitions
	if result := qm.CheckPartitionCount(tenantID, currentPartitions, newPartitions); !result.Allowed {
		return result
	}

	return QuotaCheckResult{Allowed: true}
}

// =============================================================================
// STATISTICS
// =============================================================================

// GetQuotaStats returns quota statistics for a tenant.
func (qm *QuotaManager) GetQuotaStats(tenantID string) (*QuotaStats, error) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	buckets, exists := qm.buckets[tenantID]
	if !exists {
		return nil, fmt.Errorf("tenant %s not found", tenantID)
	}

	stats := &QuotaStats{
		TenantID: tenantID,
		Quotas:   buckets.Quotas,
	}

	// Get available tokens
	if buckets.PublishRate != nil {
		stats.PublishRateAvailable = buckets.PublishRate.AvailableTokens()
	}
	if buckets.ConsumeRate != nil {
		stats.ConsumeRateAvailable = buckets.ConsumeRate.AvailableTokens()
	}
	if buckets.PublishBytesRate != nil {
		stats.PublishBytesRateAvailable = buckets.PublishBytesRate.AvailableTokens()
	}
	if buckets.ConsumeBytesRate != nil {
		stats.ConsumeBytesRateAvailable = buckets.ConsumeBytesRate.AvailableTokens()
	}

	// Get violation stats
	if violations, ok := qm.violations[tenantID]; ok {
		vs := violations.GetStats()
		stats.PublishRateViolations = vs.PublishRateViolations
		stats.ConsumeRateViolations = vs.ConsumeRateViolations
		stats.PublishBytesRateViolations = vs.PublishBytesRateViolations
		stats.ConsumeBytesRateViolations = vs.ConsumeBytesRateViolations
		stats.StorageViolations = vs.StorageViolations
		stats.TopicCountViolations = vs.TopicCountViolations
		stats.PartitionCountViolations = vs.PartitionCountViolations
		stats.MessageSizeViolations = vs.MessageSizeViolations
		stats.ConnectionViolations = vs.ConnectionViolations
		stats.ConsumerGroupViolations = vs.ConsumerGroupViolations
		stats.LastViolation = vs.LastViolation
	}

	return stats, nil
}

// ResetViolationStats resets violation counters for a tenant.
func (qm *QuotaManager) ResetViolationStats(tenantID string) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	if violations, ok := qm.violations[tenantID]; ok {
		violations.Reset()
	}
}

// GetAllQuotaStats returns quota stats for all tenants.
func (qm *QuotaManager) GetAllQuotaStats() map[string]*QuotaStats {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	stats := make(map[string]*QuotaStats)
	for tenantID := range qm.buckets {
		if s, err := qm.GetQuotaStats(tenantID); err == nil {
			stats[tenantID] = s
		}
	}
	return stats
}
