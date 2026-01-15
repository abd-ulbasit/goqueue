// =============================================================================
// QUOTA - RESOURCE LIMITS AND QUOTA DEFINITIONS
// =============================================================================
//
// WHAT ARE QUOTAS?
// Quotas are resource limits that prevent any single tenant from consuming
// more than their fair share of system resources. This prevents the
// "noisy neighbor" problem in multi-tenant environments.
//
// THE NOISY NEIGHBOR PROBLEM:
//
//   WITHOUT QUOTAS:
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │                         GoQueue Cluster                                │
//   │                                                                        │
//   │   Tenant: runaway-corp              Tenant: innocent-bystander         │
//   │   ┌────────────────────────┐       ┌────────────────────────┐          │
//   │   │ Publishing 1M msg/sec  │──────►│ Can't publish!         │          │
//   │   │ Using all disk I/O     │       │ Cluster is overloaded! │          │
//   │   │ Consuming all bandwidth│       │ Timeouts everywhere!   │          │
//   │   └────────────────────────┘       └────────────────────────┘          │
//   │                                                                        │
//   │   This is the "NOISY NEIGHBOR" problem                                 │
//   └────────────────────────────────────────────────────────────────────────┘
//
//   WITH QUOTAS:
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │                         GoQueue Cluster                                │
//   │                                                                        │
//   │   Tenant: runaway-corp              Tenant: innocent-bystander         │
//   │   ┌────────────────────────┐       ┌────────────────────────┐          │
//   │   │ Quota: 10K msg/sec     │       │ Quota: 10K msg/sec     │          │
//   │   │ ──────────────────     │       │ ──────────────────     │          │
//   │   │ Publishing 10K/sec ✓   │       │ Publishing 5K/sec ✓    │          │
//   │   │ (excess REJECTED)      │       │ (guaranteed resources) │          │
//   │   └────────────────────────┘       └────────────────────────┘          │
//   │                                                                        │
//   │   Quotas ensure FAIR RESOURCE ALLOCATION                               │
//   └────────────────────────────────────────────────────────────────────────┘
//
// QUOTA TYPES:
//
//   1. RATE QUOTAS (throughput limits)
//      - Messages per second
//      - Bytes per second
//      - Enforced via token bucket algorithm
//
//   2. STORAGE QUOTAS (capacity limits)
//      - Total storage bytes
//      - Number of topics
//      - Number of partitions
//      - Max message size
//      - Enforced via usage tracking
//
//   3. CONNECTION QUOTAS (concurrency limits)
//      - Concurrent connections
//      - Consumer groups
//      - Consumers per group
//      - Enforced via connection counting
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   | System       | Quota Types                    | Enforcement    |
//   |--------------|--------------------------------|----------------|
//   | Kafka        | producer_byte_rate,            | Token bucket   |
//   |              | consumer_byte_rate,            | (KIP-124)      |
//   |              | request_percentage             |                |
//   | AWS SQS      | Message rate (soft limit)      | Throttling     |
//   | RabbitMQ     | Memory/disk alarms             | Flow control   |
//   | Azure SB     | Plan-based limits              | Hard reject    |
//   | goqueue      | Rate + Storage + Connection    | Token bucket   |
//
// =============================================================================

package broker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// =============================================================================
// QUOTA ERRORS
// =============================================================================

var (
	// ErrQuotaExceeded means a quota limit was reached
	ErrQuotaExceeded = errors.New("quota exceeded")

	// ErrPublishRateExceeded means publish rate limit reached
	ErrPublishRateExceeded = fmt.Errorf("%w: publish rate limit exceeded", ErrQuotaExceeded)

	// ErrConsumeRateExceeded means consume rate limit reached
	ErrConsumeRateExceeded = fmt.Errorf("%w: consume rate limit exceeded", ErrQuotaExceeded)

	// ErrStorageQuotaExceeded means storage limit reached
	ErrStorageQuotaExceeded = fmt.Errorf("%w: storage limit exceeded", ErrQuotaExceeded)

	// ErrTopicCountExceeded means max topics limit reached
	ErrTopicCountExceeded = fmt.Errorf("%w: topic count limit exceeded", ErrQuotaExceeded)

	// ErrPartitionCountExceeded means max partitions limit reached
	ErrPartitionCountExceeded = fmt.Errorf("%w: partition count limit exceeded", ErrQuotaExceeded)

	// ErrMessageSizeExceeded means message is too large
	ErrMessageSizeExceeded = fmt.Errorf("%w: message size limit exceeded", ErrQuotaExceeded)

	// ErrConnectionCountExceeded means max connections reached
	ErrConnectionCountExceeded = fmt.Errorf("%w: connection count limit exceeded", ErrQuotaExceeded)

	// ErrConsumerGroupCountExceeded means max consumer groups reached
	ErrConsumerGroupCountExceeded = fmt.Errorf("%w: consumer group count limit exceeded", ErrQuotaExceeded)

	// ErrRetentionExceeded means retention period too long
	ErrRetentionExceeded = fmt.Errorf("%w: retention period exceeds limit", ErrQuotaExceeded)

	// ErrDelayExceeded means delay duration too long
	ErrDelayExceeded = fmt.Errorf("%w: delay duration exceeds limit", ErrQuotaExceeded)
)

// =============================================================================
// QUOTA TYPES
// =============================================================================

// QuotaType identifies different kinds of quotas.
type QuotaType string

const (
	// Rate quotas
	QuotaTypePublishRate      QuotaType = "publish_rate"
	QuotaTypeConsumeRate      QuotaType = "consume_rate"
	QuotaTypePublishBytesRate QuotaType = "publish_bytes_rate"
	QuotaTypeConsumeBytesRate QuotaType = "consume_bytes_rate"

	// Storage quotas
	QuotaTypeStorage     QuotaType = "storage"
	QuotaTypeTopics      QuotaType = "topics"
	QuotaTypePartitions  QuotaType = "partitions"
	QuotaTypeMessageSize QuotaType = "message_size"
	QuotaTypeRetention   QuotaType = "retention"
	QuotaTypeDelay       QuotaType = "delay"

	// Connection quotas
	QuotaTypeConnections       QuotaType = "connections"
	QuotaTypeConsumerGroups    QuotaType = "consumer_groups"
	QuotaTypeConsumersPerGroup QuotaType = "consumers_per_group"
)

// =============================================================================
// QUOTA RESULT
// =============================================================================

// QuotaCheckResult represents the result of a quota check.
//
// USAGE:
//
//	result := quotaManager.CheckPublishRate(tenantID, 1)
//	if !result.Allowed {
//	    return result.Error
//	}
//	// Proceed with operation
type QuotaCheckResult struct {
	// Allowed indicates whether the operation is permitted
	Allowed bool

	// QuotaType identifies which quota was checked
	QuotaType QuotaType

	// Limit is the configured limit
	Limit int64

	// Current is the current usage
	Current int64

	// Requested is the amount requested
	Requested int64

	// WaitTime is how long to wait before retrying (for rate limits)
	// Zero if operation allowed or for non-rate quotas
	WaitTime time.Duration

	// Error is set if Allowed is false
	Error error
}

// =============================================================================
// QUOTA ENFORCEMENT POLICY
// =============================================================================

// QuotaEnforcementPolicy defines how quota violations are handled.
type QuotaEnforcementPolicy string

const (
	// QuotaEnforcementReject immediately rejects the operation
	// Client should implement retry logic
	QuotaEnforcementReject QuotaEnforcementPolicy = "reject"

	// QuotaEnforcementWait blocks until quota is available (with timeout)
	// Useful for smoothing traffic
	QuotaEnforcementWait QuotaEnforcementPolicy = "wait"

	// QuotaEnforcementWarn allows the operation but logs a warning
	// Useful for soft limits or monitoring-only mode
	QuotaEnforcementWarn QuotaEnforcementPolicy = "warn"
)

// =============================================================================
// TOKEN BUCKET RATE LIMITER
// =============================================================================

// TokenBucket implements the token bucket algorithm for rate limiting.
//
// HOW TOKEN BUCKET WORKS:
//
//	┌────────────────────────────────────────────────────────────────────────┐
//	│                                                                        │
//	│     Tokens added                                                       │
//	│     at fixed rate    ┌─────────────────────┐                           │
//	│     (e.g., 1K/sec)   │  Token Bucket       │                           │
//	│          │           │  ┌───────────────┐  │                           │
//	│          ▼           │  │ ○○○○○○○○○○○○○ │  │  Capacity: 1000 tokens    │
//	│          ○           │  │ ○○○○○○○○○○○○○ │  │  (allows burst up to      │
//	│          │           │  │ ○○○○○        │◄──┤   1000 messages)          │
//	│          ▼           │  └───────────────┘  │                           │
//	│     ┌────────────┐   └──────────┬──────────┘                           │
//	│     │            │              │                                      │
//	│     │            │              │ Request for 100 tokens               │
//	│     └────────────┘              │ (publish 100 messages)               │
//	│                                 ▼                                      │
//	│                    ┌────────────────────────┐                          │
//	│                    │ Tokens available (800)?│                          │
//	│                    │        YES → ALLOW     │                          │
//	│                    │ Tokens now: 700        │                          │
//	│                    └────────────────────────┘                          │
//	│                                                                        │
//	│  WHY TOKEN BUCKET?                                                     │
//	│    • Allows bursts (accumulated tokens can be used at once)            │
//	│    • Enforces average rate over time                                   │
//	│    • O(1) check and consume operations                                 │
//	│    • Industry standard (used by Kafka, AWS, nginx)                     │
//	│                                                                        │
//	│  COMPARISON:                                                           │
//	│    • Fixed Window: Resets at boundaries (allows 2x burst at edge)      │
//	│    • Sliding Window: More accurate but more complex                    │
//	│    • Leaky Bucket: No bursts (constant rate only)                      │
//	│    • Token Bucket: Best balance of simplicity and flexibility          │
//	│                                                                        │
//	└────────────────────────────────────────────────────────────────────────┘
type TokenBucket struct {
	// capacity is the maximum number of tokens (burst size)
	capacity float64

	// tokens is the current number of available tokens
	tokens float64

	// rate is how many tokens are added per second
	rate float64

	// lastRefill is when tokens were last added
	lastRefill time.Time

	// mu protects the bucket
	mu sync.Mutex
}

// NewTokenBucket creates a new token bucket rate limiter.
//
// PARAMETERS:
//   - rate: tokens per second (e.g., 1000 for 1K msg/sec)
//   - capacity: max tokens (burst size, typically equal to rate)
//
// EXAMPLE:
//
//	bucket := NewTokenBucket(1000, 1000)  // 1K/sec, burst of 1K
//	bucket.Allow(1)  // Check if 1 message can be sent
func NewTokenBucket(rate, capacity float64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     capacity, // Start full
		rate:       rate,
		lastRefill: time.Now(),
	}
}

// Allow checks if n tokens are available and consumes them.
// Returns true if allowed, false if rate limit would be exceeded.
func (tb *TokenBucket) Allow(n float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= n {
		tb.tokens -= n
		return true
	}

	return false
}

// AllowWithWait checks if tokens are available.
// If not, returns how long to wait before they will be.
func (tb *TokenBucket) AllowWithWait(n float64) (allowed bool, waitTime time.Duration) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= n {
		tb.tokens -= n
		return true, 0
	}

	// Calculate wait time
	needed := n - tb.tokens
	wait := time.Duration(needed / tb.rate * float64(time.Second))

	return false, wait
}

// refill adds tokens based on elapsed time. Must hold mutex.
func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()

	// Add tokens based on time elapsed
	tb.tokens += elapsed * tb.rate

	// Cap at capacity
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}

	tb.lastRefill = now
}

// AvailableTokens returns current available tokens.
func (tb *TokenBucket) AvailableTokens() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	return tb.tokens
}

// UpdateRate changes the rate without resetting tokens.
func (tb *TokenBucket) UpdateRate(rate, capacity float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.rate = rate
	tb.capacity = capacity

	// Cap tokens at new capacity
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
}

// =============================================================================
// TENANT QUOTA BUCKETS
// =============================================================================

// TenantQuotaBuckets holds all rate limiters for a tenant.
type TenantQuotaBuckets struct {
	// Rate limit buckets
	PublishRate      *TokenBucket
	ConsumeRate      *TokenBucket
	PublishBytesRate *TokenBucket
	ConsumeBytesRate *TokenBucket

	// Quotas (for reference)
	Quotas TenantQuotas

	// CreatedAt is when these buckets were created
	CreatedAt time.Time
}

// NewTenantQuotaBuckets creates quota buckets for a tenant.
func NewTenantQuotaBuckets(quotas TenantQuotas) *TenantQuotaBuckets {
	buckets := &TenantQuotaBuckets{
		Quotas:    quotas,
		CreatedAt: time.Now(),
	}

	// Create rate limit buckets
	// Capacity = rate (allows burst of 1 second worth of messages)
	if quotas.PublishRateLimit > 0 {
		buckets.PublishRate = NewTokenBucket(
			float64(quotas.PublishRateLimit),
			float64(quotas.PublishRateLimit),
		)
	}

	if quotas.ConsumeRateLimit > 0 {
		buckets.ConsumeRate = NewTokenBucket(
			float64(quotas.ConsumeRateLimit),
			float64(quotas.ConsumeRateLimit),
		)
	}

	if quotas.PublishBytesRateLimit > 0 {
		buckets.PublishBytesRate = NewTokenBucket(
			float64(quotas.PublishBytesRateLimit),
			float64(quotas.PublishBytesRateLimit),
		)
	}

	if quotas.ConsumeBytesRateLimit > 0 {
		buckets.ConsumeBytesRate = NewTokenBucket(
			float64(quotas.ConsumeBytesRateLimit),
			float64(quotas.ConsumeBytesRateLimit),
		)
	}

	return buckets
}

// UpdateQuotas updates the quota configuration.
func (tqb *TenantQuotaBuckets) UpdateQuotas(quotas TenantQuotas) {
	tqb.Quotas = quotas

	// Update or create buckets
	if quotas.PublishRateLimit > 0 {
		if tqb.PublishRate == nil {
			tqb.PublishRate = NewTokenBucket(
				float64(quotas.PublishRateLimit),
				float64(quotas.PublishRateLimit),
			)
		} else {
			tqb.PublishRate.UpdateRate(
				float64(quotas.PublishRateLimit),
				float64(quotas.PublishRateLimit),
			)
		}
	} else {
		tqb.PublishRate = nil
	}

	if quotas.ConsumeRateLimit > 0 {
		if tqb.ConsumeRate == nil {
			tqb.ConsumeRate = NewTokenBucket(
				float64(quotas.ConsumeRateLimit),
				float64(quotas.ConsumeRateLimit),
			)
		} else {
			tqb.ConsumeRate.UpdateRate(
				float64(quotas.ConsumeRateLimit),
				float64(quotas.ConsumeRateLimit),
			)
		}
	} else {
		tqb.ConsumeRate = nil
	}

	if quotas.PublishBytesRateLimit > 0 {
		if tqb.PublishBytesRate == nil {
			tqb.PublishBytesRate = NewTokenBucket(
				float64(quotas.PublishBytesRateLimit),
				float64(quotas.PublishBytesRateLimit),
			)
		} else {
			tqb.PublishBytesRate.UpdateRate(
				float64(quotas.PublishBytesRateLimit),
				float64(quotas.PublishBytesRateLimit),
			)
		}
	} else {
		tqb.PublishBytesRate = nil
	}

	if quotas.ConsumeBytesRateLimit > 0 {
		if tqb.ConsumeBytesRate == nil {
			tqb.ConsumeBytesRate = NewTokenBucket(
				float64(quotas.ConsumeBytesRateLimit),
				float64(quotas.ConsumeBytesRateLimit),
			)
		} else {
			tqb.ConsumeBytesRate.UpdateRate(
				float64(quotas.ConsumeBytesRateLimit),
				float64(quotas.ConsumeBytesRateLimit),
			)
		}
	} else {
		tqb.ConsumeBytesRate = nil
	}
}

// =============================================================================
// QUOTA STATISTICS
// =============================================================================

// QuotaStats contains statistics about quota usage and violations.
type QuotaStats struct {
	// TenantID identifies the tenant
	TenantID string `json:"tenant_id"`

	// Rate limit stats
	PublishRateAvailable      float64 `json:"publish_rate_available"`
	ConsumeRateAvailable      float64 `json:"consume_rate_available"`
	PublishBytesRateAvailable float64 `json:"publish_bytes_rate_available"`
	ConsumeBytesRateAvailable float64 `json:"consume_bytes_rate_available"`

	// Violation counts (since last reset)
	PublishRateViolations      int64 `json:"publish_rate_violations"`
	ConsumeRateViolations      int64 `json:"consume_rate_violations"`
	PublishBytesRateViolations int64 `json:"publish_bytes_rate_violations"`
	ConsumeBytesRateViolations int64 `json:"consume_bytes_rate_violations"`
	StorageViolations          int64 `json:"storage_violations"`
	TopicCountViolations       int64 `json:"topic_count_violations"`
	PartitionCountViolations   int64 `json:"partition_count_violations"`
	MessageSizeViolations      int64 `json:"message_size_violations"`
	ConnectionViolations       int64 `json:"connection_violations"`
	ConsumerGroupViolations    int64 `json:"consumer_group_violations"`

	// Last violation time
	LastViolation *time.Time `json:"last_violation,omitempty"`

	// Quota configuration
	Quotas TenantQuotas `json:"quotas"`
}

// TenantViolationStats tracks violation counts for a tenant.
type TenantViolationStats struct {
	PublishRateViolations      int64
	ConsumeRateViolations      int64
	PublishBytesRateViolations int64
	ConsumeBytesRateViolations int64
	StorageViolations          int64
	TopicCountViolations       int64
	PartitionCountViolations   int64
	MessageSizeViolations      int64
	ConnectionViolations       int64
	ConsumerGroupViolations    int64
	LastViolation              *time.Time

	mu sync.Mutex
}

// IncrementViolation records a violation.
func (tvs *TenantViolationStats) IncrementViolation(quotaType QuotaType) {
	tvs.mu.Lock()
	defer tvs.mu.Unlock()

	now := time.Now()
	tvs.LastViolation = &now

	switch quotaType {
	case QuotaTypePublishRate:
		tvs.PublishRateViolations++
	case QuotaTypeConsumeRate:
		tvs.ConsumeRateViolations++
	case QuotaTypePublishBytesRate:
		tvs.PublishBytesRateViolations++
	case QuotaTypeConsumeBytesRate:
		tvs.ConsumeBytesRateViolations++
	case QuotaTypeStorage:
		tvs.StorageViolations++
	case QuotaTypeTopics:
		tvs.TopicCountViolations++
	case QuotaTypePartitions:
		tvs.PartitionCountViolations++
	case QuotaTypeMessageSize:
		tvs.MessageSizeViolations++
	case QuotaTypeConnections:
		tvs.ConnectionViolations++
	case QuotaTypeConsumerGroups:
		tvs.ConsumerGroupViolations++
	}
}

// GetStats returns a copy of the current stats.
func (tvs *TenantViolationStats) GetStats() TenantViolationStats {
	tvs.mu.Lock()
	defer tvs.mu.Unlock()

	return TenantViolationStats{
		PublishRateViolations:      tvs.PublishRateViolations,
		ConsumeRateViolations:      tvs.ConsumeRateViolations,
		PublishBytesRateViolations: tvs.PublishBytesRateViolations,
		ConsumeBytesRateViolations: tvs.ConsumeBytesRateViolations,
		StorageViolations:          tvs.StorageViolations,
		TopicCountViolations:       tvs.TopicCountViolations,
		PartitionCountViolations:   tvs.PartitionCountViolations,
		MessageSizeViolations:      tvs.MessageSizeViolations,
		ConnectionViolations:       tvs.ConnectionViolations,
		ConsumerGroupViolations:    tvs.ConsumerGroupViolations,
		LastViolation:              tvs.LastViolation,
	}
}

// Reset clears violation counters.
func (tvs *TenantViolationStats) Reset() {
	tvs.mu.Lock()
	defer tvs.mu.Unlock()

	tvs.PublishRateViolations = 0
	tvs.ConsumeRateViolations = 0
	tvs.PublishBytesRateViolations = 0
	tvs.ConsumeBytesRateViolations = 0
	tvs.StorageViolations = 0
	tvs.TopicCountViolations = 0
	tvs.PartitionCountViolations = 0
	tvs.MessageSizeViolations = 0
	tvs.ConnectionViolations = 0
	tvs.ConsumerGroupViolations = 0
	tvs.LastViolation = nil
}
