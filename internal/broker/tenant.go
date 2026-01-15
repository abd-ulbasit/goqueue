// =============================================================================
// TENANT - MULTI-TENANCY MANAGEMENT
// =============================================================================
//
// WHAT IS MULTI-TENANCY?
// Multi-tenancy allows a single goqueue cluster to serve multiple isolated
// customers (tenants) while sharing the same infrastructure.
//
// WHY MULTI-TENANCY?
//
//   SINGLE-TENANT (Traditional):
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │  Customer A's Cluster      │    Customer B's Cluster                    │
//   │  ┌───────────────────────┐ │    ┌───────────────────────┐               │
//   │  │ orders topic          │ │    │ orders topic          │               │
//   │  │ payments topic        │ │    │ payments topic        │               │
//   │  └───────────────────────┘ │    └───────────────────────┘               │
//   │  Own hardware, own process │    Own hardware, own process               │
//   └─────────────────────────────────────────────────────────────────────────┘
//   Problems: Expensive ($$$), wasteful, hard to manage at scale
//
//   MULTI-TENANT (Shared):
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                        Single GoQueue Cluster                           │
//   │                                                                         │
//   │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │
//   │  │   Tenant: acme  │  │  Tenant: globex │  │ Tenant: initech │          │
//   │  │  ─────────────  │  │  ─────────────  │  │  ─────────────  │          │
//   │  │   acme.orders   │  │  globex.alerts  │  │  initech.events │          │
//   │  │   acme.payments │  │  globex.metrics │  │  initech.logs   │          │
//   │  │                 │  │                 │  │                 │          │
//   │  │   Quotas:       │  │   Quotas:       │  │   Quotas:       │          │
//   │  │   - 10K msg/s   │  │   - 50K msg/s   │  │   - 5K msg/s    │          │
//   │  │   - 100GB       │  │   - 500GB       │  │   - 50GB        │          │
//   │  └─────────────────┘  └─────────────────┘  └─────────────────┘          │
//   │                                                                         │
//   │  Shared hardware, shared processes, ISOLATED data + resources           │
//   └─────────────────────────────────────────────────────────────────────────┘
//   Benefits: Cost-efficient ($), single deployment, per-tenant quotas
//
// TENANT ISOLATION MODEL:
//
//   We use NAMESPACE ISOLATION where topics are prefixed with tenant ID:
//
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │                         Topic Namespace                                │
//   │                                                                        │
//   │   Tenant "acme":                                                       │
//   │     acme.orders         acme.payments       acme.notifications         │
//   │                                                                        │
//   │   Tenant "globex":                                                     │
//   │     globex.orders       globex.events                                  │
//   │                                                                        │
//   │   Tenants CANNOT access each other's topics (enforced at API layer)    │
//   └────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   | System           | Tenant Model                    |
//   |------------------|---------------------------------|
//   | AWS SQS          | Account ID in queue ARN         |
//   | Confluent Cloud  | Organization → Environment      |
//   | Azure Service Bus| Namespace-based isolation       |
//   | RabbitMQ         | Virtual hosts (vhosts)          |
//   | goqueue          | Topic prefix (tenant.topic)     |
//
// REAL-WORLD EXAMPLES:
//   - Stripe: Each merchant is a tenant with isolated data
//   - Slack: Each workspace is a tenant with isolated channels
//   - AWS: Each account is a tenant with isolated resources
//
// =============================================================================

package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// TENANT ERRORS
// =============================================================================

var (
	// ErrTenantNotFound means the tenant doesn't exist
	ErrTenantNotFound = errors.New("tenant not found")

	// ErrTenantExists means the tenant already exists
	ErrTenantExists = errors.New("tenant already exists")

	// ErrTenantDisabled means the tenant is disabled
	ErrTenantDisabled = errors.New("tenant is disabled")

	// ErrInvalidTenantID means the tenant ID format is invalid
	ErrInvalidTenantID = errors.New("invalid tenant ID: must be alphanumeric with hyphens, 3-64 chars")

	// ErrSystemTenant means the operation is not allowed on system tenant
	ErrSystemTenant = errors.New("operation not allowed on system tenant")
)

// =============================================================================
// TENANT ENTITY
// =============================================================================

// TenantStatus represents the current state of a tenant.
type TenantStatus string

const (
	// TenantStatusActive means the tenant can perform all operations
	TenantStatusActive TenantStatus = "active"

	// TenantStatusSuspended means the tenant is temporarily disabled (quota exceeded, etc.)
	TenantStatusSuspended TenantStatus = "suspended"

	// TenantStatusDisabled means the tenant is administratively disabled
	TenantStatusDisabled TenantStatus = "disabled"

	// DefaultTenantID is the system tenant for internal topics
	DefaultTenantID = "__system"
)

// Tenant represents a customer or organizational unit in a multi-tenant deployment.
//
// LIFECYCLE:
//
//	┌──────────────┐
//	│   Created    │ ──► Active (normal operations)
//	└──────────────┘
//	       │
//	       ▼
//	┌──────────────┐
//	│    Active    │ ◄─► Suspended (quota exceeded, can resume)
//	└──────────────┘
//	       │
//	       ▼
//	┌──────────────┐
//	│   Disabled   │ ──► Can be re-enabled by admin
//	└──────────────┘
//	       │
//	       ▼
//	┌──────────────┐
//	│   Deleted    │ ──► Topics marked for cleanup
//	└──────────────┘
type Tenant struct {
	// ID is the unique identifier for this tenant.
	// Used as topic prefix: {tenantID}.{topicName}
	// Example: "acme", "globex-corp", "startup123"
	// Rules: alphanumeric + hyphens, 3-64 chars, lowercase
	ID string `json:"id"`

	// Name is a human-readable display name
	Name string `json:"name"`

	// Status indicates whether the tenant is active
	Status TenantStatus `json:"status"`

	// Quotas defines resource limits for this tenant
	Quotas TenantQuotas `json:"quotas"`

	// Metadata is arbitrary key-value data for this tenant
	// Can store billing info, contact details, plan type, etc.
	Metadata map[string]string `json:"metadata,omitempty"`

	// CreatedAt is when the tenant was created
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the tenant was last modified
	UpdatedAt time.Time `json:"updated_at"`

	// SuspendedAt is when the tenant was suspended (if applicable)
	SuspendedAt *time.Time `json:"suspended_at,omitempty"`

	// SuspendReason explains why the tenant was suspended
	SuspendReason string `json:"suspend_reason,omitempty"`
}

// TenantQuotas defines resource limits for a tenant.
//
// QUOTA TYPES:
//
//	┌────────────────────────────────────────────────────────────────────────┐
//	│                           RATE QUOTAS                                  │
//	│                                                                        │
//	│  These limit throughput (messages per second):                         │
//	│    • PublishRateLimit: Max messages published per second               │
//	│    • ConsumeRateLimit: Max messages consumed per second                │
//	│                                                                        │
//	│  Enforcement: Token bucket algorithm (allows bursts)                   │
//	│                                                                        │
//	│  Messages published:  ████████████░░░░░░░░░  (10K/sec limit)           │
//	│  Messages consumed:   ████████████████░░░░░  (50K/sec limit)           │
//	└────────────────────────────────────────────────────────────────────────┘
//
//	┌────────────────────────────────────────────────────────────────────────┐
//	│                          STORAGE QUOTAS                                │
//	│                                                                        │
//	│  These limit capacity (bytes, counts):                                 │
//	│    • MaxStorageBytes: Total bytes across all topics                    │
//	│    • MaxTopics: Maximum number of topics                               │
//	│    • MaxPartitionsPerTopic: Partitions per topic                       │
//	│    • MaxMessageSizeBytes: Single message size limit                    │
//	│                                                                        │
//	│  Storage used:    ███████████████████░░░░░░  75GB / 100GB limit        │
//	│  Topic count:     █████░░░░░░░░░░░░░░░░░░░░  50 / 200 limit            │
//	└────────────────────────────────────────────────────────────────────────┘
//
//	┌────────────────────────────────────────────────────────────────────────┐
//	│                        CONNECTION QUOTAS                               │
//	│                                                                        │
//	│  These limit concurrency:                                              │
//	│    • MaxConnections: Total concurrent connections                      │
//	│    • MaxConsumerGroups: Number of consumer groups                      │
//	│                                                                        │
//	│  Connections:     ████████░░░░░░░░░░░░  80 / 100 limit                 │
//	│  Consumer groups: ██████░░░░░░░░░░░░░░  30 / 50 limit                  │
//	└────────────────────────────────────────────────────────────────────────┘
type TenantQuotas struct {
	// Rate limits (messages per second)
	// 0 = unlimited (no rate limiting)
	PublishRateLimit int64 `json:"publish_rate_limit"` // msg/sec
	ConsumeRateLimit int64 `json:"consume_rate_limit"` // msg/sec

	// Byte rate limits (bytes per second)
	// 0 = unlimited (no rate limiting)
	PublishBytesRateLimit int64 `json:"publish_bytes_rate_limit"` // bytes/sec
	ConsumeBytesRateLimit int64 `json:"consume_bytes_rate_limit"` // bytes/sec

	// Storage limits
	MaxStorageBytes       int64 `json:"max_storage_bytes"`        // Total storage limit (0 = unlimited)
	MaxTopics             int   `json:"max_topics"`               // Max topics (0 = unlimited)
	MaxPartitionsPerTopic int   `json:"max_partitions_per_topic"` // Max partitions per topic (0 = unlimited)
	MaxTotalPartitions    int   `json:"max_total_partitions"`     // Max total partitions across all topics
	MaxMessageSizeBytes   int   `json:"max_message_size_bytes"`   // Max single message size
	MaxRetentionHours     int   `json:"max_retention_hours"`      // Max retention period
	MaxDelayHours         int   `json:"max_delay_hours"`          // Max delay for scheduled messages

	// Connection limits
	MaxConnections       int `json:"max_connections"`         // Max concurrent connections (0 = unlimited)
	MaxConsumerGroups    int `json:"max_consumer_groups"`     // Max consumer groups (0 = unlimited)
	MaxConsumersPerGroup int `json:"max_consumers_per_group"` // Max consumers in a group
}

// DefaultTenantQuotas returns sensible default quotas for new tenants.
//
// PHILOSOPHY:
//
//	These defaults are designed for a "free tier" or trial tenant.
//	Production tenants should have quotas tailored to their plan.
//
// COMPARISON WITH CLOUD PROVIDERS:
//
//	| Provider         | Default Publish Rate | Default Storage |
//	|------------------|---------------------|-----------------|
//	| AWS SQS          | Unlimited*          | Unlimited*      |
//	| Azure Service Bus| 1000 msg/sec        | 5GB             |
//	| Google Pub/Sub   | Unlimited*          | 7 days retention|
//	| Confluent Cloud  | Plan-based          | Plan-based      |
//	| goqueue (default)| 1000 msg/sec        | 10GB            |
//
//	*Pay per use with soft limits
func DefaultTenantQuotas() TenantQuotas {
	return TenantQuotas{
		// Rate limits (free tier defaults)
		PublishRateLimit:      1000,     // 1K msg/sec
		ConsumeRateLimit:      5000,     // 5K msg/sec (consumers typically read more)
		PublishBytesRateLimit: 10485760, // 10 MB/sec
		ConsumeBytesRateLimit: 52428800, // 50 MB/sec

		// Storage limits
		MaxStorageBytes:       10737418240, // 10 GB
		MaxTopics:             100,         // 100 topics
		MaxPartitionsPerTopic: 16,          // 16 partitions per topic
		MaxTotalPartitions:    500,         // 500 total partitions
		MaxMessageSizeBytes:   1048576,     // 1 MB per message
		MaxRetentionHours:     168,         // 7 days
		MaxDelayHours:         168,         // 7 days max delay

		// Connection limits
		MaxConnections:       100, // 100 concurrent connections
		MaxConsumerGroups:    50,  // 50 consumer groups
		MaxConsumersPerGroup: 20,  // 20 consumers per group
	}
}

// UnlimitedTenantQuotas returns quotas with no limits (for system tenant).
func UnlimitedTenantQuotas() TenantQuotas {
	return TenantQuotas{
		PublishRateLimit:      0, // 0 = unlimited
		ConsumeRateLimit:      0,
		PublishBytesRateLimit: 0,
		ConsumeBytesRateLimit: 0,
		MaxStorageBytes:       0,
		MaxTopics:             0,
		MaxPartitionsPerTopic: 0,
		MaxTotalPartitions:    0,
		MaxMessageSizeBytes:   0,
		MaxRetentionHours:     0,
		MaxDelayHours:         0,
		MaxConnections:        0,
		MaxConsumerGroups:     0,
		MaxConsumersPerGroup:  0,
	}
}

// =============================================================================
// TENANT USAGE TRACKING
// =============================================================================

// TenantUsage tracks current resource usage for a tenant.
//
// USAGE TRACKING:
//
//	Unlike quotas (which are limits), usage tracks ACTUAL consumption.
//	This is updated in real-time as operations occur.
//
//	┌────────────────────────────────────────────────────────────────────────┐
//	│                         USAGE vs QUOTA                                 │
//	│                                                                        │
//	│   Storage:        [██████████████████░░░░░░░░░░░] 75GB / 100GB         │
//	│   Topics:         [████████░░░░░░░░░░░░░░░░░░░░░] 40 / 100             │
//	│   Partitions:     [██████████████░░░░░░░░░░░░░░░] 280 / 500            │
//	│   Connections:    [████████████░░░░░░░░░░░░░░░░░] 60 / 100             │
//	│   Consumer Groups:[██████░░░░░░░░░░░░░░░░░░░░░░░] 15 / 50              │
//	│                                                                        │
//	│   When usage reaches quota → operation rejected with QuotaExceeded     │
//	└────────────────────────────────────────────────────────────────────────┘
type TenantUsage struct {
	// Storage usage
	StorageBytes   int64 `json:"storage_bytes"`   // Total bytes stored
	TopicCount     int   `json:"topic_count"`     // Number of topics
	PartitionCount int   `json:"partition_count"` // Total partitions
	MessageCount   int64 `json:"message_count"`   // Total messages (approximate)

	// Connection usage (current)
	ConnectionCount    int `json:"connection_count"`     // Current active connections
	ConsumerGroupCount int `json:"consumer_group_count"` // Current consumer groups

	// Throughput (rolling window)
	PublishRateCurrentWindow int64 `json:"publish_rate_current_window"` // Messages in current window
	ConsumeRateCurrentWindow int64 `json:"consume_rate_current_window"` // Messages in current window

	// Cumulative stats (all-time)
	TotalMessagesPublished int64 `json:"total_messages_published"`
	TotalMessagesConsumed  int64 `json:"total_messages_consumed"`
	TotalBytesPublished    int64 `json:"total_bytes_published"`
	TotalBytesConsumed     int64 `json:"total_bytes_consumed"`

	// Last updated
	LastUpdated time.Time `json:"last_updated"`
}

// =============================================================================
// TENANT MANAGER
// =============================================================================

// TenantManagerConfig configures the tenant manager.
type TenantManagerConfig struct {
	// DataDir is the directory for tenant data storage
	DataDir string

	// DefaultQuotas are applied to new tenants if no quotas specified
	DefaultQuotas TenantQuotas

	// EnableQuotaEnforcement controls whether quotas are enforced
	// Set to false for testing or development
	EnableQuotaEnforcement bool

	// UsageUpdateInterval is how often usage stats are persisted
	UsageUpdateInterval time.Duration
}

// DefaultTenantManagerConfig returns sensible defaults.
func DefaultTenantManagerConfig(dataDir string) TenantManagerConfig {
	return TenantManagerConfig{
		DataDir:                filepath.Join(dataDir, "tenants"),
		DefaultQuotas:          DefaultTenantQuotas(),
		EnableQuotaEnforcement: true,
		UsageUpdateInterval:    30 * time.Second,
	}
}

// TenantManager manages tenant lifecycle and quota enforcement.
//
// RESPONSIBILITIES:
//
//  1. Tenant CRUD (create, read, update, delete)
//  2. Quota configuration and updates
//  3. Usage tracking and persistence
//  4. Topic namespace enforcement
//
// STORAGE:
//
//	data/tenants/
//	├── tenants.json         # All tenant configurations
//	└── usage/
//	    ├── acme.json        # Usage stats for tenant "acme"
//	    ├── globex.json      # Usage stats for tenant "globex"
//	    └── initech.json     # Usage stats for tenant "initech"
type TenantManager struct {
	config TenantManagerConfig

	// tenants holds all tenant configurations
	tenants map[string]*Tenant

	// usage holds current usage for each tenant
	usage map[string]*TenantUsage

	// quotaManager handles rate limiting
	quotaManager *QuotaManager

	// mu protects tenants and usage maps
	mu sync.RWMutex

	// persistMu protects file operations
	persistMu sync.Mutex

	// stopCh signals background goroutines to stop
	stopCh chan struct{}

	// wg tracks background goroutines
	wg sync.WaitGroup
}

// tenantIDRegex validates tenant IDs
// Must be: 3-64 characters, alphanumeric with hyphens, lowercase
var tenantIDRegex = regexp.MustCompile(`^[a-z][a-z0-9-]{2,63}$`)

// NewTenantManager creates a new tenant manager.
func NewTenantManager(config TenantManagerConfig) (*TenantManager, error) {
	// Create data directories
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create tenant data directory: %w", err)
	}

	usageDir := filepath.Join(config.DataDir, "usage")
	if err := os.MkdirAll(usageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create usage directory: %w", err)
	}

	tm := &TenantManager{
		config:       config,
		tenants:      make(map[string]*Tenant),
		usage:        make(map[string]*TenantUsage),
		quotaManager: NewQuotaManager(),
		stopCh:       make(chan struct{}),
	}

	// Load existing tenants from disk
	if err := tm.loadTenants(); err != nil {
		return nil, fmt.Errorf("failed to load tenants: %w", err)
	}

	// Load usage data
	if err := tm.loadUsage(); err != nil {
		return nil, fmt.Errorf("failed to load usage: %w", err)
	}

	// Ensure system tenant exists
	if err := tm.ensureSystemTenant(); err != nil {
		return nil, fmt.Errorf("failed to create system tenant: %w", err)
	}

	// Start background usage persistence
	tm.wg.Add(1)
	go tm.persistUsageLoop()

	return tm, nil
}

// ensureSystemTenant creates the system tenant if it doesn't exist.
// System tenant is used for internal topics like __consumer_offsets.
func (tm *TenantManager) ensureSystemTenant() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.tenants[DefaultTenantID]; exists {
		return nil
	}

	systemTenant := &Tenant{
		ID:        DefaultTenantID,
		Name:      "System",
		Status:    TenantStatusActive,
		Quotas:    UnlimitedTenantQuotas(),
		Metadata:  map[string]string{"type": "system", "description": "Internal system tenant"},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	tm.tenants[DefaultTenantID] = systemTenant
	tm.usage[DefaultTenantID] = &TenantUsage{LastUpdated: time.Now()}

	// Initialize quota buckets for system tenant (unlimited)
	tm.quotaManager.InitializeTenant(DefaultTenantID, systemTenant.Quotas)

	return tm.persistTenantsLocked()
}

// Close shuts down the tenant manager.
func (tm *TenantManager) Close() error {
	close(tm.stopCh)
	tm.wg.Wait()

	// Final usage persistence
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.persistUsageLocked()
}

// =============================================================================
// TENANT CRUD OPERATIONS
// =============================================================================

// CreateTenant creates a new tenant.
//
// VALIDATION:
//   - ID must be unique
//   - ID must match regex (alphanumeric + hyphens, 3-64 chars, lowercase)
//   - Cannot use reserved prefixes (__system, __internal)
func (tm *TenantManager) CreateTenant(id, name string, quotas *TenantQuotas, metadata map[string]string) (*Tenant, error) {
	// Validate tenant ID
	if err := ValidateTenantID(id); err != nil {
		return nil, err
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Check if tenant already exists
	if _, exists := tm.tenants[id]; exists {
		return nil, fmt.Errorf("%w: %s", ErrTenantExists, id)
	}

	// Use default quotas if not provided
	tenantQuotas := tm.config.DefaultQuotas
	if quotas != nil {
		tenantQuotas = *quotas
	}

	// Create tenant
	now := time.Now()
	tenant := &Tenant{
		ID:        id,
		Name:      name,
		Status:    TenantStatusActive,
		Quotas:    tenantQuotas,
		Metadata:  metadata,
		CreatedAt: now,
		UpdatedAt: now,
	}

	tm.tenants[id] = tenant
	tm.usage[id] = &TenantUsage{LastUpdated: now}

	// Initialize quota buckets
	tm.quotaManager.InitializeTenant(id, tenantQuotas)

	// Persist
	if err := tm.persistTenantsLocked(); err != nil {
		delete(tm.tenants, id)
		delete(tm.usage, id)
		return nil, fmt.Errorf("failed to persist tenant: %w", err)
	}

	return tenant, nil
}

// GetTenant returns a tenant by ID.
func (tm *TenantManager) GetTenant(id string) (*Tenant, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tenant, exists := tm.tenants[id]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrTenantNotFound, id)
	}

	// Return a copy to prevent external modification
	copy := *tenant
	return &copy, nil
}

// ListTenants returns all tenants.
func (tm *TenantManager) ListTenants() []*Tenant {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tenants := make([]*Tenant, 0, len(tm.tenants))
	for _, t := range tm.tenants {
		copy := *t
		tenants = append(tenants, &copy)
	}
	return tenants
}

// UpdateTenant updates tenant properties.
func (tm *TenantManager) UpdateTenant(id string, name *string, metadata map[string]string) (*Tenant, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tenant, exists := tm.tenants[id]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrTenantNotFound, id)
	}

	// Prevent modification of system tenant
	if id == DefaultTenantID {
		return nil, ErrSystemTenant
	}

	if name != nil {
		tenant.Name = *name
	}
	if metadata != nil {
		tenant.Metadata = metadata
	}
	tenant.UpdatedAt = time.Now()

	// Persist
	if err := tm.persistTenantsLocked(); err != nil {
		return nil, fmt.Errorf("failed to persist tenant update: %w", err)
	}

	copy := *tenant
	return &copy, nil
}

// UpdateTenantQuotas updates tenant quotas.
func (tm *TenantManager) UpdateTenantQuotas(id string, quotas TenantQuotas) (*Tenant, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tenant, exists := tm.tenants[id]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrTenantNotFound, id)
	}

	// Prevent modification of system tenant
	if id == DefaultTenantID {
		return nil, ErrSystemTenant
	}

	tenant.Quotas = quotas
	tenant.UpdatedAt = time.Now()

	// Update quota manager
	tm.quotaManager.UpdateTenantQuotas(id, quotas)

	// Persist
	if err := tm.persistTenantsLocked(); err != nil {
		return nil, fmt.Errorf("failed to persist quota update: %w", err)
	}

	copy := *tenant
	return &copy, nil
}

// SetTenantStatus updates tenant status.
func (tm *TenantManager) SetTenantStatus(id string, status TenantStatus, reason string) (*Tenant, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tenant, exists := tm.tenants[id]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrTenantNotFound, id)
	}

	// Prevent modification of system tenant
	if id == DefaultTenantID {
		return nil, ErrSystemTenant
	}

	tenant.Status = status
	tenant.UpdatedAt = time.Now()

	if status == TenantStatusSuspended {
		now := time.Now()
		tenant.SuspendedAt = &now
		tenant.SuspendReason = reason
	} else {
		tenant.SuspendedAt = nil
		tenant.SuspendReason = ""
	}

	// Persist
	if err := tm.persistTenantsLocked(); err != nil {
		return nil, fmt.Errorf("failed to persist status update: %w", err)
	}

	copy := *tenant
	return &copy, nil
}

// DeleteTenant removes a tenant.
// Note: This doesn't delete topics, it just removes the tenant record.
// Topics should be cleaned up separately.
func (tm *TenantManager) DeleteTenant(id string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.tenants[id]; !exists {
		return fmt.Errorf("%w: %s", ErrTenantNotFound, id)
	}

	// Prevent deletion of system tenant
	if id == DefaultTenantID {
		return ErrSystemTenant
	}

	delete(tm.tenants, id)
	delete(tm.usage, id)

	// Remove from quota manager
	tm.quotaManager.RemoveTenant(id)

	// Persist
	if err := tm.persistTenantsLocked(); err != nil {
		return fmt.Errorf("failed to persist tenant deletion: %w", err)
	}

	// Delete usage file
	usageFile := filepath.Join(tm.config.DataDir, "usage", id+".json")
	os.Remove(usageFile) // Ignore error if doesn't exist

	return nil
}

// =============================================================================
// USAGE TRACKING
// =============================================================================

// GetUsage returns current usage for a tenant.
func (tm *TenantManager) GetUsage(tenantID string) (*TenantUsage, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	usage, exists := tm.usage[tenantID]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrTenantNotFound, tenantID)
	}

	copy := *usage
	return &copy, nil
}

// IncrementUsage atomically increments usage counters.
// This is called during publish/consume operations.
func (tm *TenantManager) IncrementUsage(tenantID string, messagesPublished, bytesPublished, messagesConsumed, bytesConsumed int64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	usage, exists := tm.usage[tenantID]
	if !exists {
		return
	}

	usage.TotalMessagesPublished += messagesPublished
	usage.TotalBytesPublished += bytesPublished
	usage.TotalMessagesConsumed += messagesConsumed
	usage.TotalBytesConsumed += bytesConsumed
	usage.PublishRateCurrentWindow += messagesPublished
	usage.ConsumeRateCurrentWindow += messagesConsumed
	usage.LastUpdated = time.Now()
}

// UpdateStorageUsage updates storage-related usage counters.
func (tm *TenantManager) UpdateStorageUsage(tenantID string, storageBytes int64, topicCount, partitionCount int, messageCount int64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	usage, exists := tm.usage[tenantID]
	if !exists {
		return
	}

	usage.StorageBytes = storageBytes
	usage.TopicCount = topicCount
	usage.PartitionCount = partitionCount
	usage.MessageCount = messageCount
	usage.LastUpdated = time.Now()
}

// UpdateConnectionUsage updates connection-related usage counters.
func (tm *TenantManager) UpdateConnectionUsage(tenantID string, connectionCount, consumerGroupCount int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	usage, exists := tm.usage[tenantID]
	if !exists {
		return
	}

	usage.ConnectionCount = connectionCount
	usage.ConsumerGroupCount = consumerGroupCount
	usage.LastUpdated = time.Now()
}

// ResetRateWindows resets the rate counters for a new time window.
// Called periodically by the quota manager.
func (tm *TenantManager) ResetRateWindows() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, usage := range tm.usage {
		usage.PublishRateCurrentWindow = 0
		usage.ConsumeRateCurrentWindow = 0
	}
}

// =============================================================================
// TOPIC NAMESPACE HELPERS
// =============================================================================

// QualifyTopicName adds tenant prefix to a topic name.
// Example: QualifyTopicName("acme", "orders") → "acme.orders"
func QualifyTopicName(tenantID, topicName string) string {
	if tenantID == DefaultTenantID {
		return topicName // System topics don't have prefix
	}
	return tenantID + "." + topicName
}

// ParseQualifiedTopicName extracts tenant ID and topic name from a qualified name.
// Example: ParseQualifiedTopicName("acme.orders") → ("acme", "orders", nil)
func ParseQualifiedTopicName(qualifiedName string) (tenantID, topicName string, err error) {
	// System topics (starting with __) belong to system tenant
	if strings.HasPrefix(qualifiedName, "__") {
		return DefaultTenantID, qualifiedName, nil
	}

	// Split on first dot
	parts := strings.SplitN(qualifiedName, ".", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid qualified topic name: %s (expected format: tenant.topic)", qualifiedName)
	}

	return parts[0], parts[1], nil
}

// IsTenantTopic checks if a topic belongs to a specific tenant.
func IsTenantTopic(tenantID, qualifiedTopicName string) bool {
	parsedTenant, _, err := ParseQualifiedTopicName(qualifiedTopicName)
	if err != nil {
		return false
	}
	return parsedTenant == tenantID
}

// GetTenantTopicPrefix returns the prefix for a tenant's topics.
func GetTenantTopicPrefix(tenantID string) string {
	if tenantID == DefaultTenantID {
		return "__" // System topics start with __
	}
	return tenantID + "."
}

// =============================================================================
// VALIDATION
// =============================================================================

// ValidateTenantID validates a tenant ID.
func ValidateTenantID(id string) error {
	if id == "" {
		return ErrInvalidTenantID
	}

	// Check reserved prefixes
	if strings.HasPrefix(id, "__") {
		return fmt.Errorf("%w: cannot use reserved prefix '__'", ErrInvalidTenantID)
	}

	if !tenantIDRegex.MatchString(id) {
		return ErrInvalidTenantID
	}

	return nil
}

// ValidateTopicName validates a topic name (without tenant prefix).
func ValidateTopicName(name string) error {
	if name == "" {
		return errors.New("topic name cannot be empty")
	}

	// Topic name rules: alphanumeric, underscores, hyphens, 1-255 chars
	if len(name) > 255 {
		return errors.New("topic name too long (max 255 chars)")
	}

	matched, _ := regexp.MatchString(`^[a-zA-Z0-9_-]+$`, name)
	if !matched {
		return errors.New("topic name must be alphanumeric with underscores and hyphens only")
	}

	return nil
}

// =============================================================================
// PERSISTENCE
// =============================================================================

// tenantsFile is the filename for tenant storage
const tenantsFile = "tenants.json"

// loadTenants loads tenants from disk.
func (tm *TenantManager) loadTenants() error {
	tm.persistMu.Lock()
	defer tm.persistMu.Unlock()

	path := filepath.Join(tm.config.DataDir, tenantsFile)

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil // No tenants yet
	}
	if err != nil {
		return fmt.Errorf("failed to read tenants file: %w", err)
	}

	var tenants []*Tenant
	if err := json.Unmarshal(data, &tenants); err != nil {
		return fmt.Errorf("failed to parse tenants file: %w", err)
	}

	for _, t := range tenants {
		tm.tenants[t.ID] = t
		// Initialize quota buckets
		tm.quotaManager.InitializeTenant(t.ID, t.Quotas)
	}

	return nil
}

// persistTenantsLocked persists tenants to disk. Must hold tm.mu.
func (tm *TenantManager) persistTenantsLocked() error {
	tm.persistMu.Lock()
	defer tm.persistMu.Unlock()

	tenants := make([]*Tenant, 0, len(tm.tenants))
	for _, t := range tm.tenants {
		tenants = append(tenants, t)
	}

	data, err := json.MarshalIndent(tenants, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal tenants: %w", err)
	}

	path := filepath.Join(tm.config.DataDir, tenantsFile)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write tenants file: %w", err)
	}

	return nil
}

// loadUsage loads usage data from disk.
func (tm *TenantManager) loadUsage() error {
	usageDir := filepath.Join(tm.config.DataDir, "usage")

	entries, err := os.ReadDir(usageDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read usage directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		tenantID := strings.TrimSuffix(entry.Name(), ".json")
		path := filepath.Join(usageDir, entry.Name())

		data, err := os.ReadFile(path)
		if err != nil {
			continue // Skip corrupted files
		}

		var usage TenantUsage
		if err := json.Unmarshal(data, &usage); err != nil {
			continue
		}

		tm.usage[tenantID] = &usage
	}

	return nil
}

// persistUsageLocked persists usage data to disk. Must hold tm.mu for reading.
func (tm *TenantManager) persistUsageLocked() error {
	tm.persistMu.Lock()
	defer tm.persistMu.Unlock()

	usageDir := filepath.Join(tm.config.DataDir, "usage")

	for tenantID, usage := range tm.usage {
		data, err := json.MarshalIndent(usage, "", "  ")
		if err != nil {
			continue
		}

		path := filepath.Join(usageDir, tenantID+".json")
		if err := os.WriteFile(path, data, 0644); err != nil {
			// Log but don't fail
			continue
		}
	}

	return nil
}

// persistUsageLoop periodically persists usage data.
func (tm *TenantManager) persistUsageLoop() {
	defer tm.wg.Done()

	ticker := time.NewTicker(tm.config.UsageUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tm.stopCh:
			return
		case <-ticker.C:
			tm.mu.RLock()
			tm.persistUsageLocked()
			tm.mu.RUnlock()
		}
	}
}

// =============================================================================
// STATISTICS
// =============================================================================

// TenantStats combines tenant info with current usage.
type TenantStats struct {
	Tenant *Tenant      `json:"tenant"`
	Usage  *TenantUsage `json:"usage"`

	// Quota percentages (usage / quota * 100)
	StorageUsagePercent    float64 `json:"storage_usage_percent"`
	TopicUsagePercent      float64 `json:"topic_usage_percent"`
	PartitionUsagePercent  float64 `json:"partition_usage_percent"`
	ConnectionUsagePercent float64 `json:"connection_usage_percent"`
}

// GetTenantStats returns comprehensive stats for a tenant.
func (tm *TenantManager) GetTenantStats(tenantID string) (*TenantStats, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tenant, exists := tm.tenants[tenantID]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrTenantNotFound, tenantID)
	}

	usage, _ := tm.usage[tenantID]
	if usage == nil {
		usage = &TenantUsage{}
	}

	stats := &TenantStats{
		Tenant: tenant,
		Usage:  usage,
	}

	// Calculate percentages
	if tenant.Quotas.MaxStorageBytes > 0 {
		stats.StorageUsagePercent = float64(usage.StorageBytes) / float64(tenant.Quotas.MaxStorageBytes) * 100
	}
	if tenant.Quotas.MaxTopics > 0 {
		stats.TopicUsagePercent = float64(usage.TopicCount) / float64(tenant.Quotas.MaxTopics) * 100
	}
	if tenant.Quotas.MaxTotalPartitions > 0 {
		stats.PartitionUsagePercent = float64(usage.PartitionCount) / float64(tenant.Quotas.MaxTotalPartitions) * 100
	}
	if tenant.Quotas.MaxConnections > 0 {
		stats.ConnectionUsagePercent = float64(usage.ConnectionCount) / float64(tenant.Quotas.MaxConnections) * 100
	}

	return stats, nil
}

// ManagerStats returns aggregate stats across all tenants.
type ManagerStats struct {
	TotalTenants        int   `json:"total_tenants"`
	ActiveTenants       int   `json:"active_tenants"`
	SuspendedTenants    int   `json:"suspended_tenants"`
	DisabledTenants     int   `json:"disabled_tenants"`
	TotalStorageBytes   int64 `json:"total_storage_bytes"`
	TotalTopics         int   `json:"total_topics"`
	TotalPartitions     int   `json:"total_partitions"`
	TotalConnections    int   `json:"total_connections"`
	TotalConsumerGroups int   `json:"total_consumer_groups"`
}

// GetManagerStats returns aggregate statistics.
func (tm *TenantManager) GetManagerStats() ManagerStats {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	stats := ManagerStats{
		TotalTenants: len(tm.tenants),
	}

	for _, tenant := range tm.tenants {
		switch tenant.Status {
		case TenantStatusActive:
			stats.ActiveTenants++
		case TenantStatusSuspended:
			stats.SuspendedTenants++
		case TenantStatusDisabled:
			stats.DisabledTenants++
		}
	}

	for _, usage := range tm.usage {
		stats.TotalStorageBytes += usage.StorageBytes
		stats.TotalTopics += usage.TopicCount
		stats.TotalPartitions += usage.PartitionCount
		stats.TotalConnections += usage.ConnectionCount
		stats.TotalConsumerGroups += usage.ConsumerGroupCount
	}

	return stats
}
