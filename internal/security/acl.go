// =============================================================================
// TOPIC-LEVEL ACCESS CONTROL - FINE-GRAINED RBAC FOR GOQUEUE
// =============================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ WHY TOPIC-LEVEL ACLs?                                                       │
// │                                                                             │
// │ Global roles (admin, producer, consumer) are not enough for multi-tenant   │
// │ environments. You need to restrict access to specific topics:              │
// │                                                                             │
// │   Producer A → Can only publish to "orders-service"                         │
// │   Producer B → Can only publish to "payments-service"                       │
// │   Consumer X → Can only read from "orders-service", "notifications"        │
// │                                                                             │
// │ COMPARISON:                                                                 │
// │   - Kafka: ACLs via kafka-acls.sh, resource patterns, prefixed resources   │
// │   - RabbitMQ: Virtual hosts, topic permissions                             │
// │   - AWS SQS: IAM policies per queue                                        │
// │   - goqueue: Topic ACLs with glob patterns                                 │
// │                                                                             │
// │ ACL STRUCTURE:                                                              │
// │   Principal (API key) + Resource (topic:orders) + Action (publish) → Allow │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package security

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-chi/chi/v5"
)

// =============================================================================
// ACL STRUCTURES
// =============================================================================

// ResourceType identifies the type of resource being protected.
type ResourceType string

const (
	ResourceTypeTopic         ResourceType = "topic"
	ResourceTypeConsumerGroup ResourceType = "group"
	ResourceTypeCluster       ResourceType = "cluster"
)

// Operation represents an action on a resource.
type Operation string

const (
	OpRead     Operation = "read"
	OpWrite    Operation = "write"
	OpCreate   Operation = "create"
	OpDelete   Operation = "delete"
	OpDescribe Operation = "describe"
	OpAlter    Operation = "alter"
	OpAll      Operation = "all"
)

// ACL represents a single access control entry.
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ ACL ENTRY STRUCTURE                                                         │
// │                                                                             │
// │   Principal    : Who (API key ID, role, or wildcard)                        │
// │   ResourceType : What type (topic, group, cluster)                          │
// │   ResourceName : Which resource (exact name or glob pattern)                │
// │   Operation    : What action (read, write, create, delete, all)             │
// │   Permission   : Allow or Deny                                              │
// │                                                                             │
// │ EXAMPLES:                                                                   │
// │   Allow key_123 to write to topic "orders"                                  │
// │   Allow role:producer to write to topic "orders-*"                          │
// │   Deny key_456 to delete topic "*"                                          │
// └─────────────────────────────────────────────────────────────────────────────┘
type ACL struct {
	// Principal is the identity (key ID, role:xxx, or * for all)
	Principal string

	// ResourceType is the type of resource
	ResourceType ResourceType

	// ResourceName is the name or pattern (supports glob: orders-*)
	ResourceName string

	// Operation is the action being performed
	Operation Operation

	// Allow is true for allow, false for deny
	Allow bool
}

// ACLManager manages access control lists.
type ACLManager struct {
	// acls stores all ACL entries
	acls []ACL

	mu sync.RWMutex
}

// NewACLManager creates a new ACL manager.
func NewACLManager() *ACLManager {
	return &ACLManager{
		acls: make([]ACL, 0),
	}
}

// AddACL adds a new ACL entry.
func (m *ACLManager) AddACL(acl ACL) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acls = append(m.acls, acl)
}

// RemoveACL removes ACL entries matching the criteria.
func (m *ACLManager) RemoveACL(principal string, resourceType ResourceType, resourceName string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	removed := 0
	newACLs := make([]ACL, 0, len(m.acls))

	for _, acl := range m.acls {
		if acl.Principal == principal &&
			acl.ResourceType == resourceType &&
			acl.ResourceName == resourceName {
			removed++
			continue
		}
		newACLs = append(newACLs, acl)
	}

	m.acls = newACLs
	return removed
}

// CheckAccess determines if a principal can perform an operation on a resource.
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ ACL EVALUATION ORDER                                                        │
// │                                                                             │
// │ 1. Check explicit DENY rules first (deny overrides allow)                   │
// │ 2. Check explicit ALLOW rules for exact principal                           │
// │ 3. Check ALLOW rules for principal's roles (role:producer, role:consumer)  │
// │ 4. Check wildcard rules (principal = *)                                     │
// │ 5. Default: DENY (secure by default)                                        │
// │                                                                             │
// │ PATTERN MATCHING:                                                           │
// │   - Exact match: "orders" matches only "orders"                             │
// │   - Glob: "orders-*" matches "orders-new", "orders-archive"                 │
// │   - Wildcard: "*" matches all resources                                     │
// └─────────────────────────────────────────────────────────────────────────────┘
func (m *ACLManager) CheckAccess(key *APIKey, resourceType ResourceType, resourceName string, op Operation) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Admin role bypasses all ACLs
	if key.HasRole(RoleAdmin) {
		return true
	}

	// Build list of principals to check
	principals := []string{
		key.ID, // Exact key ID
		"*",    // Wildcard (any authenticated user)
	}
	for _, role := range key.Roles {
		principals = append(principals, "role:"+role)
	}

	// Check for explicit denies first
	for _, acl := range m.acls {
		if !acl.Allow && m.matchesACL(acl, principals, resourceType, resourceName, op) {
			return false // Explicit deny
		}
	}

	// Check for allows
	for _, acl := range m.acls {
		if acl.Allow && m.matchesACL(acl, principals, resourceType, resourceName, op) {
			return true
		}
	}

	// Default deny (but fall back to role-based permissions for basic access)
	// If there are no ACLs defined, use role-based access
	if len(m.acls) == 0 {
		return m.checkRoleBasedAccess(key, resourceType, op)
	}

	return false
}

// matchesACL checks if an ACL matches the given criteria.
func (m *ACLManager) matchesACL(acl ACL, principals []string, resourceType ResourceType, resourceName string, op Operation) bool {
	// Check principal
	principalMatch := false
	for _, p := range principals {
		if acl.Principal == p {
			principalMatch = true
			break
		}
	}
	if !principalMatch {
		return false
	}

	// Check resource type
	if acl.ResourceType != resourceType {
		return false
	}

	// Check resource name (supports glob patterns)
	if !matchPattern(acl.ResourceName, resourceName) {
		return false
	}

	// Check operation
	if acl.Operation != OpAll && acl.Operation != op {
		return false
	}

	return true
}

// checkRoleBasedAccess falls back to basic role permissions when no ACLs are defined.
func (m *ACLManager) checkRoleBasedAccess(key *APIKey, resourceType ResourceType, op Operation) bool {
	// Map operations to permissions
	var perm Permission

	switch resourceType {
	case ResourceTypeTopic:
		switch op {
		case OpRead, OpDescribe:
			perm = PermTopicRead
		case OpWrite:
			perm = PermMessagePublish
		case OpCreate:
			perm = PermTopicCreate
		case OpDelete:
			perm = PermTopicDelete
		case OpAll:
			perm = PermAdminAll
		default:
			perm = PermTopicRead
		}
	case ResourceTypeConsumerGroup:
		switch op {
		case OpRead, OpDescribe:
			perm = PermGroupRead
		case OpWrite:
			perm = PermGroupCommit
		case OpCreate:
			perm = PermGroupJoin
		case OpDelete:
			perm = PermGroupDelete
		case OpAll:
			perm = PermAdminAll
		default:
			perm = PermGroupRead
		}
	default:
		return false
	}

	return key.HasPermission(perm)
}

// ListACLs returns all ACL entries for a resource.
func (m *ACLManager) ListACLs(resourceType ResourceType, resourceName string) []ACL {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]ACL, 0)
	for _, acl := range m.acls {
		if acl.ResourceType == resourceType &&
			(resourceName == "" || matchPattern(acl.ResourceName, resourceName)) {
			result = append(result, acl)
		}
	}
	return result
}

// ListAllACLs returns all ACL entries (for admin API).
func (m *ACLManager) ListAllACLs() []ACL {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]ACL, len(m.acls))
	copy(result, m.acls)
	return result
}

// RemoveACLByIndex removes an ACL by its index (for admin API).
// Returns an error if the index is out of bounds.
func (m *ACLManager) RemoveACLByIndex(index int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if index < 0 || index >= len(m.acls) {
		return fmt.Errorf("ACL index %d out of bounds", index)
	}

	// Remove element at index
	m.acls = append(m.acls[:index], m.acls[index+1:]...)
	return nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// matchPattern matches a resource name against a pattern.
// Supports:
//   - Exact match: "orders" matches "orders"
//   - Glob: "orders-*" matches "orders-new", "orders-old"
//   - Wildcard: "*" matches everything
func matchPattern(pattern, name string) bool {
	if pattern == "*" {
		return true
	}

	// Use filepath.Match for glob support
	matched, err := filepath.Match(pattern, name)
	if err != nil {
		return pattern == name // Fall back to exact match
	}
	return matched
}

// =============================================================================
// INTEGRATED SECURITY MANAGER
// =============================================================================

// SecurityManager combines TLS, API key auth, and ACLs into a unified security layer.
type SecurityManager struct {
	// TLS configuration for HTTP/gRPC
	TLSConfig TLSConfig

	// TLS configuration for inter-node (mTLS)
	MTLSConfig TLSConfig

	// API key manager
	Keys *APIKeyManager

	// ACL manager
	ACLs *ACLManager

	// Logger
	logger *slog.Logger
}

// NewSecurityManager creates a new security manager with default settings.
func NewSecurityManager() *SecurityManager {
	return &SecurityManager{
		TLSConfig:  DefaultTLSConfig(),
		MTLSConfig: DefaultMTLSConfig(),
		Keys:       NewAPIKeyManager(DefaultAPIKeyManagerConfig()),
		ACLs:       NewACLManager(),
		logger:     slog.Default(),
	}
}

// SecurityConfig holds all security-related configuration.
type SecurityConfig struct {
	// TLS settings for client connections
	TLS TLSConfig

	// mTLS settings for cluster communication
	MTLS TLSConfig

	// API key authentication settings
	Auth APIKeyManagerConfig

	// ACL settings
	ACLEnabled bool
}

// DefaultSecurityConfig returns a default security configuration.
func DefaultSecurityConfig() SecurityConfig {
	return SecurityConfig{
		TLS:        DefaultTLSConfig(),
		MTLS:       DefaultMTLSConfig(),
		Auth:       DefaultAPIKeyManagerConfig(),
		ACLEnabled: false,
	}
}

// LoadSecurityConfigFromEnv loads security configuration from environment.
func LoadSecurityConfigFromEnv() SecurityConfig {
	return SecurityConfig{
		TLS:        LoadTLSConfigFromEnv("GOQUEUE_TLS"),
		MTLS:       LoadTLSConfigFromEnv("GOQUEUE_CLUSTER_TLS"),
		Auth:       LoadAPIKeyConfigFromEnv(),
		ACLEnabled: os.Getenv("GOQUEUE_ACL_ENABLED") == "true",
	}
}

// NewSecurityManagerWithConfig creates a security manager from config.
func NewSecurityManagerWithConfig(config SecurityConfig) *SecurityManager {
	return &SecurityManager{
		TLSConfig:  config.TLS,
		MTLSConfig: config.MTLS,
		Keys:       NewAPIKeyManager(config.Auth),
		ACLs:       NewACLManager(),
		logger:     slog.Default(),
	}
}

// IsAuthEnabled returns true if authentication is enabled.
func (m *SecurityManager) IsAuthEnabled() bool {
	return m.Keys.config.Enabled
}

// IsTLSEnabled returns true if TLS is enabled for client connections.
func (m *SecurityManager) IsTLSEnabled() bool {
	return m.TLSConfig.Enabled
}

// IsMTLSEnabled returns true if mTLS is enabled for cluster communication.
func (m *SecurityManager) IsMTLSEnabled() bool {
	return m.MTLSConfig.Enabled
}

// ============================================================================
// PERMISSION MIDDLEWARE
// ============================================================================
//
// WHY MIDDLEWARE FOR PERMISSIONS:
// Middleware allows us to protect entire route groups with a single call.
// Instead of checking permissions in every handler, we define once at route level.
//
// HOW IT WORKS:
//  1. Client makes request to protected route
//  2. Auth middleware already extracted API key and set in context
//  3. RequirePermission middleware checks if that key has required permission
//  4. If yes → request proceeds to handler
//  5. If no → 403 Forbidden response
//
// COMPARISON:
//   - Express.js: router.use(requirePermission("admin"))
//   - Django: @permission_required("admin")
//   - Kafka: AclAuthorizer.authorize(session, operation, resource)
//   - goqueue: chi middleware pattern
//
// RequirePermission returns a middleware that enforces a specific permission.
func (m *SecurityManager) RequirePermission(perm Permission) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// ================================================================
			// STEP 1: Get API key from context (set by AuthMiddleware)
			// ================================================================
			// The auth middleware already validated the key and stored it.
			// If no key in context, auth middleware wasn't used or failed.
			//
			key, ok := r.Context().Value(APIKeyContextKey).(*APIKey)
			if !ok || key == nil {
				m.logger.Warn("permission check failed: no API key in context",
					"path", r.URL.Path,
					"permission", perm,
				)
				http.Error(w, `{"error": "unauthorized", "message": "authentication required"}`,
					http.StatusUnauthorized)
				return
			}

			// ================================================================
			// STEP 2: Check if key has required permission
			// ================================================================
			// Uses the role's permission set from auth.go
			//
			// PERMISSION INHERITANCE:
			//   admin role → has all permissions (PermAdminAll)
			//   producer role → publish + read
			//   consumer role → consume + read
			//   readonly role → read only
			//
			if !key.HasPermission(perm) {
				m.logger.Warn("permission denied",
					"key_id", key.ID,
					"roles", key.Roles,
					"required_permission", perm,
					"path", r.URL.Path,
				)
				http.Error(w, `{"error": "forbidden", "message": "insufficient permissions"}`,
					http.StatusForbidden)
				return
			}

			// ================================================================
			// STEP 3: Permission granted, proceed to handler
			// ================================================================
			m.logger.Debug("permission granted",
				"key_id", key.ID,
				"permission", perm,
				"path", r.URL.Path,
			)
			next.ServeHTTP(w, r)
		})
	}
}

// RequireTopicAccess returns a middleware that checks ACL for topic operations.
// This is more granular than RequirePermission - it checks specific topic access.
//
// EXAMPLE:
//
//	r.With(sec.RequireTopicAccess(security.OpWrite)).Post("/publish", handler)
func (m *SecurityManager) RequireTopicAccess(op Operation) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, ok := r.Context().Value(APIKeyContextKey).(*APIKey)
			if !ok || key == nil {
				http.Error(w, `{"error": "unauthorized"}`, http.StatusUnauthorized)
				return
			}

			// Extract topic from URL (assumes chi router with {topicName} param)
			topicName := chi.URLParam(r, "topicName")
			if topicName == "" {
				topicName = chi.URLParam(r, "topic")
			}

			if !m.ACLs.CheckAccess(key, ResourceTypeTopic, topicName, op) {
				m.logger.Warn("topic access denied",
					"key_id", key.ID,
					"topic", topicName,
					"operation", op,
				)
				http.Error(w, `{"error": "forbidden", "message": "topic access denied"}`,
					http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
