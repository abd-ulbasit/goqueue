// =============================================================================
// API KEY AUTHENTICATION - SECURE ACCESS CONTROL FOR GOQUEUE
// =============================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ WHY API KEYS?                                                               │
// │                                                                             │
// │ API keys provide:                                                           │
// │   1. IDENTITY: Know which client/application is making requests             │
// │   2. AUTHORIZATION: Associate permissions with each key                     │
// │   3. AUDIT TRAIL: Track who did what                                        │
// │   4. REVOCATION: Disable access without changing code                       │
// │                                                                             │
// │ FLOW:                                                                       │
// │   Client ──[X-API-Key: abc123]──► GoQueue ──[validate]──► Grant/Deny       │
// │                                                                             │
// │ COMPARISON:                                                                 │
// │   - Kafka: SASL/PLAIN, SASL/SCRAM, Kerberos                                 │
// │   - RabbitMQ: Username/password, x509 certs                                 │
// │   - AWS SQS: IAM (AWS signatures)                                           │
// │   - goqueue: API keys (simple) + future OAuth2/OIDC support                 │
// │                                                                             │
// │ SECURITY CONSIDERATIONS:                                                    │
// │   - Keys should be 32+ bytes of cryptographic randomness                    │
// │   - Store hashed (bcrypt/argon2), not plaintext                             │
// │   - Transmit only over TLS                                                  │
// │   - Rotate periodically                                                     │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package security

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// ERRORS
// =============================================================================

var (
	// ErrNoAPIKey is returned when no API key is provided
	ErrNoAPIKey = errors.New("no API key provided")

	// ErrInvalidAPIKey is returned when the API key is invalid
	ErrInvalidAPIKey = errors.New("invalid API key")

	// ErrAPIKeyExpired is returned when the API key has expired
	ErrAPIKeyExpired = errors.New("API key has expired")

	// ErrAPIKeyRevoked is returned when the API key has been revoked
	ErrAPIKeyRevoked = errors.New("API key has been revoked")

	// ErrPermissionDenied is returned when the key lacks required permissions
	ErrPermissionDenied = errors.New("permission denied")
)

// =============================================================================
// API KEY STRUCTURE
// =============================================================================

// APIKey represents an API key with associated metadata and permissions.
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ API KEY ANATOMY                                                             │
// │                                                                             │
// │ Raw key (32 bytes hex):                                                     │
// │   gq_a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef            │
// │   ^^                                                                        │
// │   Prefix (gq_ = GoQueue)                                                    │
// │                                                                             │
// │ Storage format:                                                             │
// │   - Only store SHA-256 hash of key                                          │
// │   - Raw key shown ONCE at creation time                                     │
// │   - Cannot recover raw key from hash                                        │
// └─────────────────────────────────────────────────────────────────────────────┘
type APIKey struct {
	// ID is a unique identifier for the key (not the key itself)
	ID string `json:"id"`

	// Name is a human-readable name for the key
	Name string `json:"name"`

	// KeyHash is SHA-256 hash of the actual key (never store raw key)
	KeyHash string `json:"-"` // Never serialize

	// Prefix is the first 8 chars of the key for identification
	Prefix string `json:"prefix"`

	// Roles are the RBAC roles assigned to this key
	Roles []string `json:"roles"`

	// CreatedAt is when the key was created
	CreatedAt time.Time `json:"created_at"`

	// ExpiresAt is when the key expires (zero = never)
	ExpiresAt time.Time `json:"expires_at,omitempty"`

	// LastUsedAt is the last time the key was used
	LastUsedAt time.Time `json:"last_used_at,omitempty"`

	// Revoked indicates if the key has been revoked
	Revoked bool `json:"revoked"`

	// Metadata stores arbitrary key-value pairs
	Metadata map[string]string `json:"metadata,omitempty"`
}

// IsExpired checks if the key has expired.
func (k *APIKey) IsExpired() bool {
	if k.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(k.ExpiresAt)
}

// HasRole checks if the key has a specific role.
func (k *APIKey) HasRole(role string) bool {
	for _, r := range k.Roles {
		if r == role || r == RoleAdmin { // Admin has all roles
			return true
		}
	}
	return false
}

// =============================================================================
// ROLE-BASED ACCESS CONTROL (RBAC)
// =============================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ RBAC DESIGN                                                                 │
// │                                                                             │
// │ ROLES define what actions are allowed:                                      │
// │   - admin: Full access to everything                                        │
// │   - producer: Can publish messages to topics                                │
// │   - consumer: Can consume messages and manage consumer groups               │
// │   - readonly: Can only read (list topics, view stats)                       │
// │                                                                             │
// │ PERMISSIONS are granular actions:                                           │
// │   - topic:create, topic:delete, topic:list, topic:read                      │
// │   - message:publish, message:consume                                        │
// │   - group:join, group:leave, group:commit                                   │
// │   - admin:* (all admin operations)                                          │
// │                                                                             │
// │ ROLE → PERMISSIONS MAPPING:                                                 │
// │   admin    → *:* (all permissions)                                          │
// │   producer → topic:list, topic:read, message:publish                        │
// │   consumer → topic:list, topic:read, message:consume, group:*               │
// │   readonly → topic:list, topic:read, group:list                             │
// │                                                                             │
// │ TOPIC-LEVEL ACLs (future):                                                  │
// │   - Restrict access to specific topics                                      │
// │   - Producer can only publish to topics A, B                                │
// │   - Consumer can only read from topic C                                     │
// └─────────────────────────────────────────────────────────────────────────────┘

// Built-in roles
const (
	RoleAdmin    = "admin"
	RoleProducer = "producer"
	RoleConsumer = "consumer"
	RoleReadonly = "readonly"
)

// Permission represents a single permission in the system.
type Permission string

// Permission constants
const (
	// Topic permissions
	PermTopicCreate Permission = "topic:create"
	PermTopicDelete Permission = "topic:delete"
	PermTopicList   Permission = "topic:list"
	PermTopicRead   Permission = "topic:read"

	// Message permissions
	PermMessagePublish Permission = "message:publish"
	PermMessageConsume Permission = "message:consume"

	// Consumer group permissions
	PermGroupJoin   Permission = "group:join"
	PermGroupLeave  Permission = "group:leave"
	PermGroupCommit Permission = "group:commit"
	PermGroupList   Permission = "group:list"
	PermGroupRead   Permission = "group:read"
	PermGroupDelete Permission = "group:delete"

	// Admin permissions
	PermAdminAll Permission = "admin:*"
)

// RolePermissions maps roles to their permissions.
var RolePermissions = map[string][]Permission{
	RoleAdmin: {PermAdminAll},
	RoleProducer: {
		PermTopicList,
		PermTopicRead,
		PermMessagePublish,
	},
	RoleConsumer: {
		PermTopicList,
		PermTopicRead,
		PermMessageConsume,
		PermGroupJoin,
		PermGroupLeave,
		PermGroupCommit,
		PermGroupList,
		PermGroupRead,
	},
	RoleReadonly: {
		PermTopicList,
		PermTopicRead,
		PermGroupList,
		PermGroupRead,
	},
}

// HasPermission checks if a key has the required permission.
func (k *APIKey) HasPermission(perm Permission) bool {
	for _, role := range k.Roles {
		perms, ok := RolePermissions[role]
		if !ok {
			continue
		}
		for _, p := range perms {
			if p == PermAdminAll || p == perm {
				return true
			}
		}
	}
	return false
}

// =============================================================================
// API KEY MANAGER
// =============================================================================

// APIKeyManager handles API key storage, validation, and management.
type APIKeyManager struct {
	// keys stores API keys by their hash
	keys map[string]*APIKey

	// keysByID stores API keys by ID for management
	keysByID map[string]*APIKey

	mu sync.RWMutex

	// config holds manager configuration
	config APIKeyManagerConfig

	logger *slog.Logger
}

// APIKeyManagerConfig holds configuration for the key manager.
type APIKeyManagerConfig struct {
	// Enabled turns on API key authentication
	Enabled bool

	// KeyLength is the length of generated keys in bytes
	KeyLength int

	// DefaultExpiry is the default expiry for new keys (0 = no expiry)
	DefaultExpiry time.Duration

	// AllowNoAuth allows unauthenticated access to health endpoints
	AllowNoAuth bool

	// RootKey is an optional root/admin key loaded from environment
	RootKey string
}

// DefaultAPIKeyManagerConfig returns sensible defaults.
func DefaultAPIKeyManagerConfig() APIKeyManagerConfig {
	return APIKeyManagerConfig{
		Enabled:     false,
		KeyLength:   32, // 256 bits
		AllowNoAuth: true,
		RootKey:     os.Getenv("GOQUEUE_API_ROOT_KEY"),
	}
}

// NewAPIKeyManager creates a new API key manager.
func NewAPIKeyManager(config APIKeyManagerConfig) *APIKeyManager {
	m := &APIKeyManager{
		keys:     make(map[string]*APIKey),
		keysByID: make(map[string]*APIKey),
		config:   config,
		logger:   slog.Default(),
	}

	// Create root key from environment if provided
	if config.RootKey != "" {
		rootKey := &APIKey{
			ID:        "root",
			Name:      "Root Admin Key",
			KeyHash:   hashKey(config.RootKey),
			Prefix:    config.RootKey[:min(8, len(config.RootKey))],
			Roles:     []string{RoleAdmin},
			CreatedAt: time.Now(),
		}
		m.keys[rootKey.KeyHash] = rootKey
		m.keysByID[rootKey.ID] = rootKey
		m.logger.Info("loaded root API key from environment")
	}

	return m
}

// GenerateKey creates a new API key with the given name and roles.
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ KEY GENERATION                                                              │
// │                                                                             │
// │ 1. Generate 32 bytes of cryptographic randomness                            │
// │ 2. Encode as hex with "gq_" prefix                                          │
// │ 3. Store ONLY the SHA-256 hash                                              │
// │ 4. Return raw key to caller (shown once)                                    │
// │                                                                             │
// │ OUTPUT FORMAT:                                                              │
// │   gq_a1b2c3d4e5f67890... (72 chars total: 3 prefix + 64 hex + 5 checksum)  │
// └─────────────────────────────────────────────────────────────────────────────┘
func (m *APIKeyManager) GenerateKey(name string, roles []string, expiry time.Duration) (rawKey string, key *APIKey, err error) {
	// Generate random bytes
	keyBytes := make([]byte, m.config.KeyLength)
	if _, err := rand.Read(keyBytes); err != nil {
		return "", nil, fmt.Errorf("failed to generate random key: %w", err)
	}

	// Create key string with prefix
	rawKey = "gq_" + hex.EncodeToString(keyBytes)

	// Create API key record
	key = &APIKey{
		ID:        generateID(),
		Name:      name,
		KeyHash:   hashKey(rawKey),
		Prefix:    rawKey[:11], // "gq_" + 8 chars
		Roles:     roles,
		CreatedAt: time.Now(),
		Metadata:  make(map[string]string),
	}

	if expiry > 0 {
		key.ExpiresAt = time.Now().Add(expiry)
	} else if m.config.DefaultExpiry > 0 {
		key.ExpiresAt = time.Now().Add(m.config.DefaultExpiry)
	}

	// Store the key
	m.mu.Lock()
	m.keys[key.KeyHash] = key
	m.keysByID[key.ID] = key
	m.mu.Unlock()

	m.logger.Info("generated new API key",
		"id", key.ID,
		"name", name,
		"roles", roles,
		"expires_at", key.ExpiresAt,
	)

	return rawKey, key, nil
}

// ValidateKey validates an API key and returns the associated APIKey record.
func (m *APIKeyManager) ValidateKey(rawKey string) (*APIKey, error) {
	if rawKey == "" {
		return nil, ErrNoAPIKey
	}

	hash := hashKey(rawKey)

	m.mu.RLock()
	key, exists := m.keys[hash]
	m.mu.RUnlock()

	if !exists {
		return nil, ErrInvalidAPIKey
	}

	if key.Revoked {
		return nil, ErrAPIKeyRevoked
	}

	if key.IsExpired() {
		return nil, ErrAPIKeyExpired
	}

	// Update last used time
	m.mu.Lock()
	key.LastUsedAt = time.Now()
	m.mu.Unlock()

	return key, nil
}

// RevokeKey revokes an API key by ID.
func (m *APIKeyManager) RevokeKey(keyID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key, exists := m.keysByID[keyID]
	if !exists {
		return fmt.Errorf("key not found: %s", keyID)
	}

	key.Revoked = true
	m.logger.Info("revoked API key", "id", keyID, "name", key.Name)

	return nil
}

// ListKeys returns all API keys (without revealing the actual keys).
func (m *APIKeyManager) ListKeys() []*APIKey {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]*APIKey, 0, len(m.keysByID))
	for _, key := range m.keysByID {
		keys = append(keys, key)
	}
	return keys
}

// GetKey returns an API key by ID.
func (m *APIKeyManager) GetKey(keyID string) (*APIKey, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key, exists := m.keysByID[keyID]
	if !exists {
		return nil, fmt.Errorf("key not found: %s", keyID)
	}
	return key, nil
}

// =============================================================================
// HTTP MIDDLEWARE
// =============================================================================

// contextKey is a private type for context keys
type contextKey string

const (
	// APIKeyContextKey is the context key for the authenticated API key
	APIKeyContextKey contextKey = "api_key"
)

// GetAPIKeyFromContext retrieves the API key from the request context.
func GetAPIKeyFromContext(ctx context.Context) *APIKey {
	if key, ok := ctx.Value(APIKeyContextKey).(*APIKey); ok {
		return key
	}
	return nil
}

// AuthMiddleware returns HTTP middleware that validates API keys.
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ API KEY EXTRACTION ORDER                                                    │
// │                                                                             │
// │ 1. Authorization: Bearer <key>  (preferred)                                 │
// │ 2. X-API-Key: <key>             (common alternative)                        │
// │ 3. ?api_key=<key>               (query param, less secure)                  │
// │                                                                             │
// │ SECURITY NOTE:                                                              │
// │   Query params may be logged in access logs, prefer headers.                │
// └─────────────────────────────────────────────────────────────────────────────┘
func (m *APIKeyManager) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth if disabled
		if !m.config.Enabled {
			next.ServeHTTP(w, r)
			return
		}

		// Skip auth for health endpoints
		if m.config.AllowNoAuth && isHealthEndpoint(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		// Extract API key
		apiKey := extractAPIKey(r)

		// Validate key
		key, err := m.ValidateKey(apiKey)
		if err != nil {
			m.logger.Warn("authentication failed",
				"path", r.URL.Path,
				"method", r.Method,
				"error", err,
				"remote_addr", r.RemoteAddr,
			)

			status := http.StatusUnauthorized
			if errors.Is(err, ErrPermissionDenied) {
				status = http.StatusForbidden
			}

			http.Error(w, err.Error(), status)
			return
		}

		// Add key to context
		ctx := context.WithValue(r.Context(), APIKeyContextKey, key)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequirePermission returns middleware that checks for a specific permission.
func (m *APIKeyManager) RequirePermission(perm Permission) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := GetAPIKeyFromContext(r.Context())
			if key == nil {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			if !key.HasPermission(perm) {
				m.logger.Warn("permission denied",
					"key_id", key.ID,
					"permission", perm,
					"path", r.URL.Path,
				)
				http.Error(w, "permission denied", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// hashKey creates a SHA-256 hash of the API key.
func hashKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

// generateID creates a unique ID for API keys.
func generateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("key_%s", hex.EncodeToString(b))
}

// extractAPIKey extracts the API key from the request.
func extractAPIKey(r *http.Request) string {
	// Try Authorization header first
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}

	// Try X-API-Key header
	if key := r.Header.Get("X-API-Key"); key != "" {
		return key
	}

	// Try query parameter (least preferred)
	return r.URL.Query().Get("api_key")
}

// isHealthEndpoint checks if the path is a health/readiness endpoint.
func isHealthEndpoint(path string) bool {
	healthPaths := []string{"/health", "/healthz", "/readyz", "/livez", "/metrics", "/version"}
	for _, hp := range healthPaths {
		if path == hp {
			return true
		}
	}
	return false
}

// SecureCompare performs constant-time string comparison to prevent timing attacks.
func SecureCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// min returns the smaller of two ints.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// LoadAPIKeyConfigFromEnv creates API key config from environment variables.
//
// Environment variables:
//
//	GOQUEUE_AUTH_ENABLED=true
//	GOQUEUE_API_ROOT_KEY=gq_xxxx... (root admin key)
//	GOQUEUE_AUTH_ALLOW_HEALTH=true (allow unauthenticated health checks)
func LoadAPIKeyConfigFromEnv() APIKeyManagerConfig {
	config := DefaultAPIKeyManagerConfig()

	if os.Getenv("GOQUEUE_AUTH_ENABLED") == "true" {
		config.Enabled = true
	}

	if rootKey := os.Getenv("GOQUEUE_API_ROOT_KEY"); rootKey != "" {
		config.RootKey = rootKey
	}

	if os.Getenv("GOQUEUE_AUTH_ALLOW_HEALTH") == "false" {
		config.AllowNoAuth = false
	}

	return config
}
