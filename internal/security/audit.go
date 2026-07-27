// =============================================================================
// AUDIT LOGGING - SECURITY EVENT TRACKING FOR GOQUEUE
// =============================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ WHY AUDIT LOGGING?                                                          │
// │                                                                             │
// │ Security audit logs provide a tamper-evident record of WHO did WHAT and     │
// │ WHEN. This is required for:                                                 │
// │   - Compliance (SOC2, PCI-DSS, HIPAA)                                      │
// │   - Incident investigation (who accessed what?)                             │
// │   - Anomaly detection (unusual access patterns)                             │
// │   - Forensics (post-breach analysis)                                        │
// │                                                                             │
// │ WHAT WE LOG:                                                                │
// │   - Authentication: success/failure, key used, client IP                    │
// │   - Authorization: permission checks, ACL evaluations                      │
// │   - Key management: creation, revocation, listing                          │
// │   - ACL changes: rules added, removed                                      │
// │   - Admin operations: topic deletion, config changes                       │
// │                                                                             │
// │ COMPARISON:                                                                 │
// │   - Kafka: Authorizer logs via log4j, custom audit plugins                  │
// │   - RabbitMQ: Event exchange for auth events                                │
// │   - AWS SQS: CloudTrail integration                                         │
// │   - goqueue: Structured slog with dedicated audit logger                    │
// │                                                                             │
// │ FORMAT:                                                                     │
// │   All audit events are structured JSON via slog:                            │
// │   {                                                                         │
// │     "time": "2026-02-08T12:00:00Z",                                         │
// │     "level": "INFO",                                                        │
// │     "msg": "audit",                                                         │
// │     "event": "auth_success",                                                │
// │     "principal": "api-key-abc123",                                          │
// │     "client_ip": "10.0.0.5",                                               │
// │     "resource": "/topics/orders",                                           │
// │     "action": "GET"                                                         │
// │   }                                                                         │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package security

import (
	"log/slog"
	"net/http"
	"os"
	"time"
)

// =============================================================================
// AUDIT EVENT TYPES
// =============================================================================

// AuditEvent represents the type of security event being logged.
//
// WHY TYPED CONSTANTS?
//   - Prevents typos in event names
//   - Enables filtering and aggregation
//   - Makes it easy to add alerting rules
type AuditEvent string

const (
	// Authentication events
	AuditAuthSuccess AuditEvent = "auth_success"
	AuditAuthFailure AuditEvent = "auth_failure"
	AuditAuthExpired AuditEvent = "auth_expired"

	// Key management events
	AuditKeyCreated AuditEvent = "key_created"
	AuditKeyRevoked AuditEvent = "key_revoked"
	AuditKeyListed  AuditEvent = "key_listed"

	// ACL events
	AuditACLAdded   AuditEvent = "acl_added"
	AuditACLRemoved AuditEvent = "acl_removed"
	AuditACLDenied  AuditEvent = "acl_denied"

	// Resource events
	AuditTopicCreated AuditEvent = "topic_created"
	AuditTopicDeleted AuditEvent = "topic_deleted"

	// Admin events
	AuditConfigChanged  AuditEvent = "config_changed"
	AuditTenantCreated  AuditEvent = "tenant_created"
	AuditTenantSuspend  AuditEvent = "tenant_suspended"
	AuditTenantDeleted  AuditEvent = "tenant_deleted"
	AuditSchemaRegister AuditEvent = "schema_registered"
)

// =============================================================================
// AUDIT LOGGER
// =============================================================================

// AuditLogger provides structured security audit logging.
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ DESIGN DECISIONS                                                            │
// │                                                                             │
// │ 1. SEPARATE LOGGER: Audit events go to a dedicated slog.Logger.             │
// │    This allows routing audit logs to a separate destination (file, SIEM)    │
// │    without mixing with application logs.                                    │
// │                                                                             │
// │ 2. STRUCTURED FORMAT: All fields are typed key-value pairs.                 │
// │    This enables machine parsing, filtering, and aggregation.                │
// │                                                                             │
// │ 3. ALWAYS LOG: Audit events are always logged at INFO level.                │
// │    Unlike debug logs, audit events should never be suppressed.              │
// │                                                                             │
// │ 4. IMMUTABLE: Once written, audit logs should not be modifiable.            │
// │    In production, pipe to append-only storage or a SIEM system.             │
// └─────────────────────────────────────────────────────────────────────────────┘
type AuditLogger struct {
	logger  *slog.Logger
	enabled bool
}

// AuditConfig configures the audit logger.
type AuditConfig struct {
	// Enabled turns audit logging on/off.
	// Default: true (audit logs should always be enabled in production)
	Enabled bool

	// LogFile is the path to the audit log file.
	// If empty, audit events go to stderr (same as app logs).
	// In production, set to a dedicated file or pipe to SIEM.
	LogFile string
}

// DefaultAuditConfig returns sensible defaults for audit logging.
func DefaultAuditConfig() AuditConfig {
	return AuditConfig{
		Enabled: true,
		LogFile: "", // stderr by default
	}
}

// NewAuditLogger creates a new audit logger.
//
// HOW IT WORKS:
//
//	If LogFile is set, audit events go to that file (append-only).
//	Otherwise, they go to stderr alongside application logs but
//	are distinguishable by the "component":"audit" attribute.
func NewAuditLogger(config AuditConfig) *AuditLogger {
	if !config.Enabled {
		return &AuditLogger{enabled: false}
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}

	if config.LogFile != "" {
		// Open file in append-only mode (O_APPEND prevents overwriting)
		f, err := os.OpenFile(config.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			// Fall back to stderr if file can't be opened
			slog.Error("failed to open audit log file, falling back to stderr",
				"file", config.LogFile, "error", err)
			handler = slog.NewJSONHandler(os.Stderr, opts)
		} else {
			handler = slog.NewJSONHandler(f, opts)
		}
	} else {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	}

	return &AuditLogger{
		logger:  slog.New(handler).With("component", "audit"),
		enabled: true,
	}
}

// =============================================================================
// AUDIT EVENT LOGGING METHODS
// =============================================================================

// LogEvent logs a generic audit event with key-value attributes.
//
// USAGE:
//
//	audit.LogEvent(AuditAuthSuccess, "principal", "key-abc", "resource", "/topics")
func (a *AuditLogger) LogEvent(event AuditEvent, attrs ...any) {
	if !a.enabled {
		return
	}
	allAttrs := make([]any, 0, len(attrs)+2)
	allAttrs = append(allAttrs, "event", string(event))
	allAttrs = append(allAttrs, attrs...)
	a.logger.Info("audit", allAttrs...)
}

// LogAuthSuccess logs a successful authentication.
//
// FIELDS:
//   - principal: The API key ID or name that authenticated
//   - client_ip: Source IP address
//   - method: HTTP method
//   - path: Requested resource path
func (a *AuditLogger) LogAuthSuccess(principal, clientIP, method, path string) {
	if !a.enabled {
		return
	}
	a.logger.Info("audit",
		"event", string(AuditAuthSuccess),
		"principal", principal,
		"client_ip", clientIP,
		"method", method,
		"resource", path,
		"timestamp", time.Now().UTC().Format(time.RFC3339),
	)
}

// LogAuthFailure logs a failed authentication attempt.
//
// WHY LOG FAILURES?
//
//	Failed auth attempts are often more interesting than successes:
//	  - Brute force attacks (many failures from same IP)
//	  - Credential stuffing (valid key format but wrong key)
//	  - Configuration errors (client using wrong key)
func (a *AuditLogger) LogAuthFailure(clientIP, method, path, reason string) {
	if !a.enabled {
		return
	}
	a.logger.Warn("audit",
		"event", string(AuditAuthFailure),
		"client_ip", clientIP,
		"method", method,
		"resource", path,
		"reason", reason,
		"timestamp", time.Now().UTC().Format(time.RFC3339),
	)
}

// LogKeyEvent logs API key management events (create, revoke).
func (a *AuditLogger) LogKeyEvent(event AuditEvent, keyID, keyName, actor string) {
	if !a.enabled {
		return
	}
	a.logger.Info("audit",
		"event", string(event),
		"key_id", keyID,
		"key_name", keyName,
		"actor", actor,
		"timestamp", time.Now().UTC().Format(time.RFC3339),
	)
}

// LogACLEvent logs ACL modification events.
func (a *AuditLogger) LogACLEvent(event AuditEvent, principal, resource, operation, actor string) {
	if !a.enabled {
		return
	}
	a.logger.Info("audit",
		"event", string(event),
		"principal", principal,
		"resource", resource,
		"operation", operation,
		"actor", actor,
		"timestamp", time.Now().UTC().Format(time.RFC3339),
	)
}

// LogResourceEvent logs resource lifecycle events (topic create/delete, schema register, etc).
func (a *AuditLogger) LogResourceEvent(event AuditEvent, resource, actor, clientIP string) {
	if !a.enabled {
		return
	}
	a.logger.Info("audit",
		"event", string(event),
		"resource", resource,
		"actor", actor,
		"client_ip", clientIP,
		"timestamp", time.Now().UTC().Format(time.RFC3339),
	)
}

// =============================================================================
// AUDIT MIDDLEWARE
// =============================================================================

// AuditMiddleware creates an HTTP middleware that logs authentication events.
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ MIDDLEWARE POSITION IN CHAIN                                                │
// │                                                                             │
// │ Request ──► RequestID ──► ClientIP ──► Logger ──► Recoverer                │
// │         ──► BodyLimit ──► RateLimit ──► CORS ──► Auth ──► AUDIT            │
// │         ──► Handler                                                         │
// │                                                                             │
// │ The audit middleware runs AFTER auth so it can inspect the auth result.     │
// │ If auth middleware set an API key in context → log success.                 │
// │ If the response status is 401/403 → log failure.                           │
// └─────────────────────────────────────────────────────────────────────────────┘
func AuditMiddleware(audit *AuditLogger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip if audit logger is nil or disabled
			if audit == nil || !audit.enabled {
				next.ServeHTTP(w, r)
				return
			}

			// Skip audit for health/metrics endpoints (too noisy)
			path := r.URL.Path
			if path == "/health" || path == "/healthz" || path == "/readyz" ||
				path == "/livez" || path == "/metrics" || path == "/version" {
				next.ServeHTTP(w, r)
				return
			}

			// Check if auth middleware set a key in context
			apiKey := GetAPIKeyFromContext(r.Context())

			// Wrap response writer to capture status code
			wrapped := &auditResponseWriter{ResponseWriter: w, status: 200}
			next.ServeHTTP(wrapped, r)

			// Log based on response status.
			//
			// ClientIP is the trusted-proxy-aware resolution from
			// clientip.go. Do NOT read X-Forwarded-For here: it is a
			// caller-supplied header, and reading it directly is what let
			// anyone pick the IP recorded against their own auth failures.
			clientIP := ClientIP(r)

			switch {
			case wrapped.status == http.StatusUnauthorized:
				audit.LogAuthFailure(clientIP, r.Method, path, "invalid_or_missing_key")
			case wrapped.status == http.StatusForbidden:
				reason := "insufficient_permissions"
				if apiKey != nil {
					reason = "acl_denied:" + apiKey.Name
				}
				audit.LogAuthFailure(clientIP, r.Method, path, reason)
			case apiKey != nil:
				// Successful authenticated request
				audit.LogAuthSuccess(apiKey.Name, clientIP, r.Method, path)
			}
		})
	}
}

// auditResponseWriter captures the HTTP status code for audit logging.
type auditResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *auditResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
