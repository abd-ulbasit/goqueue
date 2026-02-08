package security

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// =============================================================================
// AUDIT LOGGER TESTS (M27)
// =============================================================================

func TestNewAuditLogger_Disabled(t *testing.T) {
	logger := NewAuditLogger(AuditConfig{Enabled: false})
	if logger == nil {
		t.Fatal("NewAuditLogger should return non-nil even when disabled")
	}

	// Should not panic when logging events on a disabled logger
	logger.LogEvent(AuditAuthSuccess, "test", "value")
	logger.LogAuthSuccess("user", "127.0.0.1", "GET", "/health")
	logger.LogAuthFailure("127.0.0.1", "GET", "/api", "bad key")
	logger.LogKeyEvent(AuditKeyCreated, "key1", "test-key", "admin")
	logger.LogACLEvent(AuditACLAdded, "user", "topic-orders", "read", "admin")
	logger.LogResourceEvent(AuditTopicCreated, "orders", "admin", "127.0.0.1")
}

func TestNewAuditLogger_ToFile(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "audit.log")

	logger := NewAuditLogger(AuditConfig{
		Enabled: true,
		LogFile: logFile,
	})

	// Log some events
	logger.LogAuthSuccess("admin-key", "192.168.1.1", "POST", "/topics/orders")
	logger.LogAuthFailure("10.0.0.1", "GET", "/messages", "invalid API key")
	logger.LogResourceEvent(AuditTopicCreated, "orders", "admin", "192.168.1.1")

	// Read and verify file content
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read audit log: %v", err)
	}

	content := string(data)
	if len(content) == 0 {
		t.Fatal("audit log file should not be empty")
	}

	// Each line should be valid JSON
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	if len(lines) < 3 {
		t.Fatalf("expected at least 3 log lines, got %d", len(lines))
	}

	for i, line := range lines {
		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Errorf("line %d is not valid JSON: %v", i, err)
			continue
		}
		// All audit events should have "component":"audit"
		if comp, ok := entry["component"]; ok {
			if comp != "audit" {
				t.Errorf("line %d: expected component=audit, got %v", i, comp)
			}
		}
	}
}

func TestNewAuditLogger_Stderr(t *testing.T) {
	// When no LogFile is specified, logger should still work (writes to stderr)
	logger := NewAuditLogger(AuditConfig{Enabled: true})
	if logger == nil {
		t.Fatal("NewAuditLogger should return non-nil")
	}
	// Should not panic
	logger.LogEvent(AuditAuthSuccess, "test", true)
}

func TestAuditLogger_LogEvent(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "events.log")

	logger := NewAuditLogger(AuditConfig{
		Enabled: true,
		LogFile: logFile,
	})

	logger.LogEvent(AuditTopicDeleted, "topic", "important-topic", "actor", "admin")

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	var entry map[string]interface{}
	if err := json.Unmarshal(bytes.TrimSpace(data), &entry); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	// Verify event field
	if entry["event"] != string(AuditTopicDeleted) {
		t.Errorf("expected event=%s, got %v", AuditTopicDeleted, entry["event"])
	}
}

func TestAuditMiddleware_CapturesRequestMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "middleware.log")

	auditLogger := NewAuditLogger(AuditConfig{
		Enabled: true,
		LogFile: logFile,
	})

	// Create a handler that returns 401 (triggers auth failure audit log)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("unauthorized"))
	})

	// Wrap with audit middleware
	mw := AuditMiddleware(auditLogger)
	wrapped := mw(handler)

	// Make a test request
	req := httptest.NewRequest("POST", "/v1/topics/orders/messages", nil)
	req.RemoteAddr = "192.168.1.100:12345"
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	// Verify response was passed through
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", w.Code)
	}

	// Read audit log
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("audit log should have captured the auth failure request")
	}

	var entry map[string]interface{}
	if err := json.Unmarshal(bytes.TrimSpace(data), &entry); err != nil {
		t.Fatalf("invalid JSON: %v\ndata: %s", err, string(data))
	}

	// Verify captured fields
	if entry["method"] != "POST" {
		t.Errorf("expected method=POST, got %v", entry["method"])
	}
	if entry["resource"] != "/v1/topics/orders/messages" {
		t.Errorf("expected resource=/v1/topics/orders/messages, got %v", entry["resource"])
	}
}

func TestAuditMiddleware_SkipsHealthEndpoints(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "skip.log")

	auditLogger := NewAuditLogger(AuditConfig{
		Enabled: true,
		LogFile: logFile,
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mw := AuditMiddleware(auditLogger)
	wrapped := mw(handler)

	// Health endpoint should be skipped
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	data, _ := os.ReadFile(logFile)
	if len(data) > 0 {
		t.Error("health endpoint should not produce audit log entries")
	}
}

func TestAuditMiddleware_NilLogger(t *testing.T) {
	mw := AuditMiddleware(nil)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrapped := mw(handler)

	req := httptest.NewRequest("GET", "/topics", nil)
	w := httptest.NewRecorder()

	// Should not panic
	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestDefaultAuditConfig(t *testing.T) {
	config := DefaultAuditConfig()
	if !config.Enabled {
		t.Error("default audit config should be enabled")
	}
	if config.LogFile != "" {
		t.Error("default audit config should have empty LogFile (stderr)")
	}
}

// Verify event constants exist (compile-time check)
var _ = []AuditEvent{
	AuditAuthSuccess,
	AuditAuthFailure,
	AuditKeyCreated,
	AuditKeyRevoked,
	AuditACLAdded,
	AuditACLRemoved,
	AuditTopicCreated,
	AuditTopicDeleted,
	AuditConfigChanged,
	AuditTenantCreated,
}
