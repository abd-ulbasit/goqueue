// =============================================================================
// TENANT AND QUOTA TESTS
// =============================================================================

package broker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// =============================================================================
// TENANT ID VALIDATION TESTS
// =============================================================================

func TestValidateTenantID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		// Valid IDs
		{"simple", "acme", false},
		{"with-hyphens", "acme-corp", false},
		{"with-numbers", "company123", false},
		{"min-length", "abc", false},
		{"max-length", "abcdefghij0123456789abcdefghij0123456789abcdefghij0123456789abcd", false}, // 64 chars

		// Invalid IDs
		{"empty", "", true},
		{"too-short", "ab", true},
		{"too-long", "abcdefghij0123456789abcdefghij0123456789abcdefghij0123456789abcde", true}, // 65 chars
		{"uppercase", "ACME", true},
		{"mixed-case", "AcMe", true},
		{"underscore", "acme_corp", true},
		{"starts-with-number", "123acme", true},
		{"starts-with-hyphen", "-acme", true},
		{"special-chars", "acme@corp", true},
		{"spaces", "acme corp", true},
		{"reserved-prefix", "__internal", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTenantID(tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTenantID(%q) error = %v, wantErr %v", tt.id, err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// TOPIC NAME HELPERS TESTS
// =============================================================================

func TestQualifyTopicName(t *testing.T) {
	tests := []struct {
		tenantID  string
		topicName string
		want      string
	}{
		{"acme", "orders", "acme.orders"},
		{"globex", "events", "globex.events"},
		{DefaultTenantID, "__consumer_offsets", "__consumer_offsets"}, // System topics unchanged
	}

	for _, tt := range tests {
		t.Run(tt.tenantID+"."+tt.topicName, func(t *testing.T) {
			got := QualifyTopicName(tt.tenantID, tt.topicName)
			if got != tt.want {
				t.Errorf("QualifyTopicName(%q, %q) = %q, want %q",
					tt.tenantID, tt.topicName, got, tt.want)
			}
		})
	}
}

func TestParseQualifiedTopicName(t *testing.T) {
	tests := []struct {
		qualified  string
		wantTenant string
		wantTopic  string
		wantErr    bool
	}{
		{"acme.orders", "acme", "orders", false},
		{"globex.events", "globex", "events", false},
		{"company.nested.topic", "company", "nested.topic", false},
		{"__consumer_offsets", DefaultTenantID, "__consumer_offsets", false},
		{"__internal_topic", DefaultTenantID, "__internal_topic", false},
		{"no-dot", "", "", true}, // Invalid format
	}

	for _, tt := range tests {
		t.Run(tt.qualified, func(t *testing.T) {
			tenant, topic, err := ParseQualifiedTopicName(tt.qualified)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQualifiedTopicName(%q) error = %v, wantErr %v",
					tt.qualified, err, tt.wantErr)
				return
			}
			if tenant != tt.wantTenant || topic != tt.wantTopic {
				t.Errorf("ParseQualifiedTopicName(%q) = (%q, %q), want (%q, %q)",
					tt.qualified, tenant, topic, tt.wantTenant, tt.wantTopic)
			}
		})
	}
}

func TestIsTenantTopic(t *testing.T) {
	tests := []struct {
		tenantID   string
		topicName  string
		wantResult bool
	}{
		{"acme", "acme.orders", true},
		{"acme", "globex.orders", false},
		{"acme", "orders", false}, // Not qualified
		{DefaultTenantID, "__consumer_offsets", true},
	}

	for _, tt := range tests {
		t.Run(tt.tenantID+":"+tt.topicName, func(t *testing.T) {
			got := IsTenantTopic(tt.tenantID, tt.topicName)
			if got != tt.wantResult {
				t.Errorf("IsTenantTopic(%q, %q) = %v, want %v",
					tt.tenantID, tt.topicName, got, tt.wantResult)
			}
		})
	}
}

// =============================================================================
// TOKEN BUCKET TESTS
// =============================================================================

func TestTokenBucket_Allow(t *testing.T) {
	// Create bucket with 10 tokens/sec, capacity 10
	bucket := NewTokenBucket(10, 10)

	// Should have full capacity initially
	if tokens := bucket.AvailableTokens(); tokens != 10 {
		t.Errorf("Initial tokens = %v, want 10", tokens)
	}

	// Consume 5 tokens
	if !bucket.Allow(5) {
		t.Error("Allow(5) should succeed when 10 tokens available")
	}

	// Token count should be approximately 5 (may have refilled slightly)
	if tokens := bucket.AvailableTokens(); tokens < 4.9 || tokens > 5.1 {
		t.Errorf("After consuming 5, tokens = %.8f, want ~5", tokens)
	}

	// Consume remaining 5 (approximately)
	if !bucket.Allow(5) {
		t.Error("Allow(5) should succeed when ~5 tokens available")
	}

	// Should fail now (0 or nearly 0 tokens)
	if bucket.Allow(1) {
		t.Error("Allow(1) should fail when nearly 0 tokens available")
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	// Create bucket with 100 tokens/sec
	bucket := NewTokenBucket(100, 100)

	// Consume all tokens
	bucket.Allow(100)

	// Wait for some refill
	time.Sleep(50 * time.Millisecond)

	// Should have ~5 tokens (100/sec * 0.05sec)
	tokens := bucket.AvailableTokens()
	if tokens < 3 || tokens > 7 {
		t.Errorf("After 50ms refill, tokens = %v, want ~5", tokens)
	}
}

func TestTokenBucket_AllowWithWait(t *testing.T) {
	bucket := NewTokenBucket(10, 10)

	// Consume all
	bucket.Allow(10)

	// Should indicate wait time
	allowed, waitTime := bucket.AllowWithWait(5)
	if allowed {
		t.Error("AllowWithWait should return false when no tokens")
	}
	if waitTime <= 0 {
		t.Error("AllowWithWait should return positive wait time")
	}

	// Wait time should be ~0.5 seconds for 5 tokens at 10/sec
	expectedWait := 500 * time.Millisecond
	tolerance := 100 * time.Millisecond
	if waitTime < expectedWait-tolerance || waitTime > expectedWait+tolerance {
		t.Errorf("Wait time = %v, want ~%v", waitTime, expectedWait)
	}
}

func TestTokenBucket_UpdateRate(t *testing.T) {
	bucket := NewTokenBucket(10, 10)

	// Consume half
	bucket.Allow(5)

	// Update to higher rate and capacity
	bucket.UpdateRate(100, 100)

	// Should keep existing tokens but cap at new capacity
	tokens := bucket.AvailableTokens()
	if tokens < 4 || tokens > 6 {
		t.Errorf("After UpdateRate, tokens = %v, want ~5", tokens)
	}

	// Update to lower capacity than current tokens
	bucket.UpdateRate(10, 3)
	tokens = bucket.AvailableTokens()
	if tokens != 3 {
		t.Errorf("After UpdateRate to lower capacity, tokens = %v, want 3", tokens)
	}
}

// =============================================================================
// QUOTA MANAGER TESTS
// =============================================================================

func TestQuotaManager_PublishRateCheck(t *testing.T) {
	qm := NewQuotaManager()

	// Initialize tenant with 10 msg/sec limit
	quotas := TenantQuotas{
		PublishRateLimit: 10,
	}
	qm.InitializeTenant("acme", quotas)

	// First 10 should succeed
	for i := 0; i < 10; i++ {
		result := qm.CheckPublishRate("acme", 1)
		if !result.Allowed {
			t.Errorf("Publish %d should be allowed", i+1)
		}
	}

	// 11th should fail
	result := qm.CheckPublishRate("acme", 1)
	if result.Allowed {
		t.Error("Publish 11 should be rejected")
	}
	if result.WaitTime <= 0 {
		t.Error("Should provide wait time hint")
	}
}

func TestQuotaManager_MessageSizeCheck(t *testing.T) {
	qm := NewQuotaManager()

	quotas := TenantQuotas{
		MaxMessageSizeBytes: 1024, // 1KB limit
	}
	qm.InitializeTenant("acme", quotas)

	// Under limit
	result := qm.CheckMessageSize("acme", 512)
	if !result.Allowed {
		t.Error("512 byte message should be allowed")
	}

	// At limit
	result = qm.CheckMessageSize("acme", 1024)
	if !result.Allowed {
		t.Error("1024 byte message should be allowed")
	}

	// Over limit
	result = qm.CheckMessageSize("acme", 1025)
	if result.Allowed {
		t.Error("1025 byte message should be rejected")
	}
}

func TestQuotaManager_TopicCountCheck(t *testing.T) {
	qm := NewQuotaManager()

	quotas := TenantQuotas{
		MaxTopics: 5,
	}
	qm.InitializeTenant("acme", quotas)

	// Can create up to 5
	result := qm.CheckTopicCount("acme", 4, 1) // Current 4, adding 1
	if !result.Allowed {
		t.Error("Should allow topic creation when under limit")
	}

	// Cannot exceed 5
	result = qm.CheckTopicCount("acme", 5, 1) // Current 5, adding 1
	if result.Allowed {
		t.Error("Should reject topic creation when at limit")
	}
}

func TestQuotaManager_DisabledEnforcement(t *testing.T) {
	qm := NewQuotaManager()

	quotas := TenantQuotas{
		PublishRateLimit: 1, // Very low limit
	}
	qm.InitializeTenant("acme", quotas)

	// Consume the token
	qm.CheckPublishRate("acme", 1)

	// Should fail with enforcement enabled
	result := qm.CheckPublishRate("acme", 1)
	if result.Allowed {
		t.Error("Should reject when quota exceeded")
	}

	// Disable enforcement
	qm.SetEnabled(false)

	// Should succeed with enforcement disabled
	result = qm.CheckPublishRate("acme", 1)
	if !result.Allowed {
		t.Error("Should allow when enforcement disabled")
	}
}

func TestQuotaManager_CompositeChecks(t *testing.T) {
	qm := NewQuotaManager()

	quotas := TenantQuotas{
		PublishRateLimit:      100,
		PublishBytesRateLimit: 1000,
		MaxMessageSizeBytes:   500,
	}
	qm.InitializeTenant("acme", quotas)

	// Valid message
	result := qm.CheckPublish("acme", 100)
	if !result.Allowed {
		t.Error("Valid publish should be allowed")
	}

	// Message too large
	result = qm.CheckPublish("acme", 600)
	if result.Allowed {
		t.Error("Oversized message should be rejected")
	}
	if result.QuotaType != QuotaTypeMessageSize {
		t.Errorf("Expected MessageSize quota type, got %v", result.QuotaType)
	}
}

func TestQuotaManager_UnknownTenant(t *testing.T) {
	qm := NewQuotaManager()

	// Unknown tenant should be allowed (no quotas configured)
	result := qm.CheckPublishRate("unknown", 1000)
	if !result.Allowed {
		t.Error("Unknown tenant should be allowed (no quotas)")
	}
}

// =============================================================================
// TENANT MANAGER TESTS
// =============================================================================

func TestTenantManager_CreateTenant(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "tenant-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()

	// Create tenant
	tenant, err := tm.CreateTenant("acme", "Acme Corp", nil, map[string]string{"plan": "pro"})
	if err != nil {
		t.Fatal(err)
	}

	if tenant.ID != "acme" {
		t.Errorf("ID = %q, want 'acme'", tenant.ID)
	}
	if tenant.Name != "Acme Corp" {
		t.Errorf("Name = %q, want 'Acme Corp'", tenant.Name)
	}
	if tenant.Status != TenantStatusActive {
		t.Errorf("Status = %v, want Active", tenant.Status)
	}
	if tenant.Metadata["plan"] != "pro" {
		t.Errorf("Metadata[plan] = %q, want 'pro'", tenant.Metadata["plan"])
	}
}

func TestTenantManager_CreateDuplicateTenant(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()

	// Create first
	_, err = tm.CreateTenant("acme", "Acme Corp", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Try duplicate
	_, err = tm.CreateTenant("acme", "Acme 2", nil, nil)
	if err == nil {
		t.Error("Should fail to create duplicate tenant")
	}
}

func TestTenantManager_GetTenant(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()

	// Create
	_, err = tm.CreateTenant("acme", "Acme Corp", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Get
	tenant, err := tm.GetTenant("acme")
	if err != nil {
		t.Fatal(err)
	}
	if tenant.ID != "acme" {
		t.Errorf("ID = %q, want 'acme'", tenant.ID)
	}

	// Get non-existent
	_, err = tm.GetTenant("nonexistent")
	if err == nil {
		t.Error("Should fail for non-existent tenant")
	}
}

func TestTenantManager_UpdateQuotas(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()

	// Create with default quotas
	_, err = tm.CreateTenant("acme", "Acme Corp", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Update quotas
	newQuotas := TenantQuotas{
		PublishRateLimit: 50000,
		MaxStorageBytes:  1099511627776, // 1TB
	}
	tenant, err := tm.UpdateTenantQuotas("acme", newQuotas)
	if err != nil {
		t.Fatal(err)
	}

	if tenant.Quotas.PublishRateLimit != 50000 {
		t.Errorf("PublishRateLimit = %d, want 50000", tenant.Quotas.PublishRateLimit)
	}
	if tenant.Quotas.MaxStorageBytes != 1099511627776 {
		t.Errorf("MaxStorageBytes = %d, want 1TB", tenant.Quotas.MaxStorageBytes)
	}
}

func TestTenantManager_SetStatus(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()

	// Create
	_, err = tm.CreateTenant("acme", "Acme Corp", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Suspend
	tenant, err := tm.SetTenantStatus("acme", TenantStatusSuspended, "Quota exceeded")
	if err != nil {
		t.Fatal(err)
	}
	if tenant.Status != TenantStatusSuspended {
		t.Errorf("Status = %v, want Suspended", tenant.Status)
	}
	if tenant.SuspendReason != "Quota exceeded" {
		t.Errorf("SuspendReason = %q, want 'Quota exceeded'", tenant.SuspendReason)
	}
	if tenant.SuspendedAt == nil {
		t.Error("SuspendedAt should be set")
	}

	// Reactivate
	tenant, err = tm.SetTenantStatus("acme", TenantStatusActive, "")
	if err != nil {
		t.Fatal(err)
	}
	if tenant.Status != TenantStatusActive {
		t.Errorf("Status = %v, want Active", tenant.Status)
	}
	if tenant.SuspendedAt != nil {
		t.Error("SuspendedAt should be cleared")
	}
}

func TestTenantManager_DeleteTenant(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()

	// Create
	_, err = tm.CreateTenant("acme", "Acme Corp", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Delete
	err = tm.DeleteTenant("acme")
	if err != nil {
		t.Fatal(err)
	}

	// Verify gone
	_, err = tm.GetTenant("acme")
	if err == nil {
		t.Error("Tenant should be deleted")
	}
}

func TestTenantManager_SystemTenantProtection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()

	// Cannot delete system tenant
	err = tm.DeleteTenant(DefaultTenantID)
	if !errors.Is(err, ErrSystemTenant) {
		t.Errorf("Delete system tenant should return ErrSystemTenant, got %v", err)
	}

	// Cannot update system tenant
	_, err = tm.UpdateTenant(DefaultTenantID, nil, nil)
	if !errors.Is(err, ErrSystemTenant) {
		t.Errorf("Update system tenant should return ErrSystemTenant, got %v", err)
	}

	// Cannot change system tenant status
	_, err = tm.SetTenantStatus(DefaultTenantID, TenantStatusDisabled, "test")
	if !errors.Is(err, ErrSystemTenant) {
		t.Errorf("SetStatus system tenant should return ErrSystemTenant, got %v", err)
	}
}

func TestTenantManager_Persistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)

	// Create and close first manager
	tm1, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tm1.CreateTenant("acme", "Acme Corp", nil, map[string]string{"key": "value"})
	if err != nil {
		t.Fatal(err)
	}

	tm1.Close()

	// Create second manager, should load from disk
	tm2, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm2.Close()

	tenant, err := tm2.GetTenant("acme")
	if err != nil {
		t.Fatal("Tenant should be loaded from disk")
	}
	if tenant.Name != "Acme Corp" {
		t.Errorf("Name = %q, want 'Acme Corp'", tenant.Name)
	}
	if tenant.Metadata["key"] != "value" {
		t.Errorf("Metadata not persisted correctly")
	}
}

func TestTenantManager_ListTenants(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()

	// Create multiple tenants
	tm.CreateTenant("acme", "Acme Corp", nil, nil)
	tm.CreateTenant("globex", "Globex Inc", nil, nil)
	tm.CreateTenant("initech", "Initech LLC", nil, nil)

	tenants := tm.ListTenants()

	// Should have 4 (3 created + system)
	if len(tenants) != 4 {
		t.Errorf("ListTenants() returned %d tenants, want 4", len(tenants))
	}
}

func TestTenantManager_UsageTracking(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()

	tm.CreateTenant("acme", "Acme Corp", nil, nil)

	// Increment usage
	tm.IncrementUsage("acme", 100, 10000, 50, 5000)
	tm.UpdateStorageUsage("acme", 1000000, 5, 15, 10000)

	usage, err := tm.GetUsage("acme")
	if err != nil {
		t.Fatal(err)
	}

	if usage.TotalMessagesPublished != 100 {
		t.Errorf("TotalMessagesPublished = %d, want 100", usage.TotalMessagesPublished)
	}
	if usage.TotalBytesPublished != 10000 {
		t.Errorf("TotalBytesPublished = %d, want 10000", usage.TotalBytesPublished)
	}
	if usage.StorageBytes != 1000000 {
		t.Errorf("StorageBytes = %d, want 1000000", usage.StorageBytes)
	}
	if usage.TopicCount != 5 {
		t.Errorf("TopicCount = %d, want 5", usage.TopicCount)
	}
}

func TestTenantManager_GetStats(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()

	quotas := TenantQuotas{
		MaxStorageBytes: 1000000,
		MaxTopics:       100,
	}
	tm.CreateTenant("acme", "Acme Corp", &quotas, nil)
	tm.UpdateStorageUsage("acme", 500000, 50, 100, 5000)

	stats, err := tm.GetTenantStats("acme")
	if err != nil {
		t.Fatal(err)
	}

	if stats.StorageUsagePercent != 50.0 {
		t.Errorf("StorageUsagePercent = %v, want 50.0", stats.StorageUsagePercent)
	}
	if stats.TopicUsagePercent != 50.0 {
		t.Errorf("TopicUsagePercent = %v, want 50.0", stats.TopicUsagePercent)
	}
}

// =============================================================================
// QUOTA VIOLATION STATS TESTS
// =============================================================================

func TestQuotaViolationStats(t *testing.T) {
	qm := NewQuotaManager()

	quotas := TenantQuotas{
		PublishRateLimit: 1,
	}
	qm.InitializeTenant("acme", quotas)

	// Consume quota
	qm.CheckPublishRate("acme", 1)

	// Trigger violations
	qm.CheckPublishRate("acme", 1)
	qm.CheckPublishRate("acme", 1)
	qm.CheckPublishRate("acme", 1)

	stats, err := qm.GetQuotaStats("acme")
	if err != nil {
		t.Fatal(err)
	}

	if stats.PublishRateViolations != 3 {
		t.Errorf("PublishRateViolations = %d, want 3", stats.PublishRateViolations)
	}
	if stats.LastViolation == nil {
		t.Error("LastViolation should be set")
	}

	// Reset violations
	qm.ResetViolationStats("acme")
	stats, _ = qm.GetQuotaStats("acme")
	if stats.PublishRateViolations != 0 {
		t.Errorf("After reset, PublishRateViolations = %d, want 0", stats.PublishRateViolations)
	}
}

// =============================================================================
// QUOTA BUCKETS TESTS
// =============================================================================

func TestTenantQuotaBuckets_UpdateQuotas(t *testing.T) {
	quotas := TenantQuotas{
		PublishRateLimit: 100,
	}
	buckets := NewTenantQuotaBuckets(quotas)

	if buckets.PublishRate == nil {
		t.Fatal("PublishRate bucket should be created")
	}

	// Update to remove rate limit
	newQuotas := TenantQuotas{
		PublishRateLimit: 0, // Unlimited
	}
	buckets.UpdateQuotas(newQuotas)

	if buckets.PublishRate != nil {
		t.Error("PublishRate bucket should be nil when unlimited")
	}

	// Add rate limit back
	newQuotas.PublishRateLimit = 200
	buckets.UpdateQuotas(newQuotas)

	if buckets.PublishRate == nil {
		t.Error("PublishRate bucket should be recreated")
	}
}

// =============================================================================
// BENCHMARK TESTS
// =============================================================================

func BenchmarkTokenBucket_Allow(b *testing.B) {
	bucket := NewTokenBucket(1000000, 1000000) // High rate for benchmark

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket.Allow(1)
	}
}

func BenchmarkQuotaManager_CheckPublish(b *testing.B) {
	qm := NewQuotaManager()
	quotas := TenantQuotas{
		PublishRateLimit:      1000000,
		PublishBytesRateLimit: 100000000,
		MaxMessageSizeBytes:   10000,
	}
	qm.InitializeTenant("acme", quotas)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qm.CheckPublish("acme", 100)
	}
}

// =============================================================================
// INTEGRATION TEST: TENANT MANAGER + QUOTA MANAGER
// =============================================================================

func TestTenantQuotaIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-quota-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()

	// Create tenant with specific quotas
	quotas := TenantQuotas{
		PublishRateLimit: 5,
		MaxTopics:        2,
	}
	_, err = tm.CreateTenant("acme", "Acme Corp", &quotas, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check quotas through quota manager
	result := tm.quotaManager.CheckPublishRate("acme", 3)
	if !result.Allowed {
		t.Error("Should allow 3 messages")
	}

	result = tm.quotaManager.CheckPublishRate("acme", 3) // Now at 6, over limit
	if result.Allowed {
		t.Error("Should reject when over rate limit")
	}

	// Topic count check
	result = tm.quotaManager.CheckTopicCount("acme", 0, 2)
	if !result.Allowed {
		t.Error("Should allow creating 2 topics")
	}

	result = tm.quotaManager.CheckTopicCount("acme", 2, 1)
	if result.Allowed {
		t.Error("Should reject 3rd topic")
	}

	// Update quotas
	newQuotas := TenantQuotas{
		PublishRateLimit: 1000,
		MaxTopics:        100,
	}
	_, err = tm.UpdateTenantQuotas("acme", newQuotas)
	if err != nil {
		t.Fatal(err)
	}

	// Should now allow more
	result = tm.quotaManager.CheckTopicCount("acme", 2, 1)
	if !result.Allowed {
		t.Error("Should allow after quota increase")
	}
}

// Helper to check file exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestTenantManager_PersistenceFiles(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tenant-persist-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	config := DefaultTenantManagerConfig(tempDir)
	tm, err := NewTenantManager(config)
	if err != nil {
		t.Fatal(err)
	}

	tm.CreateTenant("acme", "Acme Corp", nil, nil)
	tm.Close()

	// Check files exist
	tenantsPath := filepath.Join(config.DataDir, "tenants.json")
	if !fileExists(tenantsPath) {
		t.Error("tenants.json should exist")
	}
}
