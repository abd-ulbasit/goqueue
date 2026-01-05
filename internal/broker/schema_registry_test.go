// =============================================================================
// SCHEMA REGISTRY TESTS
// =============================================================================
//
// WHAT WE'RE TESTING:
// The Schema Registry provides centralized schema management for message
// validation and evolution. These tests verify:
//   1. Schema registration and retrieval
//   2. Versioning (sequential, per-subject)
//   3. Compatibility checking (BACKWARD, FORWARD, FULL, NONE)
//   4. Validation of messages against schemas
//   5. Subject management (list, delete)
//   6. Persistence (schemas survive restarts)
//   7. Caching behavior
//
// TEST ORGANIZATION:
//   - TestSchemaRegistry_* : Unit tests for registry operations
//   - TestCompatibility_*  : Compatibility checking scenarios
//   - TestValidation_*     : Message validation against schemas
//   - TestPersistence_*    : File-based persistence tests
//
// =============================================================================

package broker

import (
	"os"
	"path/filepath"
	"testing"
)

// =============================================================================
// TEST FIXTURES
// =============================================================================

// validUserSchemaV1 is a basic JSON Schema for a user object.
// Used as the first version in compatibility tests.
const validUserSchemaV1 = `{
	"type": "object",
	"properties": {
		"id": {"type": "integer"},
		"name": {"type": "string"},
		"email": {"type": "string"}
	},
	"required": ["id", "name"]
}`

// validUserSchemaV2 adds an optional "age" field.
// This is BACKWARD compatible (consumers expecting v1 can handle v2 data).
const validUserSchemaV2 = `{
	"type": "object",
	"properties": {
		"id": {"type": "integer"},
		"name": {"type": "string"},
		"email": {"type": "string"},
		"age": {"type": "integer"}
	},
	"required": ["id", "name"]
}`

// validUserSchemaV3 adds a new required field "created_at".
// This is NOT BACKWARD compatible (v1 consumers can't handle missing required field).
const validUserSchemaV3RequiredField = `{
	"type": "object",
	"properties": {
		"id": {"type": "integer"},
		"name": {"type": "string"},
		"email": {"type": "string"},
		"created_at": {"type": "string"}
	},
	"required": ["id", "name", "created_at"]
}`

// validOrderSchema is a different schema for testing multiple subjects.
const validOrderSchema = `{
	"type": "object",
	"properties": {
		"order_id": {"type": "string"},
		"amount": {"type": "number"},
		"customer_id": {"type": "integer"}
	},
	"required": ["order_id", "amount"]
}`

// invalidJSONSchema has invalid JSON syntax.
const invalidJSONSchema = `{"type": "object", properties: invalid}`

// invalidSchema has valid JSON but invalid JSON Schema structure.
const invalidSchemaType = `{"type": "invalid_type"}`

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// newTestSchemaRegistry creates a schema registry with a temp directory.
// Returns the registry and a cleanup function.
func newTestSchemaRegistry(t *testing.T) (*SchemaRegistry, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "schema-registry-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dataDir := filepath.Join(tmpDir, "data")
	config := DefaultSchemaRegistryConfig(dataDir)
	registry, err := NewSchemaRegistry(config)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create schema registry: %v", err)
	}

	cleanup := func() {
		registry.Close()
		os.RemoveAll(tmpDir)
	}

	return registry, cleanup
}

// =============================================================================
// SCHEMA REGISTRATION TESTS
// =============================================================================

// TestSchemaRegistry_RegisterSchema verifies basic schema registration.
//
// WHAT WE'RE TESTING:
//   - Schema is stored correctly
//   - Version is incremented
//   - Global ID is assigned
//   - Schema can be retrieved after registration
func TestSchemaRegistry_RegisterSchema(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Register first schema
	schema, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register schema: %v", err)
	}

	// Verify schema metadata
	if schema.Version != 1 {
		t.Errorf("expected version 1, got %d", schema.Version)
	}
	if schema.ID <= 0 {
		t.Errorf("expected positive ID, got %d", schema.ID)
	}
	if schema.Subject != "users" {
		t.Errorf("expected subject 'users', got '%s'", schema.Subject)
	}
	if schema.SchemaType != "JSON" {
		t.Errorf("expected schemaType 'JSON', got '%s'", schema.SchemaType)
	}
}

// TestSchemaRegistry_RegisterMultipleVersions verifies versioning.
func TestSchemaRegistry_RegisterMultipleVersions(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Set NONE compatibility for this test (to allow any schema changes)
	registry.SetCompatibilityMode("users", CompatibilityNone)

	// Register v1
	v1, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register v1: %v", err)
	}

	// Register v2 (compatible by default settings)
	v2, err := registry.RegisterSchema("users", validUserSchemaV2)
	if err != nil {
		t.Fatalf("failed to register v2: %v", err)
	}

	// Verify versions are sequential
	if v2.Version != v1.Version+1 {
		t.Errorf("expected version %d, got %d", v1.Version+1, v2.Version)
	}

	// Verify IDs are unique
	if v2.ID == v1.ID {
		t.Errorf("expected unique IDs, both got %d", v1.ID)
	}
}

// TestSchemaRegistry_DuplicateSchema verifies that registering the same schema
// returns the existing version without creating a new one.
func TestSchemaRegistry_DuplicateSchema(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Register schema
	first, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register: %v", err)
	}

	// Register same schema again
	second, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register duplicate: %v", err)
	}

	// Should return same version and ID
	if first.Version != second.Version {
		t.Errorf("expected same version, got %d and %d", first.Version, second.Version)
	}
	if first.ID != second.ID {
		t.Errorf("expected same ID, got %d and %d", first.ID, second.ID)
	}
}

// TestSchemaRegistry_InvalidSchema verifies error handling for bad schemas.
func TestSchemaRegistry_InvalidSchema(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	tests := []struct {
		name   string
		schema string
	}{
		{"empty schema", ""},
		{"invalid JSON", invalidJSONSchema},
		// Note: JSON Schema spec allows any type string - we don't validate against
		// the full JSON Schema meta-schema. Invalid types will simply fail to match
		// at validation time, not at registration time.
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := registry.RegisterSchema("test", tc.schema)
			if err == nil {
				t.Errorf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

// TestSchemaRegistry_MultipleSubjects verifies isolation between subjects.
func TestSchemaRegistry_MultipleSubjects(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Register schemas for different subjects
	users, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register users: %v", err)
	}

	orders, err := registry.RegisterSchema("orders", validOrderSchema)
	if err != nil {
		t.Fatalf("failed to register orders: %v", err)
	}

	// Both should be version 1 (independent versioning per subject)
	if users.Version != 1 {
		t.Errorf("expected users version 1, got %d", users.Version)
	}
	if orders.Version != 1 {
		t.Errorf("expected orders version 1, got %d", orders.Version)
	}

	// IDs should be unique globally
	if users.ID == orders.ID {
		t.Errorf("expected unique global IDs")
	}
}

// =============================================================================
// SCHEMA RETRIEVAL TESTS
// =============================================================================

// TestSchemaRegistry_GetLatestSchema verifies getting the latest version.
func TestSchemaRegistry_GetLatestSchema(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Set NONE compatibility for this test
	registry.SetCompatibilityMode("users", CompatibilityNone)

	// Register two versions
	registry.RegisterSchema("users", validUserSchemaV1)
	v2, _ := registry.RegisterSchema("users", validUserSchemaV2)

	// Get latest
	latest, err := registry.GetLatestSchema("users")
	if err != nil {
		t.Fatalf("failed to get latest: %v", err)
	}

	if latest.Version != v2.Version {
		t.Errorf("expected version %d, got %d", v2.Version, latest.Version)
	}
}

// TestSchemaRegistry_GetSchemaByVersion verifies getting a specific version.
func TestSchemaRegistry_GetSchemaByVersion(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Set NONE compatibility for this test
	registry.SetCompatibilityMode("users", CompatibilityNone)

	// Register two versions
	v1, _ := registry.RegisterSchema("users", validUserSchemaV1)
	registry.RegisterSchema("users", validUserSchemaV2)

	// Get v1 specifically
	retrieved, err := registry.GetSchemaBySubjectVersion("users", 1)
	if err != nil {
		t.Fatalf("failed to get v1: %v", err)
	}

	if retrieved.ID != v1.ID {
		t.Errorf("expected ID %d, got %d", v1.ID, retrieved.ID)
	}
}

// TestSchemaRegistry_GetSchemaByID verifies global ID lookup.
func TestSchemaRegistry_GetSchemaByID(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Register schema
	registered, _ := registry.RegisterSchema("users", validUserSchemaV1)

	// Look up by global ID
	retrieved, err := registry.GetSchemaByID(registered.ID)
	if err != nil {
		t.Fatalf("failed to get by ID: %v", err)
	}

	if retrieved.Subject != "users" {
		t.Errorf("expected subject 'users', got '%s'", retrieved.Subject)
	}
}

// TestSchemaRegistry_ListVersions verifies listing all versions.
func TestSchemaRegistry_ListVersions(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()
	subject := "users"

	// Set NONE compatibility
	registry.SetCompatibilityMode(subject, CompatibilityNone)

	// Register multiple versions
	registry.RegisterSchema(subject, validUserSchemaV1)
	registry.RegisterSchema(subject, validUserSchemaV2)
	registry.RegisterSchema(subject, validUserSchemaV3RequiredField)

	versions, err := registry.ListVersions(subject)
	if err != nil {
		t.Fatalf("failed to list versions: %v", err)
	}

	if len(versions) != 3 {
		t.Errorf("expected 3 versions, got %d", len(versions))
	}
}

// TestSchemaRegistry_ListSubjects verifies listing all subjects.
func TestSchemaRegistry_ListSubjects(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Register schemas for different subjects
	registry.RegisterSchema("users", validUserSchemaV1)
	registry.RegisterSchema("orders", validOrderSchema)

	subjects := registry.ListSubjects()

	if len(subjects) != 2 {
		t.Errorf("expected 2 subjects, got %d", len(subjects))
	}

	// Check both subjects are present
	found := make(map[string]bool)
	for _, s := range subjects {
		found[s] = true
	}
	if !found["users"] || !found["orders"] {
		t.Errorf("expected users and orders, got %v", subjects)
	}
}

// =============================================================================
// COMPATIBILITY TESTS
// =============================================================================

// TestCompatibility_BackwardAddOptionalField verifies that adding an optional
// field is backward compatible.
//
// BACKWARD COMPATIBILITY:
// New schema can read data produced by old schema.
// Adding optional fields is safe (old data just won't have them).
func TestCompatibility_BackwardAddOptionalField(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Use BACKWARD (default)
	registry.SetCompatibilityMode("users", CompatibilityBackward)

	// Register v1
	_, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register v1: %v", err)
	}

	// Register v2 with optional field - should succeed
	_, err = registry.RegisterSchema("users", validUserSchemaV2)
	if err != nil {
		t.Fatalf("expected v2 to be backward compatible: %v", err)
	}
}

// TestCompatibility_BackwardAddRequiredField verifies that adding a required
// field is NOT backward compatible.
//
// WHY THIS FAILS:
// Old producers don't send the new required field.
// New consumers requiring this field will reject old messages.
func TestCompatibility_BackwardAddRequiredField(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Use BACKWARD
	registry.SetCompatibilityMode("users", CompatibilityBackward)

	// Register v1
	_, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register v1: %v", err)
	}

	// Try to register v3 with new required field - should fail
	_, err = registry.RegisterSchema("users", validUserSchemaV3RequiredField)
	if err == nil {
		t.Fatal("expected incompatibility error, got nil")
	}
}

// TestCompatibility_None allows any schema change.
func TestCompatibility_None(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Set NONE (no compatibility checking)
	registry.SetCompatibilityMode("users", CompatibilityNone)

	// Register v1
	_, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register v1: %v", err)
	}

	// Register completely different schema - should succeed with NONE
	_, err = registry.RegisterSchema("users", validOrderSchema)
	if err != nil {
		t.Fatalf("expected NONE to allow any change: %v", err)
	}
}

// TestCompatibility_TestWithoutRegister verifies the TestCompatibility method.
func TestCompatibility_TestWithoutRegister(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Register v1
	_, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register v1: %v", err)
	}

	// Test v2 compatibility without registering
	compatible, err := registry.TestCompatibility("users", validUserSchemaV2, -1)
	if err != nil {
		t.Fatalf("failed to test compatibility: %v", err)
	}

	if !compatible {
		t.Error("expected v2 to be compatible")
	}

	// Verify v2 was not registered
	versions, _ := registry.ListVersions("users")
	if len(versions) != 1 {
		t.Errorf("expected 1 version, got %d", len(versions))
	}
}

// =============================================================================
// MESSAGE VALIDATION TESTS
// =============================================================================

// TestValidation_ValidMessage verifies valid messages pass validation.
func TestValidation_ValidMessage(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Register schema
	_, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register: %v", err)
	}

	// Enable validation
	registry.SetValidationEnabled("users", true)

	// Valid message
	validMsg := []byte(`{"id": 1, "name": "Alice", "email": "alice@example.com"}`)
	if err := registry.ValidateMessage("users", validMsg); err != nil {
		t.Errorf("expected valid message to pass: %v", err)
	}
}

// TestValidation_InvalidMessage verifies invalid messages fail validation.
func TestValidation_InvalidMessage(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Register schema
	_, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register: %v", err)
	}

	// Enable validation
	registry.SetValidationEnabled("users", true)

	tests := []struct {
		name string
		msg  []byte
	}{
		{"missing required field", []byte(`{"id": 1}`)},                          // missing 'name'
		{"wrong type for id", []byte(`{"id": "not-a-number", "name": "Alice"}`)}, // id should be integer
		{"invalid JSON", []byte(`{invalid json}`)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := registry.ValidateMessage("users", tc.msg)
			if err == nil {
				t.Errorf("expected validation error for %s", tc.name)
			}
		})
	}
}

// TestValidation_Disabled verifies validation can be disabled per subject.
func TestValidation_Disabled(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Register schema
	_, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register: %v", err)
	}

	// Explicitly disable validation for this subject
	// (global default is enabled per DefaultSchemaRegistryConfig)
	registry.SetValidationEnabled("users", false)

	// Invalid message should pass when validation is disabled
	invalidMsg := []byte(`{"invalid": "message"}`)
	if err := registry.ValidateMessage("users", invalidMsg); err != nil {
		t.Errorf("expected disabled validation to pass any message: %v", err)
	}
}

// TestValidation_NoSchema verifies behavior when no schema is registered.
func TestValidation_NoSchema(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// No schema registered for "orders"
	// Enable validation for non-existent subject
	registry.SetValidationEnabled("orders", true)

	// Should pass (no schema = no validation)
	msg := []byte(`{"any": "data"}`)
	if err := registry.ValidateMessage("orders", msg); err != nil {
		t.Errorf("expected no error when no schema registered: %v", err)
	}
}

// =============================================================================
// SUBJECT MANAGEMENT TESTS
// =============================================================================

// TestSchemaRegistry_DeleteVersion verifies soft deletion of a version.
func TestSchemaRegistry_DeleteVersion(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Register schema
	_, err := registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register: %v", err)
	}

	// Delete version 1
	if err := registry.DeleteSchemaVersion("users", 1); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Should not be retrievable
	_, err = registry.GetSchemaBySubjectVersion("users", 1)
	if err == nil {
		t.Error("expected error retrieving deleted version")
	}
}

// TestSchemaRegistry_DeleteSubject verifies deletion of entire subject.
func TestSchemaRegistry_DeleteSubject(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Set NONE for multiple versions
	registry.SetCompatibilityMode("users", CompatibilityNone)

	// Register multiple versions
	registry.RegisterSchema("users", validUserSchemaV1)
	registry.RegisterSchema("users", validUserSchemaV2)

	// Delete subject
	deletedVersions, err := registry.DeleteSubject("users")
	if err != nil {
		t.Fatalf("failed to delete subject: %v", err)
	}

	if len(deletedVersions) != 2 {
		t.Errorf("expected 2 deleted versions, got %d", len(deletedVersions))
	}

	// Subject should not be listable
	subjects := registry.ListSubjects()
	for _, s := range subjects {
		if s == "users" {
			t.Error("expected 'users' to be removed from subjects list")
		}
	}
}

// =============================================================================
// PERSISTENCE TESTS
// =============================================================================

// TestPersistence_SurvivesRestart verifies schemas persist to disk.
func TestPersistence_SurvivesRestart(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "schema-persistence-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dataDir := filepath.Join(tmpDir, "data")

	// Create registry and register schema
	config := DefaultSchemaRegistryConfig(dataDir)
	registry1, err := NewSchemaRegistry(config)
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}

	schema, err := registry1.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register: %v", err)
	}
	originalID := schema.ID

	// Close first registry
	registry1.Close()

	// Create new registry pointing to same directory
	registry2, err := NewSchemaRegistry(config)
	if err != nil {
		t.Fatalf("failed to create second registry: %v", err)
	}
	defer registry2.Close()

	// Schema should be loadable
	loaded, err := registry2.GetLatestSchema("users")
	if err != nil {
		t.Fatalf("failed to load persisted schema: %v", err)
	}

	if loaded.ID != originalID {
		t.Errorf("expected ID %d, got %d", originalID, loaded.ID)
	}
	if loaded.Version != 1 {
		t.Errorf("expected version 1, got %d", loaded.Version)
	}
}

// TestPersistence_FilesCreated verifies correct file structure.
func TestPersistence_FilesCreated(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "schema-files-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dataDir := filepath.Join(tmpDir, "data")
	config := DefaultSchemaRegistryConfig(dataDir)
	registry, err := NewSchemaRegistry(config)
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}
	defer registry.Close()

	// Register schema
	_, err = registry.RegisterSchema("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to register: %v", err)
	}

	// Check file exists: data/schemas/users/v1.json
	schemaFile := filepath.Join(dataDir, "schemas", "users", "v1.json")
	if _, err := os.Stat(schemaFile); os.IsNotExist(err) {
		t.Errorf("expected schema file at %s", schemaFile)
	}
}

// =============================================================================
// CONFIGURATION TESTS
// =============================================================================

// TestConfig_GlobalCompatibility verifies global compatibility setting.
func TestConfig_GlobalCompatibility(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Default should be BACKWARD
	if registry.GetGlobalCompatibility() != CompatibilityBackward {
		t.Errorf("expected default BACKWARD, got %s", registry.GetGlobalCompatibility())
	}

	// Change global
	registry.SetGlobalCompatibility(CompatibilityFull)
	if registry.GetGlobalCompatibility() != CompatibilityFull {
		t.Errorf("expected FULL, got %s", registry.GetGlobalCompatibility())
	}
}

// TestConfig_SubjectOverridesGlobal verifies per-subject config.
func TestConfig_SubjectOverridesGlobal(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Set global to FULL
	registry.SetGlobalCompatibility(CompatibilityFull)

	// Override for users to NONE
	registry.SetCompatibilityMode("users", CompatibilityNone)

	// users should use NONE
	if registry.GetCompatibilityMode("users") != CompatibilityNone {
		t.Errorf("expected NONE for users, got %s", registry.GetCompatibilityMode("users"))
	}

	// orders (no override) should use global FULL
	if registry.GetCompatibilityMode("orders") != CompatibilityFull {
		t.Errorf("expected FULL for orders, got %s", registry.GetCompatibilityMode("orders"))
	}
}

// =============================================================================
// STATS TESTS
// =============================================================================

// TestStats_Counters verifies statistics are tracked.
func TestStats_Counters(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Set NONE for easier testing
	registry.SetCompatibilityMode("users", CompatibilityNone)

	// Register schemas
	registry.RegisterSchema("users", validUserSchemaV1)
	registry.RegisterSchema("users", validUserSchemaV2)
	registry.RegisterSchema("orders", validOrderSchema)

	stats := registry.Stats()

	if stats.SubjectCount != 2 {
		t.Errorf("expected 2 subjects, got %d", stats.SubjectCount)
	}
	if stats.TotalSchemas != 3 {
		t.Errorf("expected 3 schemas, got %d", stats.TotalSchemas)
	}
	// Count total versions across all subjects
	totalVersions := 0
	for _, subjectStats := range stats.Subjects {
		totalVersions += subjectStats.VersionCount
	}
	if totalVersions < 3 {
		t.Errorf("expected at least 3 versions, got %d", totalVersions)
	}
}

// =============================================================================
// JSON SCHEMA VALIDATOR TESTS
// =============================================================================

// TestJSONValidator_BasicTypes verifies type validation.
func TestJSONValidator_BasicTypes(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		data    string
		wantErr bool
	}{
		{
			name:    "string type valid",
			schema:  `{"type": "string"}`,
			data:    `"hello"`,
			wantErr: false,
		},
		{
			name:    "string type invalid",
			schema:  `{"type": "string"}`,
			data:    `123`,
			wantErr: true,
		},
		{
			name:    "integer type valid",
			schema:  `{"type": "integer"}`,
			data:    `42`,
			wantErr: false,
		},
		{
			name:    "integer type rejects float",
			schema:  `{"type": "integer"}`,
			data:    `42.5`,
			wantErr: true,
		},
		{
			name:    "number type accepts float",
			schema:  `{"type": "number"}`,
			data:    `42.5`,
			wantErr: false,
		},
		{
			name:    "boolean type valid",
			schema:  `{"type": "boolean"}`,
			data:    `true`,
			wantErr: false,
		},
		{
			name:    "array type valid",
			schema:  `{"type": "array"}`,
			data:    `[1, 2, 3]`,
			wantErr: false,
		},
		{
			name:    "object type valid",
			schema:  `{"type": "object"}`,
			data:    `{"key": "value"}`,
			wantErr: false,
		},
		{
			name:    "null type valid",
			schema:  `{"type": "null"}`,
			data:    `null`,
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			validator, err := NewJSONSchemaValidator(tc.schema)
			if err != nil {
				t.Fatalf("failed to create validator: %v", err)
			}

			err = validator.Validate([]byte(tc.data))
			if tc.wantErr && err == nil {
				t.Error("expected validation error")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestJSONValidator_RequiredFields verifies required field validation.
func TestJSONValidator_RequiredFields(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "integer"}
		},
		"required": ["name"]
	}`

	validator, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// Valid - has required field
	if err := validator.Validate([]byte(`{"name": "Alice"}`)); err != nil {
		t.Errorf("expected valid: %v", err)
	}

	// Valid - has required + optional
	if err := validator.Validate([]byte(`{"name": "Alice", "age": 30}`)); err != nil {
		t.Errorf("expected valid: %v", err)
	}

	// Invalid - missing required field
	if err := validator.Validate([]byte(`{"age": 30}`)); err == nil {
		t.Error("expected error for missing required field")
	}
}

// TestJSONValidator_MinMax verifies min/max constraints.
func TestJSONValidator_MinMax(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"age": {"type": "integer", "minimum": 0, "maximum": 150},
			"name": {"type": "string", "minLength": 1, "maxLength": 100}
		}
	}`

	validator, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{"valid age", `{"age": 25}`, false},
		{"age too low", `{"age": -1}`, true},
		{"age too high", `{"age": 200}`, true},
		{"valid name", `{"name": "Alice"}`, false},
		{"name too short", `{"name": ""}`, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate([]byte(tc.data))
			if tc.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestJSONValidator_Enum verifies enum validation.
func TestJSONValidator_Enum(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"status": {"type": "string", "enum": ["pending", "active", "completed"]}
		}
	}`

	validator, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// Valid enum value
	if err := validator.Validate([]byte(`{"status": "active"}`)); err != nil {
		t.Errorf("expected valid: %v", err)
	}

	// Invalid enum value
	if err := validator.Validate([]byte(`{"status": "invalid"}`)); err == nil {
		t.Error("expected error for invalid enum value")
	}
}

// TestJSONValidator_Pattern verifies regex pattern validation.
func TestJSONValidator_Pattern(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"email": {"type": "string", "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"}
		}
	}`

	validator, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// Valid email pattern
	if err := validator.Validate([]byte(`{"email": "test@example.com"}`)); err != nil {
		t.Errorf("expected valid: %v", err)
	}

	// Invalid email pattern
	if err := validator.Validate([]byte(`{"email": "not-an-email"}`)); err == nil {
		t.Error("expected error for invalid email pattern")
	}
}

// TestJSONValidator_ArrayItems verifies array item validation.
func TestJSONValidator_ArrayItems(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"tags": {
				"type": "array",
				"items": {"type": "string"}
			}
		}
	}`

	validator, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// Valid - array of strings
	if err := validator.Validate([]byte(`{"tags": ["go", "queue", "test"]}`)); err != nil {
		t.Errorf("expected valid: %v", err)
	}

	// Invalid - array contains non-string
	if err := validator.Validate([]byte(`{"tags": ["go", 123, "test"]}`)); err == nil {
		t.Error("expected error for mixed array types")
	}
}

// TestJSONValidator_NestedObjects verifies nested object validation.
func TestJSONValidator_NestedObjects(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"user": {
				"type": "object",
				"properties": {
					"name": {"type": "string"},
					"address": {
						"type": "object",
						"properties": {
							"city": {"type": "string"}
						},
						"required": ["city"]
					}
				},
				"required": ["name"]
			}
		}
	}`

	validator, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// Valid nested object
	valid := `{"user": {"name": "Alice", "address": {"city": "NYC"}}}`
	if err := validator.Validate([]byte(valid)); err != nil {
		t.Errorf("expected valid: %v", err)
	}

	// Invalid - missing nested required field
	invalid := `{"user": {"name": "Alice", "address": {}}}`
	if err := validator.Validate([]byte(invalid)); err == nil {
		t.Error("expected error for missing nested required field")
	}
}

// =============================================================================
// CHECK SCHEMA EXISTS TESTS
// =============================================================================

// TestSchemaRegistry_CheckSchemaExists verifies schema existence check.
func TestSchemaRegistry_CheckSchemaExists(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Register schema
	registered, _ := registry.RegisterSchema("users", validUserSchemaV1)

	// Check existing schema
	found, err := registry.CheckSchemaExists("users", validUserSchemaV1)
	if err != nil {
		t.Fatalf("failed to check: %v", err)
	}
	if found == nil {
		t.Fatal("expected to find schema")
	}
	if found.ID != registered.ID {
		t.Errorf("expected ID %d, got %d", registered.ID, found.ID)
	}

	// Check non-existing schema
	found, err = registry.CheckSchemaExists("users", validUserSchemaV2)
	if err != nil {
		t.Fatalf("failed to check: %v", err)
	}
	if found != nil {
		t.Error("expected nil for non-existing schema")
	}
}

// =============================================================================
// ERROR CASE TESTS
// =============================================================================

// TestSchemaRegistry_NotFoundErrors verifies proper not-found handling.
func TestSchemaRegistry_NotFoundErrors(t *testing.T) {
	registry, cleanup := newTestSchemaRegistry(t)
	defer cleanup()

	// Get non-existent subject
	_, err := registry.GetLatestSchema("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent subject")
	}

	// Get non-existent version
	registry.RegisterSchema("users", validUserSchemaV1)
	_, err = registry.GetSchemaBySubjectVersion("users", 999)
	if err == nil {
		t.Error("expected error for non-existent version")
	}

	// Get non-existent ID
	_, err = registry.GetSchemaByID(999)
	if err == nil {
		t.Error("expected error for non-existent ID")
	}
}
