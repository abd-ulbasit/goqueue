// =============================================================================
// SCHEMA REGISTRY - MESSAGE SCHEMA VALIDATION & EVOLUTION
// =============================================================================
//
// WHAT IS A SCHEMA REGISTRY?
// A centralized service that:
//   1. Stores message schemas - The "contract" defining message structure
//   2. Validates messages - Ensures producers send well-formed data
//   3. Manages evolution - Allows schema changes without breaking consumers
//   4. Provides schema IDs - Compact way to reference schemas in messages
//
// WHY SCHEMA REGISTRY?
// Without schemas, message queues are "dumb pipes" - they don't know what
// data looks like. This causes problems:
//
//   Problem 1: Silent Failures
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Producer sends: {"user_id": "123", "amount": "fifty"}                   │
//   │ Consumer expects: {"user_id": int, "amount": float}                     │
//   │ Result: Consumer crashes or produces garbage output                     │
//   └─────────────────────────────────────────────────────────────────────────┘
//
//   Problem 2: Breaking Changes
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Week 1: Producer sends {"name": "John"}                                 │
//   │ Week 2: Producer changes to {"full_name": "John"} (field renamed)       │
//   │ Result: All consumers break because they expect "name"                  │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// SCHEMA REGISTRY SOLVES THESE:
//   - Validation: Reject malformed messages at publish time
//   - Compatibility: Only allow safe schema changes
//   - Evolution: Support gradual schema changes with rules
//
// COMPARISON - Real-World Schema Registries:
//
//   ┌────────────────┬──────────────────────────────────────────────────────┐
//   │ System         │ Approach                                             │
//   ├────────────────┼──────────────────────────────────────────────────────┤
//   │ Confluent SR   │ Kafka topic storage, Avro/Protobuf/JSON Schema       │
//   │ AWS Glue       │ Managed service, tight AWS integration               │
//   │ Apicurio       │ Open source, multiple formats, SQL/Kafka storage     │
//   │ goqueue        │ File-based JSON Schema, broker-side validation       │
//   └────────────────┴──────────────────────────────────────────────────────┘
//
// GOQUEUE SCHEMA REGISTRY DESIGN:
//
//   ┌──────────────────────────────────────────────────────────────────────────┐
//   │                        SCHEMA REGISTRY                                   │
//   │                                                                          │
//   │  ┌─────────────────────────────────────────────────────────────────┐     │
//   │  │                    Storage (File-based)                         │     │
//   │  │   data/schemas/{subject}/                                       │     │
//   │  │     ├── config.json          (compatibility mode)               │     │
//   │  │     ├── v1.json              (schema version 1)                 │     │
//   │  │     ├── v2.json              (schema version 2)                 │     │
//   │  │     └── latest.json          (symlink/copy of latest)           │     │
//   │  └─────────────────────────────────────────────────────────────────┘     │
//   │                              │                                           │
//   │  ┌─────────────────────────────────────────────────────────────────┐     │
//   │  │                    In-Memory Cache                              │     │
//   │  │   • All schemas loaded on startup                               │     │
//   │  │   • File watcher for hot reload                                 │     │
//   │  │   • Schema ID → Schema lookup                                   │     │
//   │  │   • Subject → Versions lookup                                   │     │
//   │  └─────────────────────────────────────────────────────────────────┘     │
//   │                              │                                           │
//   │  ┌─────────────────────────────────────────────────────────────────┐     │
//   │  │                    Compatibility Checker                        │     │
//   │  │   • BACKWARD: New schema can read old data                      │     │
//   │  │   • FORWARD: Old schema can read new data                       │     │
//   │  │   • FULL: Both directions                                       │     │
//   │  │   • NONE: No checking (any change allowed)                      │     │
//   │  └─────────────────────────────────────────────────────────────────┘     │
//   │                              │                                           │
//   │  ┌─────────────────────────────────────────────────────────────────┐     │
//   │  │                    Validator (JSON Schema)                      │     │
//   │  │   • Validates messages against registered schemas               │     │
//   │  │   • Returns detailed error on validation failure                │     │
//   │  │   • Per-topic enable/disable                                    │     │
//   │  └─────────────────────────────────────────────────────────────────┘     │
//   │                                                                          │
//   └──────────────────────────────────────────────────────────────────────────┘
//
// SUBJECT NAMING STRATEGY:
// A "subject" is the identifier for a schema. We use TopicNameStrategy:
//   - Subject = Topic name (1:1 mapping)
//   - Simple mental model: "What's the schema for topic X?" → Subject X
//
// SCHEMA ID:
// Each schema version gets a globally unique ID. We use this ID in messages:
//   - Message header: "schema-id: 12345"
//   - Allows consumers to fetch the exact schema used for encoding
//   - Survives schema evolution (old messages keep old schema ID)
//
// FUTURE IMPROVEMENTS (M8+):
//   - Protobuf support
//   - Transitive compatibility modes
//   - Schema references ($ref support)
//   - Schema soft/hard delete
//
// =============================================================================

package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrSchemaNotFound means the requested schema doesn't exist
	ErrSchemaNotFound = errors.New("schema not found")

	// ErrSubjectNotFound means the subject (topic) has no schemas
	ErrSubjectNotFound = errors.New("subject not found")

	// ErrVersionNotFound means the specific version doesn't exist
	ErrVersionNotFound = errors.New("schema version not found")

	// ErrSchemaIncompatible means the schema violates compatibility rules
	ErrSchemaIncompatible = errors.New("schema is incompatible with previous version")

	// ErrInvalidSchema means the schema itself is malformed
	ErrInvalidSchema = errors.New("invalid schema")

	// ErrSchemaAlreadyExists means trying to register an identical schema
	ErrSchemaAlreadyExists = errors.New("schema already registered")

	// ErrValidationFailed means message doesn't match schema
	ErrValidationFailed = errors.New("message validation failed")

	// ErrSchemaRegistryClosed means the registry has been shut down
	ErrSchemaRegistryClosed = errors.New("schema registry is closed")
)

// =============================================================================
// COMPATIBILITY MODES
// =============================================================================
//
// COMPATIBILITY MATRIX:
//
// ┌─────────────┬───────────────────────────────────────────────────────────────┐
// │ Mode        │ Allowed Changes                                               │
// ├─────────────┼───────────────────────────────────────────────────────────────┤
// │ NONE        │ Any change (no checking)                                      │
// ├─────────────┼───────────────────────────────────────────────────────────────┤
// │ BACKWARD    │ • Add optional field ✓                                        │
// │             │ • Remove required field ✓ (with default)                      │
// │             │ • Remove optional field ✓                                     │
// │             │ • Add required field ✗ (breaks old consumers)                 │
// │             │ • Rename field ✗                                              │
// ├─────────────┼───────────────────────────────────────────────────────────────┤
// │ FORWARD     │ • Add required field ✓                                        │
// │             │ • Remove optional field ✓                                     │
// │             │ • Add optional field ✓                                        │
// │             │ • Remove required field ✗ (old producer fails)                │
// ├─────────────┼───────────────────────────────────────────────────────────────┤
// │ FULL        │ • Only add/remove optional fields ✓                           │
// │             │ • Most restrictive                                            │
// │             │ • Guarantees both directions work                             │
// └─────────────┴───────────────────────────────────────────────────────────────┘
//
// USE CASE GUIDANCE:
//   - BACKWARD (default): Consumer upgrades first, then producer
//   - FORWARD: Producer upgrades first, then consumer
//   - FULL: Zero-downtime rolling updates
//   - NONE: Development, breaking changes accepted
//

// CompatibilityMode defines schema evolution rules
type CompatibilityMode string

const (
	// CompatibilityNone allows any schema change (no checking)
	// Use case: Development environments, major version bumps
	CompatibilityNone CompatibilityMode = "NONE"

	// CompatibilityBackward ensures new schema can read data written by old schema
	// Use case: Consumer upgrades before producer (most common)
	// Rule: Only add optional fields, or remove fields
	CompatibilityBackward CompatibilityMode = "BACKWARD"

	// CompatibilityForward ensures old schema can read data written by new schema
	// Use case: Producer upgrades before consumer
	// Rule: Only remove optional fields, or add fields with defaults
	CompatibilityForward CompatibilityMode = "FORWARD"

	// CompatibilityFull combines Backward + Forward (most restrictive)
	// Use case: Zero-downtime deployments, rolling updates
	// Rule: Only add/remove optional fields
	CompatibilityFull CompatibilityMode = "FULL"
)

// IsValid returns true if the mode is a recognized value
func (m CompatibilityMode) IsValid() bool {
	switch m {
	case CompatibilityNone, CompatibilityBackward, CompatibilityForward, CompatibilityFull:
		return true
	default:
		return false
	}
}

// ParseCompatibilityMode converts a string to CompatibilityMode
func ParseCompatibilityMode(s string) CompatibilityMode {
	switch strings.ToUpper(s) {
	case "NONE":
		return CompatibilityNone
	case "BACKWARD":
		return CompatibilityBackward
	case "FORWARD":
		return CompatibilityForward
	case "FULL":
		return CompatibilityFull
	default:
		return CompatibilityBackward // Safe default
	}
}

// =============================================================================
// SCHEMA STRUCT
// =============================================================================

// Schema represents a registered schema with metadata
type Schema struct {
	// ID is the globally unique identifier (auto-incremented)
	ID int64 `json:"id"`

	// Subject is the schema subject (typically topic name)
	Subject string `json:"subject"`

	// Version is the version number within the subject (1, 2, 3...)
	Version int `json:"version"`

	// Schema is the JSON Schema definition
	Schema string `json:"schema"`

	// SchemaType is the schema format (always "JSON" for now)
	// Future: "PROTOBUF", "AVRO"
	SchemaType string `json:"schemaType"`

	// RegisteredAt is when this schema version was registered
	RegisteredAt time.Time `json:"registeredAt"`

	// Fingerprint is a hash of the schema for deduplication
	Fingerprint string `json:"fingerprint"`
}

// SubjectConfig holds per-subject configuration
type SubjectConfig struct {
	// Subject is the subject name (topic name)
	Subject string `json:"subject"`

	// Compatibility is the compatibility mode for this subject
	Compatibility CompatibilityMode `json:"compatibility"`

	// ValidationEnabled controls whether messages are validated on publish
	ValidationEnabled bool `json:"validationEnabled"`
}

// =============================================================================
// SCHEMA REGISTRY CONFIGURATION
// =============================================================================

// SchemaRegistryConfig holds registry configuration
type SchemaRegistryConfig struct {
	// DataDir is the root directory for schema storage
	// Structure: DataDir/schemas/{subject}/
	DataDir string

	// GlobalCompatibility is the default compatibility mode
	GlobalCompatibility CompatibilityMode

	// ValidationEnabled is the default validation setting
	ValidationEnabled bool

	// CacheSize is the maximum number of compiled schemas to cache
	CacheSize int
}

// DefaultSchemaRegistryConfig returns sensible defaults
func DefaultSchemaRegistryConfig(dataDir string) SchemaRegistryConfig {
	return SchemaRegistryConfig{
		DataDir:             filepath.Join(dataDir, "schemas"),
		GlobalCompatibility: CompatibilityBackward, // Confluent default
		ValidationEnabled:   true,                  // Validate by default
		CacheSize:           1000,                  // Max compiled schemas in cache
	}
}

// =============================================================================
// SCHEMA REGISTRY STRUCT
// =============================================================================

// SchemaRegistry manages schema storage, retrieval, and validation
type SchemaRegistry struct {
	// config holds registry configuration
	config SchemaRegistryConfig

	// schemas maps schema ID → Schema
	schemas map[int64]*Schema

	// subjects maps subject → version → Schema
	// TODO: should we make it map[string][]int]*Schema for faster lookup?
	subjects map[string]map[int]*Schema

	// subjectConfigs maps subject → SubjectConfig
	subjectConfigs map[string]*SubjectConfig

	// validators maps schema ID → compiled validator
	validators map[int64]*JSONSchemaValidator

	// nextID is the next schema ID to assign (atomic)
	nextID int64

	// mu protects all maps
	mu sync.RWMutex

	// logger for registry operations
	logger *slog.Logger

	// closed tracks if registry is shut down
	closed bool
}

// NewSchemaRegistry creates a new schema registry
//
// STARTUP PROCESS:
//  1. Create schemas directory if needed
//  2. Load all existing schemas from disk
//  3. Build in-memory indexes
//  4. Ready to serve requests
func NewSchemaRegistry(config SchemaRegistryConfig) (*SchemaRegistry, error) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create schemas directory
	if err := os.MkdirAll(config.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create schemas directory: %w", err)
	}

	sr := &SchemaRegistry{
		config:         config,
		schemas:        make(map[int64]*Schema),
		subjects:       make(map[string]map[int]*Schema),
		subjectConfigs: make(map[string]*SubjectConfig),
		validators:     make(map[int64]*JSONSchemaValidator),
		nextID:         1,
		logger:         logger,
	}

	// Load existing schemas from disk
	if err := sr.loadSchemas(); err != nil {
		return nil, fmt.Errorf("failed to load schemas: %w", err)
	}

	logger.Info("schema registry started",
		"dataDir", config.DataDir,
		"globalCompatibility", config.GlobalCompatibility,
		"validationEnabled", config.ValidationEnabled,
		"subjects", len(sr.subjects),
		"schemas", len(sr.schemas))

	return sr, nil
}

// =============================================================================
// SCHEMA REGISTRATION
// =============================================================================

// RegisterSchema registers a new schema version for a subject
//
// REGISTRATION FLOW:
//  1. Parse and validate the JSON Schema syntax
//  2. Check if identical schema already exists (return existing ID)
//  3. Check compatibility with previous version
//  4. Assign new ID and version
//  5. Persist to disk
//  6. Update in-memory cache
//  7. Compile validator
//
// RETURNS:
//   - Schema: The registered schema with ID and version
//   - error: ErrSchemaIncompatible, ErrInvalidSchema, etc.
func (sr *SchemaRegistry) RegisterSchema(subject, schemaStr string) (*Schema, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.closed {
		return nil, ErrSchemaRegistryClosed
	}

	// =========================================================================
	// STEP 1: Validate the JSON Schema syntax
	// =========================================================================
	//
	// Before storing, we need to ensure the schema is valid JSON Schema.
	// This catches syntax errors early, before they cause runtime issues.
	//
	validator, err := NewJSONSchemaValidator(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSchema, err)
	}

	// =========================================================================
	// STEP 2: Check for duplicate schema
	// =========================================================================
	//
	// If the exact same schema (by fingerprint) is already registered,
	// return the existing schema instead of creating a duplicate.
	//
	fingerprint := calculateSchemaFingerprint(schemaStr)

	versions, exists := sr.subjects[subject]
	if exists {
		for _, existing := range versions {
			if existing.Fingerprint == fingerprint {
				// Already registered - return existing
				return existing, nil
			}
		}
	}

	// =========================================================================
	// STEP 3: Check compatibility with previous version
	// =========================================================================
	//
	// Get the compatibility mode for this subject and check if the new
	// schema is compatible with the latest version.
	//
	compat := sr.getCompatibilityMode(subject)
	if compat != CompatibilityNone && exists && len(versions) > 0 {
		// Find latest version
		latestVersion := sr.getLatestVersionNumber(subject)
		latestSchema := versions[latestVersion]

		if err := sr.checkCompatibility(latestSchema.Schema, schemaStr, compat); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrSchemaIncompatible, err)
		}
	}

	// =========================================================================
	// STEP 4: Assign ID and version
	// =========================================================================
	//
	// ID: Globally unique across all subjects (auto-increment)
	// Version: Per-subject sequential number (1, 2, 3...)
	//
	id := atomic.AddInt64(&sr.nextID, 1) - 1
	version := 1
	if exists {
		version = sr.getLatestVersionNumber(subject) + 1
	}

	schema := &Schema{
		ID:           id,
		Subject:      subject,
		Version:      version,
		Schema:       schemaStr,
		SchemaType:   "JSON",
		RegisteredAt: time.Now(),
		Fingerprint:  fingerprint,
	}

	// =========================================================================
	// STEP 5: Persist to disk
	// =========================================================================
	if err := sr.persistSchema(schema); err != nil {
		return nil, fmt.Errorf("failed to persist schema: %w", err)
	}

	// =========================================================================
	// STEP 6: Update in-memory cache
	// =========================================================================
	sr.schemas[id] = schema

	if !exists {
		sr.subjects[subject] = make(map[int]*Schema)
	}
	sr.subjects[subject][version] = schema

	// =========================================================================
	// STEP 7: Cache compiled validator
	// =========================================================================
	sr.validators[id] = validator

	sr.logger.Info("schema registered",
		"subject", subject,
		"version", version,
		"id", id,
		"compatibility", compat)

	return schema, nil
}

// =============================================================================
// SCHEMA RETRIEVAL
// =============================================================================

// GetSchemaByID retrieves a schema by its global ID
func (sr *SchemaRegistry) GetSchemaByID(id int64) (*Schema, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	if sr.closed {
		return nil, ErrSchemaRegistryClosed
	}

	schema, exists := sr.schemas[id]
	if !exists {
		return nil, ErrSchemaNotFound
	}

	return schema, nil
}

// GetSchemaBySubjectVersion retrieves a specific version of a subject's schema
func (sr *SchemaRegistry) GetSchemaBySubjectVersion(subject string, version int) (*Schema, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	if sr.closed {
		return nil, ErrSchemaRegistryClosed
	}

	versions, exists := sr.subjects[subject]
	if !exists {
		return nil, ErrSubjectNotFound
	}

	schema, exists := versions[version]
	if !exists {
		return nil, ErrVersionNotFound
	}

	return schema, nil
}

// GetLatestSchema retrieves the latest version of a subject's schema
func (sr *SchemaRegistry) GetLatestSchema(subject string) (*Schema, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	if sr.closed {
		return nil, ErrSchemaRegistryClosed
	}

	versions, exists := sr.subjects[subject]
	if !exists {
		return nil, ErrSubjectNotFound
	}

	latestVersion := sr.getLatestVersionNumber(subject)
	return versions[latestVersion], nil
}

// ListVersions returns all version numbers for a subject
func (sr *SchemaRegistry) ListVersions(subject string) ([]int, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	if sr.closed {
		return nil, ErrSchemaRegistryClosed
	}

	versions, exists := sr.subjects[subject]
	if !exists {
		return nil, ErrSubjectNotFound
	}

	result := make([]int, 0, len(versions))
	for v := range versions {
		result = append(result, v)
	}
	sort.Ints(result)

	return result, nil
}

// ListSubjects returns all registered subjects
func (sr *SchemaRegistry) ListSubjects() []string {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	subjects := make([]string, 0, len(sr.subjects))
	for s := range sr.subjects {
		subjects = append(subjects, s)
	}
	sort.Strings(subjects)

	return subjects
}

// CheckSchemaExists checks if a schema is already registered under the subject
// Returns the schema if found, nil if not
func (sr *SchemaRegistry) CheckSchemaExists(subject, schemaStr string) (*Schema, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	if sr.closed {
		return nil, ErrSchemaRegistryClosed
	}

	fingerprint := calculateSchemaFingerprint(schemaStr)

	versions, exists := sr.subjects[subject]
	if !exists {
		return nil, nil
	}

	for _, schema := range versions {
		if schema.Fingerprint == fingerprint {
			return schema, nil
		}
	}

	return nil, nil
}

// =============================================================================
// SCHEMA DELETION
// =============================================================================

// DeleteSchemaVersion soft-deletes a specific schema version
func (sr *SchemaRegistry) DeleteSchemaVersion(subject string, version int) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.closed {
		return ErrSchemaRegistryClosed
	}

	versions, exists := sr.subjects[subject]
	if !exists {
		return ErrSubjectNotFound
	}

	schema, exists := versions[version]
	if !exists {
		return ErrVersionNotFound
	}

	// Remove from in-memory caches
	delete(versions, version)
	delete(sr.schemas, schema.ID)
	delete(sr.validators, schema.ID)

	// If no versions left, remove subject
	if len(versions) == 0 {
		delete(sr.subjects, subject)
		delete(sr.subjectConfigs, subject)
	}

	// Delete from disk
	subjectDir := filepath.Join(sr.config.DataDir, subject)
	versionFile := filepath.Join(subjectDir, fmt.Sprintf("v%d.json", version))
	if err := os.Remove(versionFile); err != nil && !os.IsNotExist(err) {
		sr.logger.Warn("failed to delete schema file", "file", versionFile, "error", err)
	}

	sr.logger.Info("schema version deleted", "subject", subject, "version", version)

	return nil
}

// DeleteSubject deletes all versions of a subject
func (sr *SchemaRegistry) DeleteSubject(subject string) ([]int, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.closed {
		return nil, ErrSchemaRegistryClosed
	}

	versions, exists := sr.subjects[subject]
	if !exists {
		return nil, ErrSubjectNotFound
	}

	// Collect deleted version numbers
	deletedVersions := make([]int, 0, len(versions))
	for v, schema := range versions {
		deletedVersions = append(deletedVersions, v)
		delete(sr.schemas, schema.ID)
		delete(sr.validators, schema.ID)
	}
	sort.Ints(deletedVersions)

	// Remove subject
	delete(sr.subjects, subject)
	delete(sr.subjectConfigs, subject)

	// Remove directory
	subjectDir := filepath.Join(sr.config.DataDir, subject)
	if err := os.RemoveAll(subjectDir); err != nil {
		sr.logger.Warn("failed to delete subject directory", "dir", subjectDir, "error", err)
	}

	sr.logger.Info("subject deleted", "subject", subject, "versions", deletedVersions)

	return deletedVersions, nil
}

// =============================================================================
// COMPATIBILITY MANAGEMENT
// =============================================================================

// SetCompatibilityMode sets the compatibility mode for a subject
func (sr *SchemaRegistry) SetCompatibilityMode(subject string, mode CompatibilityMode) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.closed {
		return ErrSchemaRegistryClosed
	}

	if !mode.IsValid() {
		return fmt.Errorf("invalid compatibility mode: %s", mode)
	}

	config, exists := sr.subjectConfigs[subject]
	if !exists {
		config = &SubjectConfig{
			Subject:           subject,
			Compatibility:     mode,
			ValidationEnabled: sr.config.ValidationEnabled,
		}
		sr.subjectConfigs[subject] = config
	} else {
		config.Compatibility = mode
	}

	// Persist config
	if err := sr.persistSubjectConfig(config); err != nil {
		return fmt.Errorf("failed to persist subject config: %w", err)
	}

	sr.logger.Info("compatibility mode updated", "subject", subject, "mode", mode)

	return nil
}

// GetCompatibilityMode gets the compatibility mode for a subject
func (sr *SchemaRegistry) GetCompatibilityMode(subject string) CompatibilityMode {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	return sr.getCompatibilityMode(subject)
}

// getCompatibilityMode gets compatibility without lock (internal use)
func (sr *SchemaRegistry) getCompatibilityMode(subject string) CompatibilityMode {
	config, exists := sr.subjectConfigs[subject]
	if exists {
		return config.Compatibility
	}
	return sr.config.GlobalCompatibility
}

// SetGlobalCompatibility sets the default compatibility mode
func (sr *SchemaRegistry) SetGlobalCompatibility(mode CompatibilityMode) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.closed {
		return ErrSchemaRegistryClosed
	}

	if !mode.IsValid() {
		return fmt.Errorf("invalid compatibility mode: %s", mode)
	}

	sr.config.GlobalCompatibility = mode

	// Persist global config
	if err := sr.persistGlobalConfig(); err != nil {
		return fmt.Errorf("failed to persist global config: %w", err)
	}

	sr.logger.Info("global compatibility mode updated", "mode", mode)

	return nil
}

// GetGlobalCompatibility gets the default compatibility mode
func (sr *SchemaRegistry) GetGlobalCompatibility() CompatibilityMode {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	return sr.config.GlobalCompatibility
}

// TestCompatibility tests if a new schema is compatible with existing versions
// without actually registering it
func (sr *SchemaRegistry) TestCompatibility(subject, schemaStr string, version int) (bool, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	if sr.closed {
		return false, ErrSchemaRegistryClosed
	}

	// Validate schema syntax first
	if _, err := NewJSONSchemaValidator(schemaStr); err != nil {
		return false, fmt.Errorf("%w: %w", ErrInvalidSchema, err)
	}

	versions, exists := sr.subjects[subject]
	if !exists || len(versions) == 0 {
		// No existing schemas - always compatible
		return true, nil
	}

	var targetSchema *Schema
	if version == -1 {
		// Test against latest
		latestVersion := sr.getLatestVersionNumber(subject)
		targetSchema = versions[latestVersion]
	} else {
		targetSchema = versions[version]
		if targetSchema == nil {
			return false, ErrVersionNotFound
		}
	}

	compat := sr.getCompatibilityMode(subject)
	if compat == CompatibilityNone {
		return true, nil
	}

	err := sr.checkCompatibility(targetSchema.Schema, schemaStr, compat)
	return err == nil, err
}

// =============================================================================
// MESSAGE VALIDATION
// =============================================================================

// ValidateMessage validates a message value against the latest schema for a subject
//
// VALIDATION FLOW:
//  1. Get latest schema for subject
//  2. Get/compile validator for schema
//  3. Validate message JSON against schema
//  4. Return detailed error on failure
//
// RETURNS:
//   - nil if message is valid
//   - ErrValidationFailed with details if invalid
//   - ErrSubjectNotFound if no schema registered
func (sr *SchemaRegistry) ValidateMessage(subject string, message []byte) error {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	if sr.closed {
		return ErrSchemaRegistryClosed
	}

	// Check if validation is enabled for this subject
	if !sr.isValidationEnabled(subject) {
		return nil
	}

	// Get latest schema
	versions, exists := sr.subjects[subject]
	if !exists || len(versions) == 0 {
		// No schema registered - skip validation
		return nil
	}

	latestVersion := sr.getLatestVersionNumber(subject)
	schema := versions[latestVersion]

	// Get validator
	validator, exists := sr.validators[schema.ID]
	if !exists {
		// Compile validator if not cached
		var err error
		validator, err = NewJSONSchemaValidator(schema.Schema)
		if err != nil {
			return fmt.Errorf("failed to compile schema: %w", err)
		}
		// Don't update cache here (would need write lock)
	}

	// Validate message
	if err := validator.Validate(message); err != nil {
		return fmt.Errorf("%w: %w", ErrValidationFailed, err)
	}

	return nil
}

// ValidateMessageWithSchemaID validates a message against a specific schema ID
func (sr *SchemaRegistry) ValidateMessageWithSchemaID(schemaID int64, message []byte) error {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	if sr.closed {
		return ErrSchemaRegistryClosed
	}

	schema, exists := sr.schemas[schemaID]
	if !exists {
		return ErrSchemaNotFound
	}

	validator, exists := sr.validators[schemaID]
	if !exists {
		var err error
		validator, err = NewJSONSchemaValidator(schema.Schema)
		if err != nil {
			return fmt.Errorf("failed to compile schema: %w", err)
		}
	}

	if err := validator.Validate(message); err != nil {
		return fmt.Errorf("%w: %w", ErrValidationFailed, err)
	}

	return nil
}

// IsValidationEnabled checks if validation is enabled for a subject
func (sr *SchemaRegistry) IsValidationEnabled(subject string) bool {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	return sr.isValidationEnabled(subject)
}

// isValidationEnabled without lock
func (sr *SchemaRegistry) isValidationEnabled(subject string) bool {
	config, exists := sr.subjectConfigs[subject]
	if exists {
		return config.ValidationEnabled
	}
	return sr.config.ValidationEnabled
}

// SetValidationEnabled enables/disables validation for a subject
func (sr *SchemaRegistry) SetValidationEnabled(subject string, enabled bool) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.closed {
		return ErrSchemaRegistryClosed
	}

	config, exists := sr.subjectConfigs[subject]
	if !exists {
		config = &SubjectConfig{
			Subject:           subject,
			Compatibility:     sr.config.GlobalCompatibility,
			ValidationEnabled: enabled,
		}
		sr.subjectConfigs[subject] = config
	} else {
		config.ValidationEnabled = enabled
	}

	// Persist config
	if err := sr.persistSubjectConfig(config); err != nil {
		return fmt.Errorf("failed to persist subject config: %w", err)
	}

	sr.logger.Info("validation setting updated", "subject", subject, "enabled", enabled)

	return nil
}

// =============================================================================
// SCHEMA ID HELPERS
// =============================================================================

// GetSchemaIDForSubject returns the latest schema ID for a subject
// Useful for producers to include schema-id in message headers
func (sr *SchemaRegistry) GetSchemaIDForSubject(subject string) (int64, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	if sr.closed {
		return 0, ErrSchemaRegistryClosed
	}

	versions, exists := sr.subjects[subject]
	if !exists || len(versions) == 0 {
		return 0, ErrSubjectNotFound
	}

	latestVersion := sr.getLatestVersionNumber(subject)
	return versions[latestVersion].ID, nil
}

// =============================================================================
// STATISTICS
// =============================================================================

// SchemaRegistryStats contains registry statistics
type SchemaRegistryStats struct {
	SubjectCount        int               `json:"subjectCount"`
	TotalSchemas        int               `json:"totalSchemas"`
	GlobalCompatibility CompatibilityMode `json:"globalCompatibility"`
	ValidationEnabled   bool              `json:"validationEnabled"`
	CachedValidators    int               `json:"cachedValidators"`
	Subjects            []SubjectStats    `json:"subjects"`
}

// SubjectStats contains per-subject statistics
type SubjectStats struct {
	Subject           string            `json:"subject"`
	VersionCount      int               `json:"versionCount"`
	LatestVersion     int               `json:"latestVersion"`
	LatestSchemaID    int64             `json:"latestSchemaId"`
	Compatibility     CompatibilityMode `json:"compatibility"`
	ValidationEnabled bool              `json:"validationEnabled"`
}

// Stats returns registry statistics
func (sr *SchemaRegistry) Stats() SchemaRegistryStats {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	stats := SchemaRegistryStats{
		SubjectCount:        len(sr.subjects),
		TotalSchemas:        len(sr.schemas),
		GlobalCompatibility: sr.config.GlobalCompatibility,
		ValidationEnabled:   sr.config.ValidationEnabled,
		CachedValidators:    len(sr.validators),
		Subjects:            make([]SubjectStats, 0, len(sr.subjects)),
	}

	for subject, versions := range sr.subjects {
		latestVersion := 0
		var latestSchemaID int64
		for v, schema := range versions {
			if v > latestVersion {
				latestVersion = v
				latestSchemaID = schema.ID
			}
		}

		subjectStats := SubjectStats{
			Subject:           subject,
			VersionCount:      len(versions),
			LatestVersion:     latestVersion,
			LatestSchemaID:    latestSchemaID,
			Compatibility:     sr.getCompatibilityMode(subject),
			ValidationEnabled: sr.isValidationEnabled(subject),
		}
		stats.Subjects = append(stats.Subjects, subjectStats)
	}

	// Sort subjects for consistent output
	sort.Slice(stats.Subjects, func(i, j int) bool {
		return stats.Subjects[i].Subject < stats.Subjects[j].Subject
	})

	return stats
}

// =============================================================================
// PERSISTENCE
// =============================================================================

// persistSchema writes a schema to disk
func (sr *SchemaRegistry) persistSchema(schema *Schema) error {
	// Create subject directory
	subjectDir := filepath.Join(sr.config.DataDir, schema.Subject)
	if err := os.MkdirAll(subjectDir, 0o755); err != nil {
		return err
	}

	// Write schema file
	filename := filepath.Join(subjectDir, fmt.Sprintf("v%d.json", schema.Version))
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0o600)
}

// persistSubjectConfig writes subject config to disk
func (sr *SchemaRegistry) persistSubjectConfig(config *SubjectConfig) error {
	subjectDir := filepath.Join(sr.config.DataDir, config.Subject)
	if err := os.MkdirAll(subjectDir, 0o755); err != nil {
		return err
	}

	filename := filepath.Join(subjectDir, "config.json")
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0o600)
}

// persistGlobalConfig writes global config to disk
func (sr *SchemaRegistry) persistGlobalConfig() error {
	config := struct {
		GlobalCompatibility CompatibilityMode `json:"globalCompatibility"`
		ValidationEnabled   bool              `json:"validationEnabled"`
	}{
		GlobalCompatibility: sr.config.GlobalCompatibility,
		ValidationEnabled:   sr.config.ValidationEnabled,
	}

	filename := filepath.Join(sr.config.DataDir, "config.json")
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0o600)
}

// loadSchemas loads all schemas from disk on startup
func (sr *SchemaRegistry) loadSchemas() error {
	// Load global config first
	sr.loadGlobalConfig()

	// List subject directories
	entries, err := os.ReadDir(sr.config.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No schemas yet
		}
		return err
	}

	var maxID int64

	for _, entry := range entries {
		if !entry.IsDir() {
			continue // Skip files like config.json
		}

		subject := entry.Name()
		subjectDir := filepath.Join(sr.config.DataDir, subject)

		// Load subject config
		sr.loadSubjectConfig(subject, subjectDir)

		// Load schema versions
		files, err := os.ReadDir(subjectDir)
		if err != nil {
			sr.logger.Warn("failed to read subject directory", "subject", subject, "error", err)
			continue
		}

		sr.subjects[subject] = make(map[int]*Schema)

		for _, file := range files {
			if file.IsDir() || !strings.HasPrefix(file.Name(), "v") || !strings.HasSuffix(file.Name(), ".json") {
				continue
			}

			// Parse version number from filename
			versionStr := strings.TrimPrefix(strings.TrimSuffix(file.Name(), ".json"), "v")
			version, err := strconv.Atoi(versionStr)
			if err != nil {
				continue
			}

			// Read schema file
			filePath := filepath.Join(subjectDir, file.Name())
			data, err := os.ReadFile(filePath)
			if err != nil {
				sr.logger.Warn("failed to read schema file", "file", filePath, "error", err)
				continue
			}

			var schema Schema
			if err := json.Unmarshal(data, &schema); err != nil {
				sr.logger.Warn("failed to parse schema file", "file", filePath, "error", err)
				continue
			}

			// Update caches
			sr.schemas[schema.ID] = &schema
			sr.subjects[subject][version] = &schema

			// Track max ID
			if schema.ID > maxID {
				maxID = schema.ID
			}

			// Compile validator
			validator, err := NewJSONSchemaValidator(schema.Schema)
			if err != nil {
				sr.logger.Warn("failed to compile schema", "subject", subject, "version", version, "error", err)
			} else {
				sr.validators[schema.ID] = validator
			}
		}
	}

	// Set next ID
	sr.nextID = maxID + 1

	return nil
}

// loadGlobalConfig loads global config from disk
func (sr *SchemaRegistry) loadGlobalConfig() {
	filename := filepath.Join(sr.config.DataDir, "config.json")
	data, err := os.ReadFile(filename)
	if err != nil {
		return // Use defaults
	}

	var config struct {
		GlobalCompatibility CompatibilityMode `json:"globalCompatibility"`
		ValidationEnabled   bool              `json:"validationEnabled"`
	}

	if err := json.Unmarshal(data, &config); err != nil {
		return
	}

	if config.GlobalCompatibility.IsValid() {
		sr.config.GlobalCompatibility = config.GlobalCompatibility
	}
	sr.config.ValidationEnabled = config.ValidationEnabled
}

// loadSubjectConfig loads subject config from disk
func (sr *SchemaRegistry) loadSubjectConfig(subject, subjectDir string) {
	filename := filepath.Join(subjectDir, "config.json")
	data, err := os.ReadFile(filename)
	if err != nil {
		return // Use defaults
	}

	var config SubjectConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return
	}

	sr.subjectConfigs[subject] = &config
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// getLatestVersionNumber returns the highest version number for a subject
// Must be called with lock held
func (sr *SchemaRegistry) getLatestVersionNumber(subject string) int {
	versions := sr.subjects[subject]
	if versions == nil {
		return 0
	}

	latest := 0
	for v := range versions {
		if v > latest {
			latest = v
		}
	}
	return latest
}

// calculateSchemaFingerprint computes a hash of the normalized schema
// Used for deduplication - identical schemas have identical fingerprints
func calculateSchemaFingerprint(schemaStr string) string {
	// Normalize by parsing and re-serializing (removes whitespace differences)
	var parsed interface{}
	if err := json.Unmarshal([]byte(schemaStr), &parsed); err != nil {
		// If parsing fails, use raw string
		return fmt.Sprintf("%x", schemaStr)
	}

	normalized, err := json.Marshal(parsed)
	if err != nil {
		return fmt.Sprintf("%x", schemaStr)
	}

	// Use simple hash (could use SHA256 for production)
	return fmt.Sprintf("%x", calculateCRC(normalized))
}

// calculateCRC computes CRC32 checksum
func calculateCRC(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
}

// =============================================================================
// COMPATIBILITY CHECKING
// =============================================================================

// checkCompatibility verifies schema compatibility based on the mode
//
// JSON SCHEMA COMPATIBILITY RULES:
//
// BACKWARD compatibility (new schema reads old data):
//   - Can add optional properties (new fields with default or not required)
//   - Can remove required properties (old data still valid)
//   - Cannot add required properties (old data would be invalid)
//   - Cannot change property types (old data would be invalid)
//
// FORWARD compatibility (old schema reads new data):
//   - Can add required properties (new data has more than needed)
//   - Can remove optional properties (old schema ignores extra)
//   - Cannot remove required properties (new data missing required)
//
// FULL compatibility:
//   - Only add/remove optional properties
func (sr *SchemaRegistry) checkCompatibility(oldSchemaStr, newSchemaStr string, mode CompatibilityMode) error {
	if mode == CompatibilityNone {
		return nil
	}

	oldSchema, err := parseJSONSchema(oldSchemaStr)
	if err != nil {
		return fmt.Errorf("failed to parse old schema: %w", err)
	}

	newSchema, err := parseJSONSchema(newSchemaStr)
	if err != nil {
		return fmt.Errorf("failed to parse new schema: %w", err)
	}

	switch mode {
	case CompatibilityBackward:
		return checkBackwardCompatibility(oldSchema, newSchema)
	case CompatibilityForward:
		return checkForwardCompatibility(oldSchema, newSchema)
	case CompatibilityFull:
		if err := checkBackwardCompatibility(oldSchema, newSchema); err != nil {
			return err
		}
		return checkForwardCompatibility(oldSchema, newSchema)
	default:
		return nil
	}
}

// JSONSchemaObject represents a parsed JSON Schema for compatibility checking
type JSONSchemaObject struct {
	Type                 string                       `json:"type"`
	Properties           map[string]*JSONSchemaObject `json:"properties"`
	Required             []string                     `json:"required"`
	AdditionalProperties *bool                        `json:"additionalProperties,omitempty"`
	Items                *JSONSchemaObject            `json:"items"`
	Enum                 []interface{}                `json:"enum"`
	Default              interface{}                  `json:"default"`
}

// parseJSONSchema parses a JSON Schema string into a struct
func parseJSONSchema(schemaStr string) (*JSONSchemaObject, error) {
	var schema JSONSchemaObject
	if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

// checkBackwardCompatibility verifies new schema can read old data
func checkBackwardCompatibility(oldSchema, newSchema *JSONSchemaObject) error {
	// Check for new required properties (breaks backward compatibility)
	oldRequired := make(map[string]bool)
	for _, r := range oldSchema.Required {
		oldRequired[r] = true
	}

	for _, r := range newSchema.Required {
		if !oldRequired[r] {
			// New required property - check if it exists in old schema
			if oldSchema.Properties != nil {
				if _, exists := oldSchema.Properties[r]; !exists {
					return fmt.Errorf("new required property '%s' not in old schema - breaks backward compatibility", r)
				}
			} else {
				return fmt.Errorf("new required property '%s' added - breaks backward compatibility", r)
			}
		}
	}

	// Check for type changes on existing properties
	if oldSchema.Properties != nil && newSchema.Properties != nil {
		for name, oldProp := range oldSchema.Properties {
			if newProp, exists := newSchema.Properties[name]; exists {
				if oldProp.Type != newProp.Type && newProp.Type != "" && oldProp.Type != "" {
					return fmt.Errorf("property '%s' type changed from '%s' to '%s' - breaks backward compatibility",
						name, oldProp.Type, newProp.Type)
				}
			}
		}
	}

	return nil
}

// checkForwardCompatibility verifies old schema can read new data
func checkForwardCompatibility(oldSchema, newSchema *JSONSchemaObject) error {
	// Check for removed required properties (old schema expects them)
	newRequired := make(map[string]bool)
	for _, r := range newSchema.Required {
		newRequired[r] = true
	}

	for _, r := range oldSchema.Required {
		if !newRequired[r] {
			// Required property removed - check if it's still in properties
			if newSchema.Properties != nil {
				if _, exists := newSchema.Properties[r]; !exists {
					return fmt.Errorf("required property '%s' removed - breaks forward compatibility", r)
				}
			} else {
				return fmt.Errorf("required property '%s' removed - breaks forward compatibility", r)
			}
		}
	}

	return nil
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Close shuts down the schema registry
func (sr *SchemaRegistry) Close() error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.closed {
		return nil
	}

	sr.closed = true
	sr.logger.Info("schema registry closed")

	return nil
}
