// =============================================================================
// SCHEMA VALIDATOR INTERFACE
// =============================================================================
//
// WHY AN INTERFACE?
// The schema registry started with JSON Schema only. To support multiple formats
// (JSON Schema, Protobuf, Avro), we need a common interface that all validators
// implement.
//
// VALIDATOR HIERARCHY:
//
//   ┌─────────────────────────┐
//   │   SchemaValidator       │ ← Interface: Validate([]byte) error
//   │   (common contract)     │
//   └────────┬────────────────┘
//            │
//            ├── JSONSchemaValidator     ← Validates JSON messages against JSON Schema
//            │                             (existing, full implementation)
//            │
//            ├── ProtobufSchemaValidator ← Validates .proto schema syntax
//            │                             (schema registration + syntax check)
//            │
//            └── AvroSchemaValidator     ← Validates Avro schema structure
//                                         (schema registration + structure check)
//
// COMPARISON - Schema Format Support:
//   ┌─────────────────┬───────┬───────────┬──────┐
//   │ System          │ JSON  │ Protobuf  │ Avro │
//   ├─────────────────┼───────┼───────────┼──────┤
//   │ Confluent SR    │  ✓    │    ✓      │  ✓   │
//   │ AWS Glue SR     │  ✓    │    ✓      │  ✓   │
//   │ Apicurio        │  ✓    │    ✓      │  ✓   │
//   │ goqueue (now)   │  ✓    │    ✓      │  ✓   │
//   └─────────────────┴───────┴───────────┴──────┘
//
// =============================================================================

package broker

// =============================================================================
// SCHEMA TYPE CONSTANTS
// =============================================================================

const (
	// SchemaTypeJSON identifies JSON Schema format.
	SchemaTypeJSON = "JSON"

	// SchemaTypeProtobuf identifies Protocol Buffers schema format.
	SchemaTypeProtobuf = "PROTOBUF"

	// SchemaTypeAvro identifies Apache Avro schema format.
	SchemaTypeAvro = "AVRO"
)

// =============================================================================
// SCHEMA VALIDATOR INTERFACE
// =============================================================================

// SchemaValidator is the common interface for all schema validators.
//
// IMPLEMENTATIONS:
//   - JSONSchemaValidator: Full JSON Schema Draft-07 validation
//   - ProtobufSchemaValidator: Protobuf schema syntax validation
//   - AvroSchemaValidator: Avro schema structure validation
//
// MESSAGE VALIDATION:
//   - JSON: Validates JSON messages against JSON Schema
//   - Protobuf: Validates that message is valid Protocol Buffers wire format
//     (requires compiled descriptors — currently validates schema only)
//   - Avro: Validates that message is valid Avro binary/JSON
//     (requires Avro codec — currently validates schema only)
type SchemaValidator interface {
	// Validate checks a message against the schema.
	// Returns nil if valid, descriptive error if invalid.
	Validate(message []byte) error
}

// IsValidSchemaType returns true if the schema type is supported.
func IsValidSchemaType(schemaType string) bool {
	switch schemaType {
	case SchemaTypeJSON, SchemaTypeProtobuf, SchemaTypeAvro:
		return true
	default:
		return false
	}
}
