// =============================================================================
// PROTOBUF SCHEMA VALIDATOR (#28)
// =============================================================================
//
// WHAT IS PROTOCOL BUFFERS?
// Protocol Buffers (Protobuf) is Google's language-neutral serialization format.
// Unlike JSON, Protobuf schemas define a binary wire format — smaller and faster.
//
// WHY SUPPORT PROTOBUF SCHEMAS?
//
//   JSON vs Protobuf:
//   ┌──────────────────┬──────────────────┬──────────────────────────────────┐
//   │ Aspect           │ JSON Schema      │ Protobuf Schema                  │
//   ├──────────────────┼──────────────────┼──────────────────────────────────┤
//   │ Format           │ JSON text        │ Binary wire format               │
//   │ Size             │ Larger (verbose) │ 3-10x smaller                    │
//   │ Parse speed      │ Slower           │ 10-100x faster                   │
//   │ Schema language  │ JSON             │ .proto IDL                       │
//   │ Type safety      │ Runtime          │ Compile-time (code generation)   │
//   │ Human readable   │ ✓ Yes            │ ✗ Binary (need .proto to decode) │
//   │ Use case         │ APIs, config     │ High-throughput, microservices   │
//   └──────────────────┴──────────────────┴──────────────────────────────────┘
//
// PROTOBUF IN MESSAGE QUEUES:
//   - Kafka: Native Protobuf support via Confluent Schema Registry
//   - Pulsar: Built-in Protobuf schema support
//   - gRPC: Uses Protobuf natively (goqueue already uses gRPC!)
//   - goqueue: Store .proto schemas, validate syntax, enable registration
//
// SCHEMA EXAMPLE:
//   syntax = "proto3";
//   message Order {
//     string order_id = 1;
//     string customer = 2;
//     double amount = 3;
//     repeated Item items = 4;
//   }
//   message Item {
//     string name = 1;
//     int32 quantity = 2;
//   }
//
// CURRENT IMPLEMENTATION (Phase 1):
//   ✓ Schema registration: Store .proto file content
//   ✓ Syntax validation: Basic proto file syntax checking
//   ✓ Compatibility: Field number preservation checks
//   ✗ Message validation: Requires protobuf compiler (future)
//   ✗ Code generation: Out of scope (client-side responsibility)
//
// FUTURE (Phase 2):
//   - Integrate protobuf compiler for full descriptor generation
//   - Wire format validation against compiled descriptors
//   - Cross-reference support (imports between .proto files)
//
// =============================================================================

package broker

import (
	"fmt"
	"strings"
)

// =============================================================================
// PROTOBUF SCHEMA VALIDATOR
// =============================================================================

// ProtobufSchemaValidator validates Protobuf schema (.proto file) syntax
// and provides schema-level validation.
//
// NOTE: Full message validation (validating binary Protobuf wire format)
// requires a compiled FileDescriptor, which needs the protobuf compiler.
// This validator handles schema registration and syntax checking.
type ProtobufSchemaValidator struct {
	// rawSchema is the original .proto file content
	rawSchema string

	// messageName is the primary message type name (extracted from schema)
	messageName string
}

// NewProtobufSchemaValidator creates a validator from a .proto file string.
//
// VALIDATION CHECKS:
//  1. Non-empty schema
//  2. Contains syntax declaration (proto2 or proto3)
//  3. Contains at least one message definition
//  4. Basic structural validation (matched braces)
//
// EXAMPLE:
//
//	validator, err := NewProtobufSchemaValidator(`
//	  syntax = "proto3";
//	  message Order {
//	    string order_id = 1;
//	    double amount = 2;
//	  }
//	`)
func NewProtobufSchemaValidator(schemaStr string) (*ProtobufSchemaValidator, error) {
	if strings.TrimSpace(schemaStr) == "" {
		return nil, fmt.Errorf("empty protobuf schema")
	}

	// Validate syntax declaration
	if !strings.Contains(schemaStr, `syntax`) {
		return nil, fmt.Errorf("protobuf schema must contain syntax declaration (e.g., syntax = \"proto3\")")
	}

	if !strings.Contains(schemaStr, `"proto3"`) && !strings.Contains(schemaStr, `"proto2"`) {
		return nil, fmt.Errorf("protobuf schema must specify proto2 or proto3 syntax")
	}

	// Validate at least one message definition exists
	if !strings.Contains(schemaStr, "message ") {
		return nil, fmt.Errorf("protobuf schema must contain at least one message definition")
	}

	// Extract primary message name
	messageName := extractProtobufMessageName(schemaStr)
	if messageName == "" {
		return nil, fmt.Errorf("could not extract message name from protobuf schema")
	}

	// Basic brace matching validation
	if err := validateBraceMatching(schemaStr); err != nil {
		return nil, fmt.Errorf("protobuf schema syntax error: %w", err)
	}

	return &ProtobufSchemaValidator{
		rawSchema:   schemaStr,
		messageName: messageName,
	}, nil
}

// Validate checks if a message could be valid Protobuf wire format.
//
// CURRENT LIMITATION:
//
//	Without a compiled FileDescriptor, we cannot fully validate Protobuf
//	wire format. We perform a basic check:
//	- Non-empty message
//	- Starts with a valid wire type field tag
//
// For full validation, integrate google.golang.org/protobuf/proto library
// with compiled descriptors.
func (v *ProtobufSchemaValidator) Validate(message []byte) error {
	if len(message) == 0 {
		return fmt.Errorf("empty protobuf message")
	}

	// =========================================================================
	// PROTOBUF WIRE FORMAT BASICS
	// =========================================================================
	//
	// Each field is: (field_number << 3) | wire_type
	//
	// Wire types:
	//   0: Varint (int32, int64, bool, enum)
	//   1: 64-bit (fixed64, double)
	//   2: Length-delimited (string, bytes, embedded messages)
	//   5: 32-bit (fixed32, float)
	//
	// First byte encodes: field 1, wire type = low 3 bits
	// Valid first byte wire types: 0, 1, 2, 5
	// =========================================================================

	firstByte := message[0]
	wireType := firstByte & 0x07 // Low 3 bits

	switch wireType {
	case 0, 1, 2, 5:
		// Valid wire types
		return nil
	default:
		return fmt.Errorf("invalid protobuf wire type %d in first field", wireType)
	}
}

// MessageName returns the primary message type name from the schema.
func (v *ProtobufSchemaValidator) MessageName() string {
	return v.messageName
}

// =============================================================================
// PROTOBUF HELPER FUNCTIONS
// =============================================================================

// extractProtobufMessageName extracts the first message name from a .proto file.
//
// INPUT:
//
//	syntax = "proto3";
//	message Order {
//	  string id = 1;
//	}
//
// OUTPUT: "Order"
func extractProtobufMessageName(schema string) string {
	lines := strings.Split(schema, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "message ") {
			// Extract "message Foo {" → "Foo"
			parts := strings.Fields(trimmed)
			if len(parts) >= 2 {
				name := strings.TrimSuffix(parts[1], "{")
				name = strings.TrimSpace(name)
				if name != "" {
					return name
				}
			}
		}
	}
	return ""
}

// validateBraceMatching checks that all braces are properly matched.
func validateBraceMatching(schema string) error {
	depth := 0
	for i, ch := range schema {
		switch ch {
		case '{':
			depth++
		case '}':
			depth--
			if depth < 0 {
				return fmt.Errorf("unexpected closing brace at position %d", i)
			}
		}
	}
	if depth != 0 {
		return fmt.Errorf("unclosed brace: %d opening braces without matching closing braces", depth)
	}
	return nil
}
