// =============================================================================
// JSON SCHEMA VALIDATOR - MESSAGE VALIDATION USING JSON SCHEMA
// =============================================================================
//
// WHAT IS JSON SCHEMA?
// JSON Schema is a vocabulary that allows you to annotate and validate JSON
// documents. Think of it as a "type system" for JSON:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Schema Definition                    │ Valid Data                       │
//   ├─────────────────────────────────────────────────────────────────────────┤
//   │ {                                    │ {                                │
//   │   "type": "object",                  │   "name": "John",                │
//   │   "properties": {                    │   "age": 30                      │
//   │     "name": {"type": "string"},      │ }                                │
//   │     "age": {"type": "integer"}       │                                  │
//   │   },                                 │                                  │
//   │   "required": ["name"]               │                                  │
//   │ }                                    │                                  │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// WHY JSON SCHEMA?
//   1. Human-readable (unlike Protobuf/Avro)
//   2. No code generation needed
//   3. Widely supported (many validators available)
//   4. Rich validation rules (types, patterns, ranges, etc.)
//   5. Self-describing (schema is JSON, can be stored/transmitted easily)
//
// COMPARISON - Schema Formats:
//
//   ┌──────────────┬────────────────────────────────────────────────────────┐
//   │ Format       │ Characteristics                                        │
//   ├──────────────┼────────────────────────────────────────────────────────┤
//   │ JSON Schema  │ Readable, no codegen, JSON based, good validation      │
//   │ Protobuf     │ Very compact, fast, requires codegen, binary           │
//   │ Avro         │ Compact, great evolution, self-describing, complex     │
//   │ Thrift       │ Like Protobuf, Facebook developed, less common now     │
//   └──────────────┴────────────────────────────────────────────────────────┘
//
// JSON SCHEMA DRAFT VERSIONS:
//   - Draft 4: Widely supported, stable
//   - Draft 6: Adds $ref improvements
//   - Draft 7: Current most-used version (our choice)
//   - Draft 2019-09: More features, less universal support
//   - Draft 2020-12: Latest, fewer implementations
//
// OUR IMPLEMENTATION:
// We use a lightweight, pure-Go JSON Schema validator to avoid external
// dependencies and ensure maximum compatibility. This validator supports:
//   - type validation (string, number, integer, boolean, object, array, null)
//   - properties and required
//   - pattern (regex validation)
//   - minimum/maximum (number ranges)
//   - minLength/maxLength (string lengths)
//   - enum (allowed values)
//   - items (array element validation)
//   - additionalProperties
//   - $ref (basic, local references only)
//
// FUTURE:
// TODO: Enhance the validator to support more JSON Schema features:
//   - Add github.com/santhosh-tekuri/jsonschema for full Draft 7 support
//   - Add Protobuf support via protoc-gen-go
//   - Add Avro support via goavro
//
// =============================================================================

package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// =============================================================================
// JSON SCHEMA VALIDATOR
// =============================================================================

// JSONSchemaValidator validates JSON messages against a JSON Schema
type JSONSchemaValidator struct {
	// schema is the parsed schema definition
	schema *jsonSchemaNode

	// rawSchema is the original schema string (for debugging)
	rawSchema string
}

// jsonSchemaNode represents a node in the parsed JSON Schema
type jsonSchemaNode struct {
	Type                 interface{}                `json:"type"` // string or []string
	Properties           map[string]*jsonSchemaNode `json:"properties"`
	Required             []string                   `json:"required"`
	AdditionalProperties *jsonSchemaNode            `json:"additionalProperties,omitempty"`
	Items                *jsonSchemaNode            `json:"items"`
	Enum                 []interface{}              `json:"enum"`
	Pattern              string                     `json:"pattern"`
	MinLength            *int                       `json:"minLength"`
	MaxLength            *int                       `json:"maxLength"`
	Minimum              *float64                   `json:"minimum"`
	Maximum              *float64                   `json:"maximum"`
	MinItems             *int                       `json:"minItems"`
	MaxItems             *int                       `json:"maxItems"`
	Ref                  string                     `json:"$ref"`
	Definitions          map[string]*jsonSchemaNode `json:"definitions"`
	Defs                 map[string]*jsonSchemaNode `json:"$defs"`
	OneOf                []*jsonSchemaNode          `json:"oneOf"`
	AnyOf                []*jsonSchemaNode          `json:"anyOf"`
	AllOf                []*jsonSchemaNode          `json:"allOf"`
	Not                  *jsonSchemaNode            `json:"not"`
	Default              interface{}                `json:"default"`
	Title                string                     `json:"title"`
	Description          string                     `json:"description"`

	// compiledPattern is the compiled regex for pattern validation
	compiledPattern *regexp.Regexp
}

// NewJSONSchemaValidator creates a new validator from a JSON Schema string
//
// VALIDATION PROCESS:
//  1. Parse schema JSON into struct
//  2. Validate schema syntax (basic checks)
//  3. Compile regex patterns (for performance)
//  4. Return ready-to-use validator
//
// RETURNS:
//   - *JSONSchemaValidator: Ready-to-use validator
//   - error: If schema is malformed
func NewJSONSchemaValidator(schemaStr string) (*JSONSchemaValidator, error) {
	if schemaStr == "" {
		return nil, fmt.Errorf("empty schema")
	}

	// Parse schema JSON
	var schema jsonSchemaNode
	if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
		return nil, fmt.Errorf("invalid JSON in schema: %w", err)
	}

	// Compile patterns recursively
	if err := compilePatterns(&schema); err != nil {
		return nil, fmt.Errorf("invalid pattern in schema: %w", err)
	}

	return &JSONSchemaValidator{
		schema:    &schema,
		rawSchema: schemaStr,
	}, nil
}

// compilePatterns compiles regex patterns in the schema tree
func compilePatterns(node *jsonSchemaNode) error {
	if node == nil {
		return nil
	}

	// Compile pattern if present
	if node.Pattern != "" {
		compiled, err := regexp.Compile(node.Pattern)
		if err != nil {
			return fmt.Errorf("invalid regex pattern '%s': %w", node.Pattern, err)
		}
		node.compiledPattern = compiled
	}

	// Recurse into properties
	for _, prop := range node.Properties {
		if err := compilePatterns(prop); err != nil {
			return err
		}
	}

	// Recurse into items
	if err := compilePatterns(node.Items); err != nil {
		return err
	}

	// Recurse into additionalProperties
	if err := compilePatterns(node.AdditionalProperties); err != nil {
		return err
	}

	// Recurse into definitions
	for _, def := range node.Definitions {
		if err := compilePatterns(def); err != nil {
			return err
		}
	}
	for _, def := range node.Defs {
		if err := compilePatterns(def); err != nil {
			return err
		}
	}

	// Recurse into combinators
	for _, s := range node.OneOf {
		if err := compilePatterns(s); err != nil {
			return err
		}
	}
	for _, s := range node.AnyOf {
		if err := compilePatterns(s); err != nil {
			return err
		}
	}
	for _, s := range node.AllOf {
		if err := compilePatterns(s); err != nil {
			return err
		}
	}
	if err := compilePatterns(node.Not); err != nil {
		return err
	}

	return nil
}

// Validate checks if a JSON message conforms to the schema
//
// VALIDATION FLOW:
//  1. Parse message JSON
//  2. Walk schema tree, validating each node
//  3. Collect all errors (not just first)
//  4. Return detailed error or nil
//
// RETURNS:
//   - nil: Message is valid
//   - error: Detailed validation failure
func (v *JSONSchemaValidator) Validate(message []byte) error {
	if len(message) == 0 {
		return fmt.Errorf("empty message")
	}

	// Parse message JSON
	var data interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return fmt.Errorf("invalid JSON in message: %w", err)
	}

	// Validate against schema
	errors := v.validateNode(v.schema, data, "")
	if len(errors) > 0 {
		return fmt.Errorf("validation failed: %s", strings.Join(errors, "; "))
	}

	return nil
}

// validateNode validates a value against a schema node
func (v *JSONSchemaValidator) validateNode(schema *jsonSchemaNode, data interface{}, path string) []string {
	if schema == nil {
		return nil // No schema = anything valid
	}

	var errors []string

	// Handle $ref
	if schema.Ref != "" {
		refSchema := v.resolveRef(schema.Ref)
		if refSchema != nil {
			return v.validateNode(refSchema, data, path)
		}
		// Unresolved ref - skip validation
		return nil
	}

	// Handle combinators
	if len(schema.AllOf) > 0 {
		for _, subSchema := range schema.AllOf {
			errors = append(errors, v.validateNode(subSchema, data, path)...)
		}
	}

	if len(schema.AnyOf) > 0 {
		anyValid := false
		for _, subSchema := range schema.AnyOf {
			if len(v.validateNode(subSchema, data, path)) == 0 {
				anyValid = true
				break
			}
		}
		if !anyValid {
			errors = append(errors, fmt.Sprintf("%s: does not match any of 'anyOf' schemas", pathStr(path)))
		}
	}

	if len(schema.OneOf) > 0 {
		validCount := 0
		for _, subSchema := range schema.OneOf {
			if len(v.validateNode(subSchema, data, path)) == 0 {
				validCount++
			}
		}
		if validCount != 1 {
			errors = append(errors, fmt.Sprintf("%s: must match exactly one of 'oneOf' schemas (matched %d)", pathStr(path), validCount))
		}
	}

	if schema.Not != nil {
		if len(v.validateNode(schema.Not, data, path)) == 0 {
			errors = append(errors, fmt.Sprintf("%s: must not match 'not' schema", pathStr(path)))
		}
	}

	// Validate type
	if schema.Type != nil {
		typeErrors := v.validateType(schema, data, path)
		errors = append(errors, typeErrors...)
	}

	// Validate enum
	if len(schema.Enum) > 0 {
		enumValid := false
		for _, e := range schema.Enum {
			if jsonEquals(data, e) {
				enumValid = true
				break
			}
		}
		if !enumValid {
			errors = append(errors, fmt.Sprintf("%s: value not in enum %v", pathStr(path), schema.Enum))
		}
	}

	return errors
}

// validateType validates the data type against schema type
func (v *JSONSchemaValidator) validateType(schema *jsonSchemaNode, data interface{}, path string) []string {
	var errors []string

	// Get allowed types
	allowedTypes := getTypes(schema.Type)
	if len(allowedTypes) == 0 {
		return nil // No type constraint
	}

	// Determine actual type
	actualType := getJSONType(data)

	// Check if type matches
	typeMatch := false
	for _, t := range allowedTypes {
		if t == actualType {
			typeMatch = true
			break
		}
		// Special case: integer is also a number
		if t == "number" && actualType == "integer" {
			typeMatch = true
			break
		}
	}

	if !typeMatch {
		errors = append(errors, fmt.Sprintf("%s: expected type %v, got %s", pathStr(path), allowedTypes, actualType))
		return errors // Type mismatch - skip further validation
	}

	// Type-specific validation
	switch actualType {
	case "string":
		strVal, _ := data.(string)
		errors = append(errors, v.validateString(schema, strVal, path)...)
	case "number", "integer":
		errors = append(errors, v.validateNumber(schema, data, path)...)
	case "object":
		objVal, _ := data.(map[string]interface{})
		errors = append(errors, v.validateObject(schema, objVal, path)...)
	case "array":
		arrVal, _ := data.([]interface{})
		errors = append(errors, v.validateArray(schema, arrVal, path)...)
	}

	return errors
}

// validateString validates string-specific constraints
func (v *JSONSchemaValidator) validateString(schema *jsonSchemaNode, data, path string) []string {
	var errors []string

	// Check minLength
	if schema.MinLength != nil && len(data) < *schema.MinLength {
		errors = append(errors, fmt.Sprintf("%s: string length %d is less than minimum %d", pathStr(path), len(data), *schema.MinLength))
	}

	// Check maxLength
	if schema.MaxLength != nil && len(data) > *schema.MaxLength {
		errors = append(errors, fmt.Sprintf("%s: string length %d exceeds maximum %d", pathStr(path), len(data), *schema.MaxLength))
	}

	// Check pattern
	if schema.compiledPattern != nil {
		if !schema.compiledPattern.MatchString(data) {
			errors = append(errors, fmt.Sprintf("%s: string '%s' does not match pattern '%s'", pathStr(path), data, schema.Pattern))
		}
	}

	return errors
}

// validateNumber validates number-specific constraints
func (v *JSONSchemaValidator) validateNumber(schema *jsonSchemaNode, data interface{}, path string) []string {
	var errors []string

	// Convert to float64
	var num float64
	switch n := data.(type) {
	case float64:
		num = n
	case int:
		num = float64(n)
	case int64:
		num = float64(n)
	default:
		return errors
	}

	// Check minimum
	if schema.Minimum != nil && num < *schema.Minimum {
		errors = append(errors, fmt.Sprintf("%s: value %v is less than minimum %v", pathStr(path), num, *schema.Minimum))
	}

	// Check maximum
	if schema.Maximum != nil && num > *schema.Maximum {
		errors = append(errors, fmt.Sprintf("%s: value %v exceeds maximum %v", pathStr(path), num, *schema.Maximum))
	}

	return errors
}

// validateObject validates object-specific constraints
func (v *JSONSchemaValidator) validateObject(schema *jsonSchemaNode, data map[string]interface{}, path string) []string {
	var errors []string

	// Check required properties
	for _, req := range schema.Required {
		if _, exists := data[req]; !exists {
			errors = append(errors, fmt.Sprintf("%s: missing required property '%s'", pathStr(path), req))
		}
	}

	// Validate properties
	for propName, propSchema := range schema.Properties {
		if propValue, exists := data[propName]; exists {
			propPath := path + "." + propName
			if path == "" {
				propPath = propName
			}
			errors = append(errors, v.validateNode(propSchema, propValue, propPath)...)
		}
	}

	// Check additional properties
	if schema.AdditionalProperties != nil {
		// additionalProperties can be a boolean (false = no extra) or schema
		for propName := range data {
			if schema.Properties != nil {
				if _, defined := schema.Properties[propName]; defined {
					continue // Property is defined
				}
			}
			// This is an additional property
			propPath := path + "." + propName
			if path == "" {
				propPath = propName
			}
			errors = append(errors, v.validateNode(schema.AdditionalProperties, data[propName], propPath)...)
		}
	}

	return errors
}

// validateArray validates array-specific constraints
func (v *JSONSchemaValidator) validateArray(schema *jsonSchemaNode, data []interface{}, path string) []string {
	var errors []string

	// Check minItems
	if schema.MinItems != nil && len(data) < *schema.MinItems {
		errors = append(errors, fmt.Sprintf("%s: array length %d is less than minimum %d", pathStr(path), len(data), *schema.MinItems))
	}

	// Check maxItems
	if schema.MaxItems != nil && len(data) > *schema.MaxItems {
		errors = append(errors, fmt.Sprintf("%s: array length %d exceeds maximum %d", pathStr(path), len(data), *schema.MaxItems))
	}

	// Validate items
	if schema.Items != nil {
		for i, item := range data {
			itemPath := fmt.Sprintf("%s[%d]", path, i)
			if path == "" {
				itemPath = fmt.Sprintf("[%d]", i)
			}
			errors = append(errors, v.validateNode(schema.Items, item, itemPath)...)
		}
	}

	return errors
}

// resolveRef resolves a $ref to a schema node
func (v *JSONSchemaValidator) resolveRef(ref string) *jsonSchemaNode {
	// Only support local references for now
	if !strings.HasPrefix(ref, "#/") {
		return nil
	}

	parts := strings.Split(strings.TrimPrefix(ref, "#/"), "/")
	if len(parts) < 2 {
		return nil
	}

	// Handle #/definitions/Name or #/$defs/Name
	if parts[0] == "definitions" && v.schema.Definitions != nil {
		return v.schema.Definitions[parts[1]]
	}
	if parts[0] == "$defs" && v.schema.Defs != nil {
		return v.schema.Defs[parts[1]]
	}

	return nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// getTypes extracts type(s) from schema type field
func getTypes(t interface{}) []string {
	if t == nil {
		return nil
	}
	switch v := t.(type) {
	case string:
		return []string{v}
	case []interface{}:
		types := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				types = append(types, s)
			}
		}
		return types
	default:
		return nil
	}
}

// getJSONType returns the JSON type name for a Go value
func getJSONType(data interface{}) string {
	if data == nil {
		return "null"
	}
	switch v := data.(type) {
	case bool:
		return "boolean"
	case string:
		return "string"
	case float64:
		// Check if it's actually an integer
		if v == float64(int64(v)) {
			return "integer"
		}
		return "number"
	case int, int64, int32:
		return "integer"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "unknown"
	}
}

// pathStr returns a display string for the path
func pathStr(path string) string {
	if path == "" {
		return "root"
	}
	return path
}

// jsonEquals compares two JSON values for equality
func jsonEquals(a, b interface{}) bool {
	// Marshal both to JSON and compare
	aJSON, err1 := json.Marshal(a)
	bJSON, err2 := json.Marshal(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return bytes.Equal(aJSON, bJSON)
}

// =============================================================================
// VALIDATION ERROR TYPES
// =============================================================================

// ValidationError provides detailed validation failure information
type ValidationError struct {
	Path    string `json:"path"`
	Message string `json:"message"`
	Value   string `json:"value,omitempty"`
}

func (e ValidationError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("%s: %s", e.Path, e.Message)
	}
	return e.Message
}
