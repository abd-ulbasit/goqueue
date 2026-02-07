// =============================================================================
// MESSAGE ENCODING TESTS
// =============================================================================
//
// These tests verify that our binary message format works correctly:
//   - Encode/Decode roundtrip preserves all fields
//   - CRC detects corruption
//   - Invalid messages are rejected
//   - Edge cases are handled (empty key/value, max sizes)
//
// =============================================================================

package storage

import (
	"bytes"
	"testing"
	"time"
)

func TestMessage_EncodeDecodeRoundtrip(t *testing.T) {
	testCases := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{
			name:  "simple message",
			key:   []byte("user-123"),
			value: []byte(`{"action": "purchase", "amount": 99.99}`),
		},
		{
			name:  "empty key",
			key:   nil,
			value: []byte("message without routing key"),
		},
		{
			name:  "empty value",
			key:   []byte("key"),
			value: []byte{},
		},
		{
			name:  "binary value",
			key:   []byte("binary"),
			value: []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
		},
		{
			name:  "unicode content",
			key:   []byte("greeting"),
			value: []byte("Hello, 世界! 🌍"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create message
			original := &Message{
				Offset:    12345,
				Timestamp: time.Now().UnixNano(),
				Key:       tc.key,
				Value:     tc.value,
				Flags:     0,
			}

			// Encode
			encoded, err := original.Encode()
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// Verify magic bytes
			if encoded[0] != MagicByte1 || encoded[1] != MagicByte2 {
				t.Errorf("Magic bytes incorrect: got 0x%02x 0x%02x, want 0x%02x 0x%02x",
					encoded[0], encoded[1], MagicByte1, MagicByte2)
			}

			// Decode
			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// Verify fields match
			if decoded.Offset != original.Offset {
				t.Errorf("Offset mismatch: got %d, want %d", decoded.Offset, original.Offset)
			}
			if decoded.Timestamp != original.Timestamp {
				t.Errorf("Timestamp mismatch: got %d, want %d", decoded.Timestamp, original.Timestamp)
			}
			if !bytes.Equal(decoded.Key, original.Key) {
				t.Errorf("Key mismatch: got %v, want %v", decoded.Key, original.Key)
			}
			if !bytes.Equal(decoded.Value, original.Value) {
				t.Errorf("Value mismatch: got %v, want %v", decoded.Value, original.Value)
			}
			if decoded.Flags != original.Flags {
				t.Errorf("Flags mismatch: got %d, want %d", decoded.Flags, original.Flags)
			}
		})
	}
}

func TestMessage_Size(t *testing.T) {
	msg := &Message{
		Key:   []byte("key"),   // 3 bytes
		Value: []byte("value"), // 5 bytes
	}
	expectedSize := HeaderSize + 3 + 5 // 34 + 3 + 5 = 42 (updated for 34-byte header)

	if msg.Size() != expectedSize {
		t.Errorf("Size() = %d, want %d", msg.Size(), expectedSize)
	}

	// Verify encoded size matches
	encoded, _ := msg.Encode()
	if len(encoded) != expectedSize {
		t.Errorf("Encoded size = %d, want %d", len(encoded), expectedSize)
	}
}

// =============================================================================
// HEADERS ENCODING/DECODING
// =============================================================================

func TestMessage_Headers_EncodeDecodeRoundtrip(t *testing.T) {
	testCases := []struct {
		name    string
		headers map[string]string
	}{
		{
			name:    "no headers",
			headers: nil,
		},
		{
			name:    "empty headers map",
			headers: map[string]string{},
		},
		{
			name: "single header",
			headers: map[string]string{
				"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			},
		},
		{
			name: "multiple headers",
			headers: map[string]string{
				"traceparent":    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
				"correlation_id": "user-123-order-456",
				"x-custom":       "custom-value",
			},
		},
		{
			name: "unicode header values",
			headers: map[string]string{
				"greeting": "Hello, 世界!",
				"emoji":    "🚀",
			},
		},
		{
			name: "empty value header",
			headers: map[string]string{
				"empty-value": "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create message with headers
			original := &Message{
				Offset:    12345,
				Timestamp: 1234567890,
				Key:       []byte("test-key"),
				Value:     []byte("test-value"),
				Flags:     0,
				Priority:  PriorityNormal,
				Headers:   tc.headers,
			}

			// Encode
			encoded, err := original.Encode()
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// Decode
			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// Verify headers match
			if len(decoded.Headers) != len(tc.headers) {
				t.Errorf("Headers count mismatch: got %d, want %d",
					len(decoded.Headers), len(tc.headers))
			}

			for k, v := range tc.headers {
				if decoded.Headers[k] != v {
					t.Errorf("Header %q mismatch: got %q, want %q",
						k, decoded.Headers[k], v)
				}
			}
		})
	}
}

func TestMessage_Headers_SizeCalculation(t *testing.T) {
	// Message with headers
	headers := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // 55 chars
	}

	msg := &Message{
		Key:     []byte("key"),   // 3 bytes
		Value:   []byte("value"), // 5 bytes
		Headers: headers,
	}

	// Header encoding: Count(2) + [KeyLen(2) + Key + ValLen(2) + Val] × N
	// "traceparent" = 11 bytes, value = 55 bytes
	// Total headers: 2 (count) + 2 + 11 + 2 + 55 = 72 bytes
	expectedHeadersSize := 2 // count prefix
	for k, v := range headers {
		expectedHeadersSize += 2 + len(k) + 2 + len(v)
	}
	expectedSize := HeaderSize + 3 + 5 + expectedHeadersSize // 34 + 3 + 5 + 72 = 114

	if msg.Size() != expectedSize {
		t.Errorf("Size() = %d, want %d", msg.Size(), expectedSize)
	}

	// Verify encoded size matches Size()
	encoded, _ := msg.Encode()
	if len(encoded) != msg.Size() {
		t.Errorf("Encoded size = %d, want Size() = %d", len(encoded), msg.Size())
	}
}

func TestNewMessageWithHeaders(t *testing.T) {
	headers := map[string]string{
		"traceparent": "00-abc123-def456-01",
		"custom":      "value",
	}

	msg := NewMessageWithHeaders([]byte("key"), []byte("value"), headers)

	// Verify headers are copied (not referenced)
	if len(msg.Headers) != 2 {
		t.Errorf("Headers count mismatch: got %d, want 2", len(msg.Headers))
	}

	// Modify original - should not affect message
	headers["new"] = "should-not-appear"

	if _, exists := msg.Headers["new"]; exists {
		t.Error("Headers should be copied, not referenced")
	}

	// Verify specific headers
	if msg.Headers["traceparent"] != "00-abc123-def456-01" {
		t.Errorf("traceparent header mismatch")
	}
	if msg.Headers["custom"] != "value" {
		t.Errorf("custom header mismatch")
	}
}

// =============================================================================
// CRC CORRUPTION DETECTION
// =============================================================================

func TestMessage_Decode_DetectsCorruption(t *testing.T) {
	// Create and encode a message
	msg := NewMessage([]byte("key"), []byte("value"))
	msg.Offset = 100

	encoded, err := msg.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Corrupt a byte in the value (after header)
	corruptIdx := HeaderSize + 5 // Somewhere in the value
	encoded[corruptIdx] ^= 0xFF  // Flip all bits

	// Decode should fail with CRC error
	_, err = Decode(encoded)
	if err == nil {
		t.Fatal("Decode should have failed on corrupted message")
	}

	// Should specifically be a corruption error
	if !bytes.Contains([]byte(err.Error()), []byte("CRC")) {
		t.Errorf("Error should mention CRC: %v", err)
	}
}

func TestMessage_Decode_DetectsBadMagic(t *testing.T) {
	// Create valid encoded message
	msg := NewMessage([]byte("key"), []byte("value"))
	encoded, _ := msg.Encode()

	// Corrupt magic bytes
	encoded[0] = 0x00
	encoded[1] = 0x00

	_, err := Decode(encoded)
	if err == nil {
		t.Fatal("Decode should have failed on bad magic bytes")
	}
}

func TestMessage_Decode_RejectsTruncated(t *testing.T) {
	msg := NewMessage([]byte("key"), []byte("value"))
	encoded, _ := msg.Encode()

	// Try decoding truncated data
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"too short for header", encoded[:HeaderSize-1]},
		{"header only, missing body", encoded[:HeaderSize]},
		{"partial body", encoded[:HeaderSize+2]},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode(tc.data)
			if err == nil {
				t.Error("Decode should have failed on truncated data")
			}
		})
	}
}

// =============================================================================
// FLAGS AND HELPERS
// =============================================================================

func TestMessage_Flags(t *testing.T) {
	msg := &Message{}

	// Test compressed flag
	msg.SetCompressed(true)
	if !msg.IsCompressed() {
		t.Error("IsCompressed should be true after SetCompressed(true)")
	}

	msg.SetCompressed(false)
	if msg.IsCompressed() {
		t.Error("IsCompressed should be false after SetCompressed(false)")
	}

	// Test tombstone flag
	msg.SetTombstone(true)
	if !msg.IsTombstone() {
		t.Error("IsTombstone should be true after SetTombstone(true)")
	}

	msg.SetTombstone(false)
	if msg.IsTombstone() {
		t.Error("IsTombstone should be false after SetTombstone(false)")
	}

	// Test flags are independent
	msg.SetCompressed(true)
	msg.SetTombstone(true)
	if !msg.IsCompressed() || !msg.IsTombstone() {
		t.Error("Flags should be independent")
	}
}

func TestNewMessage(t *testing.T) {
	key := []byte("my-key")
	value := []byte("my-value")

	msg := NewMessage(key, value)

	if !bytes.Equal(msg.Key, key) {
		t.Errorf("Key mismatch")
	}
	if !bytes.Equal(msg.Value, value) {
		t.Errorf("Value mismatch")
	}
	if msg.Offset != 0 {
		t.Errorf("Offset should be 0 (assigned by log)")
	}
	if msg.Timestamp == 0 {
		t.Errorf("Timestamp should be set")
	}
	if msg.Flags != 0 {
		t.Errorf("Flags should be 0")
	}
}

// =============================================================================
// SIZE LIMITS
// =============================================================================

func TestMessage_Encode_RejectsOversizedKey(t *testing.T) {
	// Create message with key exceeding MaxKeySize
	bigKey := make([]byte, MaxKeySize+1)
	msg := &Message{
		Key:   bigKey,
		Value: []byte("value"),
	}

	_, err := msg.Encode()
	if err == nil {
		t.Error("Encode should reject oversized key")
	}
}

func TestMessage_Encode_RejectsOversizedValue(t *testing.T) {
	// Create message with value exceeding MaxValueSize
	bigValue := make([]byte, MaxValueSize+1)
	msg := &Message{
		Key:   []byte("key"),
		Value: bigValue,
	}

	_, err := msg.Encode()
	if err == nil {
		t.Error("Encode should reject oversized value")
	}
}

func TestMessage_MaxSizes(t *testing.T) {
	// Test at exactly max sizes (should work)
	maxKey := make([]byte, MaxKeySize)
	for i := range maxKey {
		maxKey[i] = byte(i % 256)
	}

	msg := &Message{
		Key:   maxKey,
		Value: []byte("value"),
	}

	encoded, err := msg.Encode()
	if err != nil {
		t.Fatalf("Encode failed at max key size: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if !bytes.Equal(decoded.Key, maxKey) {
		t.Error("Max size key not preserved through encode/decode")
	}
}

// =============================================================================
// BENCHMARKS
// =============================================================================

func BenchmarkMessage_Encode(b *testing.B) {
	msg := &Message{
		Offset:    12345,
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user-12345678"),
		Value:     make([]byte, 1024), // 1KB payload
		Flags:     0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = msg.Encode()
	}
}

func BenchmarkMessage_Decode(b *testing.B) {
	msg := &Message{
		Offset:    12345,
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user-12345678"),
		Value:     make([]byte, 1024),
		Flags:     0,
	}
	encoded, _ := msg.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Decode(encoded)
	}
}

func BenchmarkCRC32_1KB(b *testing.B) {
	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = calculateCRC(data)
	}
}

func TestMessage_Priority_String(t *testing.T) {
	testCases := []struct {
		priority Priority
		expected string
	}{
		{PriorityCritical, "critical"},
		{PriorityHigh, "high"},
		{PriorityNormal, "normal"},
		{PriorityLow, "low"},
		{PriorityBackground, "background"},
		{Priority(255), "unknown(255)"}, // Invalid priority
	}

	for _, tc := range testCases {
		if tc.priority.String() != tc.expected {
			t.Errorf("Priority(%d).String() = %s, want %s", tc.priority, tc.priority.String(), tc.expected)
		}
	}
}

func TestParsePriority(t *testing.T) {
	testCases := []struct {
		input    string
		expected Priority
	}{
		{"critical", PriorityCritical},
		{"Critical", PriorityCritical},
		{"CRITICAL", PriorityCritical},
		{"high", PriorityHigh},
		{"High", PriorityHigh},
		{"HIGH", PriorityHigh},
		{"normal", PriorityNormal},
		{"Normal", PriorityNormal},
		{"NORMAL", PriorityNormal},
		{"", PriorityNormal}, // Empty defaults to normal
		{"low", PriorityLow},
		{"Low", PriorityLow},
		{"LOW", PriorityLow},
		{"background", PriorityBackground},
		{"Background", PriorityBackground},
		{"BACKGROUND", PriorityBackground},
		{"invalid", PriorityNormal}, // Invalid defaults to normal
	}

	for _, tc := range testCases {
		if ParsePriority(tc.input) != tc.expected {
			t.Errorf("ParsePriority(%q) = %v, want %v", tc.input, ParsePriority(tc.input), tc.expected)
		}
	}
}

func TestMessage_ControlRecordMethods(t *testing.T) {
	// Test regular message
	msg := NewMessage([]byte("key"), []byte("value"))
	if msg.IsControlRecord() {
		t.Error("Regular message should not be control record")
	}
	if msg.IsTransactionCommit() {
		t.Error("Regular message should not be transaction commit")
	}
	if msg.IsTransactionAbort() {
		t.Error("Regular message should not be transaction abort")
	}

	// Test commit control record
	commitMsg := NewCommitControlRecord(100, 12345, 1, "txn-123")
	if !commitMsg.IsControlRecord() {
		t.Error("Commit control record should be control record")
	}
	if !commitMsg.IsTransactionCommit() {
		t.Error("Commit control record should be transaction commit")
	}
	if commitMsg.IsTransactionAbort() {
		t.Error("Commit control record should not be transaction abort")
	}

	// Test abort control record
	abortMsg := NewAbortControlRecord(200, 12345, 1, "txn-123")
	if !abortMsg.IsControlRecord() {
		t.Error("Abort control record should be control record")
	}
	if abortMsg.IsTransactionCommit() {
		t.Error("Abort control record should not be transaction commit")
	}
	if !abortMsg.IsTransactionAbort() {
		t.Error("Abort control record should be transaction abort")
	}

	// Verify control record payload extraction
	payload, err := commitMsg.GetControlRecordPayload()
	if err != nil {
		t.Fatalf("GetControlRecordPayload failed: %v", err)
	}
	if payload.ProducerID != 12345 {
		t.Errorf("ProducerID = %d, want 12345", payload.ProducerID)
	}
	if payload.Epoch != 1 {
		t.Errorf("Epoch = %d, want 1", payload.Epoch)
	}
	if payload.TransactionalID != "txn-123" {
		t.Errorf("TransactionalID = %s, want txn-123", payload.TransactionalID)
	}

	// Verify payload extraction fails for regular message
	_, err = msg.GetControlRecordPayload()
	if err == nil {
		t.Error("GetControlRecordPayload should fail for regular message")
	}
}

func TestNewMessageWithPriority(t *testing.T) {
	key := []byte("key")
	value := []byte("value")

	// Test with valid priority
	msg := NewMessageWithPriority(key, value, PriorityHigh)
	if !bytes.Equal(msg.Key, key) {
		t.Error("Key not set correctly")
	}
	if !bytes.Equal(msg.Value, value) {
		t.Error("Value not set correctly")
	}
	if msg.Priority != PriorityHigh {
		t.Errorf("Priority = %v, want PriorityHigh", msg.Priority)
	}
	if msg.Offset != 0 {
		t.Error("Offset should be 0 initially")
	}
	if msg.Timestamp == 0 {
		t.Error("Timestamp should be set")
	}

	// Test with invalid priority (should fallback to Normal)
	invalidMsg := NewMessageWithPriority(key, value, Priority(255))
	if invalidMsg.Priority != PriorityNormal {
		t.Errorf("Invalid priority should fallback to Normal, got %v", invalidMsg.Priority)
	}
}

func BenchmarkCRC32_64KB(b *testing.B) {
	data := make([]byte, 64*1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = calculateCRC(data)
	}
}
