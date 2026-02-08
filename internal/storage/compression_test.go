package storage

import (
	"bytes"
	"crypto/rand"
	"strings"
	"testing"
)

// =============================================================================
// COMPRESSION TESTS
// =============================================================================
//
// WHY THESE TESTS?
// Compression is on the critical path for every message. We need to verify:
//   1. Round-trip: compress → decompress returns original data
//   2. Small data: messages below threshold are NOT compressed
//   3. Incompressible data: random bytes shouldn't grow after "compression"
//   4. Empty data: edge case handling
//   5. Large data: compression ratio is reasonable
//   6. Type identification: each codec reports correct type
//

// TestGzipCompressor_RoundTrip verifies compress→decompress returns original data.
func TestGzipCompressor_RoundTrip(t *testing.T) {
	codec := NewGzipCompressor()

	// Use a large enough payload to exceed MinCompressionSize
	original := []byte(strings.Repeat(`{"event":"order.created","data":{"id":"ord-123","amount":99.99}}`, 10))

	compressed, err := codec.Compress(original)
	if err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	// Should actually be compressed (smaller)
	if len(compressed) >= len(original) {
		t.Errorf("compressed size (%d) should be smaller than original (%d)", len(compressed), len(original))
	}

	decompressed, err := codec.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}

	if !bytes.Equal(decompressed, original) {
		t.Errorf("round trip failed: decompressed data doesn't match original")
	}
}

// TestFlateCompressor_RoundTrip verifies DEFLATE compress→decompress.
func TestFlateCompressor_RoundTrip(t *testing.T) {
	codec := NewFlateCompressor()

	original := []byte(strings.Repeat(`{"key":"value","timestamp":"2024-01-01T00:00:00Z"}`, 10))

	compressed, err := codec.Compress(original)
	if err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	if len(compressed) >= len(original) {
		t.Errorf("compressed size (%d) should be smaller than original (%d)", len(compressed), len(original))
	}

	decompressed, err := codec.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}

	if !bytes.Equal(decompressed, original) {
		t.Error("round trip failed: decompressed data doesn't match original")
	}
}

// TestNoopCompressor_PassThrough verifies no-op codec returns data unchanged.
func TestNoopCompressor_PassThrough(t *testing.T) {
	codec := NewNoopCompressor()

	original := []byte("hello world")

	compressed, err := codec.Compress(original)
	if err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	if !bytes.Equal(compressed, original) {
		t.Error("noop compress should return data unchanged")
	}

	decompressed, err := codec.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}

	if !bytes.Equal(decompressed, original) {
		t.Error("noop decompress should return data unchanged")
	}
}

// TestCompression_SkipsSmallData verifies messages below threshold are not compressed.
func TestCompression_SkipsSmallData(t *testing.T) {
	codecs := []struct {
		name  string
		codec Compressor
	}{
		{"gzip", NewGzipCompressor()},
		{"flate", NewFlateCompressor()},
	}

	// 10 bytes - well below MinCompressionSize (64)
	smallData := []byte("tiny")

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.codec.Compress(smallData)
			if err != nil {
				t.Fatalf("compress failed: %v", err)
			}

			// Should return data unchanged (not compressed)
			if !bytes.Equal(result, smallData) {
				t.Errorf("%s should skip compression for small data (%d bytes < %d threshold)",
					tc.name, len(smallData), MinCompressionSize)
			}
		})
	}
}

// TestCompression_IncompressibleData verifies random data doesn't grow.
func TestCompression_IncompressibleData(t *testing.T) {
	// Random data is incompressible - compression should detect this
	// and return original data unchanged
	randomData := make([]byte, 1024)
	if _, err := rand.Read(randomData); err != nil {
		t.Fatal(err)
	}

	codecs := []struct {
		name  string
		codec Compressor
	}{
		{"gzip", NewGzipCompressor()},
		{"flate", NewFlateCompressor()},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := tc.codec.Compress(randomData)
			if err != nil {
				t.Fatalf("compress failed: %v", err)
			}

			// For incompressible data, should return original (safety check)
			if len(compressed) > len(randomData) {
				t.Errorf("%s compressed size (%d) > original (%d) for random data",
					tc.name, len(compressed), len(randomData))
			}
		})
	}
}

// TestCompression_EmptyData handles edge case of empty payload.
func TestCompression_EmptyData(t *testing.T) {
	codecs := []struct {
		name  string
		codec Compressor
	}{
		{"gzip", NewGzipCompressor()},
		{"flate", NewFlateCompressor()},
		{"noop", NewNoopCompressor()},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.codec.Compress([]byte{})
			if err != nil {
				t.Fatalf("compress empty data failed: %v", err)
			}
			if len(result) != 0 {
				t.Errorf("expected empty result for empty input, got %d bytes", len(result))
			}
		})
	}
}

// TestCompression_Type verifies each codec reports the correct type.
func TestCompression_Type(t *testing.T) {
	tests := []struct {
		codec    Compressor
		expected CompressionType
	}{
		{NewGzipCompressor(), CompressionGzip},
		{NewFlateCompressor(), CompressionFlate},
		{NewNoopCompressor(), CompressionNone},
	}

	for _, tc := range tests {
		if tc.codec.Type() != tc.expected {
			t.Errorf("expected type %v, got %v", tc.expected, tc.codec.Type())
		}
	}
}

// TestNewCompressor_Factory verifies the factory function.
func TestNewCompressor_Factory(t *testing.T) {
	tests := []struct {
		ct       CompressionType
		expected CompressionType
	}{
		{CompressionNone, CompressionNone},
		{CompressionGzip, CompressionGzip},
		{CompressionFlate, CompressionFlate},
		{CompressionType(99), CompressionNone}, // Unknown → noop
	}

	for _, tc := range tests {
		codec := NewCompressor(tc.ct)
		if codec.Type() != tc.expected {
			t.Errorf("NewCompressor(%v): expected type %v, got %v",
				tc.ct, tc.expected, codec.Type())
		}
	}
}

// TestParseCompressionType verifies string parsing.
func TestParseCompressionType(t *testing.T) {
	tests := []struct {
		input    string
		expected CompressionType
	}{
		{"gzip", CompressionGzip},
		{"GZIP", CompressionGzip},
		{"flate", CompressionFlate},
		{"deflate", CompressionFlate},
		{"none", CompressionNone},
		{"", CompressionNone},
		{"snappy", CompressionNone}, // Unsupported → none
	}

	for _, tc := range tests {
		result := ParseCompressionType(tc.input)
		if result != tc.expected {
			t.Errorf("ParseCompressionType(%q): expected %v, got %v",
				tc.input, tc.expected, result)
		}
	}
}

// TestCompressValue_Integration verifies compression with Message struct.
func TestCompressValue_Integration(t *testing.T) {
	codec := NewGzipCompressor()

	// Create a message with compressible JSON payload
	original := []byte(strings.Repeat(`{"event":"order.created","data":{"id":"ord-123","amount":99.99}}`, 10))
	msg := &Message{
		Value: make([]byte, len(original)),
		Flags: 0,
	}
	copy(msg.Value, original)

	// Compress
	compressed, err := CompressValue(msg, codec)
	if err != nil {
		t.Fatalf("CompressValue failed: %v", err)
	}

	if !compressed {
		t.Error("expected compression to be applied")
	}

	// FlagCompressed should be set
	if msg.Flags&FlagCompressed == 0 {
		t.Error("FlagCompressed should be set after compression")
	}

	// Value should be smaller
	if len(msg.Value) >= len(original) {
		t.Errorf("compressed value (%d) should be smaller than original (%d)",
			len(msg.Value), len(original))
	}

	// Decompress
	err = DecompressValue(msg, codec)
	if err != nil {
		t.Fatalf("DecompressValue failed: %v", err)
	}

	// FlagCompressed should be cleared
	if msg.Flags&FlagCompressed != 0 {
		t.Error("FlagCompressed should be cleared after decompression")
	}

	// Value should match original
	if !bytes.Equal(msg.Value, original) {
		t.Error("decompressed value doesn't match original")
	}
}

// TestCompressValue_SkipsSmallMessages verifies small messages aren't compressed.
func TestCompressValue_SkipsSmallMessages(t *testing.T) {
	codec := NewGzipCompressor()
	msg := &Message{
		Value: []byte("tiny"),
		Flags: 0,
	}

	compressed, err := CompressValue(msg, codec)
	if err != nil {
		t.Fatalf("CompressValue failed: %v", err)
	}

	if compressed {
		t.Error("should NOT compress small messages")
	}

	if msg.Flags&FlagCompressed != 0 {
		t.Error("FlagCompressed should NOT be set for small messages")
	}
}

// TestCompressValue_NilCodec verifies nil codec is handled.
func TestCompressValue_NilCodec(t *testing.T) {
	msg := &Message{
		Value: []byte(strings.Repeat("test", 100)),
		Flags: 0,
	}

	compressed, err := CompressValue(msg, nil)
	if err != nil {
		t.Fatalf("CompressValue with nil codec should not error: %v", err)
	}

	if compressed {
		t.Error("should NOT compress with nil codec")
	}
}

// TestDecompressValue_NotCompressed verifies uncompressed messages pass through.
func TestDecompressValue_NotCompressed(t *testing.T) {
	codec := NewGzipCompressor()
	original := []byte("hello world")
	msg := &Message{
		Value: original,
		Flags: 0, // FlagCompressed NOT set
	}

	err := DecompressValue(msg, codec)
	if err != nil {
		t.Fatalf("DecompressValue should not error for uncompressed message: %v", err)
	}

	if !bytes.Equal(msg.Value, original) {
		t.Error("value should be unchanged for uncompressed message")
	}
}

// TestCompressionType_String verifies string representation.
func TestCompressionType_String(t *testing.T) {
	tests := []struct {
		ct       CompressionType
		expected string
	}{
		{CompressionNone, "none"},
		{CompressionGzip, "gzip"},
		{CompressionFlate, "flate"},
		{CompressionType(99), "unknown(99)"},
	}

	for _, tc := range tests {
		if tc.ct.String() != tc.expected {
			t.Errorf("expected %q, got %q", tc.expected, tc.ct.String())
		}
	}
}

// BenchmarkCompression_Gzip benchmarks gzip compression throughput.
func BenchmarkCompression_Gzip(b *testing.B) {
	codec := NewGzipCompressor()
	data := []byte(strings.Repeat(`{"event":"order.created","data":{"id":"ord-123","amount":99.99,"items":[{"sku":"ABC","qty":2}]}}`, 5))

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		compressed, err := codec.Compress(data)
		if err != nil {
			b.Fatal(err)
		}
		_, err = codec.Decompress(compressed)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompression_Flate benchmarks DEFLATE compression throughput.
func BenchmarkCompression_Flate(b *testing.B) {
	codec := NewFlateCompressor()
	data := []byte(strings.Repeat(`{"event":"order.created","data":{"id":"ord-123","amount":99.99,"items":[{"sku":"ABC","qty":2}]}}`, 5))

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		compressed, err := codec.Compress(data)
		if err != nil {
			b.Fatal(err)
		}
		_, err = codec.Decompress(compressed)
		if err != nil {
			b.Fatal(err)
		}
	}
}
