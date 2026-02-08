// =============================================================================
// MESSAGE COMPRESSION - REDUCING STORAGE AND NETWORK COSTS
// =============================================================================
//
// WHAT IS THIS?
// A codec layer that compresses/decompresses message payloads (the Value field).
// The existing FlagCompressed bit in the message format was defined but never
// wired to an actual compression codec. This module provides that codec.
//
// WHY COMPRESSION MATTERS FOR MESSAGE QUEUES:
//
//   Without compression:
//     - 1M messages/sec × 1KB avg = 1 GB/sec disk I/O
//     - 7 days retention = 604 TB (ouch)
//     - Network replication doubles the bandwidth
//
//   With compression (typical 3-5x ratio for JSON):
//     - 1 GB/sec → 200-300 MB/sec disk I/O
//     - 604 TB → 120-200 TB retention
//     - Replication bandwidth halved
//
// COMPARISON - How other systems handle compression:
//
//   | System   | Codecs Supported     | Default    | Level      |
//   |----------|----------------------|------------|------------|
//   | Kafka    | None, GZIP, Snappy,  | None       | Producer   |
//   |          | LZ4, Zstandard       |            | (per batch)|
//   | RabbitMQ | None (plugin-based)  | None       | N/A        |
//   | SQS      | None                 | N/A        | N/A        |
//   | Pulsar   | LZ4, Zstandard,      | None       | Producer   |
//   |          | Snappy, ZLIB         |            |            |
//   | goqueue  | None, GZIP, Zlib     | None       | Per-topic  |
//   |          | (extensible)         |            |            |
//
// WHERE COMPRESSION HAPPENS:
//
//   ┌──────────┐  raw value   ┌────────────────┐  compressed   ┌──────────┐
//   │ Producer │─────────────►│ Compress(value) │─────────────►│ Storage  │
//   │          │              │ Set FlagCompressed              │ (on disk)│
//   └──────────┘              └────────────────┘               └──────────┘
//
//   ┌──────────┐  raw value   ┌──────────────────┐  compressed  ┌──────────┐
//   │ Consumer │◄────────────│ Decompress(value) │◄────────────│ Storage  │
//   │          │              │ Check FlagCompressed             │ (on disk)│
//   └──────────┘              └──────────────────┘              └──────────┘
//
// DESIGN DECISIONS:
//
//   1. COMPRESS VALUE ONLY (not key, not headers)
//      Keys are typically small (user IDs, partition keys) and used for routing.
//      Compressing them would add latency to partition assignment.
//      Headers are small metadata. Only the value payload benefits from compression.
//
//   2. CODEC STORED IN RESERVED BYTE (future)
//      Currently we use FlagCompressed to indicate "compressed", and the codec
//      type is configured per-topic. In the future, we could use the Reserved
//      byte in the header to store the codec ID, allowing mixed codecs.
//
//   3. MINIMUM SIZE THRESHOLD
//      Don't compress messages smaller than 64 bytes. Compression overhead
//      can actually make small messages LARGER (worse than no compression).
//
//   4. USING STDLIB (no external dependencies)
//      Using compress/zlib from Go's stdlib. It provides DEFLATE compression
//      which gives good compression ratios. For production, snappy or lz4
//      would be faster (but require external dependencies).
//
//      | Codec    | Speed        | Ratio    | CPU     | Go Stdlib? |
//      |----------|--------------|----------|---------|------------|
//      | Snappy   | ⚡ Fastest    | ~2x      | Low     | ❌ External |
//      | LZ4      | ⚡ Very Fast  | ~2.5x    | Low     | ❌ External |
//      | Zlib     | 🏃 Medium     | ~3-4x    | Medium  | ✅ Yes     |
//      | Gzip     | 🏃 Medium     | ~3-4x    | Medium  | ✅ Yes     |
//      | Zstd     | 🏃 Fast+Good  | ~3-5x    | Medium  | ❌ External |
//
//      For message queues, Snappy is the industry standard (Kafka default)
//      because throughput matters more than ratio. We use Zlib from stdlib
//      to avoid external dependencies, with the interface designed for
//      easy addition of Snappy/LZ4/Zstd later.
//
// =============================================================================

package storage

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
	"sync"
)

// =============================================================================
// COMPRESSION TYPE
// =============================================================================

// CompressionType identifies the compression algorithm.
type CompressionType int

const (
	// CompressionNone means no compression (messages stored as-is).
	CompressionNone CompressionType = 0

	// CompressionGzip uses gzip compression (Go stdlib).
	// Good compression ratio (~3-4x), moderate CPU.
	// Compatible with most tools (gunzip, zcat, etc.)
	CompressionGzip CompressionType = 1

	// CompressionFlate uses raw DEFLATE compression (Go stdlib).
	// Slightly faster than gzip (no gzip header/footer overhead).
	// Used internally; gzip is the user-facing option.
	CompressionFlate CompressionType = 2
)

// String returns the human-readable name of the compression type.
func (ct CompressionType) String() string {
	switch ct {
	case CompressionNone:
		return "none"
	case CompressionGzip:
		return "gzip"
	case CompressionFlate:
		return "flate"
	default:
		return fmt.Sprintf("unknown(%d)", ct)
	}
}

// ParseCompressionType converts a string to a CompressionType.
// Returns CompressionNone for unrecognized strings (safe default).
func ParseCompressionType(s string) CompressionType {
	switch s {
	case "gzip", "GZIP", "Gzip":
		return CompressionGzip
	case "flate", "FLATE", "Flate", "deflate", "DEFLATE":
		return CompressionFlate
	case "none", "NONE", "None", "":
		return CompressionNone
	default:
		return CompressionNone
	}
}

// =============================================================================
// COMPRESSOR INTERFACE
// =============================================================================
//
// WHY AN INTERFACE?
// Makes it easy to add new codecs (Snappy, LZ4, Zstd) without changing
// any call sites. The broker/topic configuration specifies which codec
// to use, and the codec is resolved once at topic creation time.

// Compressor defines the interface for message compression codecs.
type Compressor interface {
	// Compress compresses the input data.
	// Returns the compressed bytes, or an error.
	Compress(data []byte) ([]byte, error)

	// Decompress decompresses the input data.
	// Returns the original bytes, or an error.
	Decompress(data []byte) ([]byte, error)

	// Type returns the compression type identifier.
	Type() CompressionType
}

// =============================================================================
// MINIMUM COMPRESSION THRESHOLD
// =============================================================================

// MinCompressionSize is the minimum payload size to compress.
// Messages smaller than this are stored uncompressed because:
//
//  1. Compression overhead can make them LARGER
//  2. CPU cost isn't worth it for tiny payloads
//  3. Most small messages are already compact (IDs, status codes)
//
// COMPARISON:
//   - Kafka: No minimum (compresses entire batches, amortizes overhead)
//   - Pulsar: No minimum (batches)
//   - goqueue: 64 bytes minimum (individual message compression)
const MinCompressionSize = 64

// =============================================================================
// GZIP COMPRESSOR
// =============================================================================
//
// WHY GZIP?
//   - Go stdlib (no external dependencies)
//   - Good compression ratio (~3-4x for JSON, ~2x for binary)
//   - Universal format (compatible with gunzip, zcat, browsers)
//   - Configurable compression level (speed vs ratio tradeoff)
//
// PERFORMANCE NOTES:
//   - Uses sync.Pool for writer recycling (avoids allocation per message)
//   - Compression level: BestSpeed (flate.BestSpeed = 1)
//     For message queues, throughput > ratio. BestSpeed gives ~80% of
//     the compression ratio at ~3x the speed of BestCompression.
//

// gzipCompressor implements Compressor using gzip from Go stdlib.
type gzipCompressor struct {
	// writerPool recycles gzip.Writer instances to reduce GC pressure.
	// Creating a new gzip.Writer allocates ~32KB for the compression table.
	// With 100K messages/sec, that's 3.2 GB/sec of garbage without pooling.
	writerPool sync.Pool
}

// NewGzipCompressor creates a new gzip compressor.
func NewGzipCompressor() Compressor {
	return &gzipCompressor{
		writerPool: sync.Pool{
			New: func() interface{} {
				// Use BestSpeed for message queues (throughput > ratio)
				w, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
				return w
			},
		},
	}
}

// Compress compresses data using gzip.
func (c *gzipCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) < MinCompressionSize {
		return data, nil // Too small to benefit from compression
	}

	var buf bytes.Buffer
	buf.Grow(len(data) / 2) // Pre-allocate ~50% of input (typical gzip ratio)

	w := c.writerPool.Get().(*gzip.Writer)
	w.Reset(&buf)

	if _, err := w.Write(data); err != nil {
		c.writerPool.Put(w)
		return nil, fmt.Errorf("gzip compress write: %w", err)
	}

	if err := w.Close(); err != nil {
		c.writerPool.Put(w)
		return nil, fmt.Errorf("gzip compress close: %w", err)
	}

	c.writerPool.Put(w)

	// If compressed is larger than original, return original uncompressed
	// This can happen with already-compressed data (images, encrypted payloads)
	if buf.Len() >= len(data) {
		return data, nil
	}

	return buf.Bytes(), nil
}

// Decompress decompresses gzip data.
func (c *gzipCompressor) Decompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip decompress reader: %w", err)
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("gzip decompress read: %w", err)
	}

	return result, nil
}

// Type returns CompressionGzip.
func (c *gzipCompressor) Type() CompressionType {
	return CompressionGzip
}

// =============================================================================
// FLATE (DEFLATE) COMPRESSOR
// =============================================================================
//
// WHY FLATE?
//   - Same algorithm as gzip but without the header/footer overhead
//   - ~10 bytes less per message than gzip
//   - Good for internal use where compatibility isn't needed
//

// flateCompressor implements Compressor using raw DEFLATE from Go stdlib.
type flateCompressor struct {
	writerPool sync.Pool
}

// NewFlateCompressor creates a new DEFLATE compressor.
func NewFlateCompressor() Compressor {
	return &flateCompressor{
		writerPool: sync.Pool{
			New: func() interface{} {
				w, _ := flate.NewWriter(io.Discard, flate.BestSpeed)
				return w
			},
		},
	}
}

// Compress compresses data using DEFLATE.
func (c *flateCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) < MinCompressionSize {
		return data, nil
	}

	var buf bytes.Buffer
	buf.Grow(len(data) / 2)

	w := c.writerPool.Get().(*flate.Writer)
	w.Reset(&buf)

	if _, err := w.Write(data); err != nil {
		c.writerPool.Put(w)
		return nil, fmt.Errorf("flate compress write: %w", err)
	}

	if err := w.Close(); err != nil {
		c.writerPool.Put(w)
		return nil, fmt.Errorf("flate compress close: %w", err)
	}

	c.writerPool.Put(w)

	if buf.Len() >= len(data) {
		return data, nil
	}

	return buf.Bytes(), nil
}

// Decompress decompresses DEFLATE data.
func (c *flateCompressor) Decompress(data []byte) ([]byte, error) {
	reader := flate.NewReader(bytes.NewReader(data))
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("flate decompress read: %w", err)
	}

	return result, nil
}

// Type returns CompressionFlate.
func (c *flateCompressor) Type() CompressionType {
	return CompressionFlate
}

// =============================================================================
// NO-OP COMPRESSOR
// =============================================================================
//
// WHY: Used when compression is disabled. Avoids nil checks everywhere.
// Strategy pattern: swap the compressor, not scatter if/else statements.

// noopCompressor passes data through unchanged.
type noopCompressor struct{}

// NewNoopCompressor creates a compressor that does nothing (pass-through).
func NewNoopCompressor() Compressor {
	return &noopCompressor{}
}

// Compress returns data unchanged.
func (c *noopCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}

// Decompress returns data unchanged.
func (c *noopCompressor) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

// Type returns CompressionNone.
func (c *noopCompressor) Type() CompressionType {
	return CompressionNone
}

// =============================================================================
// CODEC FACTORY
// =============================================================================

// NewCompressor creates a compressor for the given compression type.
//
// USAGE:
//
//	codec := storage.NewCompressor(storage.CompressionGzip)
//	compressed, err := codec.Compress(payload)
//	original, err := codec.Decompress(compressed)
func NewCompressor(ct CompressionType) Compressor {
	switch ct {
	case CompressionGzip:
		return NewGzipCompressor()
	case CompressionFlate:
		return NewFlateCompressor()
	default:
		return NewNoopCompressor()
	}
}

// =============================================================================
// HELPER FUNCTIONS FOR MESSAGE INTEGRATION
// =============================================================================

// CompressValue compresses a message's Value field and sets FlagCompressed.
// Returns true if compression was applied, false if skipped (too small or
// compressed is larger than original).
//
// FLOW:
//
//	┌───────────────┐   compress   ┌────────────────┐
//	│ msg.Value     │─────────────►│ compressed val │
//	│ "hello..."    │              │ [bytes...]     │
//	│ Flags: 0x00   │              │ Flags: 0x01    │ (FlagCompressed set)
//	└───────────────┘              └────────────────┘
func CompressValue(msg *Message, codec Compressor) (bool, error) {
	if codec == nil || codec.Type() == CompressionNone {
		return false, nil
	}

	if len(msg.Value) < MinCompressionSize {
		return false, nil
	}

	compressed, err := codec.Compress(msg.Value)
	if err != nil {
		return false, fmt.Errorf("compress value: %w", err)
	}

	// Only use compression if it actually reduced size
	if len(compressed) >= len(msg.Value) {
		return false, nil
	}

	msg.Value = compressed
	msg.Flags |= FlagCompressed
	return true, nil
}

// DecompressValue decompresses a message's Value field if FlagCompressed is set.
// After decompression, FlagCompressed is cleared.
//
// FLOW:
//
//	┌────────────────┐  decompress  ┌───────────────┐
//	│ compressed val │─────────────►│ msg.Value     │
//	│ [bytes...]     │              │ "hello..."    │
//	│ Flags: 0x01    │              │ Flags: 0x00   │ (FlagCompressed cleared)
//	└────────────────┘              └───────────────┘
func DecompressValue(msg *Message, codec Compressor) error {
	if msg.Flags&FlagCompressed == 0 {
		return nil // Not compressed, nothing to do
	}

	if codec == nil {
		return fmt.Errorf("message is compressed but no codec provided")
	}

	decompressed, err := codec.Decompress(msg.Value)
	if err != nil {
		return fmt.Errorf("decompress value: %w", err)
	}

	msg.Value = decompressed
	msg.Flags &^= FlagCompressed // Clear the compressed flag
	return nil
}
