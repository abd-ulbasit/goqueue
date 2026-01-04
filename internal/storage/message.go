// =============================================================================
// MESSAGE ENCODING - THE FOUNDATION OF STORAGE
// =============================================================================
//
// WHAT IS A MESSAGE?
// A message is the fundamental unit of data in a message queue. Think of it as
// an envelope containing:
//   - Metadata (who sent it, when, routing info, priority)
//   - Payload (the actual data)
//
// WHY BINARY ENCODING?
// We could store messages as JSON (human-readable), but binary encoding is:
//   - 3-5x smaller (no field names repeated, no quotes, no whitespace)
//   - 10x faster to parse (direct memory layout, no string parsing)
//   - Fixed-size headers enable O(1) field access
//
// COMPARISON - How other queues encode messages:
//   - Kafka: Custom binary format, very similar to ours (no native priority)
//   - RabbitMQ: AMQP binary protocol (supports 0-255 priority levels)
//   - SQS: JSON over HTTP (no priority, uses separate queues)
//   - Redis Streams: RESP protocol (no priority)
//
// OUR MESSAGE FORMAT (on disk) - VERSION 1 WITH HEADERS:
// ┌──────────────────────────────────────────────────────────────────────────┐
// │ HEADER (fixed 34 bytes)                                                  │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ Magic (2B) │ Version (1B) │ Flags (1B) │ CRC32 (4B) │ Offset (8B)        │
// │ Timestamp (8B) │ Priority (1B) │ Reserved (1B) │ KeyLen (2B) │ ValLen(4B)│
// │ HeaderLen (2B)                                                           │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ BODY (variable)                                                          │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ Key (0-65535 bytes) │ Value (0-4GB bytes) │ Headers (0-65535 bytes)      │
// └──────────────────────────────────────────────────────────────────────────┘
//
// HEADERS FORMAT (when HeaderLen > 0):
// ┌──────────────────────────────────────────────────────────────────────────┐
// │ Count (2B) │ [KeyLen (2B) │ Key bytes │ ValLen (2B) │ Val bytes] × N     │
// └──────────────────────────────────────────────────────────────────────────┘
//
// WHY 34 BYTES?
// - Extended from 32 bytes to add HeaderLen field for trace context
// - Headers enable W3C Trace Context propagation (M7: Message Tracing)
// - Headers can carry: traceparent, tracestate, custom metadata
// - Most messages have no headers (HeaderLen=0), minimal overhead
//
// FIELD EXPLANATIONS:
//
// Magic (2 bytes): 0x47 0x51 = "GQ" (GoQueue)
//   - WHY: Helps detect file corruption or wrong file type
//   - If we read a file and first bytes aren't "GQ", it's not our log file
//
// Version (1 byte): Format version (currently 2)
//   - WHY: Allows backward-compatible format changes
//   - Version 1: Original 30-byte header (deprecated)
//   - Version 2: 32-byte header with Priority field
//
// Flags (1 byte): Bit flags for message properties
//   - bit 0: compressed (future: payload is compressed)
//   - bit 1: has headers (future: key-value metadata)
//   - bit 2: tombstone (marks deleted record in compacted topics)
//   - bits 3-7: reserved
//
// CRC32 (4 bytes): Checksum of everything AFTER the CRC field
//   - WHY: Detect corruption from disk errors, partial writes, bit flips
//   - We use Castagnoli polynomial (hardware accelerated on modern CPUs)
//   - IMPORTANT: CRC covers offset through value, NOT magic/version/flags
//
// Offset (8 bytes): Unique sequential ID within partition
//   - WHY: Enables random access and consumer position tracking
//   - Consumers say "give me messages starting from offset 12345"
//   - 8 bytes = 2^64 = enough for 584 billion years at 1M msg/sec
//
// Timestamp (8 bytes): Unix nanoseconds when message was received
//   - WHY: Enables time-based queries ("replay from 1 hour ago")
//   - Nanoseconds because milliseconds aren't precise enough for ordering
//
// Priority (1 byte): Message priority level (0-4)
//   - WHY: Enables priority-based consumption ordering
//   - 0 = Critical (highest), 4 = Background (lowest)
//   - Used by Weighted Fair Queuing scheduler for delivery ordering
//   - COMPARISON:
//     - RabbitMQ: 0-255 (but only 0-9 commonly used)
//     - Kafka: No native priority (requires separate topics)
//     - SQS: No priority (use separate queues)
//
// Reserved (1 byte): Reserved for future use
//   - Candidates: compression algorithm ID, schema version hint, delivery flags
//   - MUST be set to 0 when writing, ignored when reading
//
// Key Length (2 bytes): Length of key (0 = no key)
//   - WHY: Keys determine partition routing (same key = same partition)
//   - Max 65535 bytes (64KB) - keys should be small (user IDs, order IDs)
//
// Value Length (4 bytes): Length of payload
//   - WHY: Need to know where message ends
//   - Max 4GB per message (but we'll enforce smaller limits in practice)
//
// =============================================================================

package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

// =============================================================================
// CONSTANTS
// =============================================================================

const (
	// MagicBytes identifies this as a GoQueue message
	// "GQ" in ASCII = 0x47, 0x51
	MagicByte1 = 0x47
	MagicByte2 = 0x51

	// FormatVersion allows future format changes while maintaining compatibility
	// Version 1: 34-byte header with Priority and Headers support
	// (Versioning complexity deferred until production - just use one format during dev)
	FormatVersion = 1

	// HeaderSize is the fixed size of message header in bytes
	// Magic(2) + Version(1) + Flags(1) + CRC(4) + Offset(8) + Timestamp(8) +
	// Priority(1) + Reserved(1) + KeyLen(2) + ValueLen(4) + HeaderLen(2) = 34
	//
	// WHY 34 BYTES?
	// - Extended from 32 to support message headers for tracing (M7)
	// - Headers enable W3C Trace Context propagation
	// - HeaderLen(2) supports up to 64KB of headers
	HeaderSize = 34

	// MaxKeySize is the maximum key length (64KB)
	// Keys are used for partition routing - they should be small identifiers
	MaxKeySize = 65535

	// MaxValueSize is the maximum payload size (16MB default, configurable)
	// Larger messages should be chunked or stored externally
	MaxValueSize = 16 * 1024 * 1024

	// MaxHeadersSize is the maximum total headers size (64KB)
	// Headers are for metadata like trace context, not large data
	MaxHeadersSize = 65535
)

// Message flags - bit positions in the flags byte
const (
	FlagCompressed = 1 << 0 // Message payload is compressed
	FlagHasHeaders = 1 << 1 // Message has additional headers
	FlagTombstone  = 1 << 2 // Marks deletion in compacted topics
)

// =============================================================================
// PRIORITY LEVELS
// =============================================================================
//
// WHY 5 PRIORITY LEVELS?
// Most systems use 3 (high/normal/low), but 5 gives better flexibility:
//   - Critical: System alerts, circuit breaker signals - MUST process immediately
//   - High: Payment processing, real-time events - should process quickly
//   - Normal: Default for most messages - standard processing
//   - Low: Batch jobs, reports - can wait if busy
//   - Background: Analytics, logs - process when nothing else to do
//
// COMPARISON:
//   - RabbitMQ: 0-255 (but 0-9 typically used, default 0)
//   - Kafka: No native priority - use separate topics per priority
//   - SQS: No priority - use separate queues
//   - goqueue: 5 levels with Weighted Fair Queuing
//
// NUMERICAL VALUES:
// Lower number = higher priority (common convention)
// This makes comparisons intuitive: if p1 < p2, p1 has higher priority
//

// Priority represents the priority level of a message.
// Lower values indicate higher priority (Critical=0 is highest).
type Priority uint8

const (
	// PriorityCritical is for system-critical messages that MUST be processed first.
	// Use cases: Health checks, circuit breaker signals, emergency alerts
	// WFQ Weight: 50 (gets ~50% of bandwidth when all queues have messages)
	PriorityCritical Priority = 0

	// PriorityHigh is for time-sensitive business messages.
	// Use cases: Payment processing, real-time notifications, user actions
	// WFQ Weight: 25 (gets ~25% of bandwidth)
	PriorityHigh Priority = 1

	// PriorityNormal is the default priority for standard messages.
	// Use cases: Most application messages, orders, updates
	// WFQ Weight: 15 (gets ~15% of bandwidth)
	PriorityNormal Priority = 2

	// PriorityLow is for less urgent messages that can tolerate delay.
	// Use cases: Batch processing, scheduled reports, bulk operations
	// WFQ Weight: 7 (gets ~7% of bandwidth)
	PriorityLow Priority = 3

	// PriorityBackground is for messages that should only be processed when idle.
	// Use cases: Analytics events, audit logs, non-essential notifications
	// WFQ Weight: 3 (gets ~3% of bandwidth)
	PriorityBackground Priority = 4

	// PriorityCount is the number of priority levels (used for array sizing)
	PriorityCount = 5
)

// String returns the human-readable name of the priority level.
func (p Priority) String() string {
	switch p {
	case PriorityCritical:
		return "critical"
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	case PriorityBackground:
		return "background"
	default:
		return fmt.Sprintf("unknown(%d)", p)
	}
}

// IsValid returns true if the priority is a valid level (0-4).
func (p Priority) IsValid() bool {
	return p < PriorityCount
}

// ParsePriority converts a string to a Priority level.
// Returns PriorityNormal for unrecognized strings (safe default).
//
// Accepts: "critical", "high", "normal", "low", "background" (case-insensitive)
func ParsePriority(s string) Priority {
	switch s {
	case "critical", "Critical", "CRITICAL":
		return PriorityCritical
	case "high", "High", "HIGH":
		return PriorityHigh
	case "normal", "Normal", "NORMAL", "":
		return PriorityNormal
	case "low", "Low", "LOW":
		return PriorityLow
	case "background", "Background", "BACKGROUND":
		return PriorityBackground
	default:
		return PriorityNormal // Safe default
	}
}

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================
//
// WHY SENTINEL ERRORS?
// Sentinel errors (package-level error variables) let callers check error types:
//
//   if errors.Is(err, storage.ErrCorruptedMessage) {
//       // Handle corruption specifically
//   }
//
// This is better than string matching which is fragile and slow.
//

var (
	// ErrInvalidMagic means the magic bytes don't match - wrong file type or corruption
	ErrInvalidMagic = errors.New("invalid magic bytes: not a GoQueue message")

	// ErrUnsupportedVersion means we can't read this format version
	ErrUnsupportedVersion = errors.New("unsupported message format version")

	// ErrCorruptedMessage means CRC check failed - data was modified or corrupted
	ErrCorruptedMessage = errors.New("message corrupted: CRC mismatch")

	// ErrKeyTooLarge means key exceeds maximum allowed size
	ErrKeyTooLarge = errors.New("key exceeds maximum size")

	// ErrValueTooLarge means payload exceeds maximum allowed size
	ErrValueTooLarge = errors.New("value exceeds maximum size")

	// ErrInvalidMessage means message data is malformed
	ErrInvalidMessage = errors.New("invalid message format")

	// ErrInvalidPriority means the priority value is out of valid range (0-4)
	ErrInvalidPriority = errors.New("invalid priority level")
)

// =============================================================================
// MESSAGE STRUCT
// =============================================================================

// Message represents a single message in the queue.
//
// DESIGN NOTES:
//   - Offset is assigned by the storage layer, not the producer
//   - Timestamp is set when message is received by broker
//   - Key is optional (nil = round-robin partition assignment)
//   - Value is the actual payload (can be any bytes)
//   - Priority determines consumption ordering (lower = higher priority)
//   - Headers carry metadata like trace context (W3C traceparent/tracestate)
//
// COMPARISON WITH KAFKA:
//   - Kafka has "record batches" that group messages for efficiency
//   - Kafka has no native priority (requires separate topics)
//   - Kafka has headers (added in 0.11.0) - we support the same concept
//   - We keep it simpler for now (one message at a time)
//   - Batching will be added at the producer API level
type Message struct {
	// Offset is the unique sequential position in the partition
	// Assigned by the log when message is appended
	Offset int64

	// Timestamp is when the message was received (Unix nanoseconds)
	// Set by the broker, not the producer
	Timestamp int64

	// Key is used for partition routing (optional)
	// Messages with the same key always go to the same partition
	// This guarantees ordering for related messages
	Key []byte

	// Value is the actual message payload
	Value []byte

	// Flags contains message attributes (compression, headers, etc.)
	Flags uint8

	// Priority is the message priority level (0=Critical to 4=Background)
	// Lower values indicate higher priority
	// Default is PriorityNormal (2) if not specified
	Priority Priority

	// Headers contains message metadata as key-value pairs
	// Used for: trace context (traceparent, tracestate), custom metadata
	// Most messages have nil/empty headers for efficiency
	//
	// COMMON HEADERS:
	//   - "traceparent": W3C trace context (e.g., "00-{trace-id}-{span-id}-01")
	//   - "tracestate": Vendor-specific trace state
	//   - "content-type": Payload format hint (e.g., "application/json")
	//   - "correlation-id": For request-response patterns
	Headers map[string]string
}

// =============================================================================
// CRC32 CHECKSUM - DETECTING CORRUPTION
// =============================================================================
//
// WHY CRC32?
// Disk drives can silently corrupt data (bit flips, partial writes, bad sectors).
// CRC32 is a mathematical checksum that detects ~99.999999% of corruptions.
//
// WHY CASTAGNOLI (CRC-32C)?
// - Intel CPUs have hardware instructions (SSE 4.2) for CRC-32C
// - This makes it 10x faster than software-only CRC-32 (IEEE)
// - It's what Kafka, RocksDB, and many databases use
//
// HOW IT WORKS:
// 1. Take all the bytes you want to protect
// 2. Run them through the CRC algorithm → get 4-byte checksum
// 3. Store checksum with the data
// 4. On read: recalculate checksum, compare with stored
// 5. If they differ → data was corrupted
//

// crcTable is a pre-computed lookup table for CRC-32C (Castagnoli)
// Creating the table once is faster than computing it for each checksum
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// calculateCRC computes CRC-32C checksum over the given data
func calculateCRC(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// =============================================================================
// ENCODING - MESSAGE → BYTES
// =============================================================================
//
// FLOW:
//   Message struct → Encode() → []byte → Write to disk
//
// The encoded format must be:
//   1. Self-describing (can find message boundaries without external index)
//   2. Efficient (minimize bytes, enable fast parsing)
//   3. Robust (detect corruption)
//

// Encode serializes the message to binary format for storage.
//
// BYTE LAYOUT (34-byte header with headers support):
//
//	[0:2]   Magic bytes (0x47, 0x51 = "GQ")
//	[2:3]   Version (1)
//	[3:4]   Flags
//	[4:8]   CRC32 of bytes [8:end]
//	[8:16]  Offset (int64, big-endian)
//	[16:24] Timestamp (int64, big-endian)
//	[24:25] Priority (uint8, 0-4)
//	[25:26] Reserved (uint8, must be 0)
//	[26:28] Key length (uint16, big-endian)
//	[28:32] Value length (uint32, big-endian)
//	[32:34] Headers length (uint16, big-endian)
//	[34:34+keyLen] Key bytes
//	[34+keyLen:34+keyLen+valueLen] Value bytes
//	[34+keyLen+valueLen:end] Headers bytes (if any)
//
// HEADERS FORMAT (when HeaderLen > 0):
//
//	Count (2 bytes) + [KeyLen(2) + Key + ValLen(2) + Val] × N
//
// WHY BIG-ENDIAN?
// Network byte order convention. Also makes hex dumps more readable
// (most significant byte first, like how we write numbers).
func (m *Message) Encode() ([]byte, error) {
	// -------------------------------------------------------------------------
	// STEP 1: Validate message
	// -------------------------------------------------------------------------
	if len(m.Key) > MaxKeySize {
		return nil, fmt.Errorf("%w: key is %d bytes, max is %d",
			ErrKeyTooLarge, len(m.Key), MaxKeySize)
	}
	if len(m.Value) > MaxValueSize {
		return nil, fmt.Errorf("%w: value is %d bytes, max is %d",
			ErrValueTooLarge, len(m.Value), MaxValueSize)
	}
	if !m.Priority.IsValid() {
		return nil, fmt.Errorf("%w: got %d, valid range is 0-%d",
			ErrInvalidPriority, m.Priority, PriorityCount-1)
	}

	// -------------------------------------------------------------------------
	// STEP 2: Encode headers (if any)
	// -------------------------------------------------------------------------
	var headersBytes []byte
	if len(m.Headers) > 0 {
		headersBytes = encodeHeaders(m.Headers)
		if len(headersBytes) > MaxHeadersSize {
			return nil, fmt.Errorf("headers exceed maximum size: %d > %d",
				len(headersBytes), MaxHeadersSize)
		}
	}

	// -------------------------------------------------------------------------
	// STEP 3: Calculate total size and allocate buffer
	// -------------------------------------------------------------------------
	// Pre-allocating the exact size is important for performance:
	// - Avoids multiple allocations as slice grows
	// - Reduces GC pressure
	totalSize := HeaderSize + len(m.Key) + len(m.Value) + len(headersBytes)
	buf := make([]byte, totalSize)

	// -------------------------------------------------------------------------
	// STEP 4: Write header fields
	// -------------------------------------------------------------------------
	// We write fields in order, using binary.BigEndian for multi-byte integers.
	// BigEndian means most significant byte first (network byte order).

	// Magic bytes - identify this as GoQueue format
	buf[0] = MagicByte1
	buf[1] = MagicByte2

	// Version - allows future format evolution
	buf[2] = FormatVersion

	// Flags - message attributes (set FlagHasHeaders if we have headers)
	flags := m.Flags
	if len(headersBytes) > 0 {
		flags |= FlagHasHeaders
	}
	buf[3] = flags

	// Skip CRC for now (bytes 4-7), we'll fill it after writing everything else
	// CRC covers bytes 8 onwards (offset through value and headers)

	// Offset - unique position in partition
	binary.BigEndian.PutUint64(buf[8:16], uint64(m.Offset))

	// Timestamp - when message was received
	binary.BigEndian.PutUint64(buf[16:24], uint64(m.Timestamp))

	// Priority - message priority level (0-4)
	buf[24] = uint8(m.Priority)

	// Reserved - must be 0 for forward compatibility
	buf[25] = 0

	// Key length - needed to know where key ends and value begins
	binary.BigEndian.PutUint16(buf[26:28], uint16(len(m.Key)))

	// Value length - needed to know where value ends
	binary.BigEndian.PutUint32(buf[28:32], uint32(len(m.Value)))

	// Headers length - needed to know if headers are present
	binary.BigEndian.PutUint16(buf[32:34], uint16(len(headersBytes)))

	// -------------------------------------------------------------------------
	// STEP 5: Write key, value, and headers
	// -------------------------------------------------------------------------
	// copy() is optimized in Go - uses memmove internally
	keyEnd := HeaderSize + len(m.Key)
	copy(buf[HeaderSize:keyEnd], m.Key)

	valueEnd := keyEnd + len(m.Value)
	copy(buf[keyEnd:valueEnd], m.Value)

	if len(headersBytes) > 0 {
		copy(buf[valueEnd:], headersBytes)
	}

	// -------------------------------------------------------------------------
	// STEP 6: Calculate and write CRC
	// -------------------------------------------------------------------------
	// CRC covers everything from offset onwards (bytes 8 to end)
	// This protects the actual message data, not just the framing
	crc := calculateCRC(buf[8:])
	binary.BigEndian.PutUint32(buf[4:8], crc)

	return buf, nil
}

// encodeHeaders serializes headers map to binary format.
// Format: Count(2) + [KeyLen(2) + Key + ValLen(2) + Val] × N
func encodeHeaders(headers map[string]string) []byte {
	if len(headers) == 0 {
		return nil
	}

	// Calculate size
	size := 2 // count
	for k, v := range headers {
		size += 2 + len(k) + 2 + len(v)
	}

	buf := make([]byte, size)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(headers)))

	pos := 2
	for k, v := range headers {
		// write key length and key
		binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(k)))
		pos += 2
		copy(buf[pos:], k)
		pos += len(k)
		// write value length and value
		binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(v)))
		pos += 2
		copy(buf[pos:], v)
		pos += len(v)
	}

	return buf
}

// decodeHeaders deserializes headers from binary format.
func decodeHeaders(data []byte) (map[string]string, error) {
	if len(data) < 2 {
		return nil, nil
	}

	count := binary.BigEndian.Uint16(data[0:2])
	if count == 0 {
		return nil, nil
	}

	headers := make(map[string]string, count)
	pos := 2

	for i := uint16(0); i < count; i++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("truncated header key length at position %d", pos)
		}
		keyLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2

		if pos+keyLen > len(data) {
			return nil, fmt.Errorf("truncated header key at position %d", pos)
		}
		key := string(data[pos : pos+keyLen])
		pos += keyLen

		if pos+2 > len(data) {
			return nil, fmt.Errorf("truncated header value length at position %d", pos)
		}
		valLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2

		if pos+valLen > len(data) {
			return nil, fmt.Errorf("truncated header value at position %d", pos)
		}
		val := string(data[pos : pos+valLen])
		pos += valLen

		headers[key] = val
	}

	return headers, nil
}

// =============================================================================
// DECODING - BYTES → MESSAGE
// =============================================================================
//
// FLOW:
//   []byte from disk → Decode() → Message struct
//
// This is the reverse of Encode. We must:
//   1. Validate magic bytes (is this our format?)
//   2. Check version (can we read this?)
//   3. Verify CRC (is data intact?)
//   4. Extract fields
//

// Decode deserializes a message from binary format.
//
// IMPORTANT: This validates the message integrity using CRC.
// If CRC doesn't match, the message is corrupted and we return an error.
//
// HEADER FORMAT (34 bytes):
//
//	[0:2]   Magic bytes
//	[2:3]   Version
//	[3:4]   Flags
//	[4:8]   CRC32
//	[8:16]  Offset
//	[16:24] Timestamp
//	[24:25] Priority
//	[25:26] Reserved
//	[26:28] Key length
//	[28:32] Value length
//	[32:34] Headers length
func Decode(data []byte) (*Message, error) {
	// -------------------------------------------------------------------------
	// STEP 1: Check minimum size (need at least magic + version to proceed)
	// -------------------------------------------------------------------------
	if len(data) < 3 {
		return nil, fmt.Errorf("%w: data too short (%d bytes, need at least 3)",
			ErrInvalidMessage, len(data))
	}

	// -------------------------------------------------------------------------
	// STEP 2: Validate magic bytes
	// -------------------------------------------------------------------------
	// If these don't match, this isn't a GoQueue message
	if data[0] != MagicByte1 || data[1] != MagicByte2 {
		return nil, fmt.Errorf("%w: got 0x%02x 0x%02x, expected 0x%02x 0x%02x",
			ErrInvalidMagic, data[0], data[1], MagicByte1, MagicByte2)
	}

	// -------------------------------------------------------------------------
	// STEP 3: Check version
	// -------------------------------------------------------------------------
	version := data[2]
	if version != FormatVersion {
		return nil, fmt.Errorf("%w: got version %d, expected %d",
			ErrUnsupportedVersion, version, FormatVersion)
	}

	// -------------------------------------------------------------------------
	// STEP 4: Decode 34-byte header format
	// -------------------------------------------------------------------------
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("%w: data too short (%d bytes, need at least %d)",
			ErrInvalidMessage, len(data), HeaderSize)
	}

	flags := data[3]
	storedCRC := binary.BigEndian.Uint32(data[4:8])
	offset := int64(binary.BigEndian.Uint64(data[8:16]))
	timestamp := int64(binary.BigEndian.Uint64(data[16:24]))
	priority := Priority(data[24])
	// data[25] is reserved, we ignore it
	keyLen := binary.BigEndian.Uint16(data[26:28])
	valueLen := binary.BigEndian.Uint32(data[28:32])
	headersLen := binary.BigEndian.Uint16(data[32:34])

	// Validate priority
	if !priority.IsValid() {
		// invalid priority value
		return nil, fmt.Errorf("%w: got %d, valid range is 0-%d",
			ErrInvalidPriority, priority, PriorityCount-1)
	}

	expectedSize := HeaderSize + int(keyLen) + int(valueLen) + int(headersLen)
	if len(data) < expectedSize {
		return nil, fmt.Errorf("%w: data is %d bytes, but header claims %d",
			ErrInvalidMessage, len(data), expectedSize)
	}

	// CRC covers everything from offset onwards (bytes 8 to end)
	calculatedCRC := calculateCRC(data[8:expectedSize])
	if calculatedCRC != storedCRC {
		return nil, fmt.Errorf("%w: stored CRC 0x%08x, calculated 0x%08x",
			ErrCorruptedMessage, storedCRC, calculatedCRC)
	}

	// Extract key
	var key []byte
	keyStart := HeaderSize
	if keyLen > 0 {
		key = make([]byte, keyLen)
		copy(key, data[keyStart:keyStart+int(keyLen)])
	}

	// Extract value
	valueStart := keyStart + int(keyLen)
	value := make([]byte, valueLen)
	copy(value, data[valueStart:valueStart+int(valueLen)])

	// Extract headers (if present)
	var headers map[string]string
	if headersLen > 0 {
		headersStart := valueStart + int(valueLen)
		var err error
		headers, err = decodeHeaders(data[headersStart : headersStart+int(headersLen)])
		if err != nil {
			return nil, fmt.Errorf("failed to decode headers: %w", err)
		}
	}

	return &Message{
		Offset:    offset,
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
		Flags:     flags,
		Priority:  priority,
		Headers:   headers,
	}, nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// Size returns the total encoded size of the message in bytes.
// Useful for pre-allocating buffers and calculating segment usage.
//
// SIZE CALCULATION:
//
//	HeaderSize (34) + len(Key) + len(Value) + encoded headers size
//
// HEADERS FORMAT:
//
//	Count(2) + [KeyLen(2) + Key + ValLen(2) + Val] × N
//
// NOTE: Headers are encoded as length-prefixed key-value pairs with a count prefix.
func (m *Message) Size() int {
	size := HeaderSize + len(m.Key) + len(m.Value)

	// Calculate headers size
	// Format: Count(2) + [KeyLen(2) + Key + ValLen(2) + Val] × N
	if len(m.Headers) > 0 {
		size += 2 // count prefix
		for k, v := range m.Headers {
			size += 2 + len(k) + 2 + len(v)
		}
	}

	return size
}

// NewMessage creates a new message with current timestamp and default priority (Normal).
// Offset will be assigned when the message is appended to the log.
func NewMessage(key, value []byte) *Message {
	return &Message{
		Offset:    0, // Will be set by the log
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
		Flags:     0,
		Priority:  PriorityNormal,
		Headers:   make(map[string]string),
	}
}

// NewMessageWithPriority creates a new message with specified priority.
// Offset will be assigned when the message is appended to the log.
//
// USAGE:
//
//	msg := storage.NewMessageWithPriority(key, value, storage.PriorityHigh)
func NewMessageWithPriority(key, value []byte, priority Priority) *Message {
	if !priority.IsValid() {
		priority = PriorityNormal // Safe fallback
	}
	return &Message{
		Offset:    0,
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
		Flags:     0,
		Priority:  priority,
		Headers:   make(map[string]string),
	}
}

// NewMessageWithHeaders creates a new message with custom headers for tracing and metadata.
// Headers are used for trace context propagation (W3C traceparent) and custom metadata.
//
// COMMON HEADERS:
//   - "traceparent": W3C Trace Context (e.g., "00-trace_id-span_id-01")
//   - "correlation_id": Application-level correlation
//   - Custom application headers
//
// USAGE:
//
//	headers := map[string]string{"traceparent": "00-abc123...-def456...-01"}
//	msg := storage.NewMessageWithHeaders(key, value, headers)
func NewMessageWithHeaders(key, value []byte, headers map[string]string) *Message {
	h := make(map[string]string, len(headers))
	for k, v := range headers {
		h[k] = v
	}
	return &Message{
		Offset:    0,
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
		Flags:     0,
		Priority:  PriorityNormal,
		Headers:   h,
	}
}

// IsCompressed returns true if the message payload is compressed.
func (m *Message) IsCompressed() bool {
	return m.Flags&FlagCompressed != 0
}

// IsTombstone returns true if this message marks a deletion.
// Used in compacted topics where old values for a key are removed.
func (m *Message) IsTombstone() bool {
	return m.Flags&FlagTombstone != 0
}

// SetCompressed sets or clears the compressed flag.
func (m *Message) SetCompressed(compressed bool) {
	if compressed {
		m.Flags |= FlagCompressed
	} else {
		m.Flags &^= FlagCompressed
	}
}

// SetTombstone sets or clears the tombstone flag.
func (m *Message) SetTombstone(tombstone bool) {
	if tombstone {
		m.Flags |= FlagTombstone
	} else {
		m.Flags &^= FlagTombstone
	}
}
