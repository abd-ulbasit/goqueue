// =============================================================================
// IN-FLIGHT MESSAGES - TRACKING MESSAGES BEING PROCESSED
// =============================================================================
//
// WHAT IS AN IN-FLIGHT MESSAGE?
// When a consumer receives a message, it enters an "in-flight" state:
//   - The message is being processed by a consumer
//   - Other consumers cannot see it (invisible)
//   - It has a visibility deadline (timeout for processing)
//   - If not ACKed before deadline, it becomes visible again
//
// WHY TRACK IN-FLIGHT STATE?
//
//   WITHOUT IN-FLIGHT TRACKING (offset-only):
//   ┌──────────┐     poll     ┌──────────┐     crash!    ┌──────────┐
//   │  Queue   │ ───────────► │ Consumer │ ─────────────► │ Message  │
//   │ offset=5 │              │ gets 5   │               │  LOST!   │
//   └──────────┘              └──────────┘               └──────────┘
//
//   WITH IN-FLIGHT TRACKING:
//   ┌──────────┐     poll     ┌──────────┐     crash!    ┌──────────┐
//   │  Queue   │ ───────────► │ Consumer │ ─────────────► │ Timeout  │
//   │ offset=5 │              │ gets 5   │               │ expires  │
//   │ inflight │              │ inflight │               │ msg=5    │
//   └──────────┘              └──────────┘               │ visible  │
//                                                         └──────────┘
//                                                              │
//                                                              ▼
//                                                         ┌──────────┐
//                                                         │ Redeliver│
//                                                         │ to other │
//                                                         │ consumer │
//                                                         └──────────┘
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   ┌─────────────┬─────────────────────┬──────────────────────────────────┐
//   │ System      │ In-Flight Tracking  │ How It Works                     │
//   ├─────────────┼─────────────────────┼──────────────────────────────────┤
//   │ SQS         │ VisibilityTimeout   │ Message invisible for N seconds │
//   │             │                     │ after receive. ReceiptHandle    │
//   │             │                     │ required for delete/extend.     │
//   ├─────────────┼─────────────────────┼──────────────────────────────────┤
//   │ Kafka       │ None (offset-based) │ Consumer commits offset. If it  │
//   │             │                     │ crashes before commit, another  │
//   │             │                     │ consumer restarts from last     │
//   │             │                     │ committed offset.               │
//   ├─────────────┼─────────────────────┼──────────────────────────────────┤
//   │ RabbitMQ    │ Unacked messages    │ Broker tracks unacked. If       │
//   │             │                     │ consumer disconnects, msgs      │
//   │             │                     │ are requeued.                   │
//   ├─────────────┼─────────────────────┼──────────────────────────────────┤
//   │ goqueue     │ Hybrid (M4)         │ Per-message ACK with visibility │
//   │             │                     │ timeout. Offset = highest       │
//   │             │                     │ contiguously ACKed.             │
//   └─────────────┴─────────────────────┴──────────────────────────────────┘
//
// RECEIPT HANDLE:
// A receipt handle is a unique token for each delivery attempt. It:
//   - Identifies the specific delivery (not just the message)
//   - Encodes enough info to locate the message quickly
//   - Prevents replaying old ACKs (delivery-specific)
//
// FORMAT: {topic}:{partition}:{offset}:{deliveryCount}:{timestamp}
// Example: orders:2:12345:1:1735500000000000000
//
// =============================================================================

package broker

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrInvalidReceiptHandle means the receipt handle format is invalid
	ErrInvalidReceiptHandle = errors.New("invalid receipt handle format")

	// ErrReceiptExpired means the receipt handle has expired (visibility timeout passed)
	ErrReceiptExpired = errors.New("receipt handle expired")

	// ErrReceiptNotFound means the receipt handle doesn't match any in-flight message
	ErrReceiptNotFound = errors.New("receipt handle not found")

	// ErrMaxRetriesExceeded means the message has exceeded maximum retry attempts
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")

	// ErrMessageExpired means the message TTL has expired
	ErrMessageExpired = errors.New("message TTL expired")
)

// =============================================================================
// IN-FLIGHT MESSAGE STATE
// =============================================================================

// InFlightState represents the current state of an in-flight message.
//
// STATE MACHINE:
//
//	┌──────────┐   Poll()    ┌───────────┐   Ack()    ┌───────────┐
//	│ PENDING  │ ──────────► │ IN_FLIGHT │ ─────────► │  ACKED    │
//	│ (in log) │             │ (timer)   │            │ (done)    │
//	└──────────┘             └───────────┘            └───────────┘
//	                               │
//	                               │ Nack() or
//	                               │ Timeout
//	                               ▼
//	                         ┌───────────┐
//	                         │ SCHEDULED │
//	                         │ FOR RETRY │
//	                         └───────────┘
//	                               │
//	                               │ MaxRetries or Reject()
//	                               ▼
//	                         ┌───────────┐
//	                         │   DLQ     │
//	                         └───────────┘
type InFlightState int

const (
	// StateInFlight means the message is being processed
	StateInFlight InFlightState = iota

	// StateScheduledRetry means the message will be retried after backoff
	StateScheduledRetry

	// StateAcked means the message was successfully processed
	StateAcked

	// StateRejected means the message was rejected (sent to DLQ)
	StateRejected
)

func (s InFlightState) String() string {
	switch s {
	case StateInFlight:
		return "InFlight"
	case StateScheduledRetry:
		return "ScheduledRetry"
	case StateAcked:
		return "Acked"
	case StateRejected:
		return "Rejected"
	default:
		return "Unknown"
	}
}

// =============================================================================
// IN-FLIGHT MESSAGE
// =============================================================================

// InFlightMessage represents a message that has been delivered to a consumer
// but not yet acknowledged.
//
// LIFECYCLE:
//  1. Consumer calls Poll() → message delivered, InFlightMessage created
//  2. Consumer processes message
//  3. Consumer calls Ack(receipt) → message removed from in-flight
//  4. OR: Visibility timeout expires → message becomes visible for redelivery
//  5. OR: Consumer calls Nack(receipt) → immediate retry (with backoff)
//  6. OR: Consumer calls Reject(receipt) → send to DLQ
//  7. OR: MaxRetries exceeded → automatic DLQ
type InFlightMessage struct {
	// ReceiptHandle is the unique token for this delivery attempt
	// Used by consumer to ACK/NACK/Reject this specific delivery
	ReceiptHandle string

	// MessageID is the internal identifier (topic:partition:offset)
	// Used to locate the original message in the log
	MessageID string

	// Topic is the source topic
	Topic string

	// Partition is the source partition
	Partition int

	// Offset is the message offset in the partition
	Offset int64

	// ConsumerID is the member ID that received this message
	ConsumerID string

	// GroupID is the consumer group ID
	GroupID string

	// DeliveryCount is how many times this message has been delivered
	// First delivery = 1, first retry = 2, etc.
	DeliveryCount int

	// MaxRetries is the maximum delivery attempts before DLQ
	// Set from topic/consumer config
	MaxRetries int

	// FirstDeliveryTime is when the message was first delivered (ever)
	FirstDeliveryTime time.Time

	// LastDeliveryTime is when the current delivery attempt started
	LastDeliveryTime time.Time

	// VisibilityDeadline is when this message becomes visible again
	// if not ACKed before this time
	VisibilityDeadline time.Time

	// State is the current state of this in-flight message
	State InFlightState

	// NextRetryTime is when to retry (only set if State == StateScheduledRetry)
	NextRetryTime time.Time

	// LastError is the error message from the last NACK (for debugging)
	LastError string

	// OriginalKey is the message key (for DLQ)
	OriginalKey []byte

	// OriginalValue is the message value (for DLQ)
	OriginalValue []byte

	// OriginalTimestamp is when the message was originally published
	OriginalTimestamp time.Time

	// MessageTTL is the time-to-live for this message (0 = no TTL)
	MessageTTL time.Duration
}

// =============================================================================
// RECEIPT HANDLE OPERATIONS
// =============================================================================

// GenerateReceiptHandle creates a unique receipt handle for a delivery.
//
// FORMAT: {topic}:{partition}:{offset}:{deliveryCount}:{nonce}
//
// The nonce is a random hex string to ensure uniqueness even if the same
// message is redelivered with the same delivery count (shouldn't happen,
// but defensive).
//
// WHY THIS FORMAT?
//   - Parseable: We can extract message location without database lookup
//   - Debuggable: Human-readable, useful for troubleshooting
//   - Unique: Nonce prevents replay of old receipts
//
// COMPARISON:
//   - SQS: Opaque string (contains encrypted info)
//   - goqueue: Readable format for easier debugging
func GenerateReceiptHandle(topic string, partition int, offset int64, deliveryCount int) string {
	// Generate 8-byte random nonce
	nonce := make([]byte, 8)
	rand.Read(nonce)

	return fmt.Sprintf("%s:%d:%d:%d:%s",
		topic,
		partition,
		offset,
		deliveryCount,
		hex.EncodeToString(nonce),
	)
}

// ParsedReceiptHandle contains the parsed components of a receipt handle.
type ParsedReceiptHandle struct {
	Topic         string
	Partition     int
	Offset        int64
	DeliveryCount int
	Nonce         string
}

// ParseReceiptHandle extracts components from a receipt handle.
//
// RETURNS:
//   - ParsedReceiptHandle with all fields populated
//   - ErrInvalidReceiptHandle if format is wrong
func ParseReceiptHandle(handle string) (*ParsedReceiptHandle, error) {
	parts := strings.Split(handle, ":")
	if len(parts) != 5 {
		return nil, fmt.Errorf("%w: expected 5 parts, got %d", ErrInvalidReceiptHandle, len(parts))
	}

	partition, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: invalid partition: %v", ErrInvalidReceiptHandle, err)
	}

	offset, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid offset: %v", ErrInvalidReceiptHandle, err)
	}

	deliveryCount, err := strconv.Atoi(parts[3])
	if err != nil {
		return nil, fmt.Errorf("%w: invalid delivery count: %v", ErrInvalidReceiptHandle, err)
	}

	return &ParsedReceiptHandle{
		Topic:         parts[0],
		Partition:     partition,
		Offset:        offset,
		DeliveryCount: deliveryCount,
		Nonce:         parts[4],
	}, nil
}

// MessageID creates a unique identifier for a message (for internal tracking).
// Format: {topic}:{partition}:{offset}
func MessageID(topic string, partition int, offset int64) string {
	return fmt.Sprintf("%s:%d:%d", topic, partition, offset)
}

// =============================================================================
// DLQ MESSAGE FORMAT
// =============================================================================

// DLQReason indicates why a message was sent to the dead letter queue.
type DLQReason string

const (
	// DLQReasonMaxRetries means the message exceeded maximum retry attempts
	DLQReasonMaxRetries DLQReason = "MAX_RETRIES_EXCEEDED"

	// DLQReasonRejected means the consumer explicitly rejected the message
	DLQReasonRejected DLQReason = "REJECTED"

	// DLQReasonTTLExpired means the message's time-to-live expired
	DLQReasonTTLExpired DLQReason = "TTL_EXPIRED"

	// DLQReasonPoisonMessage means the message consistently causes errors
	DLQReasonPoisonMessage DLQReason = "POISON_MESSAGE"
)

// DLQMessage contains the original message plus metadata for debugging.
//
// WHY PRESERVE METADATA?
// When a message ends up in DLQ, operators need to understand:
//   - What was the original message?
//   - Where did it come from?
//   - How many times was it tried?
//   - What went wrong?
//
// This metadata enables root cause analysis without external logging.
type DLQMessage struct {
	// Original message data
	OriginalTopic     string    `json:"original_topic"`
	OriginalPartition int       `json:"original_partition"`
	OriginalOffset    int64     `json:"original_offset"`
	OriginalTimestamp time.Time `json:"original_timestamp"`
	OriginalKey       []byte    `json:"original_key,omitempty"`
	OriginalValue     []byte    `json:"original_value"`

	// Delivery information
	DeliveryAttempts int       `json:"delivery_attempts"`
	FirstDelivery    time.Time `json:"first_delivery"`
	LastDelivery     time.Time `json:"last_delivery"`
	LastConsumer     string    `json:"last_consumer"`
	LastGroup        string    `json:"last_group"`

	// Failure information
	Reason     DLQReason `json:"reason"`
	LastError  string    `json:"last_error,omitempty"`
	DLQTime    time.Time `json:"dlq_time"`
	ReceiptLog []string  `json:"receipt_log,omitempty"` // History of receipt handles
}

// =============================================================================
// CONFIGURATION
// =============================================================================

// ReliabilityConfig holds configuration for the reliability layer (M4).
type ReliabilityConfig struct {
	// VisibilityTimeoutMs is how long a message is invisible after delivery
	// before it becomes visible again for redelivery.
	// Default: 30000 (30 seconds) - matches SQS default
	VisibilityTimeoutMs int

	// MaxRetries is the maximum number of delivery attempts before DLQ.
	// Includes the initial delivery. So MaxRetries=3 means:
	//   - 1st delivery (attempt 1)
	//   - 1st retry (attempt 2)
	//   - 2nd retry (attempt 3)
	//   - DLQ (attempt 4 would exceed max)
	// Default: 3
	MaxRetries int

	// BackoffBaseMs is the base delay for exponential backoff on retry.
	// Actual delay = BackoffBaseMs * 2^(attempt-1)
	// Default: 1000 (1 second)
	//
	// Example with BackoffBaseMs=1000:
	//   - Retry 1: 1s delay
	//   - Retry 2: 2s delay
	//   - Retry 3: 4s delay
	//   - Retry 4: 8s delay (would be DLQ with MaxRetries=3)
	BackoffBaseMs int

	// BackoffMaxMs is the maximum backoff delay (cap).
	// Prevents exponential backoff from getting too large.
	// Default: 60000 (60 seconds)
	BackoffMaxMs int

	// BackoffMultiplier is the factor to multiply backoff by each retry.
	// Default: 2.0 (standard exponential)
	BackoffMultiplier float64

	// DLQEnabled enables dead letter queue routing.
	// If false, messages exceeding MaxRetries are dropped.
	// Default: true
	DLQEnabled bool

	// DLQSuffix is appended to topic name to create DLQ topic.
	// Example: "orders" → "orders.dlq"
	// Default: ".dlq"
	DLQSuffix string

	// DLQRetentionHours is how long to retain DLQ messages.
	// Usually longer than main topic (for investigation).
	// Default: 336 (14 days)
	DLQRetentionHours int

	// MaxInFlightPerConsumer limits concurrent in-flight messages per consumer.
	// Provides backpressure to prevent memory exhaustion.
	// Default: 1000
	MaxInFlightPerConsumer int

	// VisibilityCheckIntervalMs is how often to check for expired visibility.
	// Lower = more responsive to timeouts, higher = less CPU.
	// Default: 100 (100ms)
	VisibilityCheckIntervalMs int

	// DefaultMessageTTLMs is the default time-to-live for messages.
	// 0 = no TTL (messages live forever until consumed or retention expires)
	// Can be overridden per-message or per-topic.
	// Default: 0
	DefaultMessageTTLMs int64
}

// DefaultReliabilityConfig returns sensible defaults for reliability settings.
func DefaultReliabilityConfig() ReliabilityConfig {
	return ReliabilityConfig{
		VisibilityTimeoutMs:       30000, // 30 seconds (SQS default)
		MaxRetries:                3,     // 3 attempts before DLQ
		BackoffBaseMs:             1000,  // 1 second base
		BackoffMaxMs:              60000, // 60 second max
		BackoffMultiplier:         2.0,   // Double each retry
		DLQEnabled:                true,
		DLQSuffix:                 ".dlq",
		DLQRetentionHours:         336, // 14 days
		MaxInFlightPerConsumer:    1000,
		VisibilityCheckIntervalMs: 100, // 100ms
		DefaultMessageTTLMs:       0,   // No TTL by default
	}
}

// CalculateBackoff computes the backoff duration for a given retry attempt.
//
// EXPONENTIAL BACKOFF:
//
//	delay = base * multiplier^(attempt-1)
//	capped at max
//
// WHY EXPONENTIAL?
//   - Prevents thundering herd on transient failures
//   - Gives backend time to recover
//   - Industry standard (AWS, Google, etc.)
//
// EXAMPLE (base=1s, multiplier=2):
//
//	Attempt 1: 1s
//	Attempt 2: 2s
//	Attempt 3: 4s
//	Attempt 4: 8s
//	...
func (c *ReliabilityConfig) CalculateBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return time.Duration(c.BackoffBaseMs) * time.Millisecond
	}

	// Calculate: base * multiplier^(attempt-1)
	backoffMs := float64(c.BackoffBaseMs)
	for i := 1; i < attempt; i++ {
		backoffMs *= c.BackoffMultiplier
		if backoffMs > float64(c.BackoffMaxMs) {
			backoffMs = float64(c.BackoffMaxMs)
			break
		}
	}

	return time.Duration(backoffMs) * time.Millisecond
}

// DLQTopicName returns the DLQ topic name for a given topic.
func (c *ReliabilityConfig) DLQTopicName(topic string) string {
	return topic + c.DLQSuffix
}
