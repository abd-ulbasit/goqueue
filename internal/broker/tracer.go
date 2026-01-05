// =============================================================================
// MESSAGE TRACING - END-TO-END MESSAGE JOURNEY TRACKING
// =============================================================================
//
// WHAT IS MESSAGE TRACING?
// Message tracing provides complete visibility into a message's journey through
// goqueue - from publish to consume to acknowledgment. It answers questions like:
//   - "Where is my message right now?"
//   - "Why didn't consumer X receive message Y?"
//   - "How long did message Z spend in each stage?"
//
// WHY MESSAGE TRACING?
//
//   WITHOUT TRACING:
//   ┌──────────┐     publish     ┌──────────┐     ???     ┌──────────┐
//   │ Producer │ ──────────────► │  Broker  │ ──────────► │ Consumer │
//   │          │                 │ (black   │             │ (maybe?) │
//   └──────────┘                 │  box)    │             └──────────┘
//                                └──────────┘
//
//   WITH TRACING:
//   ┌──────────┐     publish     ┌──────────┐    fetch    ┌──────────┐
//   │ Producer │ ──────────────► │  Broker  │ ──────────► │ Consumer │
//   │          │  [trace: T1]    │          │  [trace: T1]│          │
//   └──────────┘                 │ events:  │             │ events:  │
//          │                     │ - recv   │             │ - fetch  │
//          │                     │ - route  │             │ - ack    │
//          ▼                     │ - store  │             └──────────┘
//   ┌────────────────────────────────────────────────────────────────┐
//   │                      TRACE: T1                                 │
//   │  span-1: publish.received   [0ms]                              │
//   │  span-2: publish.partitioned [0.1ms]                           │
//   │  span-3: publish.persisted  [1.2ms]                            │
//   │  span-4: consume.fetched    [5020ms] (consumer polled)         │
//   │  span-5: consume.acked      [6050ms] (consumer processed)      │
//   └────────────────────────────────────────────────────────────────┘
//
// W3C TRACE CONTEXT (INDUSTRY STANDARD):
//
// We use W3C Trace Context format for interoperability with external systems.
// https://www.w3.org/TR/trace-context/
//
//   traceparent: {version}-{trace-id}-{parent-id}-{flags}
//   Example:     00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
//
//   - version: 2 hex chars (always "00")
//   - trace-id: 32 hex chars (16 bytes) - unique per trace
//   - parent-id: 16 hex chars (8 bytes) - unique per span
//   - flags: 2 hex chars (sampled = 01)
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   ┌─────────────┬──────────────────────┬────────────────────────────────┐
//   │ System      │ Tracing Support      │ How It Works                   │
//   ├─────────────┼──────────────────────┼────────────────────────────────┤
//   │ Kafka       │ None built-in        │ Requires external (Zipkin)     │
//   │             │                      │ Headers used for propagation   │
//   ├─────────────┼──────────────────────┼────────────────────────────────┤
//   │ RabbitMQ    │ Plugin-based         │ rabbitmq_tracing plugin        │
//   │             │                      │ Logs to file, no standard fmt  │
//   ├─────────────┼──────────────────────┼────────────────────────────────┤
//   │ SQS         │ X-Ray integration    │ AWS X-Ray for distributed      │
//   │             │                      │ tracing, proprietary format    │
//   ├─────────────┼──────────────────────┼────────────────────────────────┤
//   │ goqueue     │ Native, W3C format   │ Built-in tracing with export   │
//   │             │                      │ to OTLP, Jaeger, JSON          │
//   └─────────────┴──────────────────────┴────────────────────────────────┘
//
// STORAGE ARCHITECTURE:
//
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │                         TRACE STORAGE                               │
//   │                                                                     │
//   │  ┌─────────────────────┐  ┌─────────────────────┐                   │
//   │  │ Ring Buffer         │  │ File Storage        │                   │
//   │  │ (In-Memory)         │  │ (Persistent)        │                   │
//   │  │                     │  │                     │                   │
//   │  │ • Recent traces     │  │ • Long-term archive │                   │
//   │  │ • 10MB default      │  │ • data/traces/*.json│                   │
//   │  │ • Fast queries      │  │ • Rotated daily     │                   │
//   │  │ • Lost on restart   │  │ • Survives restart  │                   │
//   │  └─────────────────────┘  └─────────────────────┘                   │
//   │                                                                     │
//   │                    ┌─────────────────────┐                          │
//   │                    │ External Export     │                          │
//   │                    │                     │                          │
//   │                    │ • stdout (debug)    │                          │
//   │                    │ • OTLP (production) │                          │
//   │                    │ • Jaeger            │                          │
//   │                    └─────────────────────┘                          │
//   └─────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// =============================================================================
// TRACE ID AND SPAN ID (W3C TRACE CONTEXT)
// =============================================================================
//
// W3C Trace Context Format:
//   - TraceID: 16 bytes (32 hex characters) - unique identifier for entire trace
//   - SpanID:  8 bytes (16 hex characters) - unique identifier for each span
//
// GENERATION STRATEGY:
// We use crypto/rand for security and uniqueness. While Snowflake IDs offer
// time-sortability, W3C compatibility is more valuable for production
// observability integration.
//

// TraceID is a 16-byte unique identifier for a trace (32 hex chars).
// All spans in the same message journey share this ID.
type TraceID [16]byte

// SpanID is an 8-byte unique identifier for a span (16 hex chars).
// Each event in the trace gets a unique SpanID.
type SpanID [8]byte

// String returns the hex representation of TraceID.
func (t TraceID) String() string {
	return hex.EncodeToString(t[:])
}

// String returns the hex representation of SpanID.
func (s SpanID) String() string {
	return hex.EncodeToString(s[:])
}

// IsZero returns true if the TraceID is all zeros (not initialized).
func (t TraceID) IsZero() bool {
	for _, b := range t {
		if b != 0 {
			return false
		}
	}
	return true
}

// IsZero returns true if the SpanID is all zeros (not initialized).
func (s SpanID) IsZero() bool {
	for _, b := range s {
		if b != 0 {
			return false
		}
	}
	return true
}

// NewTraceID generates a new random TraceID.
func NewTraceID() TraceID {
	var t TraceID
	rand.Read(t[:])
	return t
}

// NewSpanID generates a new random SpanID.
func NewSpanID() SpanID {
	var s SpanID
	rand.Read(s[:])
	return s
}

// ParseTraceID parses a 32-character hex string into a TraceID.
func ParseTraceID(s string) (TraceID, error) {
	var t TraceID
	if len(s) != 32 {
		return t, fmt.Errorf("invalid trace ID length: expected 32, got %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return t, fmt.Errorf("invalid trace ID hex: %w", err)
	}
	copy(t[:], b)
	return t, nil
}

// ParseSpanID parses a 16-character hex string into a SpanID.
func ParseSpanID(s string) (SpanID, error) {
	var sp SpanID
	if len(s) != 16 {
		return sp, fmt.Errorf("invalid span ID length: expected 16, got %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return sp, fmt.Errorf("invalid span ID hex: %w", err)
	}
	copy(sp[:], b)
	return sp, nil
}

// =============================================================================
// TRACE CONTEXT (W3C FORMAT)
// =============================================================================
//
// TraceContext is propagated through message headers to maintain trace
// continuity across service boundaries.
//
// HEADER FORMAT:
//   traceparent: {version}-{trace-id}-{span-id}-{flags}
//   tracestate:  vendor-specific key-value pairs (optional)
//
// FLAGS:
//   bit 0: sampled (1 = trace is recorded, 0 = may be dropped)
//   bits 1-7: reserved
//
// =============================================================================

// TraceFlags contains trace context flags.
type TraceFlags uint8

const (
	// TraceFlagSampled indicates the trace should be recorded.
	TraceFlagSampled TraceFlags = 0x01
)

// TraceContext holds W3C Trace Context information for a message.
// This is stored in message headers and propagated through the system.
type TraceContext struct {
	// TraceID is the unique identifier for the entire trace
	TraceID TraceID `json:"trace_id"`

	// SpanID is the current span's identifier
	SpanID SpanID `json:"span_id"`

	// ParentSpanID is the parent span (zero if this is root)
	ParentSpanID SpanID `json:"parent_span_id,omitempty"`

	// Flags contain trace flags (sampled, etc.)
	Flags TraceFlags `json:"flags"`

	// TraceState holds vendor-specific trace state (optional)
	TraceState map[string]string `json:"trace_state,omitempty"`
}

// NewTraceContext creates a new root trace context.
func NewTraceContext() TraceContext {
	return TraceContext{
		TraceID: NewTraceID(),
		SpanID:  NewSpanID(),
		Flags:   TraceFlagSampled,
	}
}

// NewChildContext creates a child span context from a parent.
func (tc TraceContext) NewChildContext() TraceContext {
	return TraceContext{
		TraceID:      tc.TraceID,
		SpanID:       NewSpanID(),
		ParentSpanID: tc.SpanID,
		Flags:        tc.Flags,
		TraceState:   tc.TraceState,
	}
}

// IsSampled returns true if this trace should be recorded.
func (tc TraceContext) IsSampled() bool {
	return tc.Flags&TraceFlagSampled != 0
}

// Traceparent returns the W3C traceparent header value.
// Format: {version}-{trace-id}-{span-id}-{flags}
func (tc TraceContext) Traceparent() string {
	return fmt.Sprintf("00-%s-%s-%02x", tc.TraceID.String(), tc.SpanID.String(), tc.Flags)
}

// ParseTraceparent parses a W3C traceparent header value.
func ParseTraceparent(s string) (TraceContext, error) {
	var tc TraceContext

	if len(s) < 55 {
		return tc, fmt.Errorf("traceparent too short: %d chars", len(s))
	}

	// Parse version (must be 00)
	if s[0:2] != "00" {
		return tc, fmt.Errorf("unsupported traceparent version: %s", s[0:2])
	}

	// Parse trace-id
	traceID, err := ParseTraceID(s[3:35])
	if err != nil {
		return tc, err
	}
	tc.TraceID = traceID

	// Parse span-id
	spanID, err := ParseSpanID(s[36:52])
	if err != nil {
		return tc, err
	}
	tc.SpanID = spanID

	// Parse flags
	var flags uint8
	_, err = fmt.Sscanf(s[53:55], "%02x", &flags)
	if err != nil {
		return tc, fmt.Errorf("invalid flags: %w", err)
	}
	tc.Flags = TraceFlags(flags)

	return tc, nil
}

// =============================================================================
// SPAN EVENT TYPES
// =============================================================================
//
// SpanEventType defines the type of event in a message's lifecycle.
// Each event represents a significant point in the message journey.
//
// LIFECYCLE:
//   publish.received → publish.partitioned → publish.persisted
//   → consume.fetched → consume.acked (or consume.nacked/consume.rejected)
//
// DELAY EVENTS (M5):
//   delay.scheduled → delay.fired
//
// VISIBILITY EVENTS (M4):
//   visibility.extended / visibility.expired
//
// =============================================================================

// SpanEventType defines the type of trace event.
type SpanEventType string

const (
	// === PUBLISH EVENTS ===

	// SpanEventPublishReceived - message received by broker
	SpanEventPublishReceived SpanEventType = "publish.received"

	// SpanEventValidationFailed - message failed schema validation
	SpanEventValidationFailed SpanEventType = "publish.validation_failed"

	// SpanEventPublishPartitioned - partition assigned to message
	SpanEventPublishPartitioned SpanEventType = "publish.partitioned"

	// SpanEventPublishPersisted - message written to segment file
	SpanEventPublishPersisted SpanEventType = "publish.persisted"

	// === CONSUME EVENTS ===

	// SpanEventConsumeFetched - consumer received the message
	SpanEventConsumeFetched SpanEventType = "consume.fetched"

	// SpanEventConsumeAcked - consumer acknowledged successful processing
	SpanEventConsumeAcked SpanEventType = "consume.acked"

	// SpanEventConsumeNacked - consumer returned NACK (transient failure, will retry)
	SpanEventConsumeNacked SpanEventType = "consume.nacked"

	// SpanEventConsumeRejected - consumer rejected message (permanent failure, to DLQ)
	SpanEventConsumeRejected SpanEventType = "consume.rejected"

	// === DELAY EVENTS (M5) ===

	// SpanEventDelayScheduled - message scheduled for delayed delivery
	SpanEventDelayScheduled SpanEventType = "delay.scheduled"

	// SpanEventDelayFired - delay timer expired, message now visible
	SpanEventDelayFired SpanEventType = "delay.fired"

	// SpanEventDelayCancelled - delayed message was cancelled before delivery
	SpanEventDelayCancelled SpanEventType = "delay.cancelled"

	// === VISIBILITY EVENTS (M4) ===

	// SpanEventVisibilityExtended - visibility timeout was extended
	SpanEventVisibilityExtended SpanEventType = "visibility.extended"

	// SpanEventVisibilityExpired - visibility timeout expired (message requeued)
	SpanEventVisibilityExpired SpanEventType = "visibility.expired"

	// === DLQ EVENTS ===

	// SpanEventDLQRouted - message routed to dead letter queue
	SpanEventDLQRouted SpanEventType = "dlq.routed"

	// === REBALANCE EVENTS ===

	// SpanEventRebalanceStart - consumer group rebalancing started
	SpanEventRebalanceStart SpanEventType = "rebalance.start"

	// SpanEventRebalanceComplete - consumer group rebalancing completed
	SpanEventRebalanceComplete SpanEventType = "rebalance.complete"
)

// =============================================================================
// SPAN (SINGLE EVENT IN A TRACE)
// =============================================================================
//
// A Span represents a single event or operation in the message lifecycle.
// Multiple spans with the same TraceID form a complete trace.
//
// SPAN ATTRIBUTES:
// We follow OpenTelemetry semantic conventions for attribute names:
//   - messaging.system = "goqueue"
//   - messaging.destination.name = topic name
//   - messaging.destination.partition.id = partition number
//   - messaging.message.id = offset
//
// =============================================================================

// Span represents a single event in a message trace.
type Span struct {
	// TraceID links this span to its trace
	TraceID TraceID `json:"trace_id"`

	// SpanID uniquely identifies this span
	SpanID SpanID `json:"span_id"`

	// ParentSpanID is the parent span (zero if root)
	ParentSpanID SpanID `json:"parent_span_id,omitempty"`

	// EventType is what happened (publish.received, consume.acked, etc.)
	EventType SpanEventType `json:"event_type"`

	// Timestamp when this event occurred (Unix nanoseconds)
	Timestamp int64 `json:"timestamp"`

	// Duration of the operation in nanoseconds (0 if instant)
	Duration int64 `json:"duration_ns,omitempty"`

	// === MESSAGE CONTEXT ===

	// Topic name
	Topic string `json:"topic"`

	// Partition number
	Partition int `json:"partition"`

	// Offset within partition
	Offset int64 `json:"offset"`

	// Priority level (0-4)
	Priority uint8 `json:"priority,omitempty"`

	// === CONSUMER CONTEXT (if applicable) ===

	// ConsumerGroup ID
	ConsumerGroup string `json:"consumer_group,omitempty"`

	// ConsumerID within the group
	ConsumerID string `json:"consumer_id,omitempty"`

	// ReceiptHandle for ACK/NACK operations
	ReceiptHandle string `json:"receipt_handle,omitempty"`

	// === DELAY CONTEXT (M5) ===

	// DeliverAt is when the message is scheduled to be delivered
	DeliverAt int64 `json:"deliver_at,omitempty"`

	// DelayMs is the delay duration in milliseconds
	DelayMs int64 `json:"delay_ms,omitempty"`

	// === ERROR CONTEXT ===

	// Error message if the operation failed
	Error string `json:"error,omitempty"`

	// === BROKER CONTEXT ===

	// NodeID is the broker that processed this event
	NodeID string `json:"node_id,omitempty"`

	// Attributes holds additional key-value pairs
	Attributes map[string]string `json:"attributes,omitempty"`
}

// NewSpan creates a new span for a trace event.
func NewSpan(traceID TraceID, eventType SpanEventType, topic string, partition int, offset int64) *Span {
	return &Span{
		TraceID:   traceID,
		SpanID:    NewSpanID(),
		EventType: eventType,
		Timestamp: time.Now().UnixNano(),
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	}
}

// WithParent sets the parent span ID.
func (s *Span) WithParent(parentID SpanID) *Span {
	s.ParentSpanID = parentID
	return s
}

// WithDuration sets the duration in nanoseconds.
func (s *Span) WithDuration(d time.Duration) *Span {
	s.Duration = d.Nanoseconds()
	return s
}

// WithConsumer sets consumer context.
func (s *Span) WithConsumer(groupID, consumerID string) *Span {
	s.ConsumerGroup = groupID
	s.ConsumerID = consumerID
	return s
}

// WithReceiptHandle sets the receipt handle.
func (s *Span) WithReceiptHandle(handle string) *Span {
	s.ReceiptHandle = handle
	return s
}

// WithDelay sets delay context.
func (s *Span) WithDelay(deliverAt time.Time, delayMs int64) *Span {
	s.DeliverAt = deliverAt.UnixNano()
	s.DelayMs = delayMs
	return s
}

// WithError sets error information.
func (s *Span) WithError(err error) *Span {
	if err != nil {
		s.Error = err.Error()
	}
	return s
}

// WithNodeID sets the broker node ID.
func (s *Span) WithNodeID(nodeID string) *Span {
	s.NodeID = nodeID
	return s
}

// WithPriority sets the message priority.
func (s *Span) WithPriority(priority uint8) *Span {
	s.Priority = priority
	return s
}

// WithAttribute adds a custom attribute.
func (s *Span) WithAttribute(key, value string) *Span {
	if s.Attributes == nil {
		s.Attributes = make(map[string]string)
	}
	s.Attributes[key] = value
	return s
}

// =============================================================================
// TRACE (COLLECTION OF SPANS)
// =============================================================================
//
// A Trace is the complete journey of a message through the system.
// It aggregates all spans with the same TraceID for viewing.
//

// Trace represents the complete journey of a message.
type Trace struct {
	// TraceID uniquely identifies this trace
	TraceID TraceID `json:"trace_id"`

	// Topic the message belongs to
	Topic string `json:"topic"`

	// Partition the message is in
	Partition int `json:"partition"`

	// Offset within the partition
	Offset int64 `json:"offset"`

	// StartTime is when the trace began (first span timestamp)
	StartTime time.Time `json:"start_time"`

	// EndTime is when the trace completed (last span timestamp)
	EndTime time.Time `json:"end_time"`

	// Duration is the total trace duration
	Duration time.Duration `json:"duration"`

	// Status summarizes the trace outcome
	Status TraceStatus `json:"status"`

	// Spans are all events in this trace, ordered by timestamp
	Spans []*Span `json:"spans"`
}

// TraceStatus indicates the overall trace outcome.
type TraceStatus string

const (
	TraceStatusInProgress TraceStatus = "in_progress"
	TraceStatusCompleted  TraceStatus = "completed" // ACKed
	TraceStatusFailed     TraceStatus = "failed"    // Rejected/DLQ
	TraceStatusRetrying   TraceStatus = "retrying"  // NACKed, will retry
	TraceStatusDelayed    TraceStatus = "delayed"   // Waiting for delay
	TraceStatusExpired    TraceStatus = "expired"   // Visibility expired
)

// =============================================================================
// RING BUFFER - IN-MEMORY TRACE STORAGE
// =============================================================================
//
// WHY RING BUFFER?
// - Fixed memory usage (10MB default, ~100K traces)
// - O(1) insert (always append to head)
// - Recent traces readily available for debugging
// - Automatic eviction of old traces (FIFO)
//
// STRUCTURE:
//   ┌───┬───┬───┬───┬───┬───┬───┬───┐
//   │ T7│ T6│ T5│ T4│ T3│ T2│ T1│ T0│  (circular)
//   └───┴───┴───┴───┴───┴───┴───┴───┘
//         ▲                       ▲
//         │                       │
//       head                    tail
//     (newest)                (oldest)
//
// =============================================================================

// RingBuffer stores recent spans in memory.
type RingBuffer struct {
	spans    []*Span
	capacity int
	head     int
	count    int
	mu       sync.RWMutex
}

// NewRingBuffer creates a ring buffer with the given capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		spans:    make([]*Span, capacity),
		capacity: capacity,
	}
}

// Push adds a span to the ring buffer.
func (rb *RingBuffer) Push(span *Span) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.spans[rb.head] = span
	rb.head = (rb.head + 1) % rb.capacity
	if rb.count < rb.capacity {
		rb.count++
	}
}

// GetRecent returns the N most recent spans.
func (rb *RingBuffer) GetRecent(n int) []*Span {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if n > rb.count {
		n = rb.count
	}

	result := make([]*Span, n)
	for i := 0; i < n; i++ {
		// Start from head-1 (most recent) and go backwards
		idx := (rb.head - 1 - i + rb.capacity) % rb.capacity
		result[i] = rb.spans[idx]
	}

	return result
}

// GetByTraceID returns all spans for a given trace ID.
func (rb *RingBuffer) GetByTraceID(traceID TraceID) []*Span {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	var result []*Span
	for i := 0; i < rb.count; i++ {
		idx := (rb.head - 1 - i + rb.capacity) % rb.capacity
		if rb.spans[idx] != nil && rb.spans[idx].TraceID == traceID {
			result = append(result, rb.spans[idx])
		}
	}

	// Sort by timestamp (oldest first)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result
}

// GetByTimeRange returns spans within a time range.
func (rb *RingBuffer) GetByTimeRange(start, end time.Time) []*Span {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	startNano := start.UnixNano()
	endNano := end.UnixNano()

	var result []*Span
	for i := 0; i < rb.count; i++ {
		idx := (rb.head - 1 - i + rb.capacity) % rb.capacity
		span := rb.spans[idx]
		if span != nil && span.Timestamp >= startNano && span.Timestamp <= endNano {
			result = append(result, span)
		}
	}

	return result
}

// Count returns the number of spans in the buffer.
func (rb *RingBuffer) Count() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.count
}

// Capacity returns the buffer capacity.
func (rb *RingBuffer) Capacity() int {
	return rb.capacity
}

// =============================================================================
// TRACE EXPORTER INTERFACE
// =============================================================================
//
// Exporters send traces to external systems. We support:
//   - stdout: For debugging (writes JSON to console)
//   - file: Persistent JSON files in data/traces/
//   - OTLP: OpenTelemetry Protocol (for Jaeger, Tempo, etc.)
//   - Jaeger: Direct Jaeger Thrift protocol
//
// =============================================================================

// TraceExporter defines the interface for exporting traces.
type TraceExporter interface {
	// Export sends a span to the external system
	Export(ctx context.Context, span *Span) error

	// ExportBatch sends multiple spans
	ExportBatch(ctx context.Context, spans []*Span) error

	// Shutdown gracefully closes the exporter
	Shutdown(ctx context.Context) error

	// Name returns the exporter name (for logging)
	Name() string
}

// =============================================================================
// STDOUT EXPORTER (DEBUG)
// =============================================================================

// StdoutExporter writes traces to stdout as JSON.
type StdoutExporter struct {
	writer  io.Writer
	encoder *json.Encoder
	mu      sync.Mutex
}

// NewStdoutExporter creates a stdout exporter.
func NewStdoutExporter() *StdoutExporter {
	return &StdoutExporter{
		writer:  os.Stdout,
		encoder: json.NewEncoder(os.Stdout),
	}
}

// NewStdoutExporterWithWriter creates a stdout exporter with custom writer.
func NewStdoutExporterWithWriter(w io.Writer) *StdoutExporter {
	return &StdoutExporter{
		writer:  w,
		encoder: json.NewEncoder(w),
	}
}

func (e *StdoutExporter) Export(ctx context.Context, span *Span) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.encoder.Encode(span)
}

func (e *StdoutExporter) ExportBatch(ctx context.Context, spans []*Span) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, span := range spans {
		if err := e.encoder.Encode(span); err != nil {
			return err
		}
	}
	return nil
}

func (e *StdoutExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (e *StdoutExporter) Name() string {
	return "stdout"
}

// =============================================================================
// FILE EXPORTER (PERSISTENT JSON)
// =============================================================================
//
// FileExporter writes traces to JSON files in data/traces/.
// Files are rotated based on size or time.
//
// FILE NAMING:
//   traces-{date}-{sequence}.json
//   Example: traces-2024-01-15-001.json
//
// =============================================================================

// FileExporter writes traces to JSON files.
type FileExporter struct {
	dir         string
	maxFileSize int64
	currentFile *os.File
	encoder     *json.Encoder
	currentSize int64
	fileCount   int
	mu          sync.Mutex
}

// NewFileExporter creates a file exporter.
func NewFileExporter(dir string, maxFileSize int64) (*FileExporter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create trace directory: %w", err)
	}

	e := &FileExporter{
		dir:         dir,
		maxFileSize: maxFileSize,
	}

	if err := e.rotateFile(); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *FileExporter) rotateFile() error {
	if e.currentFile != nil {
		e.currentFile.Close()
	}

	date := time.Now().Format("2006-01-02")
	e.fileCount++
	filename := fmt.Sprintf("traces-%s-%03d.json", date, e.fileCount)
	path := filepath.Join(e.dir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create trace file: %w", err)
	}

	e.currentFile = file
	e.encoder = json.NewEncoder(file)
	e.currentSize = 0

	return nil
}

func (e *FileExporter) Export(ctx context.Context, span *Span) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Check if rotation needed
	if e.currentSize >= e.maxFileSize {
		if err := e.rotateFile(); err != nil {
			return err
		}
	}

	data, err := json.Marshal(span)
	if err != nil {
		return err
	}

	if err := e.encoder.Encode(span); err != nil {
		return err
	}

	e.currentSize += int64(len(data))
	return nil
}

func (e *FileExporter) ExportBatch(ctx context.Context, spans []*Span) error {
	for _, span := range spans {
		if err := e.Export(ctx, span); err != nil {
			return err
		}
	}
	return nil
}

func (e *FileExporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.currentFile != nil {
		return e.currentFile.Close()
	}
	return nil
}

func (e *FileExporter) Name() string {
	return "file"
}

// =============================================================================
// OTLP EXPORTER (OPENTELEMETRY PROTOCOL)
// =============================================================================
//
// OTLP is the standard protocol for OpenTelemetry. It allows goqueue to send
// traces to any OTLP-compatible backend:
//   - Jaeger (with OTLP collector)
//   - Grafana Tempo
//   - Honeycomb
//   - Datadog
//   - New Relic
//
// PRODUCTION SETUP:
//
//   LOCAL TESTING:
//     1. Run Jaeger with OTLP enabled:
//        docker run -d --name jaeger \
//          -p 16686:16686 \
//          -p 4317:4317 \
//          jaegertracing/all-in-one:latest
//     2. Configure goqueue: endpoint = "localhost:4317"
//     3. View traces at http://localhost:16686
//
//   PRODUCTION (Kubernetes):
//     1. Deploy OpenTelemetry Collector as DaemonSet or Sidecar
//     2. Configure goqueue to send to collector: endpoint = "otel-collector:4317"
//     3. Collector routes to your backend (Jaeger, Tempo, etc.)
//
//   CLOUD (Managed Jaeger/Tempo):
//     1. Get endpoint from your cloud provider
//     2. Configure goqueue with endpoint and auth headers
//
// =============================================================================

// OTLPExporterConfig configures the OTLP exporter.
type OTLPExporterConfig struct {
	// Endpoint is the OTLP collector address (e.g., "localhost:4317")
	Endpoint string

	// Insecure disables TLS (for local development)
	Insecure bool

	// Headers for authentication (e.g., API keys)
	Headers map[string]string

	// ServiceName identifies this service in traces
	ServiceName string

	// BatchSize is how many spans to batch before sending
	BatchSize int

	// FlushInterval is how often to flush batches
	FlushInterval time.Duration
}

// DefaultOTLPExporterConfig returns sensible defaults.
func DefaultOTLPExporterConfig() OTLPExporterConfig {
	return OTLPExporterConfig{
		Endpoint:      "localhost:4317",
		Insecure:      true,
		ServiceName:   "goqueue",
		BatchSize:     100,
		FlushInterval: 5 * time.Second,
	}
}

// OTLPExporter sends traces via OTLP protocol.
// NOTE: This is a simplified implementation. For production, use the official
// TODO: use that package after we are done with all the milestones.
// go.opentelemetry.io/otel/exporters/otlp/otlptrace package.
type OTLPExporter struct {
	config  OTLPExporterConfig
	batch   []*Span
	mu      sync.Mutex
	done    chan struct{}
	enabled bool
}

// NewOTLPExporter creates an OTLP exporter.
//
// PRODUCTION USAGE:
//
//	config := broker.OTLPExporterConfig{
//	    Endpoint:    "otel-collector.monitoring:4317",
//	    ServiceName: "goqueue-prod",
//	    Headers: map[string]string{
//	        "Authorization": "Bearer " + os.Getenv("OTEL_AUTH_TOKEN"),
//	    },
//	}
//	exporter, err := broker.NewOTLPExporter(config)
func NewOTLPExporter(config OTLPExporterConfig) *OTLPExporter {
	e := &OTLPExporter{
		config:  config,
		batch:   make([]*Span, 0, config.BatchSize),
		done:    make(chan struct{}),
		enabled: config.Endpoint != "",
	}

	if e.enabled {
		go e.flushLoop()
	}

	return e
}

func (e *OTLPExporter) flushLoop() {
	ticker := time.NewTicker(e.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.flush()
		case <-e.done:
			e.flush() // Final flush
			return
		}
	}
}

func (e *OTLPExporter) flush() {
	e.mu.Lock()
	if len(e.batch) == 0 {
		e.mu.Unlock()
		return
	}

	spans := e.batch
	e.batch = make([]*Span, 0, e.config.BatchSize)
	e.mu.Unlock()

	// Convert to OTLP format and send
	// In production, this would use gRPC to send to the collector
	e.sendOTLP(spans)
}

// sendOTLP converts spans to OTLP format and sends to collector.
// This is a stub - real implementation would use gRPC.
func (e *OTLPExporter) sendOTLP(spans []*Span) {
	// TODO: Implement actual OTLP protocol
	// For now, this is a placeholder for the OTLP wire format
	//
	// The real implementation would:
	// 1. Convert spans to protobuf format
	// 2. Create a TraceService.Export request
	// 3. Send via gRPC to e.config.Endpoint
	//
	// Example using official SDK:
	//   import "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	//   exporter, _ := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint))
}

func (e *OTLPExporter) Export(ctx context.Context, span *Span) error {
	if !e.enabled {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.batch = append(e.batch, span)

	if len(e.batch) >= e.config.BatchSize {
		// Trigger immediate flush
		go e.flush()
	}

	return nil
}

func (e *OTLPExporter) ExportBatch(ctx context.Context, spans []*Span) error {
	for _, span := range spans {
		if err := e.Export(ctx, span); err != nil {
			return err
		}
	}
	return nil
}

func (e *OTLPExporter) Shutdown(ctx context.Context) error {
	if e.enabled {
		close(e.done)
	}
	return nil
}

func (e *OTLPExporter) Name() string {
	return "otlp"
}

// =============================================================================
// JAEGER EXPORTER (DIRECT THRIFT)
// =============================================================================
//
// Jaeger Thrift exporter sends traces directly to Jaeger using Thrift protocol.
// This is useful when you don't want to run an OTLP collector.
//
// PRODUCTION SETUP:
//
//   LOCAL TESTING:
//     1. Run Jaeger:
//        docker run -d --name jaeger \
//          -p 16686:16686 \
//          -p 6831:6831/udp \
//          -p 14268:14268 \
//          jaegertracing/all-in-one:latest
//     2. Configure goqueue: endpoint = "localhost:14268"
//     3. View traces at http://localhost:16686
//
//   PRODUCTION:
//     - Use OTLP instead (more flexible)
//     - Or configure Jaeger collector endpoint with auth
//
// =============================================================================

// JaegerExporterConfig configures the Jaeger exporter.
type JaegerExporterConfig struct {
	// Endpoint is the Jaeger collector HTTP endpoint (e.g., "http://localhost:14268/api/traces")
	Endpoint string

	// AgentHost for UDP spans (e.g., "localhost")
	AgentHost string

	// AgentPort for UDP spans (default 6831)
	AgentPort int

	// ServiceName identifies this service
	ServiceName string

	// Tags added to all spans
	Tags map[string]string
}

// DefaultJaegerExporterConfig returns sensible defaults.
func DefaultJaegerExporterConfig() JaegerExporterConfig {
	return JaegerExporterConfig{
		Endpoint:    "http://localhost:14268/api/traces",
		AgentHost:   "localhost",
		AgentPort:   6831,
		ServiceName: "goqueue",
	}
}

// JaegerExporter sends traces directly to Jaeger.
type JaegerExporter struct {
	config  JaegerExporterConfig
	enabled bool
}

// NewJaegerExporter creates a Jaeger exporter.
func NewJaegerExporter(config JaegerExporterConfig) *JaegerExporter {
	return &JaegerExporter{
		config:  config,
		enabled: config.Endpoint != "" || config.AgentHost != "",
	}
}

func (e *JaegerExporter) Export(ctx context.Context, span *Span) error {
	if !e.enabled {
		return nil
	}

	// TODO: Implement Jaeger Thrift protocol
	// The real implementation would:
	// 1. Convert span to Jaeger Thrift format
	// 2. Send via HTTP POST to collector endpoint
	//    OR via UDP to agent
	//
	// Example using official SDK:
	//   import "go.opentelemetry.io/otel/exporters/jaeger"
	//   exporter, _ := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))

	return nil
}

func (e *JaegerExporter) ExportBatch(ctx context.Context, spans []*Span) error {
	for _, span := range spans {
		if err := e.Export(ctx, span); err != nil {
			return err
		}
	}
	return nil
}

func (e *JaegerExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (e *JaegerExporter) Name() string {
	return "jaeger"
}

// =============================================================================
// TRACER - MAIN TRACING COORDINATOR
// =============================================================================
//
// Tracer is the central component that:
//   1. Generates trace contexts for new messages
//   2. Records spans to ring buffer
//   3. Exports spans to configured exporters
//   4. Provides query APIs for trace lookup
//
// USAGE:
//
//	tracer := broker.NewTracer(config)
//	defer tracer.Shutdown()
//
//	// On publish
//	ctx := tracer.StartTrace(topic, partition, offset)
//	tracer.RecordSpan(broker.NewSpan(ctx.TraceID, broker.SpanEventPublishReceived, ...))
//
//	// On consume
//	tracer.RecordSpan(broker.NewSpan(ctx.TraceID, broker.SpanEventConsumeFetched, ...))
//
// =============================================================================

// TracerConfig configures the tracer.
type TracerConfig struct {
	// Enabled controls whether tracing is active
	Enabled bool

	// RingBufferCapacity is how many spans to keep in memory
	// Default: 100000 (~10MB at ~100 bytes per span)
	RingBufferCapacity int

	// FileExportEnabled enables persistent file export
	FileExportEnabled bool

	// FileExportDir is where trace files are written
	FileExportDir string

	// FileMaxSize is max size per trace file before rotation
	FileMaxSize int64

	// StdoutExportEnabled enables stdout export (for debugging)
	StdoutExportEnabled bool

	// OTLPConfig for OTLP export (production)
	OTLPConfig *OTLPExporterConfig

	// JaegerConfig for direct Jaeger export
	JaegerConfig *JaegerExporterConfig

	// SamplingRate is the percentage of traces to record (0.0-1.0)
	// 1.0 = all traces, 0.1 = 10% of traces
	SamplingRate float64

	// PerTopicEnabled maps topic names to their tracing enabled status
	// Topics not in this map use the default (Enabled)
	PerTopicEnabled map[string]bool

	// NodeID identifies this broker node
	NodeID string
}

// DefaultTracerConfig returns sensible defaults.
func DefaultTracerConfig(dataDir string) TracerConfig {
	return TracerConfig{
		Enabled:             true,
		RingBufferCapacity:  100000, // ~10MB
		FileExportEnabled:   true,
		FileExportDir:       filepath.Join(dataDir, "traces"),
		FileMaxSize:         10 * 1024 * 1024, // 10MB per file
		StdoutExportEnabled: false,
		SamplingRate:        1.0, // Trace everything by default
		PerTopicEnabled:     make(map[string]bool),
		NodeID:              "node-1",
	}
}

// Tracer manages message tracing.
type Tracer struct {
	config    TracerConfig
	ringBuf   *RingBuffer
	exporters []TraceExporter
	mu        sync.RWMutex
	closed    bool

	// traceIndex maps TraceID to list of spans (for efficient lookup)
	traceIndex map[TraceID][]*Span
	indexMu    sync.RWMutex
}

// NewTracer creates and starts a new tracer.
func NewTracer(config TracerConfig) (*Tracer, error) {
	if !config.Enabled {
		return &Tracer{config: config}, nil
	}

	t := &Tracer{
		config:     config,
		ringBuf:    NewRingBuffer(config.RingBufferCapacity),
		exporters:  make([]TraceExporter, 0),
		traceIndex: make(map[TraceID][]*Span),
	}

	// Set up exporters
	if config.StdoutExportEnabled {
		t.exporters = append(t.exporters, NewStdoutExporter())
	}

	if config.FileExportEnabled {
		fileExp, err := NewFileExporter(config.FileExportDir, config.FileMaxSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create file exporter: %w", err)
		}
		t.exporters = append(t.exporters, fileExp)
	}

	if config.OTLPConfig != nil && config.OTLPConfig.Endpoint != "" {
		t.exporters = append(t.exporters, NewOTLPExporter(*config.OTLPConfig))
	}

	if config.JaegerConfig != nil && (config.JaegerConfig.Endpoint != "" || config.JaegerConfig.AgentHost != "") {
		t.exporters = append(t.exporters, NewJaegerExporter(*config.JaegerConfig))
	}

	return t, nil
}

// IsEnabled returns true if tracing is enabled.
func (t *Tracer) IsEnabled() bool {
	return t.config.Enabled
}

// IsTopicEnabled checks if tracing is enabled for a specific topic.
func (t *Tracer) IsTopicEnabled(topic string) bool {
	if !t.config.Enabled {
		return false
	}

	// Check topic-specific setting
	if enabled, exists := t.config.PerTopicEnabled[topic]; exists {
		return enabled
	}

	// Default to global setting
	return true
}

// StartTrace creates a new trace context for a message.
func (t *Tracer) StartTrace(topic string, partition int, offset int64) TraceContext {
	if !t.IsTopicEnabled(topic) {
		return TraceContext{} // Return empty context
	}

	// Apply sampling
	if t.config.SamplingRate < 1.0 {
		// Simple random sampling
		var b [1]byte
		rand.Read(b[:])
		if float64(b[0])/255.0 > t.config.SamplingRate {
			return TraceContext{} // Not sampled
		}
	}

	return NewTraceContext()
}

// RecordSpan records a span to the ring buffer and exports.
func (t *Tracer) RecordSpan(span *Span) {
	if !t.config.Enabled || span == nil || span.TraceID.IsZero() {
		return
	}

	// Add node ID
	span.NodeID = t.config.NodeID

	// Store in ring buffer
	t.ringBuf.Push(span)

	// Update trace index
	t.indexMu.Lock()
	t.traceIndex[span.TraceID] = append(t.traceIndex[span.TraceID], span)
	t.indexMu.Unlock()

	// Export to all exporters
	ctx := context.Background()
	for _, exp := range t.exporters {
		go exp.Export(ctx, span)
	}
}

// GetTrace retrieves a complete trace by ID.
func (t *Tracer) GetTrace(traceID TraceID) *Trace {
	if !t.config.Enabled {
		return nil
	}

	// First try the index
	t.indexMu.RLock()
	spans, exists := t.traceIndex[traceID]
	t.indexMu.RUnlock()

	if !exists || len(spans) == 0 {
		// Fall back to ring buffer search
		spans = t.ringBuf.GetByTraceID(traceID)
		if len(spans) == 0 {
			return nil
		}
	}

	// Sort spans by timestamp
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].Timestamp < spans[j].Timestamp
	})

	// Build trace
	trace := &Trace{
		TraceID:   traceID,
		Topic:     spans[0].Topic,
		Partition: spans[0].Partition,
		Offset:    spans[0].Offset,
		StartTime: time.Unix(0, spans[0].Timestamp),
		EndTime:   time.Unix(0, spans[len(spans)-1].Timestamp),
		Spans:     spans,
	}

	trace.Duration = trace.EndTime.Sub(trace.StartTime)
	trace.Status = t.determineTraceStatus(spans)

	return trace
}

// determineTraceStatus determines the overall trace status from spans.
func (t *Tracer) determineTraceStatus(spans []*Span) TraceStatus {
	for i := len(spans) - 1; i >= 0; i-- {
		switch spans[i].EventType {
		case SpanEventConsumeAcked:
			return TraceStatusCompleted
		case SpanEventConsumeRejected, SpanEventDLQRouted:
			return TraceStatusFailed
		case SpanEventConsumeNacked:
			return TraceStatusRetrying
		case SpanEventDelayScheduled:
			return TraceStatusDelayed
		case SpanEventVisibilityExpired:
			return TraceStatusExpired
		}
	}
	return TraceStatusInProgress
}

// GetRecentTraces returns recent traces (grouped from recent spans).
func (t *Tracer) GetRecentTraces(limit int) []*Trace {
	if !t.config.Enabled {
		return nil
	}

	// Get recent spans
	recentSpans := t.ringBuf.GetRecent(limit * 10) // Get more spans to find traces

	// Group by trace ID
	traceMap := make(map[TraceID][]*Span)
	for _, span := range recentSpans {
		traceMap[span.TraceID] = append(traceMap[span.TraceID], span)
	}

	// Build traces
	traces := make([]*Trace, 0, len(traceMap))
	for traceID, spans := range traceMap {
		if len(spans) == 0 {
			continue
		}

		// Sort spans by timestamp
		sort.Slice(spans, func(i, j int) bool {
			return spans[i].Timestamp < spans[j].Timestamp
		})

		trace := &Trace{
			TraceID:   traceID,
			Topic:     spans[0].Topic,
			Partition: spans[0].Partition,
			Offset:    spans[0].Offset,
			StartTime: time.Unix(0, spans[0].Timestamp),
			EndTime:   time.Unix(0, spans[len(spans)-1].Timestamp),
			Spans:     spans,
		}
		trace.Duration = trace.EndTime.Sub(trace.StartTime)
		trace.Status = t.determineTraceStatus(spans)

		traces = append(traces, trace)
	}

	// Sort by start time (newest first)
	sort.Slice(traces, func(i, j int) bool {
		return traces[i].StartTime.After(traces[j].StartTime)
	})

	// Limit results
	if len(traces) > limit {
		traces = traces[:limit]
	}

	return traces
}

// SearchTraces searches for traces matching criteria.
func (t *Tracer) SearchTraces(query TraceQuery) []*Trace {
	if !t.config.Enabled {
		return nil
	}

	// Get spans in time range
	var spans []*Span
	if !query.StartTime.IsZero() && !query.EndTime.IsZero() {
		spans = t.ringBuf.GetByTimeRange(query.StartTime, query.EndTime)
	} else {
		// TODO: we are fetching all recent spans ? cap it ?? page no/page size ?
		spans = t.ringBuf.GetRecent(t.config.RingBufferCapacity)
	}

	// Group by trace ID and filter
	traceMap := make(map[TraceID][]*Span)
	for _, span := range spans {
		// Apply filters
		if query.Topic != "" && span.Topic != query.Topic {
			continue
		}
		if query.Partition >= 0 && span.Partition != query.Partition {
			continue
		}
		if query.ConsumerGroup != "" && span.ConsumerGroup != query.ConsumerGroup {
			continue
		}
		if query.Status != "" {
			// Will filter after building trace
		}

		traceMap[span.TraceID] = append(traceMap[span.TraceID], span)
	}

	// Build and filter traces
	traces := make([]*Trace, 0)
	for traceID, tspans := range traceMap {
		if len(tspans) == 0 {
			continue
		}

		sort.Slice(tspans, func(i, j int) bool {
			return tspans[i].Timestamp < tspans[j].Timestamp
		})

		trace := &Trace{
			TraceID:   traceID,
			Topic:     tspans[0].Topic,
			Partition: tspans[0].Partition,
			Offset:    tspans[0].Offset,
			StartTime: time.Unix(0, tspans[0].Timestamp),
			EndTime:   time.Unix(0, tspans[len(tspans)-1].Timestamp),
			Spans:     tspans,
		}
		trace.Duration = trace.EndTime.Sub(trace.StartTime)
		trace.Status = t.determineTraceStatus(tspans)

		// Filter by status
		if query.Status != "" && string(trace.Status) != query.Status {
			continue
		}

		traces = append(traces, trace)
	}

	// Sort by start time
	sort.Slice(traces, func(i, j int) bool {
		return traces[i].StartTime.After(traces[j].StartTime)
	})

	// Apply limit
	if query.Limit > 0 && len(traces) > query.Limit {
		traces = traces[:query.Limit]
	}

	return traces
}

// TraceQuery defines search criteria for traces.
type TraceQuery struct {
	TraceID       TraceID
	Topic         string
	Partition     int
	ConsumerGroup string
	Status        string
	StartTime     time.Time
	EndTime       time.Time
	Limit         int
}

// NewTraceQuery creates a default query.
func NewTraceQuery() TraceQuery {
	return TraceQuery{
		Partition: -1, // -1 means any partition
		Limit:     100,
	}
}

// Stats returns tracer statistics.
func (t *Tracer) Stats() TracerStats {
	if !t.config.Enabled {
		return TracerStats{}
	}

	t.indexMu.RLock()
	traceCount := len(t.traceIndex)
	t.indexMu.RUnlock()

	exporterNames := make([]string, len(t.exporters))
	for i, exp := range t.exporters {
		exporterNames[i] = exp.Name()
	}

	return TracerStats{
		Enabled:          t.config.Enabled,
		SpansInBuffer:    t.ringBuf.Count(),
		BufferCapacity:   t.ringBuf.Capacity(),
		TracesIndexed:    traceCount,
		SamplingRate:     t.config.SamplingRate,
		ExportersEnabled: exporterNames,
	}
}

// TracerStats contains tracer statistics.
type TracerStats struct {
	Enabled          bool     `json:"enabled"`
	SpansInBuffer    int      `json:"spans_in_buffer"`
	BufferCapacity   int      `json:"buffer_capacity"`
	TracesIndexed    int      `json:"traces_indexed"`
	SamplingRate     float64  `json:"sampling_rate"`
	ExportersEnabled []string `json:"exporters_enabled"`
}

// Shutdown gracefully shuts down the tracer.
func (t *Tracer) Shutdown() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, exp := range t.exporters {
		exp.Shutdown(ctx)
	}

	return nil
}

// SetTopicEnabled enables or disables tracing for a specific topic.
func (t *Tracer) SetTopicEnabled(topic string, enabled bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.config.PerTopicEnabled[topic] = enabled
}
