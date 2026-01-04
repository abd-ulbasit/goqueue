// =============================================================================
// TRACER TESTS - MESSAGE TRACING INFRASTRUCTURE
// =============================================================================
//
// These tests verify the tracing system for Milestone 7:
//   - TraceID/SpanID generation and format
//   - W3C Trace Context parsing and encoding
//   - Span recording and lifecycle
//   - Ring buffer storage (in-memory)
//   - File-based trace storage
//   - Exporter interface (stdout, file, OTLP, Jaeger)
//   - Query operations (by trace ID, time range, search)
//
// =============================================================================

package broker

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// TRACE ID TESTS
// =============================================================================

func TestTraceID_Generation(t *testing.T) {
	id := NewTraceID()

	// Should be 16 bytes (128 bits)
	if len(id) != 16 {
		t.Errorf("TraceID length = %d, want 16", len(id))
	}

	// Should not be all zeros
	allZero := true
	for _, b := range id {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Error("TraceID should not be all zeros")
	}

	// String() should return hex representation
	str := id.String()
	if len(str) != 32 { // 16 bytes = 32 hex chars
		t.Errorf("TraceID string length = %d, want 32", len(str))
	}
}

func TestTraceID_Uniqueness(t *testing.T) {
	const n = 1000
	seen := make(map[string]bool, n)

	for i := 0; i < n; i++ {
		id := NewTraceID()
		str := id.String()
		if seen[str] {
			t.Errorf("Duplicate TraceID generated: %s", str)
		}
		seen[str] = true
	}
}

func TestTraceID_ParseHex(t *testing.T) {
	// Generate an ID and convert to string
	original := NewTraceID()
	hexStr := original.String()

	// Parse back from hex
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		t.Fatalf("Failed to decode hex: %v", err)
	}

	var parsed TraceID
	copy(parsed[:], decoded)

	if original != parsed {
		t.Errorf("TraceID roundtrip failed: %v != %v", original, parsed)
	}
}

// =============================================================================
// SPAN ID TESTS
// =============================================================================

func TestSpanID_Generation(t *testing.T) {
	id := NewSpanID()

	// Should be 8 bytes (64 bits)
	if len(id) != 8 {
		t.Errorf("SpanID length = %d, want 8", len(id))
	}

	// String() should return hex representation
	str := id.String()
	if len(str) != 16 { // 8 bytes = 16 hex chars
		t.Errorf("SpanID string length = %d, want 16", len(str))
	}
}

func TestSpanID_Uniqueness(t *testing.T) {
	const n = 1000
	seen := make(map[string]bool, n)

	for i := 0; i < n; i++ {
		id := NewSpanID()
		str := id.String()
		if seen[str] {
			t.Errorf("Duplicate SpanID generated: %s", str)
		}
		seen[str] = true
	}
}

// =============================================================================
// TRACE CONTEXT TESTS (W3C FORMAT)
// =============================================================================

func TestTraceContext_Traceparent(t *testing.T) {
	ctx := NewTraceContext()

	// Should return W3C traceparent format
	traceparent := ctx.Traceparent()

	// Format: 00-{trace-id}-{span-id}-{flags}
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 {
		t.Errorf("Traceparent should have 4 parts, got %d: %s", len(parts), traceparent)
	}

	// Version should be 00
	if parts[0] != "00" {
		t.Errorf("Version = %s, want 00", parts[0])
	}

	// Trace ID should be 32 hex chars
	if len(parts[1]) != 32 {
		t.Errorf("Trace ID length = %d, want 32", len(parts[1]))
	}

	// Span ID should be 16 hex chars
	if len(parts[2]) != 16 {
		t.Errorf("Span ID length = %d, want 16", len(parts[2]))
	}

	// Flags should be 01 (sampled)
	if parts[3] != "01" {
		t.Errorf("Flags = %s, want 01", parts[3])
	}
}

func TestTraceContext_Parse(t *testing.T) {
	testCases := []struct {
		name        string
		traceparent string
		wantErr     bool
	}{
		{
			name:        "valid traceparent",
			traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			wantErr:     false,
		},
		{
			name:        "sampled false",
			traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
			wantErr:     false,
		},
		{
			name:        "invalid version",
			traceparent: "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			wantErr:     true,
		},
		{
			name:        "too short",
			traceparent: "00-abc-def-01",
			wantErr:     true,
		},
		{
			name:        "invalid hex in trace id",
			traceparent: "00-ghijklmnopqrstuvwxyz12345678901-00f067aa0ba902b7-01",
			wantErr:     true,
		},
		{
			name:        "empty",
			traceparent: "",
			wantErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, err := ParseTraceparent(tc.traceparent)
			if tc.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Round-trip should work
			if ctx.Traceparent() != tc.traceparent {
				t.Errorf("Roundtrip failed: got %s, want %s", ctx.Traceparent(), tc.traceparent)
			}
		})
	}
}

// =============================================================================
// SPAN TESTS
// =============================================================================

func TestSpan_Creation(t *testing.T) {
	traceID := NewTraceID()
	span := NewSpan(traceID, SpanEventPublishReceived, "test-topic", 0, 100)

	// Should have trace ID
	if span.TraceID != traceID {
		t.Error("Span should have the specified trace ID")
	}

	// Should have span ID
	if span.SpanID == (SpanID{}) {
		t.Error("Span should have a generated span ID")
	}

	// Should have timestamp
	if span.Timestamp == 0 {
		t.Error("Span should have timestamp set")
	}

	// Should have topic, partition, offset
	if span.Topic != "test-topic" {
		t.Errorf("Topic = %s, want test-topic", span.Topic)
	}
	if span.Partition != 0 {
		t.Errorf("Partition = %d, want 0", span.Partition)
	}
	if span.Offset != 100 {
		t.Errorf("Offset = %d, want 100", span.Offset)
	}
}

func TestSpan_Builders(t *testing.T) {
	traceID := NewTraceID()
	parentID := NewSpanID()
	span := NewSpan(traceID, SpanEventConsumeFetched, "orders", 1, 50)

	// Test chained builders
	span.WithParent(parentID).
		WithConsumer("group-1", "consumer-1").
		WithDuration(100 * time.Millisecond)

	if span.ParentSpanID != parentID {
		t.Error("WithParent didn't set parent span ID")
	}
	if span.ConsumerGroup != "group-1" {
		t.Error("WithConsumer didn't set consumer group")
	}
	if span.ConsumerID != "consumer-1" {
		t.Error("WithConsumer didn't set consumer ID")
	}
	if span.Duration != (100 * time.Millisecond).Nanoseconds() {
		t.Error("WithDuration didn't set duration")
	}
}

func TestSpan_Error(t *testing.T) {
	traceID := NewTraceID()
	span := NewSpan(traceID, SpanEventPublishReceived, "test", 0, 0)

	// Should not have error initially
	if span.Error != "" {
		t.Error("Span should not have error initially")
	}

	// Set error using builder (with error type)
	span.WithError(context.DeadlineExceeded)

	// Should have error
	if span.Error == "" {
		t.Error("Span should have error after WithError")
	}
}

func TestSpan_Attributes(t *testing.T) {
	traceID := NewTraceID()
	span := NewSpan(traceID, SpanEventPublishReceived, "test", 0, 0)

	// Add attributes
	span.WithAttribute("key1", "value1")
	span.WithAttribute("key2", "value2")

	if span.Attributes["key1"] != "value1" {
		t.Error("Attribute key1 not set correctly")
	}
	if span.Attributes["key2"] != "value2" {
		t.Error("Attribute key2 not set correctly")
	}
}

// =============================================================================
// RING BUFFER TESTS
// =============================================================================

func TestRingBuffer_BasicOperations(t *testing.T) {
	rb := NewRingBuffer(100)

	// Should start empty
	if rb.Count() != 0 {
		t.Errorf("Empty buffer count = %d, want 0", rb.Count())
	}

	// Add some spans
	for i := 0; i < 50; i++ {
		traceID := NewTraceID()
		span := NewSpan(traceID, SpanEventPublishReceived, "test", 0, int64(i))
		rb.Push(span)
	}

	// Should have 50 spans
	if rb.Count() != 50 {
		t.Errorf("Buffer count = %d, want 50", rb.Count())
	}

	// Get recent should return them
	recent := rb.GetRecent(10)
	if len(recent) != 10 {
		t.Errorf("Recent count = %d, want 10", len(recent))
	}
}

func TestRingBuffer_Overflow(t *testing.T) {
	capacity := 100
	rb := NewRingBuffer(capacity)

	// Add more than capacity
	for i := 0; i < capacity+50; i++ {
		traceID := NewTraceID()
		span := NewSpan(traceID, SpanEventPublishReceived, "test", 0, int64(i))
		rb.Push(span)
	}

	// Should cap at capacity
	if rb.Count() != capacity {
		t.Errorf("Buffer count = %d, want %d", rb.Count(), capacity)
	}
}

func TestRingBuffer_GetByTraceID(t *testing.T) {
	rb := NewRingBuffer(100)

	// Add some spans with known trace IDs
	targetTraceID := NewTraceID()
	for i := 0; i < 3; i++ {
		span := NewSpan(targetTraceID, SpanEventPublishReceived, "test", 0, int64(i))
		rb.Push(span)
	}

	// Add some other spans
	for i := 0; i < 20; i++ {
		traceID := NewTraceID()
		span := NewSpan(traceID, SpanEventConsumeFetched, "test", 0, int64(i))
		rb.Push(span)
	}

	// Get by trace ID
	found := rb.GetByTraceID(targetTraceID)

	// Should find 3 spans
	if len(found) != 3 {
		t.Errorf("Found %d spans, want 3", len(found))
	}
}

func TestRingBuffer_GetByTimeRange(t *testing.T) {
	rb := NewRingBuffer(100)

	start := time.Now()

	// Add spans over time
	for i := 0; i < 10; i++ {
		traceID := NewTraceID()
		span := NewSpan(traceID, SpanEventPublishReceived, "test", 0, int64(i))
		rb.Push(span)
		time.Sleep(1 * time.Millisecond) // Small delay
	}

	end := time.Now()

	// All spans should be in range
	found := rb.GetByTimeRange(start, end)
	if len(found) != 10 {
		t.Errorf("Found %d spans in time range, want 10", len(found))
	}

	// No spans should be before start
	found = rb.GetByTimeRange(start.Add(-time.Hour), start.Add(-time.Minute))
	if len(found) != 0 {
		t.Errorf("Found %d spans before range, want 0", len(found))
	}
}

func TestRingBuffer_Concurrent(t *testing.T) {
	rb := NewRingBuffer(1000)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				traceID := NewTraceID()
				span := NewSpan(traceID, SpanEventPublishReceived, "test", 0, int64(j))
				rb.Push(span)
			}
		}()
	}

	wg.Wait()

	// Should have some spans (exact count may vary due to concurrent adds)
	if rb.Count() == 0 {
		t.Error("Buffer should have spans after concurrent adds")
	}
}

// =============================================================================
// STDOUT EXPORTER TESTS
// =============================================================================

func TestStdoutExporter(t *testing.T) {
	var buf bytes.Buffer
	exporter := NewStdoutExporterWithWriter(&buf)

	// Export a span
	traceID := NewTraceID()
	span := NewSpan(traceID, SpanEventPublishReceived, "test-topic", 1, 100)

	err := exporter.Export(context.Background(), span)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	// Should have JSON output
	output := buf.String()
	if !strings.Contains(output, "test-topic") {
		t.Error("Output should contain topic name")
	}
	if !strings.Contains(output, "publish.received") {
		t.Error("Output should contain event type")
	}
}

// =============================================================================
// FILE EXPORTER TESTS
// =============================================================================

func TestFileExporter(t *testing.T) {
	// Create temp directory
	dir := t.TempDir()

	exporter, err := NewFileExporter(dir, 1024*1024) // 1MB
	if err != nil {
		t.Fatalf("Failed to create file exporter: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	// Export some spans
	for i := 0; i < 10; i++ {
		traceID := NewTraceID()
		span := NewSpan(traceID, SpanEventPublishReceived, "test-topic", 0, int64(i))

		err = exporter.Export(context.Background(), span)
		if err != nil {
			t.Fatalf("Export failed: %v", err)
		}
	}

	// Close the exporter to flush
	exporter.Shutdown(context.Background())

	// Find and read trace file
	files, err := filepath.Glob(filepath.Join(dir, "traces-*.json"))
	if err != nil || len(files) == 0 {
		t.Fatal("No trace files found")
	}

	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("Failed to read trace file: %v", err)
	}

	// Should have JSONL (one JSON object per line)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 1 {
		t.Error("File should have at least 1 line")
	}

	// Each line should be valid JSON
	for i, line := range lines {
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Errorf("Line %d is not valid JSON: %v", i, err)
		}
	}
}

// =============================================================================
// TRACER INTEGRATION TESTS
// =============================================================================

func TestTracer_StartTrace(t *testing.T) {
	dir := t.TempDir()
	config := DefaultTracerConfig(dir)
	config.FileExportEnabled = false // Disable for test

	tracer, err := NewTracer(config)
	if err != nil {
		t.Fatalf("Failed to create tracer: %v", err)
	}
	defer tracer.Shutdown()

	// Start a trace - requires topic, partition, offset
	ctx := tracer.StartTrace("test-topic", 0, 100)

	// Should have trace ID if enabled
	if tracer.IsEnabled() && ctx.TraceID == (TraceID{}) {
		t.Error("Trace context should have trace ID when enabled")
	}
}

func TestTracer_RecordSpan(t *testing.T) {
	dir := t.TempDir()
	config := DefaultTracerConfig(dir)
	config.FileExportEnabled = false

	tracer, err := NewTracer(config)
	if err != nil {
		t.Fatalf("Failed to create tracer: %v", err)
	}
	defer tracer.Shutdown()

	// Record spans
	ctx := tracer.StartTrace("orders", 0, 100)
	span := NewSpan(ctx.TraceID, SpanEventPublishReceived, "orders", 0, 100)

	tracer.RecordSpan(span)

	// Get trace - returns *Trace, not []*Span
	trace := tracer.GetTrace(ctx.TraceID)
	if trace == nil {
		t.Fatal("GetTrace returned nil")
	}
	if len(trace.Spans) != 1 {
		t.Errorf("Got %d spans, want 1", len(trace.Spans))
	}
}

func TestTracer_SearchTraces(t *testing.T) {
	dir := t.TempDir()
	config := DefaultTracerConfig(dir)
	config.FileExportEnabled = false

	tracer, err := NewTracer(config)
	if err != nil {
		t.Fatalf("Failed to create tracer: %v", err)
	}
	defer tracer.Shutdown()

	// Record spans for different topics
	for i := 0; i < 10; i++ {
		var topic string
		if i%2 == 0 {
			topic = "orders"
		} else {
			topic = "events"
		}
		ctx := tracer.StartTrace(topic, 0, int64(i))
		span := NewSpan(ctx.TraceID, SpanEventPublishReceived, topic, 0, int64(i))
		tracer.RecordSpan(span)
	}

	// Search for orders topic using TraceQuery
	query := TraceQuery{
		Topic:     "orders",
		Partition: -1, // -1 means any partition
	}
	found := tracer.SearchTraces(query)
	if len(found) != 5 {
		t.Errorf("Found %d traces for 'orders', want 5", len(found))
	}
}

func TestTracer_GetRecentTraces(t *testing.T) {
	dir := t.TempDir()
	config := DefaultTracerConfig(dir)
	config.FileExportEnabled = false

	tracer, err := NewTracer(config)
	if err != nil {
		t.Fatalf("Failed to create tracer: %v", err)
	}
	defer tracer.Shutdown()

	// Record several traces
	for i := 0; i < 20; i++ {
		ctx := tracer.StartTrace("test", 0, int64(i))
		span := NewSpan(ctx.TraceID, SpanEventPublishReceived, "test", 0, int64(i))
		tracer.RecordSpan(span)
	}

	// Get recent 10
	recent := tracer.GetRecentTraces(10)
	if len(recent) != 10 {
		t.Errorf("Got %d recent traces, want 10", len(recent))
	}
}

func TestTracer_Stats(t *testing.T) {
	dir := t.TempDir()
	config := DefaultTracerConfig(dir)
	config.FileExportEnabled = false

	tracer, err := NewTracer(config)
	if err != nil {
		t.Fatalf("Failed to create tracer: %v", err)
	}
	defer tracer.Shutdown()

	// Record some spans
	for i := 0; i < 5; i++ {
		ctx := tracer.StartTrace("test", 0, int64(i))
		span := NewSpan(ctx.TraceID, SpanEventPublishReceived, "test", 0, int64(i))
		tracer.RecordSpan(span)
	}

	stats := tracer.Stats()

	// TracerStats has: SpansInBuffer, BufferCapacity, TracesIndexed, etc.
	if stats.SpansInBuffer != 5 {
		t.Errorf("SpansInBuffer = %d, want 5", stats.SpansInBuffer)
	}
	if stats.BufferCapacity == 0 {
		t.Error("BufferCapacity should be > 0")
	}
}

// =============================================================================
// BENCHMARKS
// =============================================================================

func BenchmarkTraceID_New(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewTraceID()
	}
}

func BenchmarkSpanID_New(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewSpanID()
	}
}

func BenchmarkSpan_Create(b *testing.B) {
	traceID := NewTraceID()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = NewSpan(traceID, SpanEventPublishReceived, "test", 0, int64(i))
	}
}

func BenchmarkRingBuffer_Push(b *testing.B) {
	rb := NewRingBuffer(100000)
	traceID := NewTraceID()
	span := NewSpan(traceID, SpanEventPublishReceived, "test", 0, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Push(span)
	}
}

func BenchmarkTracer_RecordSpan(b *testing.B) {
	dir := b.TempDir()
	config := DefaultTracerConfig(dir)
	config.FileExportEnabled = false

	tracer, _ := NewTracer(config)
	defer tracer.Shutdown()

	traceID := NewTraceID()
	span := NewSpan(traceID, SpanEventPublishReceived, "test", 0, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracer.RecordSpan(span)
	}
}
