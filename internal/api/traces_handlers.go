package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"goqueue/internal/broker"
)

// =============================================================================
// TRACING HANDLERS (M7)
// =============================================================================
//
// These handlers provide HTTP endpoints for querying message traces.
// Traces capture the full lifecycle of messages: publish → consume → ack/nack/reject
//
// ENDPOINTS:
//   GET /traces          - List recent traces
//   GET /traces/{id}     - Get specific trace
//   GET /traces/search   - Search with filters
//   GET /traces/stats    - Tracer statistics
//
// =============================================================================

// listTraces returns recent traces.
//
// QUERY PARAMETERS:
//   - limit: Maximum traces to return (default: 100, max: 1000)
//
// RESPONSE FORMAT:
//
//	{
//	  "traces": [
//	    {
//	      "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
//	      "topic": "orders",
//	      "partition": 0,
//	      "offset": 100,
//	      "start_time": "2024-01-15T10:00:00Z",
//	      "end_time": "2024-01-15T10:00:01Z",
//	      "duration_ms": 1000,
//	      "status": "completed",
//	      "span_count": 3
//	    }
//	  ],
//	  "total": 100
//	}
func (s *Server) listTraces(w http.ResponseWriter, r *http.Request) {
	// Parse limit parameter
	limit := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	traces := s.broker.Tracer().GetRecentTraces(limit)

	response := map[string]interface{}{
		"traces": convertTracesToJSON(traces),
		"total":  len(traces),
	}

	s.writeJSON(w, http.StatusOK, response)
}

// getTrace returns a specific trace by ID.
//
// URL PARAMETERS:
//   - traceID: 32-character hex trace ID
//
// RESPONSE FORMAT:
//
//	{
//	  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
//	  "topic": "orders",
//	  "partition": 0,
//	  "offset": 100,
//	  "start_time": "2024-01-15T10:00:00Z",
//	  "end_time": "2024-01-15T10:00:01Z",
//	  "duration_ms": 1000,
//	  "status": "completed",
//	  "spans": [
//	    {
//	      "span_id": "00f067aa0ba902b7",
//	      "event_type": "publish.received",
//	      "timestamp": "2024-01-15T10:00:00Z",
//	      ...
//	    }
//	  ]
//	}
func (s *Server) getTrace(w http.ResponseWriter, r *http.Request) {
	traceIDStr := chi.URLParam(r, "traceID")

	// Parse trace ID from hex string
	traceID, err := broker.ParseTraceID(traceIDStr)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid trace ID format")
		return
	}

	trace := s.broker.Tracer().GetTrace(traceID)
	if trace == nil {
		s.errorResponse(w, http.StatusNotFound, "trace not found")
		return
	}

	s.writeJSON(w, http.StatusOK, convertTraceToJSON(trace))
}

// searchTraces searches traces with filters.
//
// QUERY PARAMETERS:
//   - topic: Filter by topic name
//   - partition: Filter by partition number (-1 for all)
//   - consumer_group: Filter by consumer group
//   - start: Start time (RFC3339)
//   - end: End time (RFC3339)
//   - status: Filter by status (completed, error, pending)
//   - limit: Max traces to return (default: 100)
//
// EXAMPLE:
//
//	curl "http://localhost:8080/traces/search?topic=orders&start=2024-01-15T00:00:00Z&limit=50"
func (s *Server) searchTraces(w http.ResponseWriter, r *http.Request) {
	query := broker.TraceQuery{
		Partition: -1, // Default: all partitions
	}

	// Parse query parameters
	if topic := r.URL.Query().Get("topic"); topic != "" {
		query.Topic = topic
	}
	if partitionStr := r.URL.Query().Get("partition"); partitionStr != "" {
		if p, err := strconv.Atoi(partitionStr); err == nil {
			query.Partition = p
		}
	}
	if consumerGroup := r.URL.Query().Get("consumer_group"); consumerGroup != "" {
		query.ConsumerGroup = consumerGroup
	}
	if startStr := r.URL.Query().Get("start"); startStr != "" {
		if t, err := time.Parse(time.RFC3339, startStr); err == nil {
			query.StartTime = t
		}
	}
	if endStr := r.URL.Query().Get("end"); endStr != "" {
		if t, err := time.Parse(time.RFC3339, endStr); err == nil {
			query.EndTime = t
		}
	}
	if status := r.URL.Query().Get("status"); status != "" {
		query.Status = status
	}
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			query.Limit = l
		}
	}
	if query.Limit == 0 {
		query.Limit = 100
	}

	traces := s.broker.Tracer().SearchTraces(query)

	response := map[string]interface{}{
		"traces": convertTracesToJSON(traces),
		"total":  len(traces),
		"query": map[string]interface{}{
			"topic":          query.Topic,
			"partition":      query.Partition,
			"consumer_group": query.ConsumerGroup,
			"start":          formatOptionalTime(query.StartTime),
			"end":            formatOptionalTime(query.EndTime),
			"status":         string(query.Status),
			"limit":          query.Limit,
		},
	}

	s.writeJSON(w, http.StatusOK, response)
}

// handleTracerStats returns tracer statistics.
//
// RESPONSE FORMAT:
//
//	{
//	  "enabled": true,
//	  "spans_in_buffer": 1000,
//	  "buffer_capacity": 10000,
//	  "traces_indexed": 500,
//	  "sampling_rate": 1.0,
//	  "exporters_enabled": ["file", "stdout"]
//	}
func (s *Server) handleTracerStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.Tracer().Stats()

	response := map[string]interface{}{
		"enabled":           stats.Enabled,
		"spans_in_buffer":   stats.SpansInBuffer,
		"buffer_capacity":   stats.BufferCapacity,
		"traces_indexed":    stats.TracesIndexed,
		"sampling_rate":     stats.SamplingRate,
		"exporters_enabled": stats.ExportersEnabled,
	}

	s.writeJSON(w, http.StatusOK, response)
}

// =============================================================================
// TRACE CONVERSION HELPERS
// =============================================================================

// convertTracesToJSON converts a slice of traces to JSON-friendly format.
func convertTracesToJSON(traces []*broker.Trace) []map[string]interface{} {
	result := make([]map[string]interface{}, len(traces))
	for i, trace := range traces {
		result[i] = map[string]interface{}{
			"trace_id":    trace.TraceID.String(),
			"topic":       trace.Topic,
			"partition":   trace.Partition,
			"offset":      trace.Offset,
			"start_time":  trace.StartTime.Format(time.RFC3339Nano),
			"end_time":    trace.EndTime.Format(time.RFC3339Nano),
			"duration_ms": trace.Duration.Milliseconds(),
			"status":      string(trace.Status),
			"span_count":  len(trace.Spans),
		}
	}
	return result
}

// convertTraceToJSON converts a trace with all spans to JSON-friendly format.
func convertTraceToJSON(trace *broker.Trace) map[string]interface{} {
	spans := make([]map[string]interface{}, len(trace.Spans))
	for i, span := range trace.Spans {
		spanJSON := map[string]interface{}{
			"span_id":    span.SpanID.String(),
			"event_type": string(span.EventType),
			"timestamp":  time.Unix(0, span.Timestamp).Format(time.RFC3339Nano),
			"topic":      span.Topic,
			"partition":  span.Partition,
			"offset":     span.Offset,
		}

		// Add optional fields
		if !span.ParentSpanID.IsZero() {
			spanJSON["parent_span_id"] = span.ParentSpanID.String()
		}
		if span.ConsumerGroup != "" {
			spanJSON["consumer_group"] = span.ConsumerGroup
		}
		if span.ConsumerID != "" {
			spanJSON["consumer_id"] = span.ConsumerID
		}
		if span.Duration > 0 {
			spanJSON["duration_ns"] = span.Duration
		}
		if span.Error != "" {
			spanJSON["error"] = span.Error
		}
		if len(span.Attributes) > 0 {
			spanJSON["attributes"] = span.Attributes
		}

		spans[i] = spanJSON
	}

	return map[string]interface{}{
		"trace_id":    trace.TraceID.String(),
		"topic":       trace.Topic,
		"partition":   trace.Partition,
		"offset":      trace.Offset,
		"start_time":  trace.StartTime.Format(time.RFC3339Nano),
		"end_time":    trace.EndTime.Format(time.RFC3339Nano),
		"duration_ms": trace.Duration.Milliseconds(),
		"status":      string(trace.Status),
		"spans":       spans,
	}
}

// formatOptionalTime formats a time as RFC3339 or empty string if zero.
func formatOptionalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}
