package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"goqueue/internal/broker"
)

// =============================================================================
// MILESTONE 4: MESSAGE ACKNOWLEDGMENT HANDLERS
// =============================================================================
//
// ENDPOINT OVERVIEW:
//
//   POST /messages/ack       - Acknowledge successful message processing
//   POST /messages/nack      - Signal retry (transient failure)
//   POST /messages/reject    - Send to DLQ (permanent failure)
//   POST /messages/visibility - Extend visibility timeout
//   GET  /reliability/stats  - Get reliability layer statistics
//
// RECEIPT HANDLE:
//   Each message returned from /groups/{group}/poll includes a receipt_handle.
//   This handle is required for all ACK/NACK/REJECT operations.
//
//   Format: {topic}:{partition}:{offset}:{deliveryCount}:{nonce}
//   Example: "orders:0:42:1:a1b2c3d4"
//
// ERROR CODES:
//   400 - Missing or invalid receipt handle
//   404 - Message not found (already acked, expired, or invalid handle)
//   410 - Message gone (visibility timeout expired)
//   429 - Too many in-flight messages (backpressure)
//
// =============================================================================

// AckRequest is the request body for POST /messages/ack
type AckRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
}

// ackMessage handles POST /messages/ack
//
// SEMANTICS:
//   - Message is fully processed and should not be redelivered
//   - May advance committed offset if this completes a contiguous range
//
// EXAMPLE:
//
//	curl -X POST http://localhost:8080/messages/ack \
//	  -H "Content-Type: application/json" \
//	  -d '{"receipt_handle": "orders:0:42:1:a1b2c3d4"}'
func (s *Server) ackMessage(w http.ResponseWriter, r *http.Request) {
	var req AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ReceiptHandle == "" {
		s.errorResponse(w, http.StatusBadRequest, "receipt_handle is required")
		return
	}

	result, err := s.broker.Ack(req.ReceiptHandle)
	if err != nil {
		// Map error to appropriate HTTP status
		switch {
		case errors.Is(err, broker.ErrMessageNotFound):
			s.errorResponse(w, http.StatusNotFound, "message not found or already processed")
		case errors.Is(err, broker.ErrInvalidReceiptHandle):
			s.errorResponse(w, http.StatusBadRequest, "invalid receipt handle format")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":          true,
		"topic":            result.Topic,
		"partition":        result.Partition,
		"offset":           result.Offset,
		"committed_offset": result.NewCommittedOffset,
		"offset_advanced":  result.OffsetAdvanced,
		"in_flight_count":  result.RemainingInFlight,
	})
}

// NackRequest is the request body for POST /messages/nack
type NackRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
	Reason        string `json:"reason,omitempty"` // Optional: why the processing failed
}

// nackMessage handles POST /messages/nack
//
// SEMANTICS:
//   - Processing failed but should be retried (transient failure)
//   - Message will be redelivered after exponential backoff
//   - After MaxRetries (default 3), message goes to DLQ
//
// EXAMPLE:
//
//	curl -X POST http://localhost:8080/messages/nack \
//	  -H "Content-Type: application/json" \
//	  -d '{"receipt_handle": "orders:0:42:1:a1b2c3d4", "reason": "database timeout"}'
func (s *Server) nackMessage(w http.ResponseWriter, r *http.Request) {
	var req NackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ReceiptHandle == "" {
		s.errorResponse(w, http.StatusBadRequest, "receipt_handle is required")
		return
	}

	result, err := s.broker.Nack(req.ReceiptHandle, req.Reason)
	if err != nil {
		switch {
		case errors.Is(err, broker.ErrMessageNotFound):
			s.errorResponse(w, http.StatusNotFound, "message not found or already processed")
		case errors.Is(err, broker.ErrInvalidReceiptHandle):
			s.errorResponse(w, http.StatusBadRequest, "invalid receipt handle format")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	response := map[string]interface{}{
		"success":         true,
		"action":          result.Action, // "requeued" or "dlq"
		"topic":           result.Topic,
		"partition":       result.Partition,
		"offset":          result.Offset,
		"delivery_count":  result.DeliveryCount,
		"next_visible_at": result.NextVisibleAt.Format(time.RFC3339),
	}

	if result.Action == "dlq" {
		response["dlq_topic"] = result.DLQTopic
	}

	s.writeJSON(w, http.StatusOK, response)
}

// RejectRequest is the request body for POST /messages/reject
type RejectRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
	Reason        string `json:"reason"` // Required: why this message cannot be processed
}

// rejectMessage handles POST /messages/reject
//
// SEMANTICS:
//   - Message is "poison" and cannot be processed (permanent failure)
//   - Goes directly to DLQ without retry
//
// USE CASES:
//   - Invalid message format
//   - Business logic determines message is unprocessable
//   - Schema validation failure
//
// EXAMPLE:
//
//	curl -X POST http://localhost:8080/messages/reject \
//	  -H "Content-Type: application/json" \
//	  -d '{"receipt_handle": "orders:0:42:1:a1b2c3d4", "reason": "invalid order format"}'
func (s *Server) rejectMessage(w http.ResponseWriter, r *http.Request) {
	var req RejectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ReceiptHandle == "" {
		s.errorResponse(w, http.StatusBadRequest, "receipt_handle is required")
		return
	}

	if req.Reason == "" {
		s.errorResponse(w, http.StatusBadRequest, "reason is required for reject")
		return
	}

	result, err := s.broker.Reject(req.ReceiptHandle, req.Reason)
	if err != nil {
		switch {
		case errors.Is(err, broker.ErrMessageNotFound):
			s.errorResponse(w, http.StatusNotFound, "message not found or already processed")
		case errors.Is(err, broker.ErrInvalidReceiptHandle):
			s.errorResponse(w, http.StatusBadRequest, "invalid receipt handle format")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"action":    "rejected",
		"topic":     result.Topic,
		"partition": result.Partition,
		"offset":    result.Offset,
		"dlq_topic": result.DLQTopic,
		"reason":    req.Reason,
	})
}

// ExtendVisibilityRequest is the request body for POST /messages/visibility
type ExtendVisibilityRequest struct {
	ReceiptHandle    string `json:"receipt_handle"`
	ExtensionSeconds int    `json:"extension_seconds"` // Additional time in seconds
}

// extendVisibility handles POST /messages/visibility
//
// SEMANTICS:
//   - Extends the visibility timeout for a message currently being processed
//   - Use when processing will take longer than the default timeout
//   - Can be called multiple times
//
// USE CASE:
//   - Default timeout is 30s
//   - Processing a large file will take 60s
//   - After 20s, extend visibility by 60s more
//   - Prevents timeout-based redelivery while still processing
//
// EXAMPLE:
//
//	curl -X POST http://localhost:8080/messages/visibility \
//	  -H "Content-Type: application/json" \
//	  -d '{"receipt_handle": "orders:0:42:1:a1b2c3d4", "extension_seconds": 60}'
func (s *Server) extendVisibility(w http.ResponseWriter, r *http.Request) {
	var req ExtendVisibilityRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ReceiptHandle == "" {
		s.errorResponse(w, http.StatusBadRequest, "receipt_handle is required")
		return
	}

	if req.ExtensionSeconds <= 0 {
		s.errorResponse(w, http.StatusBadRequest, "extension_seconds must be positive")
		return
	}

	extension := time.Duration(req.ExtensionSeconds) * time.Second
	newDeadline, err := s.broker.ExtendVisibility(req.ReceiptHandle, extension)
	if err != nil {
		switch {
		case errors.Is(err, broker.ErrMessageNotFound):
			s.errorResponse(w, http.StatusNotFound, "message not found or already processed")
		case errors.Is(err, broker.ErrInvalidReceiptHandle):
			s.errorResponse(w, http.StatusBadRequest, "invalid receipt handle format")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":      true,
		"new_deadline": newDeadline.Format(time.RFC3339),
		"extended_by":  req.ExtensionSeconds,
	})
}

// handleReliabilityStats handles GET /reliability/stats
//
// Returns statistics about the reliability layer:
//   - ACK manager: in-flight messages, acks, nacks, rejects
//   - Visibility tracker: tracked messages, timeouts, extensions
//   - DLQ router: routed messages per topic, total routed
func (s *Server) handleReliabilityStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.ReliabilityStats()

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"ack_manager": map[string]interface{}{
			"total_acks":     stats.AckManager.TotalAcks,
			"total_nacks":    stats.AckManager.TotalNacks,
			"total_rejects":  stats.AckManager.TotalRejects,
			"total_expired":  stats.AckManager.TotalExpired,
			"total_retries":  stats.AckManager.TotalRetries,
			"total_dlq":      stats.AckManager.TotalDLQ,
			"current_queued": stats.AckManager.CurrentQueued,
		},
		"visibility_tracker": map[string]interface{}{
			"total_tracked":     stats.Visibility.TotalTracked,
			"current_in_flight": stats.Visibility.CurrentInFlight,
			"total_expired":     stats.Visibility.TotalExpired,
			"total_acked":       stats.Visibility.TotalAcked,
			"total_extended":    stats.Visibility.TotalExtended,
		},
		"dlq": map[string]interface{}{
			"total_routed":     stats.DLQ.TotalRouted,
			"routes_by_reason": stats.DLQ.RoutesByReason,
			"routes_by_topic":  stats.DLQ.RoutesByTopic,
			"create_errors":    stats.DLQ.CreateErrors,
			"publish_errors":   stats.DLQ.PublishErrors,
		},
	})
}
