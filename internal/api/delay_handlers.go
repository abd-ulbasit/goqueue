package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

// =============================================================================
// MILESTONE 5: DELAYED MESSAGES API
// =============================================================================
//
// These endpoints provide delayed/scheduled message delivery management.
//
// ENDPOINTS:
//   GET    /topics/{name}/delayed               List pending delayed messages
//   GET    /topics/{name}/delayed/{offset}      Get specific delayed message
//   DELETE /topics/{name}/delayed/{p}/{offset}  Cancel delayed message
//   GET    /delay/stats                         Scheduler statistics
//
// =============================================================================

// DelayedMessageResponse represents a delayed message in API responses.
type DelayedMessageResponse struct {
	Topic         string `json:"topic"`
	Partition     int    `json:"partition"`
	Offset        int64  `json:"offset"`
	DeliverAt     string `json:"deliver_at"`
	TimeRemaining string `json:"time_remaining"`
	State         string `json:"state"`
}

// listDelayedMessages handles GET /topics/{topicName}/delayed
//
// Returns all pending delayed messages for a topic.
// Supports pagination via ?limit=N&skip=N query parameters.
//
// EXAMPLE:
//
//	curl http://localhost:8080/topics/orders/delayed?limit=100&skip=0
func (s *Server) listDelayedMessages(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")

	// Parse pagination params
	limit := 100
	skip := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if sk := r.URL.Query().Get("skip"); sk != "" {
		if parsed, err := strconv.Atoi(sk); err == nil && parsed >= 0 {
			skip = parsed
		}
	}

	messages, err := s.broker.GetDelayedMessages(topicName, limit, skip)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Convert to API response format
	response := make([]DelayedMessageResponse, len(messages))
	for i, msg := range messages {
		response[i] = DelayedMessageResponse{
			Topic:         msg.Topic,
			Partition:     msg.Partition,
			Offset:        msg.Offset,
			DeliverAt:     msg.DeliverAt.Format(time.RFC3339),
			TimeRemaining: msg.TimeRemaining.String(),
			State:         msg.State,
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"topic":    topicName,
		"messages": response,
		"count":    len(response),
		"limit":    limit,
		"skip":     skip,
	})
}

// getDelayedMessage handles GET /topics/{topicName}/delayed/{offset}
//
// Returns details about a specific delayed message.
// Note: This looks up by offset only - caller must know the partition.
// For full lookup, use GET /topics/{name}/delayed to list all.
//
// EXAMPLE:
//
//	curl http://localhost:8080/topics/orders/delayed/1234
func (s *Server) getDelayedMessage(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")
	offsetStr := chi.URLParam(r, "offset")

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid offset")
		return
	}

	// Try to find the message in any partition
	// This is a simple implementation - a production system might have
	// a more efficient lookup if partition is known
	messages, err := s.broker.GetDelayedMessages(topicName, 0, 0) // Get all
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	for _, msg := range messages {
		if msg.Offset == offset {
			s.writeJSON(w, http.StatusOK, DelayedMessageResponse{
				Topic:         msg.Topic,
				Partition:     msg.Partition,
				Offset:        msg.Offset,
				DeliverAt:     msg.DeliverAt.Format(time.RFC3339),
				TimeRemaining: msg.TimeRemaining.String(),
				State:         msg.State,
			})
			return
		}
	}

	s.errorResponse(w, http.StatusNotFound, "delayed message not found")
}

// cancelDelayedMessage handles DELETE /topics/{topicName}/delayed/{partition}/{offset}
//
// Cancels a pending delayed message. The message will never be delivered.
// Returns 200 if canceled, 404 if not found or already delivered.
//
// EXAMPLE:
//
//	curl -X DELETE http://localhost:8080/topics/orders/delayed/0/1234
func (s *Server) cancelDelayedMessage(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")
	partitionStr := chi.URLParam(r, "partition")
	offsetStr := chi.URLParam(r, "offset")

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid partition")
		return
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid offset")
		return
	}

	canceled, err := s.broker.CancelDelayed(topicName, partition, offset)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	if !canceled {
		s.errorResponse(w, http.StatusNotFound, "delayed message not found or already delivered")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"canceled":  true,
		"topic":     topicName,
		"partition": partition,
		"offset":    offset,
	})
}

// handleDelayStats handles GET /delay/stats
//
// Returns statistics about the delay scheduling system:
//   - Total scheduled, delivered, canceled messages
//   - Pending messages by topic
//   - Timer wheel statistics
//
// EXAMPLE:
//
//	curl http://localhost:8080/delay/stats
func (s *Server) handleDelayStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.DelayStats()

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"total_scheduled": stats.TotalScheduled,
		"total_delivered": stats.TotalDelivered,
		"total_canceled":  stats.TotalCanceled,
		"total_pending":   stats.TotalPending,
		"by_topic":        stats.ByTopic,
		"timer_wheel": map[string]interface{}{
			"total_scheduled": stats.TimerWheel.TotalScheduled,
			"total_expired":   stats.TimerWheel.TotalExpired,
			"total_canceled":  stats.TimerWheel.TotalCanceled,
			"current_active":  stats.TimerWheel.CurrentActive,
			"current_tick":    stats.TimerWheel.CurrentTick,
		},
	})
}

// =============================================================================
// PRIORITY STATS (M6)
// =============================================================================
//
// ENDPOINT: GET /priority/stats
//
// Returns per-priority-per-partition statistics across all topics.
//
// WHY PER-PRIORITY-PER-PARTITION?
// This is the most granular view, allowing:
//   - Hot partition detection at the priority level
//   - Priority imbalance detection
//   - Starvation monitoring (oldest pending message age)
//   - Capacity planning by priority
//
// RESPONSE STRUCTURE:
//
//	{
//	  "total_by_priority": [100, 50, 25, 10, 5],  // [critical, high, normal, low, background]
//	  "topics": {
//	    "orders": {
//	      "total_by_priority": [100, 50, 25, 10, 5],
//	      "partitions": {
//	        "0": {
//	          "pending": [10, 5, 3, 1, 0],
//	          "consumed": [90, 45, 22, 9, 5],
//	          "total": [100, 50, 25, 10, 5],
//	          "oldest_pending": ["2024-01-15T10:00:00Z", ...]
//	        }
//	      }
//	    }
//	  }
//	}
//
// EXAMPLE:
//
//	curl http://localhost:8080/priority/stats
func (s *Server) handlePriorityStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.PriorityStats()

	// Convert to JSON-friendly format
	response := map[string]interface{}{
		"total_by_priority": convertPriorityArray(stats.TotalByPriority),
		"topics":            make(map[string]interface{}),
	}

	for topicName, topicStats := range stats.Topics {
		partitions := make(map[string]interface{})
		for partID, partStats := range topicStats.Partitions {
			partitions[strconv.Itoa(partID)] = map[string]interface{}{
				"pending":        convertPriorityArray(partStats.Pending),
				"consumed":       convertPriorityArray(partStats.Consumed),
				"total":          convertPriorityArray(partStats.Total),
				"oldest_pending": convertPriorityTimeArray(partStats.OldestPending),
			}
		}
		topicsMap, _ := response["topics"].(map[string]interface{})
		topicsMap[topicName] = map[string]interface{}{
			"total_by_priority": convertPriorityArray(topicStats.TotalByPriority),
			"partitions":        partitions,
		}
	}

	s.writeJSON(w, http.StatusOK, response)
}

// convertPriorityArray converts a [5]int64 to a []int64 for JSON serialization.
// Array index maps to: [critical, high, normal, low, background]
func convertPriorityArray(arr [5]int64) []int64 {
	return arr[:]
}

// convertPriorityTimeArray converts a [5]time.Time to RFC3339 strings.
// Zero times become empty strings to indicate no pending messages.
func convertPriorityTimeArray(arr [5]time.Time) []string {
	result := make([]string, 5)
	for i, t := range arr {
		if !t.IsZero() {
			result[i] = t.Format(time.RFC3339)
		}
	}
	return result
}
