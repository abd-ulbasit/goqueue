package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"goqueue/internal/broker"
)

// =============================================================================
// CONSUMER GROUP HANDLERS (M3)
// =============================================================================

// JoinGroupRequest is the request body for joining a consumer group.
//
// EXAMPLE:
//
//	{
//	  "client_id": "order-processor-1",
//	  "topics": ["orders"]
//	}
type JoinGroupRequest struct {
	ClientID string   `json:"client_id"`
	Topics   []string `json:"topics"`
}

// JoinGroupResponse is the response after joining a consumer group.
//
// EXAMPLE:
//
//	{
//	  "member_id": "order-processor-1-abc123",
//	  "generation": 5,
//	  "leader_id": "order-processor-1-abc123",
//	  "partitions": [0, 1, 2],
//	  "members": ["order-processor-1-abc123", "order-processor-2-def456"]
//	}
type JoinGroupResponse struct {
	MemberID   string   `json:"member_id"`
	Generation int      `json:"generation"`
	LeaderID   string   `json:"leader_id"`
	Partitions []int    `json:"partitions"`
	Members    []string `json:"members"`
}

func (s *Server) joinGroup(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	var req JoinGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.ClientID == "" {
		s.errorResponse(w, http.StatusBadRequest, "client_id is required")
		return
	}
	if len(req.Topics) == 0 {
		s.errorResponse(w, http.StatusBadRequest, "at least one topic is required")
		return
	}

	// Verify topics exist
	for _, topic := range req.Topics {
		if !s.broker.TopicExists(topic) {
			s.errorResponse(w, http.StatusNotFound, "topic not found: "+topic)
			return
		}
	}

	coordinator := s.broker.GroupCoordinator()
	result, err := coordinator.JoinGroup(groupID, req.ClientID, req.Topics)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "failed to join group: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, JoinGroupResponse{
		MemberID:   result.MemberID,
		Generation: result.Generation,
		LeaderID:   result.LeaderID,
		Partitions: result.Partitions,
		Members:    result.Members,
	})
}

// HeartbeatRequest is the request body for sending a heartbeat.
type HeartbeatRequest struct {
	MemberID   string `json:"member_id"`
	Generation int    `json:"generation"`
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	var req HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.MemberID == "" {
		s.errorResponse(w, http.StatusBadRequest, "member_id is required")
		return
	}

	coordinator := s.broker.GroupCoordinator()
	if err := coordinator.Heartbeat(groupID, req.MemberID, req.Generation); err != nil {
		switch {
		case errors.Is(err, broker.ErrGroupNotFound):
			s.errorResponse(w, http.StatusNotFound, "group not found")
		case errors.Is(err, broker.ErrMemberNotFound):
			s.errorResponse(w, http.StatusNotFound, "member not found (may have been evicted)")
		case errors.Is(err, broker.ErrStaleGeneration):
			s.errorResponse(w, http.StatusConflict, "stale generation (rebalance occurred)")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "ok",
	})
}

// LeaveGroupRequest is the request body for leaving a consumer group.
type LeaveGroupRequest struct {
	MemberID string `json:"member_id"`
}

func (s *Server) leaveGroup(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	var req LeaveGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.MemberID == "" {
		s.errorResponse(w, http.StatusBadRequest, "member_id is required")
		return
	}

	coordinator := s.broker.GroupCoordinator()
	if err := coordinator.LeaveGroup(groupID, req.MemberID); err != nil {
		switch {
		case errors.Is(err, broker.ErrGroupNotFound):
			s.errorResponse(w, http.StatusNotFound, "group not found")
		case errors.Is(err, broker.ErrMemberNotFound):
			s.errorResponse(w, http.StatusNotFound, "member not found")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "left",
	})
}

// =============================================================================
// LONG-POLL MESSAGE CONSUMPTION (M3)
// =============================================================================

// PollResponse is the response containing messages for assigned partitions.
type PollResponse struct {
	Messages   map[int][]ConsumeMessage `json:"messages"`    // partition -> messages
	NextOffset map[int]int64            `json:"next_offset"` // partition -> next offset
}

// pollMessages implements long-polling for consumer group members.
//
// LONG-POLLING:
// Instead of returning immediately (which wastes resources if no messages),
// the server holds the request open until:
//   - Messages are available
//   - Timeout is reached (default 30 seconds)
//
// FLOW:
//  1. Validate member is in group with correct generation
//  2. Get member's assigned partitions
//  3. For each partition, get committed offset and read messages
//  4. If no messages, wait (with short polling intervals) until timeout
//  5. Return messages grouped by partition
//
// COMPARISON:
//   - Kafka: Uses fetch request with maxWaitMs parameter
//   - SQS: ReceiveMessage with WaitTimeSeconds
//   - goqueue: timeout query parameter (default 30s)
func (s *Server) pollMessages(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	// Get query parameters
	memberID := r.URL.Query().Get("member_id")
	generationStr := r.URL.Query().Get("generation")
	timeoutStr := r.URL.Query().Get("timeout")
	limitStr := r.URL.Query().Get("limit")

	if memberID == "" {
		s.errorResponse(w, http.StatusBadRequest, "member_id query param is required")
		return
	}

	generation := 0
	if generationStr != "" {
		var err error
		generation, err = strconv.Atoi(generationStr)
		if err != nil {
			s.errorResponse(w, http.StatusBadRequest, "invalid generation")
			return
		}
	}

	// Default 30 second timeout for long-polling
	timeout := 30 * time.Second
	if timeoutStr != "" {
		parsed, err := time.ParseDuration(timeoutStr)
		if err != nil {
			s.errorResponse(w, http.StatusBadRequest, "invalid timeout format (use Go duration like 30s)")
			return
		}
		timeout = parsed
		if timeout > 60*time.Second {
			timeout = 60 * time.Second // Cap at 60 seconds
		}
	}

	limit := 100
	if limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			s.errorResponse(w, http.StatusBadRequest, "invalid limit")
			return
		}
		if limit > 1000 {
			limit = 1000
		}
	}

	coordinator := s.broker.GroupCoordinator()

	// Get group and validate member
	group, err := coordinator.GetGroup(groupID)
	if err != nil {
		s.errorResponse(w, http.StatusNotFound, "group not found")
		return
	}

	partitions, groupGen, err := group.GetAssignment(memberID)
	if err != nil {
		s.errorResponse(w, http.StatusNotFound, "member not found (may have been evicted)")
		return
	}

	// Check generation
	if generation != 0 && generation != groupGen {
		s.errorResponse(w, http.StatusConflict, "stale generation (rebalance occurred)")
		return
	}

	// Get the topic (for M3, we assume single topic per group)
	if len(group.Topics) == 0 {
		s.errorResponse(w, http.StatusInternalServerError, "group has no subscribed topics")
		return
	}
	topicName := group.Topics[0]

	// Long-polling loop
	deadline := time.Now().Add(timeout)
	pollInterval := 100 * time.Millisecond // How often to check for new messages

	for {
		response := PollResponse{
			Messages:   make(map[int][]ConsumeMessage),
			NextOffset: make(map[int]int64),
		}
		totalMessages := 0

		// Fetch messages from each assigned partition
		for _, partition := range partitions {
			// Get committed offset (start from there) or start from 0
			offset, err := coordinator.GetOffset(groupID, topicName, partition)
			if err != nil {
				offset = 0 // No committed offset, start from beginning
			}

			messages, err := s.broker.Consume(topicName, partition, offset, limit)
			if err != nil {
				continue // Skip partition on error
			}

			if len(messages) > 0 {
				partitionMessages := make([]ConsumeMessage, len(messages))
				nextOffset := offset

				for i, msg := range messages {
					partitionMessages[i] = ConsumeMessage{
						Offset:    msg.Offset,
						Timestamp: msg.Timestamp.Format(time.RFC3339Nano),
						Key:       string(msg.Key),
						Value:     string(msg.Value),
					}
					if msg.Offset >= nextOffset {
						nextOffset = msg.Offset + 1
					}
				}

				response.Messages[partition] = partitionMessages
				response.NextOffset[partition] = nextOffset
				totalMessages += len(messages)
			}
		}

		// If we have messages, return immediately
		if totalMessages > 0 {
			s.writeJSON(w, http.StatusOK, response)
			return
		}

		// Check if we've exceeded the timeout
		if time.Now().After(deadline) {
			// Return empty response (no messages)
			s.writeJSON(w, http.StatusOK, response)
			return
		}

		// Wait a bit before checking again
		time.Sleep(pollInterval)
	}
}

// =============================================================================
// OFFSET HANDLERS (M3)
// =============================================================================

// CommitOffsetsRequest is the request body for committing offsets.
//
// EXAMPLE:
//
//	{
//	  "member_id": "order-processor-1-abc123",
//	  "generation": 5,
//	  "offsets": {
//	    "orders": {
//	      "0": 150,
//	      "1": 89
//	    }
//	  }
//	}
type CommitOffsetsRequest struct {
	MemberID   string                      `json:"member_id"`
	Generation int                         `json:"generation"`
	Offsets    map[string]map[string]int64 `json:"offsets"` // topic -> partition -> offset
}

func (s *Server) commitOffsets(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	var req CommitOffsetsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.MemberID == "" {
		s.errorResponse(w, http.StatusBadRequest, "member_id is required")
		return
	}
	if len(req.Offsets) == 0 {
		s.errorResponse(w, http.StatusBadRequest, "offsets are required")
		return
	}

	// Convert string partition keys to int
	offsets := make(map[string]map[int]int64)
	for topic, partitions := range req.Offsets {
		offsets[topic] = make(map[int]int64)
		for partStr, offset := range partitions {
			partInt, err := strconv.Atoi(partStr)
			if err != nil {
				s.errorResponse(w, http.StatusBadRequest, "invalid partition ID: "+partStr)
				return
			}
			offsets[topic][partInt] = offset
		}
	}

	coordinator := s.broker.GroupCoordinator()
	if err := coordinator.CommitOffsets(groupID, offsets, req.MemberID); err != nil {
		switch {
		case errors.Is(err, broker.ErrGroupNotFound):
			s.errorResponse(w, http.StatusNotFound, "group not found")
		case errors.Is(err, broker.ErrNotAssigned):
			s.errorResponse(w, http.StatusForbidden, "partition not assigned to this member")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "committed",
		"group":   groupID,
		"offsets": req.Offsets,
	})
}

func (s *Server) getOffsets(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	coordinator := s.broker.GroupCoordinator()
	groupOffsets, err := coordinator.GetGroupOffsets(groupID)
	if err != nil {
		if errors.Is(err, broker.ErrOffsetNotFound) {
			// No offsets committed yet
			s.writeJSON(w, http.StatusOK, map[string]interface{}{
				"group_id": groupID,
				"topics":   map[string]interface{}{},
			})
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Convert to JSON-friendly format (string partition keys)
	topics := make(map[string]map[string]int64)
	for topicName, topicOffsets := range groupOffsets.Topics {
		topics[topicName] = make(map[string]int64)
		for partID, partOffset := range topicOffsets.Partitions {
			topics[topicName][strconv.Itoa(partID)] = partOffset.Offset
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"group_id":   groupOffsets.GroupID,
		"topics":     topics,
		"generation": groupOffsets.Generation,
		"updated_at": groupOffsets.UpdatedAt.Format(time.RFC3339),
	})
}

// =============================================================================
// GROUP MANAGEMENT HANDLERS
// =============================================================================

func (s *Server) listGroups(w http.ResponseWriter, r *http.Request) {
	coordinator := s.broker.GroupCoordinator()
	groups := coordinator.GetAllGroupsInfo()

	// Simplify for list view
	groupList := make([]map[string]interface{}, len(groups))
	for i, g := range groups {
		groupList[i] = map[string]interface{}{
			"id":         g.ID,
			"state":      g.State,
			"members":    len(g.Members),
			"generation": g.Generation,
			"topics":     g.Topics,
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"groups": groupList,
	})
}

func (s *Server) getGroup(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	coordinator := s.broker.GroupCoordinator()
	info, err := coordinator.GetGroupInfo(groupID)
	if err != nil {
		if errors.Is(err, broker.ErrGroupNotFound) {
			s.errorResponse(w, http.StatusNotFound, "group not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Convert members to JSON-friendly format
	members := make([]map[string]interface{}, len(info.Members))
	for i, m := range info.Members {
		members[i] = map[string]interface{}{
			"id":                  m.ID,
			"client_id":           m.ClientID,
			"assigned_partitions": m.AssignedPartitions,
			"last_heartbeat":      m.LastHeartbeat.Format(time.RFC3339),
			"joined_at":           m.JoinedAt.Format(time.RFC3339),
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":         info.ID,
		"state":      info.State,
		"generation": info.Generation,
		"topics":     info.Topics,
		"members":    members,
		"created_at": info.CreatedAt.Format(time.RFC3339),
	})
}

func (s *Server) deleteGroup(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	coordinator := s.broker.GroupCoordinator()
	if err := coordinator.DeleteGroup(groupID); err != nil {
		if errors.Is(err, broker.ErrGroupNotFound) {
			s.errorResponse(w, http.StatusNotFound, "group not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"deleted": true,
		"group":   groupID,
	})
}
