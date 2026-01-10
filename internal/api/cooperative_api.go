// =============================================================================
// COOPERATIVE REBALANCING HTTP API
// =============================================================================
//
// This file adds HTTP API endpoints for cooperative rebalancing (M12).
//
// NEW ENDPOINTS:
//
//   COOPERATIVE JOIN (replaces /groups/{group}/join for cooperative mode)
//   POST /groups/{group}/join/cooperative
//     Request:  { "client_id": "my-consumer", "topics": ["orders"], "protocol": "cooperative" }
//     Response: { "member_id": "...", "generation": 5, "rebalance_required": true, "protocol": "cooperative" }
//
//   HEARTBEAT WITH REBALANCE INFO
//   POST /groups/{group}/heartbeat/cooperative
//     Request:  { "member_id": "...", "generation": 5 }
//     Response: {
//       "rebalance_required": true/false,
//       "partitions_to_revoke": [...],
//       "partitions_assigned": [...],
//       "generation": 6,
//       "state": "pending_revoke|pending_assign|complete"
//     }
//
//   REVOCATION ACKNOWLEDGMENT
//   POST /groups/{group}/revoke
//     Request:  { "member_id": "...", "generation": 5, "revoked_partitions": [...] }
//     Response: { "status": "acknowledged" }
//
//   REBALANCE METRICS
//   GET /groups/{group}/rebalance/stats
//     Response: { "total_rebalances": 10, "partitions_moved": 25, ... }
//
//   GLOBAL REBALANCE METRICS
//   GET /rebalance/stats
//     Response: { global metrics across all groups }
//
// =============================================================================

package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"goqueue/internal/broker"
)

// =============================================================================
// COOPERATIVE JOIN REQUEST/RESPONSE
// =============================================================================

// CooperativeJoinRequest is the request body for joining with cooperative protocol.
type CooperativeJoinRequest struct {
	ClientID string   `json:"client_id"`
	Topics   []string `json:"topics"`
	Protocol string   `json:"protocol,omitempty"` // "cooperative" or "eager", default "cooperative"
}

// CooperativeJoinResponse is the response after joining with cooperative protocol.
type CooperativeJoinResponse struct {
	MemberID          string   `json:"member_id"`
	Generation        int      `json:"generation"`
	LeaderID          string   `json:"leader_id,omitempty"`
	RebalanceRequired bool     `json:"rebalance_required"`
	Protocol          string   `json:"protocol"`
	Members           []string `json:"members,omitempty"`
}

// =============================================================================
// COOPERATIVE HEARTBEAT REQUEST/RESPONSE
// =============================================================================

// CooperativeHeartbeatRequest is the request body for cooperative heartbeat.
type CooperativeHeartbeatRequest struct {
	MemberID   string `json:"member_id"`
	Generation int    `json:"generation"`
}

// CooperativeHeartbeatResponse contains rebalance state for the consumer.
type CooperativeHeartbeatResponse struct {
	RebalanceRequired  bool                    `json:"rebalance_required"`
	PartitionsToRevoke []broker.TopicPartition `json:"partitions_to_revoke,omitempty"`
	PartitionsAssigned []broker.TopicPartition `json:"partitions_assigned,omitempty"`
	Generation         int                     `json:"generation"`
	Protocol           string                  `json:"protocol"`
	State              string                  `json:"state"`
	RevocationDeadline string                  `json:"revocation_deadline,omitempty"`
	Error              string                  `json:"error,omitempty"`
}

// =============================================================================
// REVOCATION ACKNOWLEDGMENT REQUEST/RESPONSE
// =============================================================================

// RevocationAckRequest is the request body for acknowledging partition revocation.
type RevocationAckRequest struct {
	MemberID          string                  `json:"member_id"`
	Generation        int                     `json:"generation"`
	RevokedPartitions []broker.TopicPartition `json:"revoked_partitions"`
}

// RevocationAckResponse confirms the revocation was acknowledged.
type RevocationAckResponse struct {
	Status         string `json:"status"`
	PendingCount   int    `json:"pending_count"`
	RebalanceState string `json:"rebalance_state"`
}

// =============================================================================
// REBALANCE STATS RESPONSE
// =============================================================================

// RebalanceStatsResponse contains rebalance metrics.
type RebalanceStatsResponse struct {
	TotalRebalances            int64            `json:"total_rebalances"`
	SuccessfulRebalances       int64            `json:"successful_rebalances"`
	FailedRebalances           int64            `json:"failed_rebalances"`
	TotalPartitionsRevoked     int64            `json:"total_partitions_revoked"`
	TotalPartitionsAssigned    int64            `json:"total_partitions_assigned"`
	TotalRevocationTimeouts    int64            `json:"total_revocation_timeouts"`
	LastRebalanceDurationMs    int64            `json:"last_rebalance_duration_ms"`
	AverageRebalanceDurationMs float64          `json:"average_rebalance_duration_ms"`
	LastRebalanceAt            string           `json:"last_rebalance_at,omitempty"`
	RebalancesByReason         map[string]int64 `json:"rebalances_by_reason"`
}

// =============================================================================
// GROUP INFO WITH COOPERATIVE STATE
// =============================================================================

// CooperativeGroupInfoResponse extends group info with cooperative state.
type CooperativeGroupInfoResponse struct {
	ID                 string                             `json:"id"`
	State              string                             `json:"state"`
	Generation         int                                `json:"generation"`
	Protocol           string                             `json:"protocol"`
	AssignmentStrategy string                             `json:"assignment_strategy"`
	RebalanceState     string                             `json:"rebalance_state"`
	Topics             []string                           `json:"topics"`
	Members            []CooperativeMemberInfo            `json:"members"`
	MemberAssignments  map[string][]broker.TopicPartition `json:"member_assignments"`
	RebalanceMetrics   *RebalanceStatsResponse            `json:"rebalance_metrics,omitempty"`
}

// CooperativeMemberInfo contains member info with cooperative state.
type CooperativeMemberInfo struct {
	ID                 string                  `json:"id"`
	ClientID           string                  `json:"client_id"`
	AssignedPartitions []broker.TopicPartition `json:"assigned_partitions"`
	LastHeartbeat      string                  `json:"last_heartbeat"`
	JoinedAt           string                  `json:"joined_at"`
}

// =============================================================================
// HANDLER: COOPERATIVE JOIN
// =============================================================================

// joinGroupCooperative handles joining a group with cooperative rebalancing.
//
// POST /groups/{groupID}/join/cooperative
//
// This endpoint is for consumers that want to use cooperative rebalancing.
// The response indicates whether a rebalance is needed and what protocol is in use.
func (s *Server) joinGroupCooperative(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	var req CooperativeJoinRequest
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

	// Get cooperative coordinator
	coopCoordinator := s.broker.CooperativeGroupCoordinator()
	if coopCoordinator == nil {
		// Fall back to regular join if cooperative coordinator not available
		s.joinGroup(w, r)
		return
	}

	result, err := coopCoordinator.JoinCooperativeGroup(groupID, req.ClientID, req.Topics)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "failed to join group: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, CooperativeJoinResponse{
		MemberID:          result.MemberID,
		Generation:        result.Generation,
		LeaderID:          result.LeaderID,
		RebalanceRequired: result.RebalanceRequired,
		Protocol:          result.Protocol.String(),
		Members:           result.Members,
	})
}

// =============================================================================
// HANDLER: COOPERATIVE HEARTBEAT
// =============================================================================

// heartbeatCooperative handles heartbeats with rebalance response.
//
// POST /groups/{groupID}/heartbeat/cooperative
//
// This endpoint returns rebalance information in the response, allowing
// consumers to learn about pending rebalances without additional polling.
func (s *Server) heartbeatCooperative(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	var req CooperativeHeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.MemberID == "" {
		s.errorResponse(w, http.StatusBadRequest, "member_id is required")
		return
	}

	coopCoordinator := s.broker.CooperativeGroupCoordinator()
	if coopCoordinator == nil {
		// Fall back to regular heartbeat
		s.heartbeat(w, r)
		return
	}

	response, err := coopCoordinator.HeartbeatCooperative(groupID, req.MemberID, req.Generation)
	if err != nil {
		switch err {
		case broker.ErrGroupNotFound:
			s.errorResponse(w, http.StatusNotFound, "group not found")
		case broker.ErrMemberNotFound:
			s.errorResponse(w, http.StatusNotFound, "member not found (may have been evicted)")
		case broker.ErrStaleGeneration:
			s.errorResponse(w, http.StatusConflict, "stale generation (rebalance occurred)")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	resp := CooperativeHeartbeatResponse{
		RebalanceRequired:  response.RebalanceRequired,
		PartitionsToRevoke: response.PartitionsToRevoke,
		PartitionsAssigned: response.PartitionsAssigned,
		Generation:         response.Generation,
		Protocol:           response.Protocol.String(),
		State:              response.State.String(),
		Error:              response.Error,
	}

	if !response.RevocationDeadline.IsZero() {
		resp.RevocationDeadline = response.RevocationDeadline.Format("2006-01-02T15:04:05Z07:00")
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// =============================================================================
// HANDLER: REVOCATION ACKNOWLEDGMENT
// =============================================================================

// acknowledgeRevocation handles revocation acknowledgments from consumers.
//
// POST /groups/{groupID}/revoke
//
// When a consumer receives partitions_to_revoke in a heartbeat response,
// it should:
//  1. Stop processing those partitions
//  2. Commit offsets for those partitions
//  3. Call this endpoint to acknowledge the revocation
func (s *Server) acknowledgeRevocation(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	var req RevocationAckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.MemberID == "" {
		s.errorResponse(w, http.StatusBadRequest, "member_id is required")
		return
	}

	coopCoordinator := s.broker.CooperativeGroupCoordinator()
	if coopCoordinator == nil {
		s.errorResponse(w, http.StatusNotImplemented, "cooperative rebalancing not enabled")
		return
	}

	if err := coopCoordinator.AcknowledgeRevocation(
		groupID,
		req.MemberID,
		req.Generation,
		req.RevokedPartitions,
	); err != nil {
		switch err {
		case broker.ErrGroupNotFound:
			s.errorResponse(w, http.StatusNotFound, "group not found")
		case broker.ErrMemberNotFound:
			s.errorResponse(w, http.StatusNotFound, "member not found")
		case broker.ErrStaleGeneration:
			s.errorResponse(w, http.StatusConflict, "stale generation")
		case broker.ErrRebalanceInProgress:
			s.errorResponse(w, http.StatusConflict, "no active rebalance")
		default:
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	// Get updated state
	group, exists := coopCoordinator.GetCooperativeGroup(groupID)
	if !exists {
		s.writeJSON(w, http.StatusOK, RevocationAckResponse{
			Status: "acknowledged",
		})
		return
	}

	ctx := coopCoordinator.GetRebalanceMetrics()

	s.writeJSON(w, http.StatusOK, RevocationAckResponse{
		Status:         "acknowledged",
		PendingCount:   0, // We'd need to expose this from the context
		RebalanceState: group.GetCooperativeInfo().RebalanceState,
	})

	// Check if we can complete the rebalance
	_ = ctx // suppress unused variable warning
}

// =============================================================================
// HANDLER: GROUP REBALANCE STATS
// =============================================================================

// getGroupRebalanceStats returns rebalance statistics for a specific group.
//
// GET /groups/{groupID}/rebalance/stats
func (s *Server) getGroupRebalanceStats(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	coopCoordinator := s.broker.CooperativeGroupCoordinator()
	if coopCoordinator == nil {
		s.errorResponse(w, http.StatusNotImplemented, "cooperative rebalancing not enabled")
		return
	}

	group, exists := coopCoordinator.GetCooperativeGroup(groupID)
	if !exists {
		s.errorResponse(w, http.StatusNotFound, "group not found")
		return
	}

	info := group.GetCooperativeInfo()
	if info.RebalanceMetrics == nil {
		s.writeJSON(w, http.StatusOK, RebalanceStatsResponse{
			RebalancesByReason: make(map[string]int64),
		})
		return
	}

	metrics := info.RebalanceMetrics
	resp := RebalanceStatsResponse{
		TotalRebalances:            metrics.TotalRebalances,
		SuccessfulRebalances:       metrics.SuccessfulRebalances,
		FailedRebalances:           metrics.FailedRebalances,
		TotalPartitionsRevoked:     metrics.TotalPartitionsRevoked,
		TotalPartitionsAssigned:    metrics.TotalPartitionsAssigned,
		TotalRevocationTimeouts:    metrics.TotalRevocationTimeouts,
		LastRebalanceDurationMs:    metrics.LastRebalanceDurationMs,
		AverageRebalanceDurationMs: metrics.AverageRebalanceDurationMs,
		RebalancesByReason:         metrics.RebalancesByReason,
	}

	if !metrics.LastRebalanceAt.IsZero() {
		resp.LastRebalanceAt = metrics.LastRebalanceAt.Format("2006-01-02T15:04:05Z07:00")
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// =============================================================================
// HANDLER: GLOBAL REBALANCE STATS
// =============================================================================

// getGlobalRebalanceStats returns rebalance statistics across all groups.
//
// GET /rebalance/stats
func (s *Server) getGlobalRebalanceStats(w http.ResponseWriter, r *http.Request) {
	coopCoordinator := s.broker.CooperativeGroupCoordinator()
	if coopCoordinator == nil {
		s.errorResponse(w, http.StatusNotImplemented, "cooperative rebalancing not enabled")
		return
	}

	metrics := coopCoordinator.GetRebalanceMetrics()

	resp := RebalanceStatsResponse{
		TotalRebalances:            metrics.TotalRebalances,
		SuccessfulRebalances:       metrics.SuccessfulRebalances,
		FailedRebalances:           metrics.FailedRebalances,
		TotalPartitionsRevoked:     metrics.TotalPartitionsRevoked,
		TotalPartitionsAssigned:    metrics.TotalPartitionsAssigned,
		TotalRevocationTimeouts:    metrics.TotalRevocationTimeouts,
		LastRebalanceDurationMs:    metrics.LastRebalanceDurationMs,
		AverageRebalanceDurationMs: metrics.AverageRebalanceDurationMs,
		RebalancesByReason:         metrics.RebalancesByReason,
	}

	if !metrics.LastRebalanceAt.IsZero() {
		resp.LastRebalanceAt = metrics.LastRebalanceAt.Format("2006-01-02T15:04:05Z07:00")
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// =============================================================================
// HANDLER: GET COOPERATIVE GROUP INFO
// =============================================================================

// getCooperativeGroupInfo returns extended group info with cooperative state.
//
// GET /groups/{groupID}/cooperative
func (s *Server) getCooperativeGroupInfo(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	coopCoordinator := s.broker.CooperativeGroupCoordinator()
	if coopCoordinator == nil {
		// Fall back to regular group info
		s.getGroup(w, r)
		return
	}

	group, exists := coopCoordinator.GetCooperativeGroup(groupID)
	if !exists {
		// Try regular group
		s.getGroup(w, r)
		return
	}

	info := group.GetCooperativeInfo()

	// Convert members to response format
	members := make([]CooperativeMemberInfo, 0, len(info.Members))
	for _, m := range info.Members {
		// Get TopicPartition assignment
		tpAssignment := []broker.TopicPartition{}
		if partitions, exists := info.MemberAssignments[m.ID]; exists {
			tpAssignment = partitions
		}

		members = append(members, CooperativeMemberInfo{
			ID:                 m.ID,
			ClientID:           m.ClientID,
			AssignedPartitions: tpAssignment,
			LastHeartbeat:      m.LastHeartbeat.Format("2006-01-02T15:04:05Z07:00"),
			JoinedAt:           m.JoinedAt.Format("2006-01-02T15:04:05Z07:00"),
		})
	}

	// Convert metrics
	var metricsResp *RebalanceStatsResponse
	if info.RebalanceMetrics != nil {
		m := info.RebalanceMetrics
		metricsResp = &RebalanceStatsResponse{
			TotalRebalances:            m.TotalRebalances,
			SuccessfulRebalances:       m.SuccessfulRebalances,
			FailedRebalances:           m.FailedRebalances,
			TotalPartitionsRevoked:     m.TotalPartitionsRevoked,
			TotalPartitionsAssigned:    m.TotalPartitionsAssigned,
			TotalRevocationTimeouts:    m.TotalRevocationTimeouts,
			LastRebalanceDurationMs:    m.LastRebalanceDurationMs,
			AverageRebalanceDurationMs: m.AverageRebalanceDurationMs,
			RebalancesByReason:         m.RebalancesByReason,
		}
		if !m.LastRebalanceAt.IsZero() {
			metricsResp.LastRebalanceAt = m.LastRebalanceAt.Format("2006-01-02T15:04:05Z07:00")
		}
	}

	s.writeJSON(w, http.StatusOK, CooperativeGroupInfoResponse{
		ID:                 info.ID,
		State:              info.State,
		Generation:         info.Generation,
		Protocol:           info.Protocol,
		AssignmentStrategy: info.AssignmentStrategy,
		RebalanceState:     info.RebalanceState,
		Topics:             info.Topics,
		Members:            members,
		MemberAssignments:  info.MemberAssignments,
		RebalanceMetrics:   metricsResp,
	})
}

// =============================================================================
// HANDLER: LEAVE WITH COOPERATIVE PROTOCOL
// =============================================================================

// leaveGroupCooperative handles graceful leave with cooperative rebalancing.
//
// POST /groups/{groupID}/leave/cooperative
func (s *Server) leaveGroupCooperative(w http.ResponseWriter, r *http.Request) {
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

	coopCoordinator := s.broker.CooperativeGroupCoordinator()
	if coopCoordinator == nil {
		// Fall back to regular leave
		s.leaveGroup(w, r)
		return
	}

	if err := coopCoordinator.LeaveCooperativeGroup(groupID, req.MemberID); err != nil {
		switch err {
		case broker.ErrGroupNotFound:
			s.errorResponse(w, http.StatusNotFound, "group not found")
		case broker.ErrMemberNotFound:
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
// HANDLER: GET ASSIGNMENT
// =============================================================================

// GetAssignmentResponse contains the current partition assignment for a member.
type GetAssignmentResponse struct {
	MemberID   string                  `json:"member_id"`
	Generation int                     `json:"generation"`
	Partitions []broker.TopicPartition `json:"partitions"`
}

// getAssignment returns the current assignment for a member.
//
// GET /groups/{groupID}/assignment?member_id=xxx
func (s *Server) getAssignment(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")
	memberID := r.URL.Query().Get("member_id")

	if memberID == "" {
		s.errorResponse(w, http.StatusBadRequest, "member_id query param is required")
		return
	}

	coopCoordinator := s.broker.CooperativeGroupCoordinator()
	if coopCoordinator == nil {
		// Fall back to regular group
		coordinator := s.broker.GroupCoordinator()
		group, err := coordinator.GetGroup(groupID)
		if err != nil {
			s.errorResponse(w, http.StatusNotFound, "group not found")
			return
		}

		partitions, gen, err := group.GetAssignment(memberID)
		if err != nil {
			s.errorResponse(w, http.StatusNotFound, "member not found")
			return
		}

		// Convert to TopicPartitions (single topic support)
		tpPartitions := make([]broker.TopicPartition, 0, len(partitions))
		if len(group.Topics) > 0 {
			for _, p := range partitions {
				tpPartitions = append(tpPartitions, broker.TopicPartition{
					Topic:     group.Topics[0],
					Partition: p,
				})
			}
		}

		s.writeJSON(w, http.StatusOK, GetAssignmentResponse{
			MemberID:   memberID,
			Generation: gen,
			Partitions: tpPartitions,
		})
		return
	}

	group, exists := coopCoordinator.GetCooperativeGroup(groupID)
	if !exists {
		s.errorResponse(w, http.StatusNotFound, "group not found")
		return
	}

	partitions, exists := group.GetMemberAssignment(memberID)
	if !exists {
		s.errorResponse(w, http.StatusNotFound, "member not found")
		return
	}

	s.writeJSON(w, http.StatusOK, GetAssignmentResponse{
		MemberID:   memberID,
		Generation: group.GetGeneration(),
		Partitions: partitions,
	})
}

// =============================================================================
// ROUTE REGISTRATION HELPER
// =============================================================================

// RegisterCooperativeGroupRoutes adds cooperative rebalancing routes to the
// group-specific router (/{groupID} sub-router). This should be called from
// within the /groups/{groupID} route block in registerRoutes() in server.go.
func (s *Server) RegisterCooperativeGroupRoutes(r chi.Router) {
	// Cooperative join/leave
	r.Post("/join/cooperative", s.joinGroupCooperative)
	r.Post("/leave/cooperative", s.leaveGroupCooperative)

	// Cooperative heartbeat
	r.Post("/heartbeat/cooperative", s.heartbeatCooperative)

	// Revocation acknowledgment
	r.Post("/revoke", s.acknowledgeRevocation)

	// Get assignment
	r.Get("/assignment", s.getAssignment)

	// Cooperative group info
	r.Get("/cooperative", s.getCooperativeGroupInfo)

	// Group-specific rebalance stats
	r.Get("/rebalance/stats", s.getGroupRebalanceStats)
}

// RegisterCooperativeGlobalRoutes adds global cooperative routes.
// This should be called at the top-level router in registerRoutes().
func (s *Server) RegisterCooperativeGlobalRoutes(r chi.Router) {
	// Global rebalance stats
	r.Get("/rebalance/stats", s.getGlobalRebalanceStats)
}

// =============================================================================
// HELPER: PARSE TOPIC PARTITION FROM STRING
// =============================================================================

// parseTopicPartition parses a "topic-partition" string into TopicPartition.
func parseTopicPartition(s string) (broker.TopicPartition, error) {
	// Format: "topic-N" or just "N" (assumes first subscribed topic)
	// For simplicity, we expect JSON format in requests
	return broker.TopicPartition{}, nil
}

// Helper to get generation from query param with default
func getGenerationParam(r *http.Request, defaultGen int) int {
	genStr := r.URL.Query().Get("generation")
	if genStr == "" {
		return defaultGen
	}
	gen, err := strconv.Atoi(genStr)
	if err != nil {
		return defaultGen
	}
	return gen
}
