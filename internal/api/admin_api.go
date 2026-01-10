// =============================================================================
// ADMIN API - PARTITION SCALING & REASSIGNMENT ENDPOINTS
// =============================================================================
//
// WHAT IS THIS?
// Administrative endpoints for online partition operations:
//   - Adding partitions to existing topics
//   - Reassigning partition replicas between nodes
//   - Monitoring reassignment progress
//
// THESE ARE DANGEROUS OPERATIONS!
// Production systems should:
//   1. Require authentication/authorization
//   2. Log all admin operations
//   3. Rate limit or require approval
//   4. Have rollback procedures
//
// ENDPOINT OVERVIEW:
//
//   PARTITION SCALING:
//   POST   /admin/topics/{name}/partitions           Add partitions to topic
//   GET    /admin/topics/{name}/scaling-status       Check if scaling in progress
//
//   PARTITION REASSIGNMENT:
//   POST   /admin/reassignment                       Start reassignment
//   GET    /admin/reassignment                       List active reassignments
//   GET    /admin/reassignment/{id}                  Get reassignment status
//   DELETE /admin/reassignment/{id}                  Cancel reassignment
//
//   COORDINATOR MANAGEMENT:
//   GET    /admin/coordinators                       List coordinator assignments
//   GET    /admin/coordinators/groups/{group}        Find group's coordinator
//
// =============================================================================

package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"goqueue/internal/broker"
	"goqueue/internal/cluster"
)

// =============================================================================
// ADMIN HANDLER DEPENDENCIES
// =============================================================================

// AdminDependencies holds dependencies for admin endpoints.
// These are optional - admin endpoints gracefully degrade if not set.
type AdminDependencies struct {
	// PartitionScaler handles partition expansion.
	PartitionScaler *broker.PartitionScaler

	// ReassignmentManager handles partition reassignment.
	ReassignmentManager *broker.ReassignmentManager

	// CoordinatorRouter routes group requests to coordinators.
	CoordinatorRouter *broker.CoordinatorRouter

	// MetadataStore provides cluster metadata.
	MetadataStore *cluster.MetadataStore
}

// adminDeps holds the current admin dependencies.
// Set via SetAdminDependencies before using admin endpoints.
var adminDeps *AdminDependencies

// SetAdminDependencies configures the admin dependencies.
// Must be called during server initialization.
func SetAdminDependencies(deps *AdminDependencies) {
	adminDeps = deps
}

// =============================================================================
// ROUTE REGISTRATION
// =============================================================================

// RegisterAdminRoutes adds admin endpoints to the server.
// Call this in registerRoutes() to enable admin functionality.
func (s *Server) RegisterAdminRoutes(r chi.Router) {
	r.Route("/admin", func(r chi.Router) {
		// Partition Scaling
		r.Route("/topics/{topicName}", func(r chi.Router) {
			r.Post("/partitions", s.addPartitions)
			r.Get("/scaling-status", s.getScalingStatus)
		})

		// Partition Reassignment
		r.Route("/reassignment", func(r chi.Router) {
			r.Post("/", s.startReassignment)
			r.Get("/", s.listReassignments)
			r.Get("/{reassignmentID}", s.getReassignment)
			r.Delete("/{reassignmentID}", s.cancelReassignment)
		})

		// Coordinator Management
		r.Route("/coordinators", func(r chi.Router) {
			r.Get("/", s.listCoordinators)
			r.Get("/groups/{groupID}", s.findGroupCoordinator)
		})
	})
}

// =============================================================================
// PARTITION SCALING ENDPOINTS
// =============================================================================

// AddPartitionsRequest is the request body for adding partitions.
//
// JSON EXAMPLE:
//
//	{
//	    "count": 6,                              // Total target partition count
//	    "replica_assignments": {                 // Optional: explicit assignments
//	        "3": ["node-a", "node-b", "node-c"], // Partition 3 replicas
//	        "4": ["node-b", "node-c", "node-a"], // Partition 4 replicas
//	        "5": ["node-c", "node-a", "node-b"]  // Partition 5 replicas
//	    }
//	}
type AddPartitionsRequest struct {
	// Count is the new total partition count (not additional count).
	// If topic has 3 partitions and Count is 6, adds 3 new partitions.
	Count int `json:"count"`

	// ReplicaAssignments optionally specifies where to place new partitions.
	// If omitted, controller auto-assigns using round-robin.
	ReplicaAssignments map[string][]string `json:"replica_assignments,omitempty"`
}

// AddPartitionsResponse is the response from adding partitions.
type AddPartitionsResponse struct {
	// Success indicates if the operation completed.
	Success bool `json:"success"`

	// TopicName is the topic that was scaled.
	TopicName string `json:"topic_name"`

	// OldPartitionCount before scaling.
	OldPartitionCount int `json:"old_partition_count"`

	// NewPartitionCount after scaling.
	NewPartitionCount int `json:"new_partition_count"`

	// PartitionsAdded lists the IDs of new partitions.
	PartitionsAdded []int `json:"partitions_added"`

	// Error message if failed.
	Error string `json:"error,omitempty"`
}

// addPartitions handles POST /admin/topics/{topicName}/partitions
//
// EXAMPLE REQUEST:
//
//	POST /admin/topics/orders/partitions
//	{
//	    "count": 12
//	}
//
// EXAMPLE RESPONSE:
//
//	{
//	    "success": true,
//	    "topic_name": "orders",
//	    "old_partition_count": 6,
//	    "new_partition_count": 12,
//	    "partitions_added": [6, 7, 8, 9, 10, 11]
//	}
func (s *Server) addPartitions(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")

	// Check dependencies
	if adminDeps == nil || adminDeps.PartitionScaler == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"partition scaler not configured")
		return
	}

	// Parse request
	var req AddPartitionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest,
			"invalid request body: "+err.Error())
		return
	}

	// Validate
	if req.Count <= 0 {
		s.errorResponse(w, http.StatusBadRequest,
			"count must be positive")
		return
	}

	// Convert replica assignments
	var replicaAssignments map[int][]cluster.NodeID
	if req.ReplicaAssignments != nil {
		replicaAssignments = make(map[int][]cluster.NodeID)
		for partStr, nodes := range req.ReplicaAssignments {
			partID, err := strconv.Atoi(partStr)
			if err != nil {
				s.errorResponse(w, http.StatusBadRequest,
					"invalid partition ID: "+partStr)
				return
			}
			nodeIDs := make([]cluster.NodeID, len(nodes))
			for i, n := range nodes {
				nodeIDs[i] = cluster.NodeID(n)
			}
			replicaAssignments[partID] = nodeIDs
		}
	}

	// Execute scaling
	scaleReq := &broker.PartitionScaleRequest{
		TopicName:          topicName,
		NewPartitionCount:  req.Count,
		ReplicaAssignments: replicaAssignments,
	}

	result, err := adminDeps.PartitionScaler.AddPartitions(scaleReq)
	if err != nil {
		resp := AddPartitionsResponse{
			Success:   false,
			TopicName: topicName,
			Error:     err.Error(),
		}
		if result != nil {
			resp.OldPartitionCount = result.OldPartitionCount
		}
		s.writeJSON(w, http.StatusBadRequest, resp)
		return
	}

	// Success response
	resp := AddPartitionsResponse{
		Success:           true,
		TopicName:         result.TopicName,
		OldPartitionCount: result.OldPartitionCount,
		NewPartitionCount: result.NewPartitionCount,
		PartitionsAdded:   result.PartitionsAdded,
	}

	s.writeJSON(w, http.StatusOK, resp)

	s.logger.Info("partitions added",
		"topic", topicName,
		"old_count", result.OldPartitionCount,
		"new_count", result.NewPartitionCount,
		"added", len(result.PartitionsAdded))
}

// getScalingStatus handles GET /admin/topics/{topicName}/scaling-status
//
// EXAMPLE RESPONSE:
//
//	{
//	    "topic": "orders",
//	    "scaling_in_progress": false
//	}
func (s *Server) getScalingStatus(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topicName")

	if adminDeps == nil || adminDeps.PartitionScaler == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"partition scaler not configured")
		return
	}

	inProgress := adminDeps.PartitionScaler.IsScalingInProgress(topicName)

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"topic":               topicName,
		"scaling_in_progress": inProgress,
	})
}

// =============================================================================
// PARTITION REASSIGNMENT ENDPOINTS
// =============================================================================

// StartReassignmentRequest is the request body for starting a reassignment.
//
// JSON EXAMPLE:
//
//	{
//	    "partitions": [
//	        {
//	            "topic": "orders",
//	            "partition": 3,
//	            "old_replicas": ["node-a", "node-b", "node-c"],
//	            "new_replicas": ["node-a", "node-b", "node-d"]
//	        }
//	    ],
//	    "throttle_bytes_per_sec": 10485760,
//	    "timeout_per_partition_sec": 3600
//	}
type StartReassignmentRequest struct {
	// Partitions to reassign.
	Partitions []PartitionReassignmentRequest `json:"partitions"`

	// ThrottleBytesPerSec limits replication bandwidth (0 = default 10MB/s).
	ThrottleBytesPerSec int64 `json:"throttle_bytes_per_sec"`

	// TimeoutPerPartitionSec is the timeout in seconds (0 = default 24 hours).
	TimeoutPerPartitionSec int `json:"timeout_per_partition_sec"`
}

// PartitionReassignmentRequest describes one partition's reassignment.
type PartitionReassignmentRequest struct {
	Topic       string   `json:"topic"`
	Partition   int      `json:"partition"`
	OldReplicas []string `json:"old_replicas"`
	NewReplicas []string `json:"new_replicas"`
}

// startReassignment handles POST /admin/reassignment
func (s *Server) startReassignment(w http.ResponseWriter, r *http.Request) {
	if adminDeps == nil || adminDeps.ReassignmentManager == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"reassignment manager not configured")
		return
	}

	var req StartReassignmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest,
			"invalid request body: "+err.Error())
		return
	}

	if len(req.Partitions) == 0 {
		s.errorResponse(w, http.StatusBadRequest,
			"at least one partition required")
		return
	}

	// Convert request
	partitions := make([]broker.PartitionReassignment, len(req.Partitions))
	for i, pr := range req.Partitions {
		partitions[i] = broker.PartitionReassignment{
			Topic:       pr.Topic,
			Partition:   pr.Partition,
			OldReplicas: toNodeIDs(pr.OldReplicas),
			NewReplicas: toNodeIDs(pr.NewReplicas),
		}
	}

	brokerReq := &broker.ReassignmentRequest{
		Partitions:          partitions,
		ThrottleBytesPerSec: req.ThrottleBytesPerSec,
	}
	if req.TimeoutPerPartitionSec > 0 {
		brokerReq.TimeoutPerPartition = time.Duration(req.TimeoutPerPartitionSec) * time.Second
	}

	reassignmentID, err := adminDeps.ReassignmentManager.StartReassignment(brokerReq)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	s.writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"success":         true,
		"reassignment_id": reassignmentID,
		"message":         "reassignment started",
	})

	s.logger.Info("reassignment started",
		"id", reassignmentID,
		"partitions", len(req.Partitions))
}

// listReassignments handles GET /admin/reassignment
func (s *Server) listReassignments(w http.ResponseWriter, r *http.Request) {
	if adminDeps == nil || adminDeps.ReassignmentManager == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"reassignment manager not configured")
		return
	}

	reassignments := adminDeps.ReassignmentManager.ListActiveReassignments()

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"count":         len(reassignments),
		"reassignments": reassignments,
	})
}

// getReassignment handles GET /admin/reassignment/{reassignmentID}
func (s *Server) getReassignment(w http.ResponseWriter, r *http.Request) {
	reassignmentID := chi.URLParam(r, "reassignmentID")

	if adminDeps == nil || adminDeps.ReassignmentManager == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"reassignment manager not configured")
		return
	}

	status, err := adminDeps.ReassignmentManager.GetStatus(reassignmentID)
	if err != nil {
		s.errorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, status)
}

// cancelReassignment handles DELETE /admin/reassignment/{reassignmentID}
func (s *Server) cancelReassignment(w http.ResponseWriter, r *http.Request) {
	reassignmentID := chi.URLParam(r, "reassignmentID")

	if adminDeps == nil || adminDeps.ReassignmentManager == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"reassignment manager not configured")
		return
	}

	if err := adminDeps.ReassignmentManager.CancelReassignment(reassignmentID); err != nil {
		s.errorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "reassignment cancellation requested",
	})

	s.logger.Info("reassignment cancelled", "id", reassignmentID)
}

// =============================================================================
// COORDINATOR MANAGEMENT ENDPOINTS
// =============================================================================

// listCoordinators handles GET /admin/coordinators
//
// EXAMPLE RESPONSE:
//
//	{
//	    "internal_topic": "__consumer_offsets",
//	    "partition_count": 50,
//	    "coordinators": [
//	        {
//	            "partition": 0,
//	            "leader": "node-a",
//	            "replicas": ["node-a", "node-b", "node-c"]
//	        },
//	        ...
//	    ]
//	}
func (s *Server) listCoordinators(w http.ResponseWriter, r *http.Request) {
	if adminDeps == nil || adminDeps.MetadataStore == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"metadata store not configured")
		return
	}

	// Get __consumer_offsets topic assignments
	assignments := adminDeps.MetadataStore.GetAssignmentsForTopic(broker.ConsumerOffsetsTopicName)

	coordinators := make([]map[string]interface{}, len(assignments))
	for i, a := range assignments {
		coordinators[i] = map[string]interface{}{
			"partition": a.Partition,
			"leader":    a.Leader,
			"replicas":  a.Replicas,
			"isr":       a.ISR,
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"internal_topic":  broker.ConsumerOffsetsTopicName,
		"partition_count": len(assignments),
		"coordinators":    coordinators,
	})
}

// findGroupCoordinator handles GET /admin/coordinators/groups/{groupID}
//
// EXAMPLE RESPONSE:
//
//	{
//	    "group_id": "my-consumer-group",
//	    "coordinator_partition": 23,
//	    "coordinator_node": "node-b",
//	    "routing_info": {
//	        "hash_value": 1234567,
//	        "partition_formula": "hash % 50 = 23"
//	    }
//	}
func (s *Server) findGroupCoordinator(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	if adminDeps == nil || adminDeps.CoordinatorRouter == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"coordinator router not configured")
		return
	}

	coordinator, err := adminDeps.CoordinatorRouter.FindCoordinator(groupID)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Calculate hash for debugging info (use default 50 partitions)
	const defaultPartitions = 50
	partition := broker.GroupToPartition(groupID, defaultPartitions)

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"group_id":              groupID,
		"coordinator_partition": partition,
		"coordinator_node":      coordinator.NodeID,
		"routing_info": map[string]interface{}{
			"partition_count": defaultPartitions,
			"partition":       partition,
		},
	})
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// toNodeIDs converts string slice to NodeID slice.
func toNodeIDs(strs []string) []cluster.NodeID {
	result := make([]cluster.NodeID, len(strs))
	for i, s := range strs {
		result[i] = cluster.NodeID(s)
	}
	return result
}
