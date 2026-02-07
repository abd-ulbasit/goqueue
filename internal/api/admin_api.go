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
	"goqueue/internal/security"
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
//
// ============================================================================
// ADMIN API SECURITY MODEL
// ============================================================================
//
// WHY ADMIN ROUTES NEED SPECIAL PROTECTION:
// Admin endpoints can:
//   - Create/revoke API keys (full system access)
//   - Add partitions (resource allocation)
//   - Reassign data (availability impact)
//   - Manage coordinators (cluster stability)
//
// COMPARISON TO OTHER SYSTEMS:
//   - Kafka: Separate admin client with SASL auth + ACLs
//   - RabbitMQ: Management plugin with separate users/permissions
//   - SQS: IAM policies for admin operations
//   - goqueue: RequirePermission("admin:*") on all admin routes
//
// SECURITY LAYERS:
//  1. TLS encryption (transport security)
//  2. API key authentication (identity)
//  3. Permission check (authorization)
//  4. Audit logging (accountability)
func (s *Server) RegisterAdminRoutes(r chi.Router) {
	r.Route("/admin", func(r chi.Router) {
		// ====================================================================
		// API KEY MANAGEMENT
		// ====================================================================
		// These endpoints allow creating, listing, and revoking API keys.
		// Only admin role can access these endpoints.
		//
		// SECURITY: Double protection
		//   1. Auth middleware already verified caller has valid API key
		//   2. RequirePermission ensures caller has "admin:keys" permission
		//
		r.Route("/keys", func(r chi.Router) {
			// Require admin permission for all key management
			if s.security != nil {
				r.Use(s.security.RequirePermission("admin:keys"))
			}
			r.Post("/", s.createAPIKey)          // Create new API key
			r.Get("/", s.listAPIKeys)            // List all keys (hashed)
			r.Delete("/{keyID}", s.revokeAPIKey) // Revoke specific key
		})

		// ====================================================================
		// ACCESS CONTROL LISTS (ACLs)
		// ====================================================================
		// Manage topic-level permissions for API keys.
		//
		// EXAMPLE USE CASE:
		//   - Give key X read access to topic "orders.*"
		//   - Give key Y write access to topic "payments"
		//
		r.Route("/acls", func(r chi.Router) {
			if s.security != nil {
				r.Use(s.security.RequirePermission("admin:acls"))
			}
			r.Post("/", s.addACL)             // Add ACL rule
			r.Get("/", s.listACLs)            // List all ACL rules
			r.Delete("/{aclID}", s.removeACL) // Remove ACL rule
		})

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

		// ====================================================================
		// BACKUP & RESTORE
		// ====================================================================
		// Backup and restore broker metadata (topics, offsets, schemas).
		// For VolumeSnapshot-based data backup, use Kubernetes VolumeSnapshot API.
		//
		r.Route("/backup", func(r chi.Router) {
			if s.security != nil {
				r.Use(s.security.RequirePermission("admin:backup"))
			}
			r.Post("/", s.createBackup)                    // Create new backup
			r.Get("/", s.listBackups)                      // List all backups
			r.Get("/{backupID}", s.getBackup)              // Get backup details
			r.Delete("/{backupID}", s.deleteBackup)        // Delete a backup
			r.Post("/{backupID}/restore", s.restoreBackup) // Restore from backup
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

	s.logger.Info("reassignment canceled", "id", reassignmentID)
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

// =============================================================================
// API KEY MANAGEMENT HANDLERS
// =============================================================================
//
// These handlers manage the lifecycle of API keys used for authentication.
// Only users with admin permissions can access these endpoints.
//
// COMPARISON TO OTHER SYSTEMS:
//   - Kafka: Uses SASL/SCRAM with kafka-configs.sh for credential management
//   - RabbitMQ: rabbitmqctl add_user / set_permissions
//   - AWS SQS: IAM CreateAccessKey API
//   - goqueue: HTTP API for key management
//
// SECURITY CONSIDERATIONS:
//   - Raw keys are only shown at creation time
//   - Only key metadata (prefix, roles) is shown when listing
//   - Revoked keys cannot be used or restored
//
// =============================================================================

// CreateAPIKeyRequest is the request body for creating an API key.
type CreateAPIKeyRequest struct {
	// Name is a human-readable name for the key
	Name string `json:"name"`

	// Roles are the RBAC roles to assign (admin, producer, consumer, readonly)
	Roles []string `json:"roles"`

	// ExpiresIn is the duration until expiration (e.g., "720h" for 30 days)
	// Empty string means never expires
	ExpiresIn string `json:"expires_in,omitempty"`

	// Metadata stores arbitrary key-value pairs
	Metadata map[string]string `json:"metadata,omitempty"`
}

// CreateAPIKeyResponse is returned when creating an API key.
// IMPORTANT: The raw Key field is only shown at creation time!
type CreateAPIKeyResponse struct {
	// ID is the unique identifier for the key
	ID string `json:"id"`

	// Key is the actual API key - ONLY SHOWN HERE, SAVE IT!
	Key string `json:"key"`

	// Prefix is the first 8 chars for identification
	Prefix string `json:"prefix"`

	// Name is the human-readable name
	Name string `json:"name"`

	// Roles assigned to this key
	Roles []string `json:"roles"`

	// ExpiresAt is when the key expires (null = never)
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// createAPIKey handles POST /admin/keys
// Creates a new API key with specified roles.
//
// REQUEST:
//
//	{
//	    "name": "order-service-prod",
//	    "roles": ["producer"],
//	    "expires_in": "720h",
//	    "metadata": {"environment": "production"}
//	}
//
// RESPONSE (200 OK):
//
//	{
//	    "id": "key_abc123...",
//	    "key": "gq_live_abc123...",    // ONLY SHOWN ONCE!
//	    "prefix": "gq_live_",
//	    "name": "order-service-prod",
//	    "roles": ["producer"],
//	    "expires_at": "2024-02-15T00:00:00Z"
//	}
func (s *Server) createAPIKey(w http.ResponseWriter, r *http.Request) {
	// ========================================================================
	// STEP 1: Validate security manager is configured
	// ========================================================================
	if s.security == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"authentication not configured")
		return
	}

	// ========================================================================
	// STEP 2: Parse request body
	// ========================================================================
	var req CreateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest,
			"invalid request body: "+err.Error())
		return
	}

	// ========================================================================
	// STEP 3: Validate request
	// ========================================================================
	if req.Name == "" {
		s.errorResponse(w, http.StatusBadRequest, "name is required")
		return
	}

	if len(req.Roles) == 0 {
		s.errorResponse(w, http.StatusBadRequest,
			"at least one role is required (admin, producer, consumer, readonly)")
		return
	}

	// Validate roles
	validRoles := map[string]bool{
		security.RoleAdmin:    true,
		security.RoleProducer: true,
		security.RoleConsumer: true,
		security.RoleReadonly: true,
	}
	for _, role := range req.Roles {
		if !validRoles[role] {
			s.errorResponse(w, http.StatusBadRequest,
				"invalid role: "+role+", valid roles: admin, producer, consumer, readonly")
			return
		}
	}

	// ========================================================================
	// STEP 4: Parse expiration duration
	// ========================================================================
	var expiry time.Duration
	if req.ExpiresIn != "" {
		duration, err := time.ParseDuration(req.ExpiresIn)
		if err != nil {
			s.errorResponse(w, http.StatusBadRequest,
				"invalid expires_in duration: "+err.Error())
			return
		}
		expiry = duration
	}

	// ========================================================================
	// STEP 5: Create the API key
	// ========================================================================
	// GenerateKey creates a secure random key, hashes it, and stores only
	// the hash. The raw key is returned ONLY in this response.
	//
	rawKey, key, err := s.security.Keys.GenerateKey(req.Name, req.Roles, expiry)
	if err != nil {
		s.logger.Error("failed to create API key", "error", err, "name", req.Name)
		s.errorResponse(w, http.StatusInternalServerError,
			"failed to create API key")
		return
	}

	s.logger.Info("created API key",
		"key_id", key.ID,
		"name", req.Name,
		"roles", req.Roles,
	)

	// ========================================================================
	// STEP 6: Return response with raw key (only time it's shown!)
	// ========================================================================
	resp := CreateAPIKeyResponse{
		ID:     key.ID,
		Key:    rawKey,
		Prefix: key.Prefix,
		Name:   key.Name,
		Roles:  key.Roles,
	}
	if !key.ExpiresAt.IsZero() {
		resp.ExpiresAt = &key.ExpiresAt
	}

	s.writeJSON(w, http.StatusCreated, resp)
}

// listAPIKeys handles GET /admin/keys
// Lists all API keys (without exposing the actual keys).
func (s *Server) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	if s.security == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"authentication not configured")
		return
	}

	keys := s.security.Keys.ListKeys()

	// Don't expose full key info - just metadata
	result := make([]map[string]interface{}, len(keys))
	for i, key := range keys {
		result[i] = map[string]interface{}{
			"id":           key.ID,
			"name":         key.Name,
			"prefix":       key.Prefix,
			"roles":        key.Roles,
			"created_at":   key.CreatedAt,
			"expires_at":   key.ExpiresAt,
			"last_used_at": key.LastUsedAt,
			"revoked":      key.Revoked,
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"keys":  result,
		"count": len(keys),
	})
}

// revokeAPIKey handles DELETE /admin/keys/{keyID}
// Revokes an API key so it can no longer be used.
func (s *Server) revokeAPIKey(w http.ResponseWriter, r *http.Request) {
	if s.security == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"authentication not configured")
		return
	}

	keyID := chi.URLParam(r, "keyID")
	if keyID == "" {
		s.errorResponse(w, http.StatusBadRequest, "key ID is required")
		return
	}

	if err := s.security.Keys.RevokeKey(keyID); err != nil {
		s.logger.Warn("failed to revoke API key", "key_id", keyID, "error", err)
		s.errorResponse(w, http.StatusNotFound, "key not found")
		return
	}

	s.logger.Info("revoked API key", "key_id", keyID)
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "key revoked",
		"key_id":  keyID,
	})
}

// =============================================================================
// ACL MANAGEMENT HANDLERS
// =============================================================================
//
// ACLs (Access Control Lists) provide fine-grained, topic-level permissions.
// While roles give broad access (producer can publish to all topics), ACLs
// can restrict access to specific topics.
//
// EXAMPLE USE CASE:
//   Producer A should only publish to "orders-*" topics
//   Consumer B should only consume from "payments" topic
//
// =============================================================================

// AddACLRequest is the request body for adding an ACL rule.
type AddACLRequest struct {
	// Principal is the API key ID that this ACL applies to
	Principal string `json:"principal"`

	// ResourceType is "topic", "group", or "cluster"
	ResourceType string `json:"resource_type"`

	// ResourceName is the resource name (supports wildcards: orders-*)
	ResourceName string `json:"resource_name"`

	// Operation is the allowed operation: read, write, create, delete, all
	Operation string `json:"operation"`

	// Effect is "allow" or "deny"
	Effect string `json:"effect"`
}

// addACL handles POST /admin/acls
// Adds a new ACL rule.
func (s *Server) addACL(w http.ResponseWriter, r *http.Request) {
	if s.security == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"authentication not configured")
		return
	}

	var req AddACLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest,
			"invalid request body: "+err.Error())
		return
	}

	// Validate resource type
	var resourceType security.ResourceType
	switch req.ResourceType {
	case "topic":
		resourceType = security.ResourceTypeTopic
	case "group":
		resourceType = security.ResourceTypeConsumerGroup
	case "cluster":
		resourceType = security.ResourceTypeCluster
	default:
		s.errorResponse(w, http.StatusBadRequest,
			"invalid resource_type: must be topic, group, or cluster")
		return
	}

	// Validate operation
	var op security.Operation
	switch req.Operation {
	case "read":
		op = security.OpRead
	case "write":
		op = security.OpWrite
	case "create":
		op = security.OpCreate
	case "delete":
		op = security.OpDelete
	case "all":
		op = security.OpAll
	default:
		s.errorResponse(w, http.StatusBadRequest,
			"invalid operation: must be read, write, create, delete, or all")
		return
	}

	// Validate effect (allow/deny)
	var allow bool
	switch req.Effect {
	case "allow":
		allow = true
	case "deny":
		allow = false
	default:
		s.errorResponse(w, http.StatusBadRequest,
			"invalid effect: must be allow or deny")
		return
	}

	// Create ACL
	acl := security.ACL{
		Principal:    req.Principal,
		ResourceType: resourceType,
		ResourceName: req.ResourceName,
		Operation:    op,
		Allow:        allow,
	}

	s.security.ACLs.AddACL(acl)

	s.logger.Info("added ACL",
		"principal", req.Principal,
		"resource_type", req.ResourceType,
		"resource_name", req.ResourceName,
		"operation", req.Operation,
		"effect", req.Effect,
	)

	s.writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success": true,
		"message": "ACL added",
		"acl":     acl,
	})
}

// listACLs handles GET /admin/acls
// Lists all ACL rules.
func (s *Server) listACLs(w http.ResponseWriter, r *http.Request) {
	if s.security == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"authentication not configured")
		return
	}

	acls := s.security.ACLs.ListAllACLs()

	// Add index as "id" for each ACL for removal purposes
	result := make([]map[string]interface{}, len(acls))
	for i, acl := range acls {
		effect := "deny"
		if acl.Allow {
			effect = "allow"
		}
		result[i] = map[string]interface{}{
			"id":            i, // Use index as ID
			"principal":     acl.Principal,
			"resource_type": acl.ResourceType,
			"resource_name": acl.ResourceName,
			"operation":     acl.Operation,
			"effect":        effect,
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"acls":  result,
		"count": len(acls),
	})
}

// removeACL handles DELETE /admin/acls/{aclID}
// Removes an ACL rule by index (the "id" returned from listACLs).
func (s *Server) removeACL(w http.ResponseWriter, r *http.Request) {
	if s.security == nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			"authentication not configured")
		return
	}

	aclIDStr := chi.URLParam(r, "aclID")
	if aclIDStr == "" {
		s.errorResponse(w, http.StatusBadRequest, "ACL ID is required")
		return
	}

	aclID, err := strconv.Atoi(aclIDStr)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "ACL ID must be a number")
		return
	}

	if err := s.security.ACLs.RemoveACLByIndex(aclID); err != nil {
		s.logger.Warn("failed to remove ACL", "acl_id", aclID, "error", err)
		s.errorResponse(w, http.StatusNotFound, "ACL not found")
		return
	}

	s.logger.Info("removed ACL", "acl_id", aclID)
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "ACL removed",
		"acl_id":  aclID,
	})
}

// =============================================================================
// BACKUP & RESTORE ENDPOINTS
// =============================================================================
//
// KUBERNETES-FIRST BACKUP STRATEGY:
//   1. VolumeSnapshot (CSI) - Fast point-in-time snapshots of PVC data
//   2. Metadata Backup (these endpoints) - Topics, offsets, schemas
//
// WHY BOTH?
//   - VolumeSnapshot handles actual message data (fast, storage-level)
//   - Metadata backup is portable (can restore to different cluster)
//
// =============================================================================

// CreateBackupRequest is the request body for creating a backup.
type CreateBackupRequest struct {
	IncludeTopics  bool `json:"include_topics"`
	IncludeOffsets bool `json:"include_offsets"`
	IncludeSchemas bool `json:"include_schemas"`
	IncludeConfig  bool `json:"include_config"`
}

// createBackup handles POST /admin/backup
// Creates a new backup of broker metadata.
func (s *Server) createBackup(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var req CreateBackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Default to full backup if no body
		req = CreateBackupRequest{
			IncludeTopics:  true,
			IncludeOffsets: true,
			IncludeSchemas: true,
			IncludeConfig:  true,
		}
	}

	// Create backup manager
	config := broker.BackupConfig{
		IncludeTopics:   req.IncludeTopics,
		IncludeOffsets:  req.IncludeOffsets,
		IncludeSchemas:  req.IncludeSchemas,
		IncludeConfig:   req.IncludeConfig,
		RetentionCount:  7,
		CompressBackups: false,
	}
	bm := broker.NewBackupManager(s.broker, config)

	// Create backup
	metadata, err := bm.CreateBackup(r.Context())
	if err != nil {
		s.logger.Error("failed to create backup", "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to create backup: "+err.Error())
		return
	}

	// Return backup info
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"backup_id":  metadata.Timestamp.UTC().Format("2006-01-02-150405"),
		"timestamp":  metadata.Timestamp,
		"broker_id":  metadata.BrokerID,
		"cluster_id": metadata.ClusterID,
		"stats":      metadata.Stats,
	})
}

// listBackups handles GET /admin/backup
// Lists all available backups.
func (s *Server) listBackups(w http.ResponseWriter, r *http.Request) {
	// Create backup manager
	config := broker.DefaultBackupConfig()
	bm := broker.NewBackupManager(s.broker, config)

	// List backups
	backups, err := bm.ListBackups()
	if err != nil {
		s.logger.Error("failed to list backups", "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to list backups: "+err.Error())
		return
	}

	// Transform to response format
	result := make([]map[string]interface{}, len(backups))
	for i, b := range backups {
		result[i] = map[string]interface{}{
			"backup_id":       b.Timestamp.UTC().Format("2006-01-02-150405"),
			"timestamp":       b.Timestamp,
			"broker_id":       b.BrokerID,
			"topics":          b.Stats.TopicCount,
			"consumer_groups": b.Stats.ConsumerGroupCount,
			"schemas":         b.Stats.SchemaCount,
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"backups": result,
	})
}

// getBackup handles GET /admin/backup/{backupID}
// Returns details for a specific backup.
func (s *Server) getBackup(w http.ResponseWriter, r *http.Request) {
	backupID := chi.URLParam(r, "backupID")
	if backupID == "" {
		s.errorResponse(w, http.StatusBadRequest, "backup ID is required")
		return
	}

	// Create backup manager
	config := broker.DefaultBackupConfig()
	bm := broker.NewBackupManager(s.broker, config)

	// Get backup
	metadata, err := bm.GetBackup(backupID)
	if err != nil {
		s.logger.Warn("backup not found", "backup_id", backupID, "error", err)
		s.errorResponse(w, http.StatusNotFound, "backup not found")
		return
	}

	// Transform topics for response
	topics := make([]map[string]interface{}, len(metadata.Topics))
	for i, t := range metadata.Topics {
		topics[i] = map[string]interface{}{
			"name":           t.Name,
			"num_partitions": t.NumPartitions,
		}
	}

	// Transform consumer groups for response
	groups := make([]map[string]interface{}, len(metadata.ConsumerGroups))
	for i, g := range metadata.ConsumerGroups {
		groups[i] = map[string]interface{}{
			"group_id": g.GroupID,
			"state":    g.State,
		}
	}

	// Transform schemas for response
	schemas := make([]map[string]interface{}, len(metadata.Schemas))
	for i, s := range metadata.Schemas {
		schemas[i] = map[string]interface{}{
			"subject": s.Subject,
			"version": s.Version,
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"backup_id":       backupID,
		"version":         metadata.Version,
		"timestamp":       metadata.Timestamp,
		"broker_id":       metadata.BrokerID,
		"cluster_id":      metadata.ClusterID,
		"topics":          topics,
		"consumer_groups": groups,
		"schemas":         schemas,
		"stats":           metadata.Stats,
	})
}

// deleteBackup handles DELETE /admin/backup/{backupID}
// Deletes a backup.
func (s *Server) deleteBackup(w http.ResponseWriter, r *http.Request) {
	backupID := chi.URLParam(r, "backupID")
	if backupID == "" {
		s.errorResponse(w, http.StatusBadRequest, "backup ID is required")
		return
	}

	// Create backup manager
	config := broker.DefaultBackupConfig()
	bm := broker.NewBackupManager(s.broker, config)

	// Delete backup
	if err := bm.DeleteBackup(backupID); err != nil {
		s.logger.Warn("failed to delete backup", "backup_id", backupID, "error", err)
		s.errorResponse(w, http.StatusNotFound, "backup not found or delete failed")
		return
	}

	s.logger.Info("deleted backup", "backup_id", backupID)
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"message":   "backup deleted",
		"backup_id": backupID,
	})
}

// RestoreBackupRequest is the request body for restoring a backup.
type RestoreBackupRequest struct {
	SkipExisting bool `json:"skip_existing"`
}

// restoreBackup handles POST /admin/backup/{backupID}/restore
// Restores broker state from a backup.
func (s *Server) restoreBackup(w http.ResponseWriter, r *http.Request) {
	backupID := chi.URLParam(r, "backupID")
	if backupID == "" {
		s.errorResponse(w, http.StatusBadRequest, "backup ID is required")
		return
	}

	// Parse request
	var req RestoreBackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Default to not skipping existing
		req = RestoreBackupRequest{SkipExisting: false}
	}

	// Create backup manager
	config := broker.DefaultBackupConfig()
	bm := broker.NewBackupManager(s.broker, config)

	// Get backup path
	backupPath := bm.GetBackupPath(backupID)

	// Restore from backup
	if err := bm.RestoreFromBackup(r.Context(), backupPath, req.SkipExisting); err != nil {
		s.logger.Error("failed to restore backup", "backup_id", backupID, "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to restore: "+err.Error())
		return
	}

	s.logger.Info("restored from backup", "backup_id", backupID)
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"message":   "restore completed",
		"backup_id": backupID,
	})
}
