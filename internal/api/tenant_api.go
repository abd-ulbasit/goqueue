// =============================================================================
// TENANT MANAGEMENT API
// =============================================================================
//
// WHAT IS THIS?
// HTTP REST API for managing tenants and their quotas. This is the admin
// interface for multi-tenancy, allowing operators to:
//   - Create and manage tenants
//   - Configure per-tenant quotas
//   - View tenant usage and statistics
//   - Suspend/disable/enable tenants
//
// ENDPOINT OVERVIEW:
//
//   TENANT MANAGEMENT
//   POST   /admin/tenants                        Create new tenant
//   GET    /admin/tenants                        List all tenants
//   GET    /admin/tenants/{tenantID}             Get tenant details
//   PATCH  /admin/tenants/{tenantID}             Update tenant
//   DELETE /admin/tenants/{tenantID}             Delete tenant
//
//   TENANT LIFECYCLE
//   POST   /admin/tenants/{tenantID}/suspend     Suspend tenant
//   POST   /admin/tenants/{tenantID}/activate    Activate tenant
//   POST   /admin/tenants/{tenantID}/disable     Disable tenant
//
//   QUOTA MANAGEMENT
//   GET    /admin/tenants/{tenantID}/quotas      Get tenant quotas
//   PUT    /admin/tenants/{tenantID}/quotas      Update tenant quotas
//
//   USAGE & STATS
//   GET    /admin/tenants/{tenantID}/usage       Get tenant usage
//   GET    /admin/tenants/{tenantID}/stats       Get tenant statistics
//   POST   /admin/tenants/{tenantID}/usage/reset Reset usage counters
//
//   TENANT-SCOPED OPERATIONS
//   GET    /admin/tenants/{tenantID}/topics      List tenant's topics
//   GET    /admin/tenants/{tenantID}/groups      List tenant's consumer groups
//
// AUTHENTICATION:
//   - All /admin/* endpoints should be protected by admin authentication
//   - This implementation assumes authentication is handled by middleware
//   - In production: Add API key, JWT, or mTLS authentication
//
// COMPARISON:
//   - Kafka: Admin client API + ACLs for quota management
//   - RabbitMQ: HTTP API at /api/vhosts for virtual host management
//   - AWS: IAM for account-level quotas and permissions
//
// =============================================================================

package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"goqueue/internal/broker"

	"github.com/go-chi/chi/v5"
)

// =============================================================================
// ROUTE REGISTRATION
// =============================================================================

// RegisterTenantRoutes registers all tenant management API endpoints.
// Call this from Server.registerRoutes() to add tenant management.
func (s *Server) RegisterTenantRoutes(r chi.Router) {
	r.Route("/admin/tenants", func(r chi.Router) {
		// Tenant CRUD
		r.Post("/", s.createTenant)
		r.Get("/", s.listTenants)

		r.Route("/{tenantID}", func(r chi.Router) {
			r.Get("/", s.getTenant)
			r.Patch("/", s.updateTenant)
			r.Delete("/", s.deleteTenant)

			// Lifecycle
			r.Post("/suspend", s.suspendTenant)
			r.Post("/activate", s.activateTenant)
			r.Post("/disable", s.disableTenant)

			// Quotas
			r.Get("/quotas", s.getTenantQuotas)
			r.Put("/quotas", s.updateTenantQuotas)

			// Usage & Stats
			r.Get("/usage", s.getTenantUsage)
			r.Get("/stats", s.getTenantStats)

			// Tenant-scoped resources
			r.Get("/topics", s.listTenantTopics)
		})
	})
}

// =============================================================================
// REQUEST/RESPONSE TYPES
// =============================================================================

// CreateTenantRequest is the request body for creating a tenant.
type CreateTenantRequest struct {
	ID          string            `json:"id"`                    // Required: unique tenant identifier
	Name        string            `json:"name"`                  // Required: human-readable name
	Description string            `json:"description,omitempty"` // Optional: description
	Quotas      *TenantQuotasDTO  `json:"quotas,omitempty"`      // Optional: custom quotas
	Metadata    map[string]string `json:"metadata,omitempty"`    // Optional: custom metadata
}

// TenantQuotasDTO is the data transfer object for tenant quotas.
// All fields are optional - unset fields keep defaults or current values.
type TenantQuotasDTO struct {
	// Rate limits (per second)
	PublishRateLimit      *int64 `json:"publish_rate_limit,omitempty"`       // Max messages/sec to publish
	ConsumeRateLimit      *int64 `json:"consume_rate_limit,omitempty"`       // Max messages/sec to consume
	PublishBytesRateLimit *int64 `json:"publish_bytes_rate_limit,omitempty"` // Max bytes/sec to publish
	ConsumeBytesRateLimit *int64 `json:"consume_bytes_rate_limit,omitempty"` // Max bytes/sec to consume

	// Storage limits
	MaxStorageBytes *int64 `json:"max_storage_bytes,omitempty"` // Max total storage

	// Count limits
	MaxTopics          *int `json:"max_topics,omitempty"`           // Max topics
	MaxTotalPartitions *int `json:"max_total_partitions,omitempty"` // Max total partitions

	// Size limits
	MaxMessageSizeBytes *int `json:"max_message_size_bytes,omitempty"` // Max single message size

	// Connection limits
	MaxConnections        *int `json:"max_connections,omitempty"`          // Max concurrent connections
	MaxConsumerGroups     *int `json:"max_consumer_groups,omitempty"`      // Max consumer groups
	MaxConsumersPerGroup  *int `json:"max_consumers_per_group,omitempty"`  // Max consumers per group
	MaxRetentionHours     *int `json:"max_retention_hours,omitempty"`      // Max retention period in hours
	MaxDelayHours         *int `json:"max_delay_hours,omitempty"`          // Max delay for delayed messages in hours
	MaxPartitionsPerTopic *int `json:"max_partitions_per_topic,omitempty"` // Max partitions per topic
}

// TenantResponse is the response for tenant operations.
type TenantResponse struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Status        string            `json:"status"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	SuspendedAt   *time.Time        `json:"suspended_at,omitempty"`
	SuspendReason string            `json:"suspend_reason,omitempty"`
	Quotas        TenantQuotasDTO   `json:"quotas"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// TenantUsageResponse is the response for tenant usage.
type TenantUsageResponse struct {
	TenantID               string    `json:"tenant_id"`
	TotalMessagesPublished int64     `json:"total_messages_published"`
	TotalBytesPublished    int64     `json:"total_bytes_published"`
	TotalMessagesConsumed  int64     `json:"total_messages_consumed"`
	TotalBytesConsumed     int64     `json:"total_bytes_consumed"`
	StorageBytes           int64     `json:"storage_bytes"`
	TopicCount             int       `json:"topic_count"`
	PartitionCount         int       `json:"partition_count"`
	ConnectionCount        int       `json:"connection_count"`
	LastUpdated            time.Time `json:"last_updated"`
}

// TenantStatsResponse is the response for tenant statistics.
type TenantStatsResponse struct {
	TenantID               string  `json:"tenant_id"`
	StorageUsagePercent    float64 `json:"storage_usage_percent"`
	TopicUsagePercent      float64 `json:"topic_usage_percent"`
	PartitionUsagePercent  float64 `json:"partition_usage_percent"`
	ConnectionUsagePercent float64 `json:"connection_usage_percent"`
}

// UpdateTenantRequest is the request body for updating a tenant.
type UpdateTenantRequest struct {
	Name        *string           `json:"name,omitempty"`
	Description *string           `json:"description,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// SuspendTenantRequest is the request body for suspending a tenant.
type SuspendTenantRequest struct {
	Reason string `json:"reason"` // Required: reason for suspension
}

// =============================================================================
// TENANT CRUD HANDLERS
// =============================================================================

// createTenant handles POST /admin/tenants
func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	var req CreateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Validate required fields
	if req.ID == "" {
		s.errorResponse(w, http.StatusBadRequest, "id is required")
		return
	}
	if req.Name == "" {
		s.errorResponse(w, http.StatusBadRequest, "name is required")
		return
	}

	// Convert DTO to broker quotas
	var quotas *broker.TenantQuotas
	if req.Quotas != nil {
		q := dtoToQuotas(req.Quotas)
		quotas = &q
	}

	// Create tenant
	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	tenant, err := tm.CreateTenant(req.ID, req.Name, quotas, req.Metadata)
	if err != nil {
		if errors.Is(err, broker.ErrTenantExists) {
			s.errorResponse(w, http.StatusConflict, "tenant already exists: "+req.ID)
			return
		}
		if errors.Is(err, broker.ErrInvalidTenantID) {
			s.errorResponse(w, http.StatusBadRequest, "invalid tenant ID: "+err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to create tenant: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusCreated, tenantToResponse(tenant))
}

// listTenants handles GET /admin/tenants
func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	// Parse query parameters for filtering
	status := r.URL.Query().Get("status")

	tenants := tm.ListTenants()
	response := make([]*TenantResponse, 0, len(tenants))

	for _, tenant := range tenants {
		// Filter by status if specified
		if status != "" && string(tenant.Status) != status {
			continue
		}
		response = append(response, tenantToResponse(tenant))
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"tenants": response,
		"count":   len(response),
	})
}

// getTenant handles GET /admin/tenants/{tenantID}
func (s *Server) getTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	tenant, err := tm.GetTenant(tenantID)
	if err != nil {
		if errors.Is(err, broker.ErrTenantNotFound) {
			s.errorResponse(w, http.StatusNotFound, "tenant not found: "+tenantID)
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to get tenant: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, tenantToResponse(tenant))
}

// updateTenant handles PATCH /admin/tenants/{tenantID}
func (s *Server) updateTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	var req UpdateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	tenant, err := tm.UpdateTenant(tenantID, req.Name, req.Metadata)
	if err != nil {
		if errors.Is(err, broker.ErrTenantNotFound) {
			s.errorResponse(w, http.StatusNotFound, "tenant not found: "+tenantID)
			return
		}
		if errors.Is(err, broker.ErrSystemTenant) {
			s.errorResponse(w, http.StatusForbidden, "cannot modify system tenant")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to update tenant: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, tenantToResponse(tenant))
}

// deleteTenant handles DELETE /admin/tenants/{tenantID}
func (s *Server) deleteTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	// Check for force parameter
	force := r.URL.Query().Get("force") == "true"

	// If not force, check if tenant has resources
	if !force {
		topics, _ := s.broker.ListTopicsForTenant(tenantID)
		if len(topics) > 0 {
			s.errorResponse(w, http.StatusConflict,
				"tenant has "+strconv.Itoa(len(topics))+" topics, use ?force=true to delete anyway")
			return
		}
	}

	err := tm.DeleteTenant(tenantID)
	if err != nil {
		if errors.Is(err, broker.ErrTenantNotFound) {
			s.errorResponse(w, http.StatusNotFound, "tenant not found: "+tenantID)
			return
		}
		if errors.Is(err, broker.ErrSystemTenant) {
			s.errorResponse(w, http.StatusForbidden, "cannot delete system tenant")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to delete tenant: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{
		"status":    "deleted",
		"tenant_id": tenantID,
	})
}

// =============================================================================
// TENANT LIFECYCLE HANDLERS
// =============================================================================

// suspendTenant handles POST /admin/tenants/{tenantID}/suspend
func (s *Server) suspendTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	var req SuspendTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Reason == "" {
		s.errorResponse(w, http.StatusBadRequest, "reason is required")
		return
	}

	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	tenant, err := tm.SetTenantStatus(tenantID, broker.TenantStatusSuspended, req.Reason)
	if err != nil {
		if errors.Is(err, broker.ErrTenantNotFound) {
			s.errorResponse(w, http.StatusNotFound, "tenant not found: "+tenantID)
			return
		}
		if errors.Is(err, broker.ErrSystemTenant) {
			s.errorResponse(w, http.StatusForbidden, "cannot suspend system tenant")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to suspend tenant: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, tenantToResponse(tenant))
}

// activateTenant handles POST /admin/tenants/{tenantID}/activate
func (s *Server) activateTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	tenant, err := tm.SetTenantStatus(tenantID, broker.TenantStatusActive, "")
	if err != nil {
		if errors.Is(err, broker.ErrTenantNotFound) {
			s.errorResponse(w, http.StatusNotFound, "tenant not found: "+tenantID)
			return
		}
		if errors.Is(err, broker.ErrSystemTenant) {
			s.errorResponse(w, http.StatusForbidden, "cannot modify system tenant")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to activate tenant: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, tenantToResponse(tenant))
}

// disableTenant handles POST /admin/tenants/{tenantID}/disable
func (s *Server) disableTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	tenant, err := tm.SetTenantStatus(tenantID, broker.TenantStatusDisabled, "disabled by admin")
	if err != nil {
		if errors.Is(err, broker.ErrTenantNotFound) {
			s.errorResponse(w, http.StatusNotFound, "tenant not found: "+tenantID)
			return
		}
		if errors.Is(err, broker.ErrSystemTenant) {
			s.errorResponse(w, http.StatusForbidden, "cannot disable system tenant")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to disable tenant: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, tenantToResponse(tenant))
}

// =============================================================================
// QUOTA HANDLERS
// =============================================================================

// getTenantQuotas handles GET /admin/tenants/{tenantID}/quotas
func (s *Server) getTenantQuotas(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	tenant, err := tm.GetTenant(tenantID)
	if err != nil {
		if errors.Is(err, broker.ErrTenantNotFound) {
			s.errorResponse(w, http.StatusNotFound, "tenant not found: "+tenantID)
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to get tenant: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, quotasToDTO(tenant.Quotas))
}

// updateTenantQuotas handles PUT /admin/tenants/{tenantID}/quotas
func (s *Server) updateTenantQuotas(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	var dto TenantQuotasDTO
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	quotas := dtoToQuotas(&dto)
	tenant, err := tm.UpdateTenantQuotas(tenantID, quotas)
	if err != nil {
		if errors.Is(err, broker.ErrTenantNotFound) {
			s.errorResponse(w, http.StatusNotFound, "tenant not found: "+tenantID)
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to update quotas: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "updated",
		"tenant_id": tenantID,
		"quotas":    quotasToDTO(tenant.Quotas),
	})
}

// =============================================================================
// USAGE & STATS HANDLERS
// =============================================================================

// getTenantUsage handles GET /admin/tenants/{tenantID}/usage
func (s *Server) getTenantUsage(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	usage, err := tm.GetUsage(tenantID)
	if err != nil {
		if errors.Is(err, broker.ErrTenantNotFound) {
			s.errorResponse(w, http.StatusNotFound, "tenant not found: "+tenantID)
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to get usage: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, TenantUsageResponse{
		TenantID:               tenantID,
		TotalMessagesPublished: usage.TotalMessagesPublished,
		TotalBytesPublished:    usage.TotalBytesPublished,
		TotalMessagesConsumed:  usage.TotalMessagesConsumed,
		TotalBytesConsumed:     usage.TotalBytesConsumed,
		StorageBytes:           usage.StorageBytes,
		TopicCount:             usage.TopicCount,
		PartitionCount:         usage.PartitionCount,
		ConnectionCount:        usage.ConnectionCount,
		LastUpdated:            usage.LastUpdated,
	})
}

// getTenantStats handles GET /admin/tenants/{tenantID}/stats
func (s *Server) getTenantStats(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	tm := s.broker.TenantManager()
	if tm == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "multi-tenancy not enabled")
		return
	}

	stats, err := tm.GetTenantStats(tenantID)
	if err != nil {
		if errors.Is(err, broker.ErrTenantNotFound) {
			s.errorResponse(w, http.StatusNotFound, "tenant not found: "+tenantID)
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to get stats: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, TenantStatsResponse{
		TenantID:               tenantID,
		StorageUsagePercent:    stats.StorageUsagePercent,
		TopicUsagePercent:      stats.TopicUsagePercent,
		PartitionUsagePercent:  stats.PartitionUsagePercent,
		ConnectionUsagePercent: stats.ConnectionUsagePercent,
	})
}

// listTenantTopics handles GET /admin/tenants/{tenantID}/topics
func (s *Server) listTenantTopics(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	topics, err := s.broker.ListTopicsForTenant(tenantID)
	if err != nil {
		if errors.Is(err, broker.ErrTenantNotFound) {
			s.errorResponse(w, http.StatusNotFound, "tenant not found: "+tenantID)
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to list topics: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"tenant_id": tenantID,
		"topics":    topics,
		"count":     len(topics),
	})
}

// =============================================================================
// CONVERSION HELPERS
// =============================================================================

// tenantToResponse converts a broker.Tenant to TenantResponse.
func tenantToResponse(t *broker.Tenant) *TenantResponse {
	return &TenantResponse{
		ID:            t.ID,
		Name:          t.Name,
		Status:        string(t.Status),
		CreatedAt:     t.CreatedAt,
		UpdatedAt:     t.UpdatedAt,
		SuspendedAt:   t.SuspendedAt,
		SuspendReason: t.SuspendReason,
		Quotas:        quotasToDTO(t.Quotas),
		Metadata:      t.Metadata,
	}
}

// quotasToDTO converts broker.TenantQuotas to TenantQuotasDTO.
func quotasToDTO(q broker.TenantQuotas) TenantQuotasDTO {
	return TenantQuotasDTO{
		PublishRateLimit:      &q.PublishRateLimit,
		ConsumeRateLimit:      &q.ConsumeRateLimit,
		PublishBytesRateLimit: &q.PublishBytesRateLimit,
		ConsumeBytesRateLimit: &q.ConsumeBytesRateLimit,
		MaxStorageBytes:       &q.MaxStorageBytes,
		MaxTopics:             &q.MaxTopics,
		MaxTotalPartitions:    &q.MaxTotalPartitions,
		MaxMessageSizeBytes:   &q.MaxMessageSizeBytes,
		MaxConnections:        &q.MaxConnections,
		MaxConsumerGroups:     &q.MaxConsumerGroups,
		MaxConsumersPerGroup:  &q.MaxConsumersPerGroup,
		MaxRetentionHours:     &q.MaxRetentionHours,
		MaxDelayHours:         &q.MaxDelayHours,
		MaxPartitionsPerTopic: &q.MaxPartitionsPerTopic,
	}
}

// dtoToQuotas converts TenantQuotasDTO to broker.TenantQuotas.
// Uses defaults for unset fields.
func dtoToQuotas(dto *TenantQuotasDTO) broker.TenantQuotas {
	q := broker.DefaultTenantQuotas()

	if dto.PublishRateLimit != nil {
		q.PublishRateLimit = *dto.PublishRateLimit
	}
	if dto.ConsumeRateLimit != nil {
		q.ConsumeRateLimit = *dto.ConsumeRateLimit
	}
	if dto.PublishBytesRateLimit != nil {
		q.PublishBytesRateLimit = *dto.PublishBytesRateLimit
	}
	if dto.ConsumeBytesRateLimit != nil {
		q.ConsumeBytesRateLimit = *dto.ConsumeBytesRateLimit
	}
	if dto.MaxStorageBytes != nil {
		q.MaxStorageBytes = *dto.MaxStorageBytes
	}
	if dto.MaxTopics != nil {
		q.MaxTopics = *dto.MaxTopics
	}
	if dto.MaxTotalPartitions != nil {
		q.MaxTotalPartitions = *dto.MaxTotalPartitions
	}
	if dto.MaxMessageSizeBytes != nil {
		q.MaxMessageSizeBytes = *dto.MaxMessageSizeBytes
	}
	if dto.MaxConnections != nil {
		q.MaxConnections = *dto.MaxConnections
	}
	if dto.MaxConsumerGroups != nil {
		q.MaxConsumerGroups = *dto.MaxConsumerGroups
	}
	if dto.MaxConsumersPerGroup != nil {
		q.MaxConsumersPerGroup = *dto.MaxConsumersPerGroup
	}
	if dto.MaxRetentionHours != nil {
		q.MaxRetentionHours = *dto.MaxRetentionHours
	}
	if dto.MaxDelayHours != nil {
		q.MaxDelayHours = *dto.MaxDelayHours
	}
	if dto.MaxPartitionsPerTopic != nil {
		q.MaxPartitionsPerTopic = *dto.MaxPartitionsPerTopic
	}

	return q
}
