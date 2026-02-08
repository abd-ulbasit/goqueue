package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"goqueue/internal/broker"
)

// =============================================================================
// SCHEMA REGISTRY HANDLERS (M8)
// =============================================================================
//
// WHAT IS A SCHEMA REGISTRY?
// The schema registry provides centralized schema management for message
// validation and evolution. It ensures producers send well-formed data
// and manages safe schema changes over time.
//
// API DESIGN:
// Our API follows the Confluent Schema Registry API pattern for familiarity,
// adapted for our JSON Schema focus.
//
// KEY CONCEPTS:
//   - Subject: Schema identifier (we use topic name - TopicNameStrategy)
//   - Version: Sequential version number within a subject
//   - Schema ID: Global unique ID across all subjects
//   - Compatibility: Rules for safe schema evolution
//
// =============================================================================

// registerSchema handles POST /schemas/subjects/{subject}/versions
//
// REGISTERS a new schema version for a subject.
// Validates JSON Schema syntax, checks compatibility with previous versions,
// and assigns a global ID.
//
// REQUEST BODY:
//
//	{
//	  "schema": "{\"type\":\"object\",\"properties\":{...}}"
//	}
//
// RESPONSE:
//
//	{
//	  "id": 1,
//	  "version": 1
//	}
func (s *Server) registerSchema(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	if subject == "" {
		s.errorResponse(w, http.StatusBadRequest, "subject is required")
		return
	}

	var req struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.Schema == "" {
		s.errorResponse(w, http.StatusBadRequest, "schema is required")
		return
	}

	schema, err := s.broker.SchemaRegistry().RegisterSchema(subject, req.Schema)
	if err != nil {
		if strings.Contains(err.Error(), "incompatible") {
			s.errorResponse(w, http.StatusConflict, "schema incompatible: "+err.Error())
			return
		}
		if strings.Contains(err.Error(), "invalid") {
			s.errorResponse(w, http.StatusUnprocessableEntity, "invalid schema: "+err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, "failed to register schema: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":      schema.ID,
		"version": schema.Version,
	})
}

// listSchemaVersions handles GET /schemas/subjects/{subject}/versions
//
// LISTS all version numbers for a subject.
//
// RESPONSE:
//
//	[1, 2, 3]
func (s *Server) listSchemaVersions(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	if subject == "" {
		s.errorResponse(w, http.StatusBadRequest, "subject is required")
		return
	}

	versions, err := s.broker.SchemaRegistry().ListVersions(subject)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "subject not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, versions)
}

// getSchemaVersion handles GET /schemas/subjects/{subject}/versions/{version}
//
// GETS a specific schema version.
//
// RESPONSE:
//
//	{
//	  "subject": "orders",
//	  "version": 1,
//	  "id": 1,
//	  "schema": "{...}",
//	  "schemaType": "JSON"
//	}
func (s *Server) getSchemaVersion(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	versionStr := chi.URLParam(r, "version")

	version, err := strconv.Atoi(versionStr)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid version number")
		return
	}

	schema, err := s.broker.SchemaRegistry().GetSchemaBySubjectVersion(subject, version)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"subject":    schema.Subject,
		"version":    schema.Version,
		"id":         schema.ID,
		"schema":     schema.Schema,
		"schemaType": schema.SchemaType,
	})
}

// getLatestSchema handles GET /schemas/subjects/{subject}/versions/latest
//
// GETS the latest schema version for a subject.
func (s *Server) getLatestSchema(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	if subject == "" {
		s.errorResponse(w, http.StatusBadRequest, "subject is required")
		return
	}

	schema, err := s.broker.SchemaRegistry().GetLatestSchema(subject)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "subject not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"subject":    schema.Subject,
		"version":    schema.Version,
		"id":         schema.ID,
		"schema":     schema.Schema,
		"schemaType": schema.SchemaType,
	})
}

// deleteSchemaVersion handles DELETE /schemas/subjects/{subject}/versions/{version}
//
// DELETES a specific schema version (soft delete).
func (s *Server) deleteSchemaVersion(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	versionStr := chi.URLParam(r, "version")

	version, err := strconv.Atoi(versionStr)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid version number")
		return
	}

	if err := s.broker.SchemaRegistry().DeleteSchemaVersion(subject, version); err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"deleted": version,
	})
}

// deleteSchemaSubject handles DELETE /schemas/subjects/{subject}
//
// DELETES a subject and all its versions.
func (s *Server) deleteSchemaSubject(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	if subject == "" {
		s.errorResponse(w, http.StatusBadRequest, "subject is required")
		return
	}

	versions, err := s.broker.SchemaRegistry().DeleteSubject(subject)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "subject not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, versions)
}

// listSchemaSubjects handles GET /schemas/subjects
//
// LISTS all registered subjects.
func (s *Server) listSchemaSubjects(w http.ResponseWriter, r *http.Request) {
	subjects := s.broker.SchemaRegistry().ListSubjects()
	s.writeJSON(w, http.StatusOK, subjects)
}

// checkSchemaExists handles POST /schemas/subjects/{subject}
//
// CHECKS if a schema is already registered under the subject.
// Returns the schema if found, 404 if not.
func (s *Server) checkSchemaExists(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	if subject == "" {
		s.errorResponse(w, http.StatusBadRequest, "subject is required")
		return
	}

	var req struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	schema, err := s.broker.SchemaRegistry().CheckSchemaExists(subject, req.Schema)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	if schema == nil {
		s.errorResponse(w, http.StatusNotFound, "schema not found")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"subject": schema.Subject,
		"version": schema.Version,
		"id":      schema.ID,
	})
}

// getSchemaByID handles GET /schemas/ids/{id}
//
// GETS a schema by its global ID.
func (s *Server) getSchemaByID(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid schema ID")
		return
	}

	schema, err := s.broker.SchemaRegistry().GetSchemaByID(id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, "schema not found")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"schema":     schema.Schema,
		"schemaType": schema.SchemaType,
		"subject":    schema.Subject,
		"version":    schema.Version,
		"id":         schema.ID,
	})
}

// testSchemaCompatibility handles POST /schemas/compatibility/subjects/{subject}/versions/{version}
//
// TESTS if a schema is compatible with a specific version (or latest if version=-1).
func (s *Server) testSchemaCompatibility(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	versionStr := chi.URLParam(r, "version")

	// Handle "latest" as -1
	var version int
	if versionStr == "latest" {
		version = -1
	} else {
		var err error
		version, err = strconv.Atoi(versionStr)
		if err != nil {
			s.errorResponse(w, http.StatusBadRequest, "invalid version number")
			return
		}
	}

	var req struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	isCompatible, err := s.broker.SchemaRegistry().TestCompatibility(subject, req.Schema, version)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "invalid") {
			s.errorResponse(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"is_compatible": isCompatible,
	})
}

// getGlobalSchemaConfig handles GET /schemas/config
//
// GETS the global compatibility mode.
func (s *Server) getGlobalSchemaConfig(w http.ResponseWriter, r *http.Request) {
	compat := s.broker.SchemaRegistry().GetGlobalCompatibility()
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"compatibility": string(compat),
	})
}

// setGlobalSchemaConfig handles PUT /schemas/config
//
// SETS the global compatibility mode.
func (s *Server) setGlobalSchemaConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Compatibility string `json:"compatibility"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	mode := broker.ParseCompatibilityMode(req.Compatibility)
	if err := s.broker.SchemaRegistry().SetGlobalCompatibility(mode); err != nil {
		s.errorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"compatibility": string(mode),
	})
}

// getSubjectSchemaConfig handles GET /schemas/config/{subject}
//
// GETS the compatibility mode for a specific subject.
func (s *Server) getSubjectSchemaConfig(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")
	compat := s.broker.SchemaRegistry().GetCompatibilityMode(subject)
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"compatibility": string(compat),
	})
}

// setSubjectSchemaConfig handles PUT /schemas/config/{subject}
//
// SETS the compatibility mode for a specific subject.
func (s *Server) setSubjectSchemaConfig(w http.ResponseWriter, r *http.Request) {
	subject := chi.URLParam(r, "subject")

	var req struct {
		Compatibility string `json:"compatibility"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	mode := broker.ParseCompatibilityMode(req.Compatibility)
	if err := s.broker.SchemaRegistry().SetCompatibilityMode(subject, mode); err != nil {
		s.errorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"compatibility": string(mode),
	})
}

// handleSchemaStats handles GET /schemas/stats
//
// RETURNS schema registry statistics.
func (s *Server) handleSchemaStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.SchemaStats()
	s.writeJSON(w, http.StatusOK, stats)
}
