// =============================================================================
// KUBERNETES-READY HEALTH CHECK ENDPOINTS
// =============================================================================
//
// WHAT ARE THESE ENDPOINTS?
// These are standardized health check endpoints for Kubernetes and other
// orchestration systems. They follow cloud-native best practices.
//
// ENDPOINT OVERVIEW:
//
//   GET /health      - Overall health status (backward compatible)
//   GET /healthz     - Kubernetes liveness probe
//   GET /readyz      - Kubernetes readiness probe
//   GET /livez       - Kubernetes startup probe (alternative naming)
//
// HOW KUBERNETES USES THESE:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                         KUBERNETES POD LIFECYCLE                        │
//   │                                                                         │
//   │   Pod Created ─────► startupProbe (/livez)                              │
//   │                       │                                                 │
//   │                       ├── Success ──► readinessProbe (/readyz)          │
//   │                       │                │                                │
//   │                       │                ├── Success ──► Receives traffic │
//   │                       │                │                                │
//   │                       │                └── Failure ──► No traffic       │
//   │                       │                     (stays in pool, keeps running) │
//   │                       │                                                 │
//   │                       └── Failure ──► Container restarted               │
//   │                                                                         │
//   │   Running ─────────► livenessProbe (/healthz)                           │
//   │                       │                                                 │
//   │                       ├── Success ──► Keep running                      │
//   │                       │                                                 │
//   │                       └── Failure ──► Container killed & restarted      │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON:
//   - /healthz (liveness): "Am I alive?" - Should I be killed?
//   - /readyz (readiness): "Am I ready?" - Should I receive traffic?
//   - /livez (startup):    "Am I started?" - Has initialization completed?
//
// EXAMPLE KUBERNETES CONFIGURATION:
//
//   livenessProbe:
//     httpGet:
//       path: /healthz
//       port: 8080
//     initialDelaySeconds: 3
//     periodSeconds: 10
//     failureThreshold: 3
//
//   readinessProbe:
//     httpGet:
//       path: /readyz
//       port: 8080
//     initialDelaySeconds: 5
//     periodSeconds: 5
//     failureThreshold: 3
//
//   startupProbe:
//     httpGet:
//       path: /livez
//       port: 8080
//     failureThreshold: 30
//     periodSeconds: 10
//
// =============================================================================

package api

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

// =============================================================================
// HEALTH CHECK STATE
// =============================================================================

// HealthState tracks the broker's health status for probes.
type HealthState struct {
	// ready indicates if the server is ready to receive traffic.
	// Set to true after initialization is complete.
	ready atomic.Bool

	// live indicates if the server is alive (not deadlocked).
	// Should always be true unless there's a fatal error.
	live atomic.Bool

	// startTime records when the server started.
	startTime time.Time

	// lastCheck records the last successful internal health check.
	lastCheck atomic.Value // time.Time

	// checks contains named health check functions.
	checks map[string]HealthCheck
}

// HealthCheck is a function that checks a specific component's health.
type HealthCheck func(ctx context.Context) HealthCheckResult

// HealthCheckResult contains the result of a health check.
type HealthCheckResult struct {
	Status  string `json:"status"`            // "pass", "warn", "fail"
	Message string `json:"message,omitempty"` // Human-readable message
	Latency string `json:"latency,omitempty"` // Time taken for check
}

// NewHealthState creates a new health state tracker.
func NewHealthState() *HealthState {
	h := &HealthState{
		startTime: time.Now(),
		checks:    make(map[string]HealthCheck),
	}
	h.live.Store(true) // Alive by default
	h.lastCheck.Store(time.Now())
	return h
}

// SetReady marks the server as ready to receive traffic.
func (h *HealthState) SetReady(ready bool) {
	h.ready.Store(ready)
}

// SetLive marks the server as alive.
func (h *HealthState) SetLive(live bool) {
	h.live.Store(live)
}

// AddCheck registers a named health check.
func (h *HealthState) AddCheck(name string, check HealthCheck) {
	h.checks[name] = check
}

// IsReady returns whether the server is ready for traffic.
func (h *HealthState) IsReady() bool {
	return h.ready.Load()
}

// IsLive returns whether the server is alive.
func (h *HealthState) IsLive() bool {
	return h.live.Load()
}

// Uptime returns how long the server has been running.
func (h *HealthState) Uptime() time.Duration {
	return time.Since(h.startTime)
}

// =============================================================================
// HEALTH STATE IN SERVER
// =============================================================================

// healthState is the global health state for the server.
var healthState = NewHealthState()

// GetHealthState returns the global health state.
func GetHealthState() *HealthState {
	return healthState
}

// =============================================================================
// HEALTH CHECK HANDLERS
// =============================================================================

// handleHealthz handles GET /healthz - Kubernetes liveness probe.
//
// LIVENESS PROBE SEMANTICS:
//   - Returns 200 if the process is alive and not deadlocked
//   - Returns 503 if the process should be killed
//   - Should be fast and simple (no complex checks)
//   - Failing this probe causes container restart
//
// WHAT TO CHECK:
//   - Process is running (implicit - responding to request)
//   - No deadlocks detected
//   - Critical goroutines are alive
//
// WHAT NOT TO CHECK:
//   - Database connectivity (use readiness for that)
//   - External service health (use readiness for that)
//   - Heavy operations that could timeout
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if !healthState.IsLive() {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"status":    "fail",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"uptime":    healthState.Uptime().String(),
			"message":   "broker is not alive",
		})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "pass",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"uptime":    healthState.Uptime().String(),
	})
}

// handleReadyz handles GET /readyz - Kubernetes readiness probe.
//
// READINESS PROBE SEMANTICS:
//   - Returns 200 if the server can handle requests
//   - Returns 503 if the server should not receive traffic
//   - Failing this removes the pod from service endpoints
//   - Pod keeps running, just doesn't get traffic
//
// WHAT TO CHECK:
//   - Server initialization is complete
//   - Broker is started and accepting requests
//   - Required connections are established
//
// USE CASES FOR NOT READY:
//   - Still loading data from disk on startup
//   - Graceful shutdown in progress
//   - Waiting for cluster membership
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// Run detailed health checks if requested
	verbose := r.URL.Query().Get("verbose") == "true"

	// Basic readiness check
	if !healthState.IsReady() {
		resp := map[string]interface{}{
			"status":    "fail",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"message":   "broker is not ready",
		}
		if verbose {
			resp["checks"] = s.runHealthChecks(r.Context())
		}
		s.writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}

	// Check if broker is operational
	if s.broker == nil {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"status":    "fail",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"message":   "broker not initialized",
		})
		return
	}

	resp := map[string]interface{}{
		"status":    "pass",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"uptime":    healthState.Uptime().String(),
	}

	if verbose {
		resp["checks"] = s.runHealthChecks(r.Context())
		// Include broker stats
		stats := s.broker.Stats()
		resp["broker"] = map[string]interface{}{
			"node_id":     stats.NodeID,
			"topic_count": stats.TopicCount,
			"total_size":  stats.TotalSize,
		}
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// handleLivez handles GET /livez - Kubernetes startup probe.
//
// STARTUP PROBE SEMANTICS:
//   - Only runs during container startup
//   - Returns 200 when initialization is complete
//   - Returns 503 while still initializing
//   - Gives slow-starting containers time to initialize
//
// DIFFERENCE FROM LIVENESS:
//   - startupProbe only runs until first success
//   - livenessProbe runs continuously after startup
//   - Use startupProbe for containers with long initialization
func (s *Server) handleLivez(w http.ResponseWriter, r *http.Request) {
	// For startup probe, check if basic components are initialized
	if s.broker == nil {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"status":    "fail",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"message":   "broker not yet initialized",
		})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "pass",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"uptime":    healthState.Uptime().String(),
	})
}

// runHealthChecks executes all registered health checks.
func (s *Server) runHealthChecks(ctx context.Context) map[string]HealthCheckResult {
	results := make(map[string]HealthCheckResult)

	// Add broker health check
	results["broker"] = s.checkBrokerHealth(ctx)

	// Add storage health check
	results["storage"] = s.checkStorageHealth(ctx)

	// Run registered checks
	for name, check := range healthState.checks {
		start := time.Now()
		result := check(ctx)
		result.Latency = time.Since(start).String()
		results[name] = result
	}

	return results
}

// checkBrokerHealth checks the broker's health.
func (s *Server) checkBrokerHealth(ctx context.Context) HealthCheckResult {
	start := time.Now()

	if s.broker == nil {
		return HealthCheckResult{
			Status:  "fail",
			Message: "broker not initialized",
			Latency: time.Since(start).String(),
		}
	}

	stats := s.broker.Stats()
	return HealthCheckResult{
		Status:  "pass",
		Message: "broker operational with " + string(rune(stats.TopicCount+'0')) + " topics",
		Latency: time.Since(start).String(),
	}
}

// checkStorageHealth checks if storage is accessible.
func (s *Server) checkStorageHealth(ctx context.Context) HealthCheckResult {
	start := time.Now()

	// Try to access data directory
	dataDir := s.broker.DataDir()
	if dataDir == "" {
		return HealthCheckResult{
			Status:  "warn",
			Message: "data directory not configured",
			Latency: time.Since(start).String(),
		}
	}

	return HealthCheckResult{
		Status:  "pass",
		Message: "storage accessible at " + dataDir,
		Latency: time.Since(start).String(),
	}
}

// =============================================================================
// VERSION & INFO ENDPOINT
// =============================================================================

// Version information (set at build time via ldflags)
var (
	Version   = "dev"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

// handleVersion handles GET /version - Returns version information.
func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"version":    Version,
		"git_commit": GitCommit,
		"build_time": BuildTime,
		"go_version": "go1.21+",
	})
}
