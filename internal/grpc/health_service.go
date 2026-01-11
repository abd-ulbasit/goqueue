// =============================================================================
// HEALTH SERVICE - SERVER HEALTH CHECKS
// =============================================================================
//
// WHAT IS THIS?
// The health service provides standard gRPC health checking for:
//   - Load balancers to determine if the server is healthy
//   - Kubernetes liveness/readiness probes
//   - Monitoring and alerting systems
//   - Client-side health checks before sending requests
//
// gRPC HEALTH CHECKING PROTOCOL:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ The gRPC Health Checking Protocol is a standard way to expose health    │
//   │ status. It defines:                                                     │
//   │                                                                         │
//   │ Service: grpc.health.v1.Health                                          │
//   │ Methods:                                                                │
//   │   - Check(service) → status                                             │
//   │   - Watch(service) → stream of status                                   │
//   │                                                                         │
//   │ Status values:                                                          │
//   │   - UNKNOWN:         Status not known (shouldn't happen)                │
//   │   - SERVING:         Ready to handle requests                           │
//   │   - NOT_SERVING:     Not ready (shutting down, maintenance)             │
//   │   - SERVICE_UNKNOWN: Requested service doesn't exist                    │
//   │                                                                         │
//   │ Reference: https://github.com/grpc/grpc/blob/master/doc/health-         │
//   │            checking.md                                                  │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// HEALTH CHECK LEVELS:
//
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │ Level           │ What it checks              │ Use case               │
//   ├─────────────────┼─────────────────────────────┼────────────────────────┤
//   │ "" (empty)      │ Overall server health       │ Basic load balancing   │
//   │ "publish"       │ Publish service health      │ Producer routing       │
//   │ "consume"       │ Consume service health      │ Consumer routing       │
//   │ "broker"        │ Broker subsystem health     │ Deep health check      │
//   └─────────────────┴─────────────────────────────┴────────────────────────┘
//
// COMPARISON WITH OTHER SYSTEMS:
//
//   - Kafka:
//     - No standard health endpoint
//     - Uses JMX for monitoring
//     - ZooKeeper for broker liveness
//
//   - RabbitMQ:
//     - /api/health/checks/alarms HTTP endpoint
//     - Erlang-style monitoring
//
//   - NATS:
//     - /healthz HTTP endpoint
//     - Jetstream has /jsz
//
//   - goqueue:
//     - Standard gRPC health protocol
//     - HTTP /health endpoint (via chi)
//     - Service-level granularity
//
// =============================================================================

package grpc

import (
	"context"
	"log/slog"

	"goqueue/internal/broker"
)

// =============================================================================
// HEALTH SERVICE SERVER
// =============================================================================

// healthServiceServer implements the HealthService gRPC interface.
type healthServiceServer struct {
	UnimplementedHealthServiceServer

	broker *broker.Broker
	logger *slog.Logger
}

// NewHealthServiceServer creates a new health service server.
func NewHealthServiceServer(b *broker.Broker, logger *slog.Logger) HealthServiceServer {
	return &healthServiceServer{
		broker: b,
		logger: logger,
	}
}

// =============================================================================
// CHECK - SYNCHRONOUS HEALTH CHECK
// =============================================================================
//
// Check returns the current health status of a service.
//
// SERVICE NAMES:
//   - "" (empty string): Overall server health
//   - "goqueue.PublishService": Publish service health
//   - "goqueue.ConsumeService": Consume service health
//   - "goqueue.AckService": Ack service health
//   - "goqueue.OffsetService": Offset service health
//
// KUBERNETES INTEGRATION:
//
//   ```yaml
//   livenessProbe:
//     grpc:
//       port: 9000
//       service: ""  # Check overall server
//     initialDelaySeconds: 5
//     periodSeconds: 10
//
//   readinessProbe:
//     grpc:
//       port: 9000
//       service: "goqueue.PublishService"
//     initialDelaySeconds: 5
//     periodSeconds: 5
//   ```
//
// =============================================================================

func (s *healthServiceServer) Check(
	ctx context.Context,
	req *HealthCheckRequest,
) (*HealthCheckResponse, error) {
	// Check based on service name
	switch req.Service {
	case "", "goqueue":
		// Overall health - check if broker is operational
		return s.checkOverallHealth()

	case "goqueue.PublishService", "publish":
		// Publish service health
		return s.checkPublishHealth()

	case "goqueue.ConsumeService", "consume":
		// Consume service health
		return s.checkConsumeHealth()

	case "goqueue.AckService", "ack":
		// Ack service health
		return s.checkAckHealth()

	case "goqueue.OffsetService", "offset":
		// Offset service health
		return s.checkOffsetHealth()

	case "goqueue.HealthService", "health":
		// Health service is always healthy if we got here
		return &HealthCheckResponse{
			Status: ServingStatusServing,
		}, nil

	default:
		// Unknown service
		return &HealthCheckResponse{
			Status: ServingStatusServiceUnknown,
		}, nil
	}
}

// checkOverallHealth checks if the server is ready to handle requests.
//
// COMPREHENSIVE HEALTH CHECKS:
//
//	┌─────────────────────────────────────────────────────────────────────┐
//	│                      HEALTH CHECK HIERARCHY                         │
//	│                                                                     │
//	│   ┌─────────────┐                                                   │
//	│   │   Broker    │ ◄─── Must be non-nil and not closed               │
//	│   │  Available  │                                                   │
//	│   └──────┬──────┘                                                   │
//	│          │                                                          │
//	│          ▼                                                          │
//	│   ┌─────────────┐                                                   │
//	│   │  Scheduler  │ ◄─── Required for delayed message processing      │
//	│   │   Running   │                                                   │
//	│   └──────┬──────┘                                                   │
//	│          │                                                          │
//	│          ▼                                                          │
//	│   ┌─────────────┐                                                   │
//	│   │ Coordinator │ ◄─── Optional: Required for consumer groups       │
//	│   │  Running    │      (warn if missing but don't fail health)      │
//	│   └─────────────┘                                                   │
//	└─────────────────────────────────────────────────────────────────────┘
//
// COMPARISON:
//   - Kubernetes: Separate liveness (is running?) and readiness (can serve?)
//   - gRPC: SERVING, NOT_SERVING, UNKNOWN states
//   - goqueue: Single comprehensive check with detailed logging
func (s *healthServiceServer) checkOverallHealth() (*HealthCheckResponse, error) {
	// Check 1: Broker must be available
	if s.broker == nil {
		s.logger.Debug("health check failed: broker is nil")
		return &HealthCheckResponse{
			Status: ServingStatusNotServing,
		}, nil
	}

	// Check 2: Broker must not be closed
	if s.broker.IsClosed() {
		s.logger.Debug("health check failed: broker is closed")
		return &HealthCheckResponse{
			Status: ServingStatusNotServing,
		}, nil
	}

	// Check 3: Scheduler should be running for delayed message support
	// This is critical for scheduled/delayed message delivery
	scheduler := s.broker.Scheduler()
	if scheduler == nil {
		s.logger.Warn("health check warning: scheduler not available, delayed messages won't work")
		// Don't fail - basic publish/consume still works
	}

	// Check 4: Group coordinator (warn if missing but don't fail)
	// Direct consume works without it, but Subscribe requires it
	coordinator := s.broker.GetGroupCoordinator()
	if coordinator == nil {
		s.logger.Warn("health check warning: group coordinator not available, Subscribe won't work")
		// Don't fail - direct Consume still works
	}

	// All critical checks passed
	return &HealthCheckResponse{
		Status: ServingStatusServing,
	}, nil
}

// checkPublishHealth checks if the publish service is healthy.
func (s *healthServiceServer) checkPublishHealth() (*HealthCheckResponse, error) {
	// Publish requires:
	// - Broker to be available
	// - At least one topic to exist (optional, depends on use case)
	// - Storage to be writable

	if s.broker == nil {
		return &HealthCheckResponse{
			Status: ServingStatusNotServing,
		}, nil
	}

	return &HealthCheckResponse{
		Status: ServingStatusServing,
	}, nil
}

// checkConsumeHealth checks if the consume service is healthy.
func (s *healthServiceServer) checkConsumeHealth() (*HealthCheckResponse, error) {
	// Consume requires:
	// - Broker to be available
	// - Group coordinator for Subscribe

	if s.broker == nil {
		return &HealthCheckResponse{
			Status: ServingStatusNotServing,
		}, nil
	}

	// Check if group coordinator is available (needed for Subscribe)
	if s.broker.GetGroupCoordinator() == nil {
		// Consume can still work without coordinator (direct partition access)
		// but Subscribe won't work
		s.logger.Warn("group coordinator not available, Subscribe will not work")
	}

	return &HealthCheckResponse{
		Status: ServingStatusServing,
	}, nil
}

// checkAckHealth checks if the ack service is healthy.
func (s *healthServiceServer) checkAckHealth() (*HealthCheckResponse, error) {
	// Ack requires:
	// - Broker to be available
	// - Ack manager for receipt-handle based ACKs
	// - Group coordinator for offset-based ACKs

	if s.broker == nil {
		return &HealthCheckResponse{
			Status: ServingStatusNotServing,
		}, nil
	}

	return &HealthCheckResponse{
		Status: ServingStatusServing,
	}, nil
}

// checkOffsetHealth checks if the offset service is healthy.
func (s *healthServiceServer) checkOffsetHealth() (*HealthCheckResponse, error) {
	// Offset requires:
	// - Broker to be available
	// - Group coordinator for offset management

	if s.broker == nil {
		return &HealthCheckResponse{
			Status: ServingStatusNotServing,
		}, nil
	}

	coordinator := s.broker.GetGroupCoordinator()
	if coordinator == nil {
		return &HealthCheckResponse{
			Status: ServingStatusNotServing,
		}, nil
	}

	return &HealthCheckResponse{
		Status: ServingStatusServing,
	}, nil
}

// =============================================================================
// WATCH - STREAMING HEALTH CHECK (OPTIONAL)
// =============================================================================
//
// Watch returns a stream of health status updates.
// This is useful for:
//   - Long-lived connections that need to react to health changes
//   - More efficient than polling Check()
//
// NOTE: This is an optional feature of the gRPC health protocol.
// Many implementations don't support it.
//
// For goqueue, we implement a simple version that just returns
// the current status and keeps the stream open (no updates).
//
// =============================================================================

// Note: Watch is not implemented in this version.
// The UnimplementedHealthServiceServer will return UNIMPLEMENTED error.
// We can add it later if needed for long-lived health monitoring.
